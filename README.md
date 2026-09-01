# w3

`w3` is a small WakaTime-compatible HTTP endpoint that stores heartbeats in S3-compatible object
storage and compacts them into queryable Parquet.

WakaTime already buffers heartbeats in its local BoltDB, so `w3` does not maintain another queue.
For each bulk request, it:

1. Adds the request's machine name to every heartbeat.
2. Encodes the batch as NDJSON and compresses it with Zstandard.
3. Uploads it to a content-addressed `raw/<sha256>.ndjson.zst` key.
4. Returns WakaTime's expected `201` only after object storage accepts the upload.

An upload failure propagates as a server error, allowing WakaTime's offline queue and backoff to
retry.

## Run

```sh
docker run --detach \
  --name w3 \
  --restart always \
  --pull always \
  --publish 6767:6767 \
  --volume w3_tmp:/data \
  -e HTTP_PORT=6767 \
  -e API_KEY=your-wakatime-shaped-api-key \
  -e AWS_S3_BUCKET=w3 \
  -e AWS_REGION=auto \
  -e AWS_ENDPOINT_URL_S3=https://ACCOUNT_ID.r2.cloudflarestorage.com \
  -e AWS_ACCESS_KEY_ID=your-r2-access-key-id \
  -e AWS_SECRET_ACCESS_KEY=your-r2-secret-access-key \
  ghcr.io/ruslandoga/wakatime-3:latest
```

The image defaults `TMPDIR` to `/data`. The shown volume keeps compaction I/O out of Docker's
writable container layer; without it, compaction still works, but its temporary files use that
ephemeral layer.

The credentials need permission to list the bucket and to read, write, and delete its objects. Keep
the bucket private: raw and processed data contain file paths, project and branch names, and machine
metadata.

Point WakaTime at the service in `~/.wakatime.cfg`:

```ini
[settings]
api_url = http://127.0.0.1:6767
api_key = your-wakatime-shaped-api-key
heartbeat_rate_limit_seconds = 300
```

## Storage and compaction

Raw and processed objects share the configured bucket. Processed data is flat, not Hive-partitioned:

```text
raw/<sha256-of-enriched-ndjson>.ndjson.zst
processed/<sha256-of-sorted-input-keys>.parquet
```

The raw compactor runs at startup and then 30 minutes after each successful run. It snapshots
`raw/`, downloads those objects concurrently, converts valid events into one Parquet file, uploads
it, and only then deletes the snapshotted raw objects. New raw objects wait for the next run. A
deterministic key makes a retry of the same snapshot overwrite the same object.

A second compactor also runs at startup and then one day after each successful run. When at least
two Parquet files exist, it snapshots `processed/`, downloads those files concurrently, unions their
schemas, removes exact duplicate source rows, resolves fallback metadata, and writes one replacement
ordered by event time. It uploads the replacement before deleting exactly the snapshotted files, so
Parquet files created during the run remain for the next pass. Zero- and one-file snapshots are left
unchanged.

Rows missing `time`, `entity`, `type`, or `machine_name` are discarded; an all-invalid snapshot
produces no Parquet object. Malformed typed values fail raw compaction and leave its snapshot
available for inspection or retry. A failed Parquet merge likewise leaves its input snapshot intact.
Both jobs use full-jitter exponential backoff with a 250-millisecond base and a 5-second cap.

Parquet files use UTC `TIMESTAMPTZ`, Parquet V2, Zstandard compression, and a target row-group size of
8,192 rows. Rows are ordered by `time`, then `entity`, so bounded time predicates can prune row
groups using Parquet min/max statistics. Temporary files live under `TMPDIR` (`/data` in the
container image) and are removed when each compaction task exits.

## Query

Query `processed/` directly and explicitly disable Hive inference. `union_by_name` keeps older and
newer schemas queryable together. DuckDB rejects the glob until the first processed file exists.
Replacing several S3 objects is not atomic for concurrent readers; retry a query if it overlaps the
daily replacement and reports that a snapshotted object disappeared.

```sql
WITH heartbeat AS (
  SELECT *
  FROM read_parquet(
    's3://w3/processed/*.parquet',
    hive_partitioning = false,
    union_by_name = true
  )
  WHERE time >= TIMESTAMPTZ '2026-01-01 00:00:00+00'
    AND time <  TIMESTAMPTZ '2027-01-01 00:00:00+00'
), deduplicated AS (
  SELECT DISTINCT
    time,
    entity,
    type,
    category,
    project,
    branch,
    language,
    dependencies,
    lines,
    lineno,
    cursorpos,
    is_write,
    machine_name
  FROM heartbeat
)
SELECT year(timezone('UTC', time)) AS year, count(*) AS heartbeats
FROM deduplicated
GROUP BY year
ORDER BY year;
```

Deduplicate across files because WakaTime may regroup retried heartbeats into different raw batches,
new batches can coexist with the latest daily compacted file, and a partial source-deletion failure
can leave overlap between runs. Daily Parquet compaction removes exact duplicates within its own
snapshot before calculating derived columns.

The source `project`, `branch`, and `language` columns keep fallback literals such as
`<<LAST_PROJECT>>`, `<<LAST_BRANCH>>`, and `<<LAST_LANGUAGE>>`. Daily compaction materializes their
event-time values as `resolved_project`, `resolved_branch`, and `resolved_language`, preserving the
source columns so a later out-of-order heartbeat can repair earlier results. Project fallback and
the `previous_heartbeat_at` and `next_heartbeat_at` adjacency columns use one global heartbeat stream
across machines. Branch and language fallbacks use the history of the resolved project, preventing
one project's branch or language from leaking into another. Equal-time heartbeats do not provide
fallback state to each other. Use `next_heartbeat_at - time` for duration attribution and
`time - previous_heartbeat_at` to identify session starts after an idle timeout.

Run derived analytics after a successful daily Parquet compaction; newer raw-compaction fragments
receive resolved metadata and global adjacency during the next daily pass. For example, total active
seconds by project with a configurable five-minute idle cutoff:

```sql
WITH heartbeat AS (
  SELECT *
  FROM read_parquet(
    's3://w3/processed/*.parquet',
    hive_partitioning = false,
    union_by_name = true
  )
  WHERE time >= TIMESTAMPTZ '2026-01-01 00:00:00+00'
    AND time <  TIMESTAMPTZ '2027-01-01 00:00:00+00'
)
SELECT
  coalesce(resolved_project, '(none)') AS project,
  coalesce(
    sum(epoch(next_heartbeat_at - time)) FILTER (
      WHERE next_heartbeat_at - time < INTERVAL '5 minutes'
    ),
    0
  ) AS seconds
FROM heartbeat
GROUP BY resolved_project
ORDER BY seconds DESC;
```

Find session starts inside a bounded range without scanning for a preceding row outside that range:

```sql
WITH heartbeat AS (
  SELECT *
  FROM read_parquet(
    's3://w3/processed/*.parquet',
    hive_partitioning = false,
    union_by_name = true
  )
)
SELECT
  time AS session_started_at,
  resolved_project,
  resolved_branch,
  machine_name
FROM heartbeat
WHERE time >= TIMESTAMPTZ '2026-08-01 00:00:00+00'
  AND time <  TIMESTAMPTZ '2026-09-01 00:00:00+00'
  AND (
    previous_heartbeat_at IS NULL
    OR time - previous_heartbeat_at >= INTERVAL '5 minutes'
  )
ORDER BY time;
```

## Development

The project uses the Elixir and Erlang versions in `.tool-versions`.

```sh
mise install
mix deps.get
docker compose up --detach --wait minio
mix test
mix format --check-formatted
mix dialyzer
```

MinIO-backed tests are skipped when MinIO is unavailable.

## Previous versions

- [`wakatime-2`](https://github.com/ruslandoga/wakatime-2) — SQLite, Phoenix LiveView, and Litestream.
- [`wakatime-1`](https://github.com/ruslandoga/wakatime-1) — PostgreSQL, Grafana, and a heartbeat
  ingester.
