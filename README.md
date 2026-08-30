# w3

`w3` is a small WakaTime-compatible HTTP endpoint that stores heartbeats in S3-compatible object
storage and compacts them into queryable Parquet.

WakaTime already buffers heartbeats in its local BoltDB, so `w3` does not maintain another queue.
For each bulk request, it:

1. Adds the request's machine name to every heartbeat.
2. Encodes the batch as NDJSON and compresses it with Zstandard.
3. Uploads it to a content-addressed `raw/<sha256>.ndjson.zst` key.
4. Returns WakaTime's expected `201` only after object storage accepts the upload.

An upload failure returns `503`, allowing WakaTime's offline queue and backoff to retry.

## Run

```sh
docker run --detach \
  --name w3 \
  --restart always \
  --pull always \
  --publish 6767:6767 \
  --volume w3_tmp:/data \
  -e HTTP_PORT=6767 \
  -e TMPDIR=/data \
  -e API_KEY=your-wakatime-shaped-api-key \
  -e AWS_S3_BUCKET=w3 \
  -e AWS_REGION=auto \
  -e AWS_ENDPOINT_URL_S3=https://ACCOUNT_ID.r2.cloudflarestorage.com \
  -e AWS_ACCESS_KEY_ID=your-r2-access-key-id \
  -e AWS_SECRET_ACCESS_KEY=your-r2-secret-access-key \
  ghcr.io/ruslandoga/wakatime-3:latest
```

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
processed/batch-<sha256-of-sorted-raw-keys>.parquet
```

The compactor runs at startup and then 30 minutes after each successful run. It snapshots `raw/`,
downloads those objects concurrently, converts valid events into one Parquet file, uploads it, and
only then deletes the snapshotted raw objects. New raw objects wait for the next run. A deterministic
batch key makes a retry of the same snapshot overwrite the same object.

Rows missing `time`, `entity`, `type`, or `machine_name` are discarded; an all-invalid snapshot
produces no Parquet object. Malformed typed values fail the compaction and leave its raw snapshot
available for inspection or retry. Failures use full-jitter exponential backoff with a
250-millisecond base and a 5-second cap.

Parquet files use UTC `TIMESTAMPTZ`, Parquet V2, Zstandard compression, and a target row-group size of
8,192 rows. Rows are ordered by `time`, then `entity`, so bounded time predicates can prune row groups
using Parquet min/max statistics. Temporary files live under `TMPDIR` (`/tmp` by default) and are
removed when the compaction task exits.

## Query

Query `processed/` directly and explicitly disable Hive inference. `union_by_name` keeps older and
newer schemas queryable together. DuckDB rejects the glob until the first processed file exists.

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
and a partial source-deletion failure can leave overlap between compaction runs. Fallback values such
as `<<LAST_PROJECT>>`, `<<LAST_BRANCH>>`, and `<<LAST_LANGUAGE>>` remain literal; resolve them with
event-time history in the query layer when needed.

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
