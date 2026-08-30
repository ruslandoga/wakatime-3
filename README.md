# w3

`w3` is a small WakaTime-compatible HTTP endpoint that forwards raw heartbeats directly to object storage.

> [!NOTE]
>
> WakaTime already batches heartbeats in its local BoltDB, so `w3` does not maintain another buffer.

For each incoming bulk request it:

1. Adds the request's machine name to every heartbeat.
2. Encodes the batch as NDJSON and compresses it with zstd.
3. Uploads it once to `raw/<sha256>.ndjson.zst`.
4. Returns WakaTime's expected `201` response only after the object store returns 2xx.

An upload failure returns `503`, allowing WakaTime's offline queue and backoff to retry later. The
content-addressed key makes an identical request retry idempotent. WakaTime can regroup retried
heartbeats into a different batch, so a separate compactor deduplicates individual events into
queryable Parquet.

## Previous versions

- [`wakatime-2`](https://github.com/ruslandoga/wakatime-2) — a single-container WakaTime
  clone built with SQLite, Phoenix LiveView, and Litestream.
- [`wakatime-1`](https://github.com/ruslandoga/wakatime-1) — a local Docker Compose setup
  using PostgreSQL, Grafana, and a heartbeat ingester.

## Run

```sh
docker run --detach \
  --name w3 \
  --restart always \
  --pull always \
  --publish 6767:6767 \
  -e HTTP_PORT=6767 \
  -e API_KEY=your-wakatime-shaped-api-key \
  -e AWS_S3_BUCKET=w3 \
  -e AWS_REGION=auto \
  -e AWS_ENDPOINT_URL_S3=https://ACCOUNT_ID.r2.cloudflarestorage.com \
  -e AWS_ACCESS_KEY_ID=your-r2-access-key-id \
  -e AWS_SECRET_ACCESS_KEY=your-r2-secret-access-key \
  ghcr.io/ruslandoga/wakatime-3:latest
```

Point WakaTime at the service in `~/.wakatime.cfg`:

```ini
[settings]
api_url = http://127.0.0.1:6767
api_key = your-wakatime-shaped-api-key
heartbeat_rate_limit_seconds = 300
```

## Compact

Raw requests and queryable Parquet live in the same private bucket:

```text
raw/<sha256>.ndjson.zst
v1/fragments/raw-<sha256-of-raw-key>.parquet
v1/year=YYYY/heartbeats.parquet                 # compacted/legacy
```

Each compaction lists `raw/` once and treats those exact keys as the run's generation. It uses
Req to download that snapshot concurrently, opens a temporary local DuckDB database, and converts
each raw object into one Parquet fragment. It never downloads existing Parquet, so routine work is
proportional to new raw data instead of total history. Non-empty fragments are uploaded concurrently.
Each source raw object is deleted only after its own fragment upload succeeds; successfully converted
objects can therefore progress even when another object is malformed. New raw uploads wait for the
next run.

Each fragment key is derived from its source raw key. Since raw objects are content-addressed, a retry
writes the same fragment instead of creating another copy. Heartbeats missing
`time`, `entity`, `type`, or `machine_name` are discarded; an object containing only such rows produces
no fragment but is still deleted after successful processing. A malformed typed value fails only its
source object and leaves it in `raw/` for inspection or retry. Deduplication within one raw object
happens during conversion. Deduplication across differently batched retries is intentionally deferred
to analytics or a less-frequent Parquet-to-Parquet compaction job.

For now the compactor uses the application's existing read/write S3 credentials and bucket. Keep the
bucket private: do not enable `r2.dev`, a public custom domain, or browser CORS. `W3.Compactor` uses
a supervised `W3.Periodic` state machine. Its first run starts after 30 minutes, and the next starts
30 minutes after each successful attempt finishes. Failed attempts retry with full-jitter exponential
backoff using a one-second base and one-minute cap.
Compactions emit telemetry under `[:w3, :compact]`, heartbeat uploads under `[:w3, :upload]`, and
plugin logs under `[:w3, :log]`. All application logging is performed by a central telemetry
handler; the endpoint, ingester, and compactor only emit events. Span logs include elapsed time;
upload logs also include heartbeat and compressed-byte counts, while exceptions include formatted
error details.

Set `DATA_PATH` on the running container to choose the parent directory for temporary compaction
files. It defaults to the system temporary directory; only its `w3-compactor` child is cleared.

Once conversion starts, its database and connection are scoped to that run and closed on success or
failure; the state machine retains no DuckDB handle between runs.

Fragments use UTC `TIMESTAMPTZ`, Parquet V2 data pages, Zstandard compression, and a physical UTC
`year` column. Existing annual files remain readable alongside fragments. Until a
separate Parquet compactor folds fragments into the annual files, analytics should query both layouts
and deduplicate events—not raw NDJSON:

```sql
WITH heartbeat AS (
  SELECT *
  FROM read_parquet(
    's3://w3/v1/year=*/heartbeats.parquet',
    hive_partitioning = true,
    union_by_name = true
  )
  UNION ALL BY NAME
  SELECT *
  FROM read_parquet(
    's3://w3/v1/fragments/*.parquet',
    hive_partitioning = false,
    union_by_name = true
  )
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
    machine_name,
    year
  FROM heartbeat
)
SELECT year, count(*) AS heartbeats
FROM deduplicated
GROUP BY year
ORDER BY year;
```

Include only the `read_parquet` arms whose prefixes currently contain files; DuckDB rejects an
unmatched Parquet glob. The two layouts use different Hive partition shapes, so they must be read
separately before `UNION ALL BY NAME`. The example deliberately deduplicates on the legacy-stable
event identity, so a heartbeat present in both an older annual file and a richer fragment counts once.
Queries over AI or project-root fields should group on that same identity and prefer the fragment's
non-null values.

Fallback values such as `<<LAST_PROJECT>>`, `<<LAST_BRANCH>>`, and `<<LAST_LANGUAGE>>` stay literal in
raw data and Parquet fragments. They need prior-history context, so they should be resolved using
event-time semantics in the query layer, not by the stateless raw compactor. Range queries must seed
that state from earlier history and carry it across year boundaries; equal timestamps also need a
stable event-field tie-breaker. Carry the last concrete project globally, then the last concrete branch
and language within the effective project.

The event-level Parquet files contain private paths, project and branch names, and machine metadata,
so only privacy-reviewed aggregates should be published to GitHub Pages.
