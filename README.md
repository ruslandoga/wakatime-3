# w3

`w3` is a small WakaTime-compatible HTTP endpoint that forwards raw heartbeats directly to object storage.

> [!NOTE]
>
> WakaTime already batches heartbeats in its local BoltDB, so `w3` does not maintain another buffer.

For each incoming bulk request it:

1. Adds the request's machine name and timezone to every heartbeat.
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
docker run --rm -p 4000:4000 \
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
api_url = http://127.0.0.1:4000
api_key = your-wakatime-shaped-api-key
heartbeat_rate_limit_seconds = 300
```

## Compact

Raw requests and queryable Parquet live in the same private bucket:

```text
raw/<sha256>.ndjson.zst
v1/year=YYYY/heartbeats.parquet
```

`W3.Compactor.run!()` lists `raw/` once and treats those exact keys as the run's generation. It uses
Req to download that snapshot and the annual Parquet files, opens a temporary local DuckDB database,
runs one merge, closes DuckDB, and uploads the same deterministic annual keys. Only after every
upload succeeds does it delete the snapshotted raw keys. New uploads are not in the snapshot and wait
for the next run; a failed or interrupted run safely retries because the merge deduplicates events.
No manifest, persistent state, dated generation, or lifecycle rule is needed.

For now the compactor uses the application's existing read/write S3 credentials and bucket. Keep the
bucket private: do not enable `r2.dev`, a public custom domain, or browser CORS. Invoke it in the
running release, for example from the host scheduler:

```sh
docker exec CONTAINER /app/bin/w3 rpc 'W3.Compactor.run!()'
```

Start with one local run per day rather than one run per heartbeat upload. The database and connection
are created and closed on every non-empty run, including failures.

Canonical files use UTC `TIMESTAMPTZ`, Parquet V2 data pages, Zstandard compression, and 122,880-row
groups. Routine analytics query only this dataset—not raw NDJSON:

```sql
SELECT year, count(*) AS heartbeats
FROM read_parquet(
  's3://wakatimeless/v1/year=*/heartbeats.parquet',
  hive_partitioning = true,
  union_by_name = true
)
GROUP BY year
ORDER BY year;
```

The event-level Parquet files contain private paths, project and branch names, and machine metadata,
so only privacy-reviewed aggregates should be published to GitHub Pages.
