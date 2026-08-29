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

Run compaction as a separate, one-shot container. It reads immutable raw objects and appends new
events to a Hive-partitioned Parquet dataset:

```text
raw bucket:       raw/<sha256>.ndjson.zst
canonical bucket: v1/year=YYYY/*.parquet
```

Use two private buckets and two workload-specific R2 credentials: Object Read Only for the raw
bucket and Object Read & Write for the canonical bucket. Neither bucket should have `r2.dev`, a
public custom domain, or browser CORS enabled.

Have the scheduler or host secret manager export the four credential variables before invoking
Docker. Passing only their names below keeps the values out of the command line:

```sh
docker run --rm \
  -e RAW_S3_BUCKET=w3-raw \
  -e RAW_S3_REGION=auto \
  -e RAW_S3_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com \
  -e RAW_S3_ACCESS_KEY_ID \
  -e RAW_S3_SECRET_ACCESS_KEY \
  -e CANONICAL_S3_BUCKET=w3-heartbeats \
  -e CANONICAL_S3_REGION=auto \
  -e CANONICAL_S3_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com \
  -e CANONICAL_S3_ACCESS_KEY_ID \
  -e CANONICAL_S3_SECRET_ACCESS_KEY \
  ghcr.io/ruslandoga/wakatime-3:latest /app/bin/compact
```

Native `*_FILE` support for Docker secret mounts is tracked separately in
[#29](https://github.com/ruslandoga/wakatime-3/issues/29); do not bake credentials into the image or
commit an environment file.

The command opens one in-memory DuckDB database, makes one connection, runs one SQL batch, and closes
both before the process exits—even when compaction fails. DuckDB normalizes and deduplicates the raw
events, excludes identities already present in canonical Parquet, and appends a UUID-named fragment
to each affected UTC-year partition. The imported `heartbeats.parquet` files remain ordinary members
of the same dataset. An unchanged rerun writes no files.

Start by scheduling this command once per day. Running it after every heartbeat upload would repeat
the raw scan for tiny batches and couple ingestion latency to analytics work. Keep the scheduler
single-instance.

Clean up raw objects with an R2 lifecycle rule on the `raw/` prefix instead of giving the compactor
delete permission. A 30-day expiry is a reasonable starting point for a daily schedule: it provides
a recovery window while canonical deduplication makes repeated scans harmless. Monitor successful
compactor runs, because an outage longer than the expiry window would otherwise discard unprocessed
raw events.

```sh
npx wrangler r2 bucket lifecycle add w3-raw expire-compacted-raw raw/ --expire-days 30
```

Use the actual raw bucket name. This is a bucket administration command; the runtime compactor token
does not need lifecycle or delete permission.

Canonical files use UTC `TIMESTAMPTZ`, Parquet V2 data pages, Zstandard compression, and 122,880-row
groups. Routine analytics query only this dataset—not raw NDJSON:

```sql
SELECT year, count(*) AS heartbeats
FROM read_parquet(
  's3://w3-heartbeats/v1/year=*/*.parquet',
  hive_partitioning = true,
  union_by_name = true
)
GROUP BY year
ORDER BY year;
```

Configure a temporary, bucket-scoped DuckDB S3 secret before running that query. Do not persist the
secret in a DuckDB file; persistent DuckDB secrets are not encrypted. The event-level Parquet files
contain private paths, project and branch names, and machine metadata, so only privacy-reviewed
aggregates should be published to GitHub Pages.
