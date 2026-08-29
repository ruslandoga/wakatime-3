# w3

`w3` is a small WakaTime-compatible HTTP endpoint that forwards raw heartbeats directly to S3 or
Cloudflare R2.

WakaTime already batches heartbeats in its local BoltDB, so `w3` does not maintain another buffer.
For each incoming bulk request it:

1. Adds the request's machine name and timezone to every heartbeat.
2. Encodes the batch as NDJSON and compresses it with zstd.
3. Uploads it once to `raw/<sha256>.ndjson.zst`.
4. Returns WakaTime's expected `201` response only after the object store returns 2xx.

An upload failure returns `503`, allowing WakaTime's offline queue and backoff to retry later. The
content-addressed key makes an identical request retry idempotent. WakaTime can regroup retried
heartbeats into a different batch, so the later Parquet stage should still deduplicate individual
events.

## Run

Set the HTTP and object-store configuration, then run `mix run --no-halt`:

```sh
export API_KEY=your-wakatime-shaped-api-key
export AWS_S3_BUCKET=w3
export AWS_REGION=auto
export AWS_ENDPOINT_URL_S3=https://ACCOUNT_ID.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID=your-r2-access-key-id
export AWS_SECRET_ACCESS_KEY=your-r2-secret-access-key

mix run --no-halt
```

`HTTP_PORT` defaults to `4000`.

Point WakaTime at the service in `~/.wakatime.cfg`:

```ini
[settings]
api_url = http://127.0.0.1:4000
api_key = your-wakatime-shaped-api-key
offline = true
heartbeat_rate_limit_seconds = 300
```

The five-minute rate limit is optional; WakaTime defaults to two minutes. It stores intervening
heartbeats locally and sends up to ten in each bulk request. Do not deliberately return errors to
create batches: API failures activate WakaTime's exponential backoff. A crash-safe server-side
outbox is intentionally deferred to [#21](https://github.com/ruslandoga/wakatime-3/issues/21).
