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
heartbeats into a different batch, so the later Parquet stage should still deduplicate individual
events.

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
offline = true
heartbeat_rate_limit_seconds = 300
```
