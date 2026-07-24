import Config

parse_integer = fn name, default ->
  name
  |> System.get_env(default)
  |> String.to_integer()
end

test? = config_env() == :test

env = fn name, default ->
  if test?, do: System.get_env(name, default), else: System.fetch_env!(name)
end

http_scheme =
  case System.get_env("HTTP_SCHEME", "http") do
    "http" -> :http
    "https" -> :https
    other -> raise ArgumentError, "Invalid HTTP_SCHEME: #{other}. Must be 'http' or 'https'."
  end

default_api_key = if config_env() == :test, do: "406fe41f-6d69-4183-a4cc-121e0c524c2b"
api_key = System.get_env("API_KEY", default_api_key)
http_port = if test?, do: "0", else: "4000"

config :w3,
  api_key: api_key,
  http: [
    scheme: http_scheme,
    port: parse_integer.("HTTP_PORT", http_port)
  ],
  ingester: [
    interval: parse_integer.("W3_BATCH_INTERVAL_MS", "30000"),
    max_buffer_size: parse_integer.("W3_MAX_BUFFER_SIZE", "10000000")
  ],
  s3: [
    bucket: env.("AWS_S3_BUCKET", "w3-test"),
    region: System.get_env("AWS_REGION", "us-east-1"),
    endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
    access_key_id: env.("AWS_ACCESS_KEY_ID", "minioadmin"),
    secret_access_key: env.("AWS_SECRET_ACCESS_KEY", "minioadmin")
  ]
