import Config

parse_integer = fn name, default ->
  System.get_env(name, to_string(default))
  |> String.to_integer()
end

fetch = fn key, env ->
  System.get_env(env) || Application.get_env(:w3, key) || System.fetch_env!(env)
end

fetch_in = fn path, env ->
  System.get_env(env) || get_in(Application.get_all_env(:w3), path) || System.fetch_env!(env)
end

http_scheme =
  case System.get_env("HTTP_SCHEME", "http") do
    "http" -> :http
    "https" -> :https
    other -> raise ArgumentError, "Invalid HTTP_SCHEME: #{other}. Must be 'http' or 'https'."
  end

config :w3,
  api_key: fetch.(:api_key, "API_KEY"),
  http: [
    scheme: http_scheme,
    port:
      parse_integer.("HTTP_PORT", get_in(Application.get_all_env(:w3), [:http, :port]) || "4000")
  ],
  ingester: [
    interval: parse_integer.("W3_BATCH_INTERVAL_MS", "30000"),
    max_buffer_size: parse_integer.("W3_MAX_BUFFER_SIZE", "10000000"),
    spool_dir: System.get_env("W3_SPOOL_DIR", "data/spool")
  ],
  s3: [
    bucket: fetch_in.([:s3, :bucket], "AWS_S3_BUCKET"),
    region: System.get_env("AWS_REGION", "us-east-1"),
    endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
    access_key_id: fetch_in.([:s3, :access_key_id], "AWS_ACCESS_KEY_ID"),
    secret_access_key: fetch_in.([:s3, :secret_access_key], "AWS_SECRET_ACCESS_KEY")
  ]
