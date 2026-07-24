import Config

parse_integer = fn name, default ->
  name
  |> System.get_env(default)
  |> String.to_integer()
end

http_scheme =
  case System.get_env("HTTP_SCHEME", "http") do
    "http" -> :http
    "https" -> :https
    other -> raise ArgumentError, "Invalid HTTP_SCHEME: #{other}. Must be 'http' or 'https'."
  end

api_key = System.get_env("API_KEY")

config :w3,
  api_key: api_key,
  http: [
    scheme: http_scheme,
    port: parse_integer.("HTTP_PORT", "4000")
  ],
  ingester: [
    interval: parse_integer.("W3_BATCH_INTERVAL_MS", "30000"),
    max_buffer_size: parse_integer.("W3_MAX_BUFFER_SIZE", "10000000")
  ],
  s3: %{
    bucket: System.get_env("S3_BUCKET"),
    region: System.get_env("AWS_REGION"),
    endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3")
  }
