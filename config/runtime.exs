import Config

env_file = ".#{config_env()}.env"

if File.exists?(env_file) do
  File.read!(env_file)
  |> String.split("\n", trim: true)
  |> Enum.each(fn line ->
    [key, value] = line |> String.split("=", parts: 2) |> Enum.map(&String.trim/1)
    System.put_env(key, value)
  end)
end

http_scheme =
  case System.get_env("HTTP_SCHEME", "http") do
    "http" -> :http
    "https" -> :https
    other -> raise ArgumentError, "Invalid HTTP_SCHEME: #{other}. Must be 'http' or 'https'."
  end

config :w3,
  api_key: System.fetch_env!("API_KEY"),
  data_path: System.get_env("DATA_PATH", System.tmp_dir!()),
  http: [
    scheme: http_scheme,
    port: "HTTP_PORT" |> System.get_env("4000") |> String.to_integer()
  ],
  s3: [
    bucket: System.fetch_env!("AWS_S3_BUCKET"),
    region: System.get_env("AWS_REGION", "us-east-1"),
    endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
    access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
  ]
