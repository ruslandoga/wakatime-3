import Config

http_scheme =
  case System.get_env("HTTP_SCHEME", "http") do
    "http" -> :http
    "https" -> :https
    other -> raise ArgumentError, "Invalid HTTP_SCHEME: #{other}. Must be 'http' or 'https'."
  end

http_port_raw = System.get_env("HTTP_PORT", "4000")

http_port =
  case Integer.parse(http_port_raw) do
    {port, ""} when port > 0 and port < 65536 ->
      port

    _ ->
      raise ArgumentError,
            "Invalid HTTP_PORT: #{http_port_raw}. Must be a valid port number (1-65535)."
  end

config :w3,
  http_scheme: http_scheme,
  http_port: http_port,
  ingester_data_path: System.get_env("INGESTER_DATA_PATH", "/tmp/w3-ingester-data")
