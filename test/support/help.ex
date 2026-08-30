defmodule Help do
  @moduledoc false

  def s3_credentials(:minio) do
    %{
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
      region: System.get_env("AWS_REGION", "us-east-1"),
      endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    }
  end

  def s3_req(credentials) do
    req = Req.new(retry: :transient)

    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      endpoint_url: endpoint_url
    } = credentials

    ReqS3.attach(req,
      aws_sigv4: [
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        region: region
      ],
      aws_endpoint_url_s3: endpoint_url
    )
  end

  def start_endpoint(options) do
    options =
      options
      |> Keyword.put_new(:port, 0)
      |> Keyword.put_new(:startup_log, false)

    endpoint_pid = ExUnit.Callbacks.start_supervised!({W3.Endpoint, options})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint_pid)
    "http://localhost:#{port}/"
  end

  def create_s3(bucket) do
    credentials = s3_credentials(:minio)
    :ok = create_bucket(credentials, bucket)
    Map.put(credentials, :bucket, bucket)
  end

  def create_bucket(credentials, bucket) do
    s3_req = s3_req(credentials)
    ExUnit.Callbacks.on_exit(fn -> Req.delete!(s3_req, url: "s3://#{bucket}") end)
    %{status: 200} = Req.put!(s3_req, url: "s3://#{bucket}")
    :ok
  end

  def start_duck(s3_credentials) do
    duck = DuckNIF.open()
    conn = DuckNIF.connect(duck)

    ExUnit.Callbacks.on_exit(fn ->
      DuckNIF.disconnect(conn)
      DuckNIF.close(duck)
    end)

    %{"Success" => [true]} =
      W3.Duck.query(conn, """
      CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        ENDPOINT '#{String.replace(s3_credentials.endpoint_url, "http://", "")}',
        KEY_ID '#{s3_credentials.access_key_id}',
        SECRET '#{s3_credentials.secret_access_key}',
        REGION '#{s3_credentials.region}',
        URL_STYLE path,
        USE_SSL false
      );
      """)

    conn
  end

  def attach_telemetry(events) do
    ref = :telemetry_test.attach_event_handlers(self(), events)
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end

  def heartbeat(options \\ []) do
    Map.merge(
      %{
        "branch" => "main",
        "category" => "coding",
        "cursorpos" => 1,
        "dependencies" => nil,
        "entity" => "/Users/q/Developer/copycat/w1/test/endpoint_test.exs",
        "is_write" => nil,
        "language" => "Elixir",
        "lineno" => 1,
        "lines" => 4,
        "project" => "w1",
        "time" => 1_653_576_917.486633,
        "type" => "file",
        "user_agent" =>
          "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 vscode/1.68.0-insider vscode-wakatime/18.1.5"
      },
      Map.new(options, fn {key, value} -> {to_string(key), value} end)
    )
  end
end
