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

  def start_ingester(options) do
    {bucket, options} = Keyword.pop!(options, :bucket)

    options =
      options
      |> Keyword.put_new_lazy(:s3, fn ->
        s3_credentials = s3_credentials(:minio)
        s3_req = s3_req(s3_credentials)
        ExUnit.Callbacks.on_exit(fn -> Req.delete!(s3_req, url: "s3://#{bucket}") end)
        %{status: 200} = Req.put!(s3_req, url: "s3://#{bucket}")
        Map.put(s3_credentials, :bucket, bucket)
      end)
      |> Keyword.put_new(:interval, to_timeout(second: 1))
      |> Keyword.put_new(:max_buffer_size, 1)

    ExUnit.Callbacks.start_supervised!({W3.Ingester, options})
  end

  def start_duck(s3_credentials) do
    database = ExUnit.Callbacks.start_supervised!({Adbc.Database, driver: :duckdb})
    duck = ExUnit.Callbacks.start_supervised!({Adbc.Connection, database: database})

    %{"Success" => [true]} =
      quack(duck, """
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

    duck
  end

  def quack(duck, sql, params \\ []) do
    duck
    |> Adbc.Connection.query!(sql, params)
    |> Adbc.Result.to_map()
  end

  def attach_telemetry(events) do
    ref = :telemetry_test.attach_event_handlers(self(), events)
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end
end
