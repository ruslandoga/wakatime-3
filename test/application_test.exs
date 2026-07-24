defmodule W3.ApplicationTest do
  use ExUnit.Case, async: true

  test "does not start runtime children without an api key" do
    assert W3.Application.children(api_key: nil) == []
  end

  test "builds runtime children with default process wiring" do
    s3 = %{
      bucket: "w3-test",
      region: "us-east-1",
      endpoint_url: "http://localhost:9000"
    }

    assert [
             {W3.Ingester, ingester_options},
             {W3.Endpoint, endpoint_options}
           ] =
             W3.Application.children(
               api_key: "secret",
               http: [scheme: :http, port: 0],
               s3: s3
             )

    assert ingester_options[:name] == W3.Ingester
    assert ingester_options[:s3] == s3
    assert endpoint_options[:api_key] == "secret"
    assert endpoint_options[:ingester] == W3.Ingester
    assert endpoint_options[:scheme] == :http
    assert endpoint_options[:port] == 0
  end

  @tag :minio
  test "starts supervised endpoint and ingester from runtime options" do
    bucket =
      "w3-application-test-#{System.system_time(:second)}-#{System.unique_integer([:positive])}"

    api_key = Base.encode64(:crypto.strong_rand_bytes(32))
    s3 = Help.s3_credentials(:minio)
    :ok = Help.create_bucket(s3, bucket)

    children =
      W3.Application.children(
        api_key: api_key,
        http: [scheme: :http, port: 0, startup_log: false],
        ingester: [interval: to_timeout(second: 1), max_buffer_size: 1],
        s3: Map.put(s3, :bucket, bucket)
      )

    supervisor =
      start_supervised!(%{
        id: :w3_application_test_supervisor,
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
      })

    {W3.Endpoint, endpoint_pid, :supervisor, _modules} =
      supervisor
      |> Supervisor.which_children()
      |> Enum.find(fn {id, _pid, _type, _modules} -> id == W3.Endpoint end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint_pid)

    req =
      Req.new(
        base_url: "http://localhost:#{port}/",
        auth: {:basic, api_key},
        url: "/heartbeats",
        headers: %{
          "x-machine-name" => "mac3.local",
          "content-type" => "application/json",
          "accept" => "application/json"
        }
      )

    telemetry_ref = Help.attach_telemetry([[:w3, :ingester, :upload, :stop]])

    body = JSON.encode_to_iodata!(%{"_json" => [Help.heartbeat()]})

    assert %{status: 201, body: %{"responses" => [[nil, 201]]}} = Req.post!(req, body: body)
    assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _, %{bucket: ^bucket}}
  end
end
