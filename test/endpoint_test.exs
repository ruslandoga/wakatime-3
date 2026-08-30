defmodule W3.EndpointTest do
  use ExUnit.Case, async: true

  @moduletag :minio

  setup do
    bucket =
      "w3-ingester-test-#{System.system_time(:second)}-#{System.unique_integer([:positive])}"

    s3 = Help.create_s3(bucket)
    api_key = "406fe41f-6d69-4183-a4cc-121e0c524c2b"
    url = Help.start_endpoint(api_key: api_key, s3: s3)
    req = Req.new(base_url: url, auth: {:basic, api_key})
    {:ok, req: req, bucket: bucket}
  end

  describe "auth" do
    test "missing", %{req: req} do
      assert %{status: 401, headers: %{"www-authenticate" => ["Basic"]}} =
               req |> Req.merge(auth: nil) |> Req.get!()
    end

    test "wrong", %{req: req} do
      assert %{status: 401, headers: %{"www-authenticate" => ["Basic"]}} =
               req |> Req.merge(auth: {:basic, "wrong_api_key"}) |> Req.get!()
    end
  end

  test "404", %{req: req} do
    assert %{status: 404, body: "not found"} = Req.get!(req)
  end

  test "logs", %{req: req} do
    telemetry_ref = Help.attach_telemetry([[:w3, :plugin, :log]])

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        assert %{status: 201} =
                 Req.post!(req,
                   url: "/plugins/errors",
                   headers: %{
                     "content-type" => "application/json",
                     "accept" => "application/json"
                   },
                   body:
                     JSON.encode_to_iodata!(%{
                       "logs" => """
                       {"level": "debug", "message": "some debug info"}
                       {"level": "warning", "message": "this is the last warning"}
                       {"level": "error", "message": "something's wrong"}\
                       """
                     })
                 )
      end)

    assert_receive {[:w3, :plugin, :log], ^telemetry_ref, %{},
                    %{level: :debug, log: %{"message" => "some debug info"}}}

    assert_receive {[:w3, :plugin, :log], ^telemetry_ref, %{},
                    %{level: :warning, log: %{"message" => "this is the last warning"}}}

    assert_receive {[:w3, :plugin, :log], ^telemetry_ref, %{},
                    %{level: :error, log: %{"message" => "something's wrong"}}}

    assert logs =~ "some debug info"
    assert logs =~ "this is the last warning"
    assert logs =~ "something's wrong"
  end

  describe "heartbeats" do
    setup %{req: req} do
      req =
        Req.merge(req,
          url: "/users/current/heartbeats.bulk",
          headers: %{
            "x-machine-name" => "mac3.local",
            "timezone" => "Europe/Moscow",
            "content-type" => "application/json",
            "accept" => "application/json"
          }
        )

      {:ok, req: req}
    end

    test "ingest", %{req: req, bucket: bucket} do
      body = JSON.encode_to_iodata!([Help.heartbeat(branch: "add-ingester")])

      telemetry_ref = Help.attach_telemetry([[:w3, :ingester, :upload, :stop]])

      assert %{status: 201, body: %{"responses" => [[nil, 201]]}} =
               Req.post!(req, body: body)

      assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _,
                      %{bucket: ^bucket, key: key}}

      assert key =~ ~r|\Araw/[0-9a-f]{64}\.ndjson\.zst\z|
      duck = Help.start_duck(Help.s3_credentials(:minio))

      assert Help.quack(
               duck,
               "select * from 's3://#{bucket}/raw/*.ndjson.zst'"
             ) == %{
               "branch" => ["add-ingester"],
               "category" => ["coding"],
               "cursorpos" => [1],
               "dependencies" => [nil],
               "entity" => ["/Users/q/Developer/copycat/w1/test/endpoint_test.exs"],
               "is_write" => [nil],
               "language" => ["Elixir"],
               "lineno" => [1],
               "lines" => [4],
               "machine_name" => ["mac3.local"],
               "project" => ["w1"],
               "time" => [1_653_576_917.486633],
               "timezone" => ["Europe/Moscow"],
               "type" => ["file"],
               "user_agent" => [
                 "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 vscode/1.68.0-insider vscode-wakatime/18.1.5"
               ]
             }
    end

    test "ingest does not overwrite multiple flushes in the same minute", %{
      req: req,
      bucket: bucket
    } do
      telemetry_ref = Help.attach_telemetry([[:w3, :ingester, :upload, :stop]])

      body1 =
        JSON.encode_to_iodata!([
          Help.heartbeat(entity: "first.ex", project: "w1", time: 1_653_576_917.486633)
        ])

      body2 =
        JSON.encode_to_iodata!([
          Help.heartbeat(entity: "second.ex", project: "w2", time: 1_653_576_918.486633)
        ])

      assert %{status: 201} = Req.post!(req, body: body1)
      assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _, %{bucket: ^bucket}}

      assert %{status: 201} = Req.post!(req, body: body2)
      assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _, %{bucket: ^bucket}}

      duck = Help.start_duck(Help.s3_credentials(:minio))

      assert Help.quack(
               duck,
               """
               select project, entity, time
               from 's3://#{bucket}/raw/*.ndjson.zst'
               order by time
               """
             ) == %{
               "entity" => ["first.ex", "second.ex"],
               "project" => ["w1", "w2"],
               "time" => [1_653_576_917.486633, 1_653_576_918.486633]
             }
    end

    test "returns 503 when S3 rejects the upload", %{req: req, bucket: bucket} do
      Req.delete!(Help.s3_req(Help.s3_credentials(:minio)), url: "s3://#{bucket}")

      telemetry_ref = Help.attach_telemetry([[:w3, :ingester, :upload, :stop]])

      logs =
        ExUnit.CaptureLog.capture_log(fn ->
          assert %{status: 503, body: "service unavailable"} =
                   Req.post!(req, body: JSON.encode_to_iodata!([Help.heartbeat()]))
        end)

      assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _,
                      %{bucket: ^bucket, result: {:error, {:http_status, 404}}}}

      assert logs =~ "failed to upload heartbeats: {:http_status, 404}"
    end
  end
end
