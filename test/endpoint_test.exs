defmodule W3.EndpointTest do
  use ExUnit.Case, async: true

  setup do
    bucket =
      "w3-ingester-test-#{System.system_time(:second)}-#{System.unique_integer([:positive])}"

    ingester = Help.start_ingester(bucket: bucket)
    api_key = Base.encode64(:crypto.strong_rand_bytes(32))
    url = Help.start_endpoint(api_key: api_key, ingester: ingester)
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

    assert logs =~ "some debug info"
    assert logs =~ "this is the last warning"
    assert logs =~ "something's wrong"
  end

  describe "heartbeats" do
    setup %{req: req} do
      req =
        Req.merge(req,
          url: "/heartbeats",
          headers: %{
            "x-machine-name" => "mac3.local",
            "content-type" => "application/json",
            "accept" => "application/json"
          }
        )

      {:ok, req: req}
    end

    test "ingest", %{req: req, bucket: bucket} do
      body =
        JSON.encode_to_iodata!(%{
          "_json" => [
            %{
              "branch" => "add-ingester",
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
            }
          ]
        })

      telemetry_ref = Help.attach_telemetry([[:w3, :ingester, :upload, :stop]])

      assert %{status: 201, body: %{"responses" => [[nil, 201]]}} =
               Req.post!(req, body: body)

      assert_receive {[:w3, :ingester, :upload, :stop], ^telemetry_ref, _, %{bucket: ^bucket}}
      duck = Help.start_duck(Help.s3_credentials(:minio))

      assert Help.quack(
               duck,
               "select * exclude(date, minute, hour) from 's3://#{bucket}/**/*.ndjson.zst'"
             ) == %{
               "branch" => ["add-ingester"],
               "category" => ["coding"],
               "cursorpos" => [1],
               "dependencies" => [nil],
               "editor" => ["vscode/1.68.0-insider"],
               "entity" => ["/Users/q/Developer/copycat/w1/test/endpoint_test.exs"],
               "is_write" => [false],
               "language" => ["Elixir"],
               "lineno" => [1],
               "lines" => [4],
               "machine_name" => ["mac3.local"],
               "operating_system" => ["darwin-21.4.0-arm64"],
               "project" => ["w1"],
               "time" => [1_653_576_917.486633],
               "type" => ["file"],
               "user_agent" => [
                 "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 vscode/1.68.0-insider vscode-wakatime/18.1.5"
               ]
             }
    end
  end
end
