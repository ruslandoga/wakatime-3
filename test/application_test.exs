defmodule W3.ApplicationTest do
  use ExUnit.Case, async: true

  test "loads test runtime defaults" do
    config = W3.config()

    assert Keyword.fetch!(config, :api_key) == "406fe41f-6d69-4183-a4cc-121e0c524c2b"
    assert Keyword.fetch!(config, :port) == 0
    refute Keyword.fetch!(config, :start_compactor)

    assert Keyword.fetch!(config, :s3) == %W3.S3{
             bucket: "w3-test",
             region: System.get_env("AWS_REGION", "us-east-1"),
             endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
             access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
             secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin")
           }
  end

  test "starts endpoint from runtime config" do
    children = Supervisor.which_children(W3.Supervisor)

    assert {W3.TaskSupervisor, tasks, :supervisor, _} =
             List.keyfind(children, W3.TaskSupervisor, 0)

    assert {W3.Endpoint, endpoint, :supervisor, _} = List.keyfind(children, W3.Endpoint, 0)

    assert Process.alive?(tasks)
    assert {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint)
    assert is_integer(port)
    refute List.keyfind(children, W3.Periodic, 0)
  end

  test "configures production compaction every 30 minutes" do
    s3 = Keyword.fetch!(W3.config(), :s3)

    assert {W3.Periodic, options} =
             W3.Application.children("api-key", 0, s3, true)
             |> List.keyfind(W3.Periodic, 0)

    assert Keyword.fetch!(options, :interval) == to_timeout(minute: 30)
    assert is_function(Keyword.fetch!(options, :task), 0)
  end
end
