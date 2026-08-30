defmodule W3.ApplicationTest do
  use ExUnit.Case, async: true

  test "loads test runtime defaults" do
    config = W3.config()

    assert Keyword.fetch!(config, :api_key) == "406fe41f-6d69-4183-a4cc-121e0c524c2b"
    assert Keyword.fetch!(config, :http) == [scheme: :http, port: 0]
    refute Keyword.has_key?(config, :ingester)

    assert Keyword.fetch!(config, :s3) == [
             bucket: "w3-test",
             region: System.get_env("AWS_REGION", "us-east-1"),
             endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
             access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
             secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin")
           ]
  end

  test "starts endpoint and compactor from runtime config" do
    children = Supervisor.which_children(W3.Supervisor)

    assert {W3.TaskSupervisor, tasks, :supervisor, _} =
             List.keyfind(children, W3.TaskSupervisor, 0)

    assert {W3.Endpoint, endpoint, :supervisor, _} = List.keyfind(children, W3.Endpoint, 0)
    assert {W3.Compactor, compactor, :worker, _} = List.keyfind(children, W3.Compactor, 0)

    assert Process.alive?(tasks)
    assert {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint)
    assert is_integer(port)
    assert Process.alive?(compactor)
  end
end
