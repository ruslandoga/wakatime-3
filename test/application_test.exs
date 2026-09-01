defmodule W3.ApplicationTest do
  use ExUnit.Case, async: true

  test "loads test runtime defaults" do
    config = W3.config()

    assert Keyword.fetch!(config, :api_key) == "406fe41f-6d69-4183-a4cc-121e0c524c2b"
    assert Keyword.fetch!(config, :port) == 0

    assert Keyword.fetch!(config, :s3) == %W3.S3{
             bucket: "w3-test",
             region: System.get_env("AWS_REGION", "us-east-1"),
             endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "s3.amazonaws.com"),
             access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
             secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin")
           }
  end

  test "starts endpoint and compactors from runtime config" do
    children = Supervisor.which_children(W3.Supervisor)
    raw_interval = to_timeout(minute: 30)
    parquet_interval = to_timeout(day: 1)

    assert {W3.TaskSupervisor, tasks, :supervisor, _} =
             List.keyfind(children, W3.TaskSupervisor, 0)

    assert {W3.Endpoint, endpoint, :supervisor, _} = List.keyfind(children, W3.Endpoint, 0)

    assert {:raw_compactor, raw_compactor, :worker, [W3.Periodic]} =
             List.keyfind(children, :raw_compactor, 0)

    assert {:parquet_compactor, parquet_compactor, :worker, [W3.Periodic]} =
             List.keyfind(children, :parquet_compactor, 0)

    assert Process.alive?(tasks)
    assert Process.alive?(raw_compactor)
    assert Process.alive?(parquet_compactor)
    assert {_state, %{interval: ^raw_interval}} = :sys.get_state(raw_compactor)

    assert {_state, %{interval: ^parquet_interval}} = :sys.get_state(parquet_compactor)

    assert {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint)
    assert is_integer(port)
  end
end
