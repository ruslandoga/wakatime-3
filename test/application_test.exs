defmodule W3.ApplicationTest do
  use ExUnit.Case, async: true

  test "loads test runtime defaults" do
    config = W3.config()

    assert Keyword.fetch!(config, :api_key) == "406fe41f-6d69-4183-a4cc-121e0c524c2b"
    assert Keyword.fetch!(config, :http) == [scheme: :http, port: 0]
    assert Keyword.fetch!(config, :ingester) == [interval: 30_000, max_buffer_size: 10_000_000]

    assert Keyword.fetch!(config, :s3) == [
             bucket: "w3-test",
             region: "us-east-1",
             endpoint_url: "s3.amazonaws.com",
             access_key_id: "minioadmin",
             secret_access_key: "minioadmin"
           ]
  end

  test "starts ingester and endpoint from runtime config" do
    assert [
             {W3.Endpoint, endpoint, :supervisor, _},
             {W3.Ingester, ingester, :worker, [W3.Ingester]}
           ] = Supervisor.which_children(W3.Supervisor)

    assert Process.alive?(ingester)
    assert {:ok, {_ip, port}} = ThousandIsland.listener_info(endpoint)
    assert is_integer(port)
  end
end
