defmodule W3.EndpointTest do
  use ExUnit.Case, async: true

  test "no auth" do
    url = Help.start_endpoint(api_key: "some_api_key")
    assert %Req.Response{status: 401, headers: %{"www-authenticate" => ["Basic"]}} = Req.get!(url)
  end

  test "wrong auth" do
    url = Help.start_endpoint(api_key: "some_api_key")

    assert %Req.Response{status: 401, headers: %{"www-authenticate" => ["Basic"]}} =
             Req.get!(url, auth: {:basic, "wrong_api_key"})
  end

  test "correct auth" do
    url = Help.start_endpoint(api_key: "some_api_key")

    assert %Req.Response{status: 200, body: "hello world"} =
             Req.get!(url <> "hello/world", auth: {:basic, "some_api_key"})

    assert %Req.Response{status: 404, body: "not found"} =
             Req.get!(url <> "unknown/path", auth: {:basic, "some_api_key"})
  end
end
