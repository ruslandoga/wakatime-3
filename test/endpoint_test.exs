defmodule W3.EndpointTest do
  use ExUnit.Case, async: true

  describe "bad auth" do
    setup do
      url = Help.start_endpoint(api_key: "api_key")
      req = Req.new(base_url: url)
      {:ok, req: req}
    end

    test "no auth", %{req: req} do
      assert %Req.Response{status: 401, headers: %{"www-authenticate" => ["Basic"]}} =
               Req.get!(req)
    end

    test "wrong auth", %{req: req} do
      assert %Req.Response{status: 401, headers: %{"www-authenticate" => ["Basic"]}} =
               Req.get!(req, auth: {:basic, "wrong_api_key"})
    end
  end

  describe "with auth" do
    setup do
      api_key = Base.encode64(:crypto.strong_rand_bytes(32))
      url = Help.start_endpoint(api_key: api_key)
      req = Req.new(base_url: url, auth: {:basic, api_key})
      {:ok, req: req}
    end

    test "known path returns 200 auth", %{req: req} do
      assert %Req.Response{status: 200, body: "hello world"} =
               Req.get!(req, url: "/hello/world")
    end

    test "unknown path returns 404", %{req: req} do
      assert %Req.Response{status: 404, body: "not found"} =
               Req.get!(req, url: "/unknown/path")
    end
  end
end
