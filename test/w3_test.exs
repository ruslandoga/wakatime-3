defmodule W3Test do
  use ExUnit.Case, async: true

  setup_all do
    s3_credentials = Help.s3_credentials(:minio)
    s3_req = Help.s3_req(s3_credentials)
    {:ok, s3_req: s3_req}
  end

  @tag :minio
  test "s3 create bucket, put and get object, delete bucket", %{s3_req: s3_req} do
    bucket =
      "w3-test-#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"

    object_url = "s3://#{bucket}/test-key"

    on_exit(fn ->
      Req.delete!(s3_req, url: object_url)
      Req.delete!(s3_req, url: "s3://#{bucket}")
    end)

    assert %{status: 200} = Req.put!(s3_req, url: "s3://#{bucket}")

    assert %{status: 200} = Req.put!(s3_req, url: object_url, body: "Hello, S3!")

    assert %{status: 200, body: "Hello, S3!"} = Req.get!(s3_req, url: object_url)
  end
end
