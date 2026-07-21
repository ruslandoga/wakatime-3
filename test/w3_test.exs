defmodule W3Test do
  use ExUnit.Case, async: true

  setup_all do
    s3_credentials = Help.s3_credentials(:minio)
    s3_req = Help.s3_req(s3_credentials)
    {:ok, s3_req: s3_req}
  end

  @tag :minio
  test "s3 create bucket, put and get object, delete bucket", %{s3_req: s3_req} do
    # Create bucket and delete on exit
    on_exit(fn -> Req.delete!(s3_req, url: "s3://w3-test-bucket") end)
    assert Req.put!(s3_req, url: "s3://w3-test-bucket")

    # Put object
    assert Req.put!(s3_req, url: "s3://w3-test-bucket/test-key", body: "Hello, S3!")

    # Get object
    assert Req.get!(s3_req, url: "s3://w3-test-bucket/test-key").body == "Hello, S3!"
  end
end
