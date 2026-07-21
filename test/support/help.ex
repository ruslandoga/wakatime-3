defmodule Help do
  @moduledoc false

  def s3_credentials(:minio) do
    %{
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
      region: System.get_env("AWS_REGION", "us-east-1"),
      endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    }
  end

  def s3_req(credentials) do
    req =
      Req.new(
        aws_sigv4: [
          service: :s3,
          access_key_id: credentials[:access_key_id],
          secret_access_key: credentials[:secret_access_key],
          region: credentials[:region]
        ],
        retry: :transient
      )

    ReqS3.attach(req, aws_endpoint_url_s3: credentials[:endpoint_url])
  end
end
