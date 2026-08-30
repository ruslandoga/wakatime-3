defmodule W3.S3 do
  defstruct [:access_key_id, :secret_access_key, :endpoint_url, :region, :bucket]

  defimpl Inspect, for: W3.S3 do
    def inspect(s3, _opts) do
      "#S3<bucket=#{s3.bucket}, region=#{s3.region}>"
    end
  end

  def response_error(%Req.Response{status: status, body: body, headers: headers}) do
    error = error_fields(body)

    code = safe_error_field(error["Code"])

    request_id =
      safe_error_field(error["RequestId"]) ||
        safe_error_field(headers["x-amz-request-id"] |> List.wrap() |> List.first())

    [
      "HTTP ",
      Integer.to_string(status),
      if(code, do: [": ", code], else: []),
      if(request_id, do: [" (request ", request_id, ")"], else: [])
    ]
    |> IO.iodata_to_binary()
  end

  def delete_objects!(s3, keys) do
    keys
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn keys ->
      body =
        IO.iodata_to_binary([
          ~s|<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
          Enum.map(keys, fn key ->
            ["<Object><Key>", xml_escape(key), "</Key></Object>"]
          end),
          "<Quiet>true</Quiet></Delete>"
        ])

      response =
        s3
        |> base_req()
        |> Req.post!(
          url: "s3://#{s3.bucket}",
          params: [delete: ""],
          headers: %{
            "content-md5" => :md5 |> :crypto.hash(body) |> Base.encode64(),
            "content-type" => "application/xml"
          },
          body: body
        )

      unless response.status == 200 do
        raise "failed to delete S3 objects: #{response_error(response)}"
      end

      case ReqS3.XML.parse_s3(response.body) do
        %{"DeleteResult" => nil} -> :ok
        %{"DeleteResult" => %{"Error" => nil}} -> :ok
        %{"DeleteResult" => _result} -> raise "S3 object deletion returned errors"
        _response -> raise "invalid S3 object deletion response"
      end
    end)
  end

  defp xml_escape(value) do
    value
    |> Plug.HTML.html_escape()
    |> String.replace("\r", "&#13;")
    |> String.replace("\n", "&#10;")
  end

  def list_objects(s3, prefix \\ nil) do
    list_objects(base_req(s3), s3.bucket, prefix, _continuation_token = nil, _acc = [])
  end

  defp list_objects(base_req, bucket, prefix, continuation_token, acc) do
    params =
      %{"list-type" => "2"}
      |> maybe_put("prefix", prefix)
      |> maybe_put("continuation-token", continuation_token)

    response = Req.get!(base_req, url: "s3://#{bucket}", params: params)

    result =
      case response do
        %{status: 200, body: %{"ListBucketResult" => result}} -> result
        _ -> raise "failed to list S3 objects: #{response_error(response)}"
      end

    page = Enum.map(result["Contents"] || [], &Map.fetch!(&1, "Key"))
    acc = [page | acc]

    if result["IsTruncated"] == "true" do
      list_objects(base_req, bucket, prefix, Map.fetch!(result, "NextContinuationToken"), acc)
    else
      acc |> Enum.reverse() |> List.flatten()
    end
  end

  def base_req(s3) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      endpoint_url: endpoint_url,
      region: region
    } = s3

    Req.new(retry: :transient)
    |> ReqS3.attach(
      aws_sigv4: [
        region: region,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key
      ],
      aws_endpoint_url_s3: endpoint_url
    )
  end

  def object_url(s3, key) do
    key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "s3://#{s3.bucket}/#{key}"
  end

  defp maybe_put(map, key, value) do
    if value, do: Map.put(map, key, value), else: map
  end

  defp safe_error_field(value) when is_binary(value) and byte_size(value) <= 256 do
    if String.match?(value, ~r/\A[[:alnum:].:_-]+\z/), do: value
  end

  defp safe_error_field(_value), do: nil

  defp error_fields(%{"Error" => %{} = error}), do: error
  defp error_fields(%{} = error), do: error

  defp error_fields(body) when is_binary(body) do
    body
    |> ReqS3.XML.parse_s3()
    |> error_fields()
  rescue
    _error -> %{}
  catch
    _kind, _reason -> %{}
  end

  defp error_fields(_body), do: %{}
end
