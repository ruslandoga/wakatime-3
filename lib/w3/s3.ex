defmodule W3.S3 do
  defstruct [:access_key_id, :secret_access_key, :endpoint_url, :region, :bucket]

  defimpl Inspect, for: W3.S3 do
    def inspect(s3, _opts) do
      "#S3<bucket=#{s3.bucket}, region=#{s3.region}, endpoint_url=#{s3.endpoint_url}>"
    end
  end

  defp delete_objects!(s3, keys) do
    Enum.each(Enum.chunk_every(keys, 1_000), fn keys ->
      body =
        IO.iodata_to_binary([
          ~s|<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
          Enum.map(keys, fn key ->
            ["<Object><Key>", xml_escape(key), "</Key></Object>"]
          end),
          "<Quiet>true</Quiet></Delete>"
        ])

      %{status: 200, body: response_body} =
        Req.post!(req(s3),
          url: "s3://#{s3.bucket}",
          params: [delete: ""],
          headers: %{
            "content-md5" => :md5 |> :crypto.hash(body) |> Base.encode64(),
            "content-type" => "application/xml"
          },
          body: body
        )

      case ReqS3.XML.parse_s3(response_body) do
        %{"DeleteResult" => nil} -> :ok
        %{"DeleteResult" => result} -> nil = result["Error"]
      end
    end)
  end

  defp object_url(s3, key) do
    key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "s3://#{s3.bucket}/#{key}"
  end

  defp xml_escape(value) do
    value
    |> Plug.HTML.html_escape()
    |> String.replace("\r", "&#13;")
    |> String.replace("\n", "&#10;")
  end

  def s3_ls(s3, prefix \\ nil) do
    s3_ls(s3_req(s3), s3.bucket, prefix, _continuation_token = nil, _acc = [])
  end

  defp s3_ls(base_req, bucket, prefix, continuation_token, acc) do
    params =
      %{"list-type" => "2"}
      |> maybe_put("prefix", prefix)
      |> maybe_put("continuation-token", continuation_token)

    %{status: 200, body: %{"ListBucketResult" => result}} =
      Req.get!(base_req, url: "s3://#{bucket}", params: params)

    page = Enum.map(result["Contents"] || [], &Map.fetch!(&1, "Key"))
    acc = [page | acc]

    if result["IsTruncated"] == "true" do
      s3_ls(base_req, bucket, prefix, Map.fetch!(result, "NextContinuationToken"), acc)
    else
      acc |> Enum.reverse() |> List.flatten()
    end
  end

  def s3_download(s3, keys, dir) do
    File.mkdir_p!(dir)
    base_req = s3_req(s3)

    keys
    |> Enum.with_index()
    |> W3.async_map!(
      fn {key, idx} ->
        %{status: 200, body: body} =
          Req.get!(base_req, url: "s3://#{s3.bucket}/#{key}", raw: true)

        path = Path.join(dir, "#{idx}.#{Path.extname(key)}")
        File.write!(path, body)
        %{key: key, path: path}
      end,
      max_concurrency: System.schedulers_online() * 2
    )
  end

  def s3_upload(s3, files) do
    base_req = s3_req(s3)

    W3.async_map!(
      files,
      fn %{key: key, path: path, type: type} ->
        %{status: 200} =
          Req.put!(base_req,
            url: "s3://#{s3.bucket}/#{key}",
            headers: %{"content-type" => type},
            body: File.read!(path)
          )
      end,
      max_concurrency: System.schedulers_online() * 2
    )

    :ok
  end

  def s3_req(s3) do
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

  defp maybe_put(map, key, value) do
    if value, do: Map.put(map, key, value), else: map
  end
end
