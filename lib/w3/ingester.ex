defmodule W3.Ingester do
  @moduledoc false

  def insert_heartbeats(_s3, [], _metadata), do: :ok

  def insert_heartbeats(s3, heartbeats, metadata) do
    ndjson =
      heartbeats
      |> Enum.map(fn heartbeat ->
        [heartbeat |> Map.merge(metadata) |> JSON.encode_to_iodata!(), ?\n]
      end)
      |> IO.iodata_to_binary()

    id = :crypto.hash(:sha256, ndjson) |> Base.encode16(case: :lower)
    upload(s3, "raw/#{id}.ndjson.zst", :zstd.compress(ndjson))
  end

  defp upload(s3, key, body) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      endpoint_url: endpoint_url,
      region: region,
      bucket: bucket
    } = Map.new(s3)

    metadata = %{bucket: bucket, key: key}

    :telemetry.span([:w3, :ingester, :upload], metadata, fn ->
      response =
        Req.new(retry: false)
        |> ReqS3.attach(
          aws_sigv4: [
            region: region,
            access_key_id: access_key_id,
            secret_access_key: secret_access_key
          ],
          aws_endpoint_url_s3: endpoint_url
        )
        |> Req.put(
          headers: %{
            "content-encoding" => "zstd",
            "content-type" => "application/x-ndjson"
          },
          url: "s3://#{bucket}/#{key}",
          body: body
        )

      result =
        case response do
          {:ok, %{status: status}} when status in 200..299 -> :ok
          {:ok, %{status: status}} -> {:error, {:http_status, status}}
          {:error, exception} -> {:error, exception}
        end

      {result, metadata}
    end)
  rescue
    exception -> {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
