defmodule W3.Ingester do
  @moduledoc false

  def insert_heartbeats(_s3, [], _machine_name), do: :ok

  def insert_heartbeats(s3, heartbeats, machine_name) do
    ndjson =
      heartbeats
      |> Enum.map(fn heartbeat ->
        [heartbeat |> Map.put("machine_name", machine_name) |> JSON.encode_to_iodata!(), ?\n]
      end)
      |> IO.iodata_to_binary()

    id = :crypto.hash(:sha256, ndjson) |> Base.encode16(case: :lower)
    key = "raw/#{id}.ndjson.zst"
    body = :zstd.compress(ndjson)
    heartbeat_count = length(heartbeats)
    byte_count = IO.iodata_length(body)

    metadata = %{
      bucket: s3.bucket,
      key: key,
      heartbeats: heartbeat_count,
      bytes: byte_count
    }

    :telemetry.span([:w3, :upload], metadata, fn ->
      result = upload!(s3, key, body)
      measurements = %{heartbeats: heartbeat_count, bytes: byte_count}
      metadata = Map.put(metadata, :result, result)
      {result, measurements, metadata}
    end)
  end

  defp upload!(s3, key, body) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      endpoint_url: endpoint_url,
      region: region,
      bucket: bucket
    } = s3

    response =
      Req.new(retry: :transient)
      |> ReqS3.attach(
        aws_sigv4: [
          region: region,
          access_key_id: access_key_id,
          secret_access_key: secret_access_key
        ],
        aws_endpoint_url_s3: endpoint_url
      )
      |> Req.put!(
        headers: %{"content-encoding" => "zstd", "content-type" => "application/x-ndjson"},
        url: "s3://#{bucket}/#{key}",
        body: body
      )

    if response.status in 200..299 do
      :ok
    else
      {:error, response}
    end
  end
end
