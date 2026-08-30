defmodule W3.Ingester do
  @moduledoc false

  def insert_heartbeats(_s3, [], _machine_name), do: :ok

  def insert_heartbeats(s3, heartbeats, machine_name) do
    ndjson =
      Enum.map_intersperse(
        heartbeats,
        ?\n,
        fn heartbeat ->
          heartbeat |> Map.put("machine_name", machine_name) |> JSON.encode_to_iodata!()
        end
      )

    id = Base.encode16(:crypto.hash(:sha256, ndjson), case: :lower)
    url = "s3://#{s3.bucket}/raw/#{id}.ndjson.zst"
    body = :zstd.compress(ndjson)
    heartbeat_count = length(heartbeats)
    byte_count = IO.iodata_length(body)
    metadata = %{url: url}

    :telemetry.span([:w3, :upload], metadata, fn ->
      upload!(s3, url, body)
      measurements = %{heartbeats: heartbeat_count, bytes: byte_count}
      {:ok, measurements, metadata}
    end)
  end

  defp upload!(s3, url, body) do
    %{status: 200} =
      s3
      |> W3.S3.base_req()
      |> Req.put!(
        headers: %{
          "content-encoding" => "zstd",
          "content-type" => "application/x-ndjson"
        },
        url: url,
        body: body
      )
  end
end
