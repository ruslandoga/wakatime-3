defmodule W3.Ingester.Uploader do
  @moduledoc false
  use GenServer

  require Logger

  def start_link(options) do
    {gen_options, options} = Keyword.split(options, [:name])
    GenServer.start_link(__MODULE__, options, gen_options)
  end

  @impl GenServer
  def init(options) do
    interval = Keyword.fetch!(options, :interval)

    state = %{
      sealed_dir: options |> Keyword.fetch!(:spool_dir) |> Path.join("sealed"),
      interval: interval,
      s3: options |> Keyword.fetch!(:s3) |> Map.new()
    }

    send(self(), :upload)
    Process.send_after(self(), :scan, interval)
    {:ok, state}
  end

  @impl GenServer
  def handle_cast(:upload, state) do
    upload_sealed_files(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:upload, state) do
    upload_sealed_files(state)
    {:noreply, state}
  end

  def handle_info(:scan, state) do
    upload_sealed_files(state)
    Process.send_after(self(), :scan, state.interval)
    {:noreply, state}
  end

  defp upload_sealed_files(state) do
    state.sealed_dir
    |> Path.join("**/*.ndjson")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.each(fn path ->
      case upload(state, path) do
        :ok ->
          File.rm!(path)

        {:error, reason} ->
          Logger.warning("failed to upload #{path}: #{inspect(reason)}")
      end
    end)
  end

  defp upload(state, path) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      endpoint_url: endpoint_url,
      region: region,
      bucket: bucket
    } = state.s3

    key = "raw/#{Path.basename(path)}.zst"
    metadata = %{bucket: bucket, key: key}

    :telemetry.span([:w3, :ingester, :upload], metadata, fn ->
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
        |> Req.put(
          headers: %{
            "content-encoding" => "zstd",
            "content-type" => "application/x-ndjson"
          },
          url: "s3://#{bucket}/#{key}",
          body: path |> File.read!() |> :zstd.compress()
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
  end
end
