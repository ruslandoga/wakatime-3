defmodule W3.Ingester do
  @moduledoc false
  use GenServer

  def start_link(options) do
    {gen_options, options} = Keyword.split(options, [:name])
    GenServer.start_link(__MODULE__, options, gen_options)
  end

  def insert_heartbeats!(ingester, heartbeats, machine_name) do
    ndjson = heartbeats_to_ndjson(heartbeats, machine_name)
    GenServer.call(ingester, {:buffer, ndjson})
  end

  @impl GenServer
  def init(options) do
    s3 = Keyword.fetch!(options, :s3)
    interval = Keyword.get(options, :interval, to_timeout(second: 30))
    max_buffer_size = Keyword.get(options, :max_buffer_size, 10_000_000)
    timer = Process.send_after(self(), :flush, interval)

    state = %{
      buffer: nil,
      size: 0,
      max_size: max_buffer_size,
      interval: interval,
      timer: timer,
      s3: s3
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:buffer, ndjson}, _from, state) do
    %{size: buffer_size, buffer: buffer, max_size: max_buffer_size} = state
    buffer_size = buffer_size + IO.iodata_length(ndjson)
    buffer = add_to_buffer(buffer, ndjson)

    state =
      if buffer_size > max_buffer_size do
        %{interval: interval, timer: timer, s3: s3} = state
        Process.cancel_timer(timer)
        flush(s3, buffer)
        timer = Process.send_after(self(), :flush, interval)
        %{state | size: 0, buffer: nil, timer: timer}
      else
        %{state | size: buffer_size, buffer: buffer}
      end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    %{size: buffer_size, buffer: buffer, timer: timer, interval: interval} = state
    Process.cancel_timer(timer)

    state =
      if buffer_size > 0 do
        %{interval: interval, s3: s3} = state
        flush(s3, buffer)
        timer = Process.send_after(self(), :flush, interval)
        %{state | size: 0, buffer: nil, timer: timer}
      else
        timer = Process.send_after(self(), :flush, interval)
        %{state | timer: timer}
      end

    {:noreply, state}
  end

  defp heartbeats_to_ndjson(heartbeats, machine_name) do
    heartbeats
    |> Enum.map(fn heartbeat ->
      json = heartbeat |> prepare_heartbeat(machine_name) |> JSON.encode!()
      json <> "\n"
    end)
    |> IO.iodata_to_binary()
  end

  defp prepare_heartbeat(%{"user_agent" => user_agent} = heartbeat, machine_name) do
    ["wakatime/" <> _wakatime_version, os | rest] = String.split(user_agent, " ")
    editor = Enum.find(rest, &String.starts_with?(&1, "vscode/"))

    os = String.replace(os, ["(", ")"], "")

    heartbeat
    |> Map.put("editor", editor)
    |> Map.put("operating_system", os)
    |> Map.put("machine_name", machine_name)
    |> Map.update("is_write", false, fn is_write -> !!is_write end)
  end

  @compile inline: [add_to_buffer: 2]
  defp add_to_buffer(nil, data), do: data
  defp add_to_buffer(buffer, data), do: [buffer | data]

  defp flush(s3, data) do
    datetime = DateTime.utc_now(:second)
    date = DateTime.to_date(datetime)

    bucket = Map.fetch!(s3, :bucket)
    region = Map.get(s3, :region)
    endpoint_url = Map.get(s3, :endpoint_url)

    id = "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

    key =
      "raw/date=#{date}/hour=#{datetime.hour}/minute=#{datetime.minute}/#{id}.ndjson.zst"

    metadata = %{bucket: bucket, key: key}

    :telemetry.span([:w3, :ingester, :upload], metadata, fn ->
      req_options =
        [
          aws_sigv4:
            [
              region: region,
              access_key_id: s3[:access_key_id],
              secret_access_key: s3[:secret_access_key]
            ]
            |> Keyword.reject(fn {_key, value} -> is_nil(value) end)
        ]
        |> put_optional(:aws_endpoint_url_s3, endpoint_url)

      %{status: 200} =
        Req.new(retry: :transient)
        |> ReqS3.attach(req_options)
        |> Req.put!(
          headers: %{
            "content-encoding" => "zstd",
            "content-type" => "application/x-ndjson"
          },
          url: "s3://#{bucket}/#{key}",
          body: :zstd.compress(data)
        )

      {:ok, metadata}
    end)
  end

  defp put_optional(options, _key, nil), do: options
  defp put_optional(options, key, value), do: Keyword.put(options, key, value)
end
