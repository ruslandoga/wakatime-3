defmodule W3.Ingester.Writer do
  @moduledoc false
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  def insert_heartbeats!(writer, heartbeats, machine_name) do
    ndjson = heartbeats_to_ndjson(heartbeats, machine_name)
    GenServer.call(writer, {:append, ndjson})
  end

  @impl GenServer
  def init(options) do
    spool_dir = Keyword.fetch!(options, :spool_dir)
    active_path = Path.join(spool_dir, "current.ndjson.open")
    sealed_dir = Path.join(spool_dir, "sealed")
    File.mkdir_p!(sealed_dir)
    recover_active(active_path, sealed_dir)

    {:ok, file} = File.open(active_path, [:append, :binary])
    size = File.stat!(active_path).size
    interval = Keyword.fetch!(options, :interval)

    state = %{
      file: file,
      active_path: active_path,
      sealed_dir: sealed_dir,
      size: size,
      max_size: Keyword.fetch!(options, :max_buffer_size),
      interval: interval,
      timer: Process.send_after(self(), :rotate, interval),
      uploader: Keyword.fetch!(options, :uploader)
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:append, ndjson}, _from, state) do
    :ok = IO.binwrite(state.file, ndjson)
    state = %{state | size: state.size + byte_size(ndjson)}

    state =
      if state.size >= state.max_size do
        rotate(state)
      else
        state
      end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:rotate, state), do: {:noreply, rotate(state)}

  @impl GenServer
  def terminate(_reason, state) do
    File.close(state.file)
  end

  defp rotate(state) do
    Process.cancel_timer(state.timer)

    state =
      if state.size > 0 do
        :ok = File.close(state.file)
        sealed_path = sealed_path(state.sealed_dir)
        File.mkdir_p!(Path.dirname(sealed_path))
        :ok = File.rename(state.active_path, sealed_path)
        notify_uploader(state.uploader)
        {:ok, file} = File.open(state.active_path, [:append, :binary])
        %{state | file: file, size: 0}
      else
        state
      end

    %{state | timer: Process.send_after(self(), :rotate, state.interval)}
  end

  defp recover_active(active_path, sealed_dir) do
    case File.stat(active_path) do
      {:ok, %{size: size}} when size > 0 ->
        sealed_path = sealed_path(sealed_dir)
        File.mkdir_p!(Path.dirname(sealed_path))
        File.rename!(active_path, sealed_path)

      _other ->
        :ok
    end
  end

  defp sealed_path(sealed_dir) do
    id = "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"
    Path.join(sealed_dir, "#{id}.ndjson")
  end

  defp notify_uploader(nil), do: :ok
  defp notify_uploader(uploader), do: GenServer.cast(uploader, :upload)

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
end
