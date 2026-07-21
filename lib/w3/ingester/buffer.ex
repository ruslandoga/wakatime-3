defmodule W3.Ingester.Buffer do
  @moduledoc false
  @behaviour NimblePool

  def start_link(opts) do
    data_path = Keyword.fetch!(opts, :data_path)
    name = Keyword.fetch!(opts, :name)
    NimblePool.start_link(worker: {__MODULE__, data_path}, pool_size: 1, name: name)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def insert(pool, events) when is_list(events) do
    events = validate_events!(events)
    ndjson_iodata = [Enum.map_intersperse(events, ?\n, &JSON.encode_to_iodata!/1) | ?\n]
    ndjson_size = IO.iodata_length(ndjson_iodata)

    NimblePool.checkout!(pool, :checkout, fn _pool_info, file ->
      :ok = IO.binwrite(file, ndjson_iodata)
      {:ok, ndjson_size}
    end)
  end

  defp validate_events!(events) do
    # TODO
    events
  end

  @impl NimblePool
  def init_pool(data_path) do
    # TODO recompute size from File.stat maybe
    {:ok, %{data_path: data_path, file: open_temp_file(data_path), size: 0}}
  end

  defp open_temp_file(data_path) do
    # TODO reopen prev buffer maybe
    temp_file_path =
      Path.join(
        data_path,
        "ingester_buffer_#{System.system_time(:millisecond)}-#{:erlang.unique_integer([:positive])}.ndjson"
      )

    File.open!(temp_file_path, [:append, :binary])
  end

  @impl NimblePool
  def init_worker(data_path) do
    {:ok, :idle, data_path}
  end
end
