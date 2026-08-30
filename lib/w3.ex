defmodule W3 do
  @moduledoc false

  def config do
    Application.get_all_env(:w3)
  end

  def compact!(%{s3: s3, data_path: data_path}) do
    metadata = %{bucket: s3.bucket}

    :telemetry.span([:w3, :compact], metadata, fn ->
      {W3.Compactor.compact!(s3, data_path), metadata}
    end)
  end

  def async_map!(enumerable, fun, options \\ []) do
    options = Keyword.merge([ordered: false, timeout: to_timeout(second: 60)], options)

    W3.TaskSupervisor
    |> Task.Supervisor.async_stream(enumerable, fun, options)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
