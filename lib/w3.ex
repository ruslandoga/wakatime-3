defmodule W3 do
  @moduledoc false

  def config do
    Application.get_all_env(:w3)
  end

  def task_supervisor do
    W3.TaskSupervisor
  end

  def async_map!(enumerable, fun, options \\ []) do
    task_supervisor()
    |> Task.Supervisor.async_stream(enumerable, fun, options)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
