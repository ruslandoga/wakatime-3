defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    W3.LoggerTelemetryHandler.attach()

    config = W3.config()

    api_key = Keyword.fetch!(config, :api_key)
    http = Keyword.fetch!(config, :http)
    s3 = config |> Keyword.fetch!(:s3) |> Map.new()
    data_path = Keyword.fetch!(config, :data_path)

    children = [
      {Task.Supervisor, name: W3.TaskSupervisor},
      {W3.Endpoint, Keyword.merge(http, api_key: api_key, s3: s3)},
      {W3.Periodic,
       interval: to_timeout(minute: 30),
       backoff: %{base: to_timeout(second: 1), max: to_timeout(second: 60)},
       task: fn -> W3.compact!(%{s3: s3, data_path: data_path}) end}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def stop(_state) do
    W3.LoggerTelemetryHandler.detach()
  end
end
