defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    W3.LoggerTelemetryHandler.attach()

    config = W3.config()

    api_key = Keyword.fetch!(config, :api_key)
    port = Keyword.fetch!(config, :port)
    s3 = Keyword.fetch!(config, :s3)

    children = children(api_key, port, s3, Keyword.get(config, :start_compactor, true))

    Supervisor.start_link(children, strategy: :one_for_one, name: W3.Supervisor)
  end

  @doc false
  def children(api_key, port, s3, start_compactor?) do
    children = [
      {Task.Supervisor, name: W3.task_supervisor()},
      {W3.Endpoint, port: port, api_key: api_key, s3: s3}
    ]

    if start_compactor? do
      children ++
        [
          {W3.Periodic,
           interval: to_timeout(minute: 30),
           task: fn -> W3.Compactor.compact_raw_files_into_parquet(s3) end}
        ]
    else
      children
    end
  end

  @impl true
  def stop(_state) do
    W3.LoggerTelemetryHandler.detach()
  end
end
