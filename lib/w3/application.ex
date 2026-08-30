defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    W3.LoggerTelemetryHandler.attach()

    config = W3.config()

    api_key = Keyword.fetch!(config, :api_key)
    http = Keyword.fetch!(config, :http)
    s3 = Keyword.fetch!(config, :s3)

    children = [
      {W3.Endpoint, Keyword.merge(http, api_key: api_key, s3: s3)},
      {W3.Compactor, s3: s3}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def stop(_state) do
    W3.LoggerTelemetryHandler.detach()
  end
end
