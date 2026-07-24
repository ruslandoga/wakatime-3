defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    config = W3.config()

    api_key = Keyword.fetch!(config, :api_key)
    http = Keyword.fetch!(config, :http)
    ingester = Keyword.fetch!(config, :ingester)
    s3 = Keyword.fetch!(config, :s3)

    children = [
      {W3.Ingester, Keyword.merge(ingester, s3: s3, name: W3.Ingester)},
      {W3.Endpoint, Keyword.merge(http, api_key: api_key, ingester: W3.Ingester)}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
