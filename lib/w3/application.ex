defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    attach_compactor_logger()

    config = W3.config()

    api_key = Keyword.fetch!(config, :api_key)
    compactor = Keyword.get(config, :compactor, [])
    http = Keyword.fetch!(config, :http)
    s3 = Keyword.fetch!(config, :s3)

    children = [
      {W3.Endpoint, Keyword.merge(http, api_key: api_key, s3: s3)},
      {W3.Compactor, {s3, compactor}}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def stop(_state) do
    :telemetry.detach({W3.Compactor, :logger})
  end

  defp attach_compactor_logger do
    case :telemetry.attach_many(
           {W3.Compactor, :logger},
           [
             [:w3, :compactor, :run, :stop],
             [:w3, :compactor, :run, :exception]
           ],
           &W3.Compactor.handle_event/4,
           nil
         ) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end
end
