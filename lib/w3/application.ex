defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    http_scheme = Application.fetch_env!(:w3, :http_scheme)
    http_port = Application.fetch_env!(:w3, :http_port)
    ingester_data_path = Application.fetch_env!(:w3, :ingester_data_path)

    children = [
      {W3.Ingester, data_path: ingester_data_path, process_options: [name: W3.Ingester]},
      {W3.Endpoint, scheme: http_scheme, port: http_port}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
