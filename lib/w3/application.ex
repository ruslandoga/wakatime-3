defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    http_uri = Application.fetch_env!(:w3, :http_uri)
    data_path = Application.fetch_env!(:w3, :data_path)

    children = [
      {W3.Endpoint, uri: http_uri, data_path: data_path},
      {W3.Compactor, data_path: data_path}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
