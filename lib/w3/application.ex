defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    _scheme = Application.fetch_env!(:w3, :http_scheme)
    _port = Application.fetch_env!(:w3, :http_port)

    children = []

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
