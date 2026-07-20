defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    _scheme = Application.fetch_env!(:w3, :http_scheme)
    _port = Application.fetch_env!(:w3, :http_port)

    children = [
      {Adbc.Database, driver: :duckdb, process_options: [name: W3.DuckDB]},
      {Adbc.Connection, database: W3.DuckDB, process_options: [name: W3.DuckConn]}
    ]

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
