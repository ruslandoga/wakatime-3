defmodule W3.Ingester do
  @moduledoc """
  TODO
  """

  @behaviour Supervisor

  require Logger

  @doc """
  Starts the ingester supervisor.

  Options:
  - `:data_path` - the base path for temporary files.
  - `:process_options` - options to pass to the underlying `Supervisor.start_link/3` call.
  """
  def start_link(options) do
    process_options = Keyword.get(options, :process_options, [])
    data_path = Keyword.fetch!(options, :data_path)
    Supervisor.start_link(__MODULE__, data_path, process_options)
  end

  @impl true
  def init(nil) do
    Logger.notice("Ingester is disabled because no data path was provided.")
    :ignore
  end

  def init(data_path) do
    Logger.notice("Starting Ingester with data path: #{data_path}")

    children = [
      {Adbc.Database, driver: :duckdb, process_options: [name: W3.DuckDB]},
      {Adbc.Connection, database: W3.DuckDB, process_options: [name: W3.DuckConn]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
