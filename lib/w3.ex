defmodule W3 do
  @moduledoc """
  Documentation for `W3`.
  """

  def list_remote do
  end

  def list_local do
  end

  def query(sql, params \\ []) do
    result = Adbc.Connection.query!(W3.DuckConn, sql, params)
    Adbc.Result.to_map(result)
  end
end
