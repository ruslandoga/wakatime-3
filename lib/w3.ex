defmodule W3 do
  @moduledoc """
  TODO
  """

  def query(sql, params \\ []) do
    result = Adbc.Connection.query!(W3.DuckConn, sql, params)
    Adbc.Result.to_map(result)
  end
end
