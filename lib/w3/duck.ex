defmodule W3.Duck do
  @moduledoc """
  TODO
  """

  def with_duck(fun) when is_function(fun, 1) do
    db = DuckNIF.open()

    try do
      conn = DuckNIF.connect(db)

      try do
        fun.(conn)
      after
        DuckNIF.disconnect(conn)
      end
    after
      DuckNIF.close(db)
    end
  end

  def query(conn, sql, params \\ %{}) when is_map(params) do
    stmt = DuckNIF.prepare(conn, sql)

    try do
      bind_all(stmt, params)
      result = DuckNIF.execute_prepared_dirty_io(stmt)

      try do
        Stream.repeatedly(fn -> DuckNIF.fetch_chunk(result) end)
        |> Stream.take_while(&is_reference/1)
        |> Enum.map(fn chunk ->
          Map.new(0..(DuckNIF.column_count(result) - 1), fn i ->
            {DuckNIF.column_name(result, i), DuckNIF.data_chunk_get_vector(chunk, i)}
          end)
        end)
      after
        DuckNIF.destroy_result(result)
      end
    after
      DuckNIF.destroy_prepare(stmt)
    end
  end

  defp bind_all(stmt, params) do
    Enum.each(params, fn {name, value} ->
      idx = DuckNIF.bind_parameter_index(stmt, name)

      cond do
        is_binary(value) ->
          DuckNIF.bind_varchar(stmt, idx, value)

        is_integer(value) ->
          DuckNIF.bind_int64(stmt, idx, value)

        is_float(value) ->
          DuckNIF.bind_double(stmt, idx, value)

        is_boolean(value) ->
          DuckNIF.bind_boolean(stmt, idx, value)

        is_nil(value) ->
          DuckNIF.bind_null(stmt, idx)

        true ->
          raise ArgumentError,
                "unsupported parameter type for #{inspect(name)}: #{inspect(value)}"
      end
    end)
  end
end
