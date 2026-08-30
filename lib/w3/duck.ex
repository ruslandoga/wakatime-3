defmodule W3.Duck do
  @moduledoc """
  Resource-safe helpers around DuckNIF's low-level database, statement, result,
  and data-chunk APIs.
  """

  @doc "Opens a DuckDB connection for the function and closes it afterward."
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

  @doc "Executes a parameterized query and returns its column vectors."
  def query(conn, sql, params \\ %{}) when is_map(params) do
    stmt = DuckNIF.prepare(conn, sql)

    try do
      bind_all(stmt, params)
      result = DuckNIF.execute_prepared_dirty_io(stmt)

      try do
        columns = columns(result)
        values = Map.new(columns, fn {_index, name} -> {name, []} end)
        fetch_chunks(result, columns, values)
      after
        DuckNIF.destroy_result(result)
      end
    after
      DuckNIF.destroy_prepare(stmt)
    end
  end

  defp fetch_chunks(result, columns, values) do
    case DuckNIF.fetch_chunk(result) do
      nil ->
        Map.new(values, fn {name, vectors} ->
          {name, vectors |> Enum.reverse() |> Enum.flat_map(& &1)}
        end)

      chunk ->
        values =
          try do
            Enum.reduce(columns, values, fn {index, name}, values ->
              Map.update!(values, name, &[DuckNIF.data_chunk_get_vector(chunk, index) | &1])
            end)
          after
            DuckNIF.destroy_data_chunk(chunk)
          end

        fetch_chunks(result, columns, values)
    end
  end

  defp columns(result) do
    case DuckNIF.column_count(result) do
      0 -> []
      count -> Enum.map(0..(count - 1), &{&1, DuckNIF.column_name(result, &1)})
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
