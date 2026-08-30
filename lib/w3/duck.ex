defmodule W3.Duck do
  @moduledoc """
  Resource-safe helpers around DuckNIF's low-level database, statement, result,
  and data-chunk APIs.
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

  @doc "Executes a parameterized query and returns its column vectors by data chunk."
  def query(conn, sql, params \\ %{}) when is_map(params) do
    stmt = DuckNIF.prepare(conn, sql)

    try do
      bind_all(stmt, params)
      result = DuckNIF.execute_prepared_dirty_io(stmt)

      try do
        read_chunks(result)
      after
        DuckNIF.destroy_result(result)
      end
    after
      DuckNIF.destroy_prepare(stmt)
    end
  end

  defp read_chunks(result) do
    read_chunks(result, columns(result), [])
  end

  defp read_chunks(result, columns, chunks) do
    case DuckNIF.fetch_chunk(result) do
      chunk when is_reference(chunk) ->
        values =
          try do
            Map.new(columns, fn {index, name} ->
              {name, DuckNIF.data_chunk_get_vector(chunk, index)}
            end)
          after
            DuckNIF.destroy_data_chunk(chunk)
          end

        read_chunks(result, columns, [values | chunks])

      nil ->
        Enum.reverse(chunks)
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
