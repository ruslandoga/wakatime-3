defmodule W3.DuckTest do
  use ExUnit.Case, async: true

  alias W3.Duck

  test "query/3 binds supported values and returns column-vector chunks" do
    Duck.with_duck(fn conn ->
      assert [
               %{
                 "boolean_value" => [true],
                 "float_value" => [1.5],
                 "integer_value" => [42],
                 "is_null" => [true],
                 "text_value" => ["hello"]
               }
             ] =
               Duck.query(
                 conn,
                 """
                 SELECT
                   CAST($text AS VARCHAR) AS text_value,
                   CAST($integer AS BIGINT) AS integer_value,
                   CAST($float AS DOUBLE) AS float_value,
                   CAST($boolean AS BOOLEAN) AS boolean_value,
                   CAST($nothing AS VARCHAR) IS NULL AS is_null
                 """,
                 %{
                   "text" => "hello",
                   "integer" => 42,
                   "float" => 1.5,
                   "boolean" => true,
                   "nothing" => nil
                 }
               )
    end)
  end

  test "query/3 returns every chunk and leaves the connection reusable after errors" do
    Duck.with_duck(fn conn ->
      chunks = Duck.query(conn, "SELECT value FROM range(5000) AS numbers(value)", %{})

      assert length(chunks) > 1
      assert Enum.all?(chunks, &match?(%{"value" => values} when is_list(values), &1))

      assert chunks |> Enum.flat_map(& &1["value"]) == Enum.to_list(0..4999)

      assert_raise ArgumentError, ~r/unsupported parameter type/, fn ->
        Duck.query(conn, "SELECT CAST($value AS VARCHAR)", %{"value" => []})
      end

      assert_raise DuckNIF.Error, fn ->
        Duck.query(conn, "SELECT CAST($value AS BIGINT)", %{"value" => "not-an-integer"})
      end

      assert [%{"answer" => [42]}] = Duck.query(conn, "SELECT 42 AS answer", %{})
    end)
  end

  test "query/3 handles statements without result columns" do
    Duck.with_duck(fn conn ->
      assert [] = Duck.query(conn, "CREATE TEMP TABLE entries(value BIGINT)", %{})

      assert [%{"Count" => [1]}] =
               Duck.query(conn, "INSERT INTO entries VALUES ($value)", %{"value" => 42})

      assert [%{"value" => [42]}] = Duck.query(conn, "SELECT value FROM entries", %{})
    end)
  end
end
