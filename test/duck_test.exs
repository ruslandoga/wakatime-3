defmodule W3.DuckTest do
  use ExUnit.Case, async: true

  alias W3.Duck

  test "query/3 binds supported values and returns column vectors" do
    Duck.with_duck(fn conn ->
      assert %{
               "boolean_value" => [true],
               "float_value" => [1.5],
               "integer_value" => [42],
               "is_null" => [true],
               "text_value" => ["hello"]
             } =
               Duck.query(
                 conn,
                 """
                 SELECT
                   $text AS text_value,
                   $integer AS integer_value,
                   $float AS float_value,
                   $boolean AS boolean_value,
                   $nothing IS NULL AS is_null
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
      assert %{"value" => values} =
               Duck.query(conn, "SELECT value FROM range(5000) AS numbers(value)", %{})

      assert values == Enum.to_list(0..4999)

      assert_raise ArgumentError, ~r/unsupported parameter type/, fn ->
        Duck.query(conn, "SELECT CAST($value AS VARCHAR)", %{"value" => []})
      end

      assert_raise DuckNIF.Error, fn ->
        Duck.query(conn, "SELECT CAST($value AS BIGINT)", %{"value" => "not-an-integer"})
      end

      assert %{"answer" => [42]} = Duck.query(conn, "SELECT 42 AS answer", %{})
    end)
  end

  test "query/3 handles statements without result columns" do
    Duck.with_duck(fn conn ->
      assert %{} = Duck.query(conn, "CREATE TEMP TABLE entries(value BIGINT)", %{})

      assert %{"Count" => [1]} =
               Duck.query(conn, "INSERT INTO entries VALUES ($value)", %{"value" => 42})

      assert %{"value" => [42]} = Duck.query(conn, "SELECT value FROM entries", %{})
    end)
  end
end
