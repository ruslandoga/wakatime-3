defmodule W3.CompactorTest do
  use ExUnit.Case, async: true

  alias W3.Compactor

  @user_agent "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 " <>
                "vscode/1.68.0-insider vscode-wakatime/18.1.5"

  @tag :minio
  test "one DuckDB query appends only new canonical events" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    raw_bucket = "w3-compact-raw-#{suffix}"
    canonical_bucket = "w3-compact-canonical-#{suffix}"
    first_raw_key = "raw/first.ndjson.zst"
    second_raw_key = "raw/second.ndjson.zst"

    :ok = Help.create_bucket(credentials, raw_bucket)
    :ok = Help.create_bucket(credentials, canonical_bucket)

    duck = Help.start_duck(credentials)
    request = Help.s3_req(credentials)

    on_exit(fn ->
      files =
        Help.quack(
          duck,
          "SELECT file FROM glob('s3://#{canonical_bucket}/v1/year=*/*.parquet')"
        )["file"]

      Enum.each(files, &Req.delete(request, url: &1))
      Req.delete(request, url: "s3://#{raw_bucket}/#{first_raw_key}")
      Req.delete(request, url: "s3://#{raw_bucket}/#{second_raw_key}")
    end)

    seed_legacy!(duck, canonical_bucket)

    existing =
      heartbeat(
        time: unix_seconds(~U[2025-06-15 12:00:00Z]),
        entity: "synthetic/existing.ex",
        is_write: false
      )

    new =
      heartbeat(
        time: unix_seconds(~U[2025-12-31 23:30:00Z]),
        entity: "synthetic/new.ex",
        is_write: true
      )

    variant = Map.put(new, "is_write", false)

    put_raw!(request, raw_bucket, first_raw_key, [existing, new])
    put_raw!(request, raw_bucket, second_raw_key, [new, variant])

    assert :ok =
             Compactor.run!(store(credentials, raw_bucket), store(credentials, canonical_bucket))

    canonical_glob = "s3://#{canonical_bucket}/v1/year=*/*.parquet"
    _ = Help.quack(duck, "SET TimeZone = 'Europe/Moscow'")

    assert Help.quack(
             duck,
             """
             SELECT year, entity, is_write, timezone, user_agent
             FROM read_parquet(
               '#{canonical_glob}',
               hive_partitioning = true,
               union_by_name = true
             )
             ORDER BY year, entity, is_write
             """
           ) == %{
             "entity" => ["synthetic/existing.ex", "synthetic/new.ex", "synthetic/new.ex"],
             "is_write" => [false, false, true],
             "timezone" => [nil, "Europe/Moscow", "Europe/Moscow"],
             "user_agent" => [nil, @user_agent, @user_agent],
             "year" => [2025, 2025, 2025]
           }

    files_before =
      Help.quack(duck, "SELECT file FROM glob('#{canonical_glob}') ORDER BY file")["file"]

    assert length(files_before) == 2
    assert Enum.any?(files_before, &String.ends_with?(&1, "/year=2025/heartbeats.parquet"))

    assert Enum.any?(files_before, fn file ->
             String.match?(file, ~r{/year=2025/heartbeats_[0-9a-f-]+\.parquet$})
           end)

    new_file = Enum.find(files_before, &String.contains?(&1, "/heartbeats_"))

    assert Help.quack(
             duck,
             """
             SELECT
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata('#{new_file}')
             CROSS JOIN parquet_file_metadata('#{new_file}')
             """
           ) == %{"format_version" => [2], "non_zstd" => [0]}

    assert :ok =
             Compactor.run!(store(credentials, raw_bucket), store(credentials, canonical_bucket))

    assert Help.quack(duck, "SELECT file FROM glob('#{canonical_glob}') ORDER BY file")["file"] ==
             files_before
  end

  test "the database and connection close after every run" do
    assert :ok =
             Compactor.run!(
               fake_store("raw"),
               fake_store("canonical"),
               W3.CompactorTest.Adapter
             )

    assert_receive :open
    assert_receive :connect
    assert_receive {:query, sql}
    assert length(Regex.scan(~r/\bCOPY \(/, sql)) == 1
    assert_receive :destroy_result
    assert_receive :disconnect
    assert_receive :close
  end

  test "the database and connection also close when DuckDB raises" do
    assert_raise RuntimeError, "query failed", fn ->
      Compactor.run!(
        fake_store("raw"),
        fake_store("canonical"),
        W3.CompactorTest.FailingAdapter
      )
    end

    assert_receive :open
    assert_receive :connect
    assert_receive :disconnect
    assert_receive :close
  end

  defmodule Adapter do
    def open do
      send(self(), :open)
      :database
    end

    def connect(:database) do
      send(self(), :connect)
      :connection
    end

    def query_dirty_io(:connection, sql) do
      send(self(), {:query, sql})
      :result
    end

    def destroy_result(:result) do
      send(self(), :destroy_result)
      :ok
    end

    def disconnect(:connection) do
      send(self(), :disconnect)
      :ok
    end

    def close(:database) do
      send(self(), :close)
      :ok
    end
  end

  defmodule FailingAdapter do
    def open do
      send(self(), :open)
      :database
    end

    def connect(:database) do
      send(self(), :connect)
      :connection
    end

    def query_dirty_io(:connection, _sql), do: raise("query failed")

    def disconnect(:connection) do
      send(self(), :disconnect)
      :ok
    end

    def close(:database) do
      send(self(), :close)
      :ok
    end
  end

  defp seed_legacy!(duck, bucket) do
    Help.quack(
      duck,
      """
      COPY (
        SELECT
          TIMESTAMPTZ '2025-06-15 12:00:00+00' AS time,
          'synthetic/existing.ex'::VARCHAR AS entity,
          'file'::VARCHAR AS type,
          'coding'::VARCHAR AS category,
          'synthetic'::VARCHAR AS project,
          'main'::VARCHAR AS branch,
          'Elixir'::VARCHAR AS language,
          NULL::VARCHAR[] AS dependencies,
          4::BIGINT AS lines,
          1::BIGINT AS lineno,
          1::BIGINT AS cursorpos,
          false::BOOLEAN AS is_write,
          'vscode/1.68.0-insider'::VARCHAR AS editor,
          'darwin-21.4.0-arm64'::VARCHAR AS operating_system,
          'synthetic-machine'::VARCHAR AS machine_name,
          NULL::VARCHAR AS timezone,
          NULL::VARCHAR AS user_agent
      ) TO 's3://#{bucket}/v1/year=2025/heartbeats.parquet' (
        FORMAT PARQUET,
        PARQUET_VERSION V2,
        COMPRESSION ZSTD
      )
      """
    )
  end

  defp heartbeat(options) do
    Help.heartbeat(options)
    |> Map.put("project", "synthetic")
    |> Map.put("dependencies", nil)
    |> Map.put("machine_name", "synthetic-machine")
    |> Map.put("timezone", "Europe/Moscow")
    |> Map.put("user_agent", @user_agent)
  end

  defp put_raw!(request, bucket, key, heartbeats) do
    body =
      heartbeats
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> :zstd.compress()

    assert %{status: status} = Req.put!(request, url: "s3://#{bucket}/#{key}", body: body)
    assert status in 200..299
  end

  defp unix_seconds(datetime), do: DateTime.to_unix(datetime, :microsecond) / 1_000_000

  defp store(credentials, bucket), do: Map.put(credentials, :bucket, bucket)

  defp fake_store(bucket) do
    %{
      bucket: bucket,
      region: "auto",
      endpoint_url: "https://example.com",
      access_key_id: "access",
      secret_access_key: "secret"
    }
  end
end
