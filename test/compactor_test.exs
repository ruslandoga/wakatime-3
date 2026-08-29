defmodule W3.CompactorTest do
  use ExUnit.Case, async: true

  alias W3.Compactor

  @user_agent "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 " <>
                "vscode/1.68.0-insider vscode-wakatime/18.1.5"

  @tag :minio
  test "merges a raw snapshot locally and deletes exactly that snapshot" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-#{suffix}"
    first_raw_key = "raw/first.ndjson.zst"
    second_raw_key = "raw/second.ndjson.zst"
    late_raw_key = "raw/late.ndjson.zst"

    :ok = Help.create_bucket(credentials, bucket)

    duck = Help.start_duck(credentials)
    request = Help.s3_req(credentials)

    on_exit(fn ->
      for key <- [
            first_raw_key,
            second_raw_key,
            late_raw_key,
            "v1/year=2025/heartbeats.parquet",
            "v1/year=2026/heartbeats.parquet"
          ] do
        Req.delete(request, url: "s3://#{bucket}/#{key}")
      end
    end)

    seed_legacy!(duck, bucket)

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

    late =
      heartbeat(
        time: unix_seconds(~U[2026-01-01 00:01:00Z]),
        entity: "synthetic/late.ex",
        is_write: true
      )

    put_raw!(request, bucket, first_raw_key, [existing, new])
    put_raw!(request, bucket, second_raw_key, [new, variant])

    compactor = start_supervised!({Compactor, store(credentials, bucket)})
    _ = :sys.get_state(compactor)

    assert object_status(request, bucket, first_raw_key) == 404
    assert object_status(request, bucket, second_raw_key) == 404

    canonical_glob = "s3://#{bucket}/v1/year=*/heartbeats.parquet"
    _ = Help.quack(duck, "SET TimeZone = 'Europe/Moscow'")

    assert canonical_rows(duck, canonical_glob) == %{
             "entity" => ["synthetic/existing.ex", "synthetic/new.ex", "synthetic/new.ex"],
             "is_write" => [false, false, true],
             "timezone" => ["Europe/Moscow", "Europe/Moscow", "Europe/Moscow"],
             "year" => [2025, 2025, 2025]
           }

    canonical_2025 = "s3://#{bucket}/v1/year=2025/heartbeats.parquet"

    assert Help.quack(
             duck,
             """
             SELECT
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata('#{canonical_2025}')
             CROSS JOIN parquet_file_metadata('#{canonical_2025}')
             """
           ) == %{"format_version" => [2], "non_zstd" => [0]}

    put_raw!(request, bucket, late_raw_key, [late])
    assert :ok = Compactor.run!(store(credentials, bucket))
    assert object_status(request, bucket, late_raw_key) == 404

    assert canonical_rows(duck, canonical_glob) == %{
             "entity" => [
               "synthetic/existing.ex",
               "synthetic/new.ex",
               "synthetic/new.ex",
               "synthetic/late.ex"
             ],
             "is_write" => [false, false, true, true],
             "timezone" => [
               "Europe/Moscow",
               "Europe/Moscow",
               "Europe/Moscow",
               "Europe/Moscow"
             ],
             "year" => [2025, 2025, 2025, 2026]
           }

    assert :ok = Compactor.run!(store(credentials, bucket))

    refute File.exists?(Path.join(System.get_env("DATA_PATH", System.tmp_dir!()), "w3-compactor"))
  end

  @tag :minio
  test "a failed local merge closes DuckDB and preserves raw" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-failure-#{suffix}"
    raw_key = "raw/failure.ndjson.zst"

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    put_raw!(request, bucket, raw_key, [%{"entity" => "synthetic/failure.ex"}])

    on_exit(fn -> Req.delete(request, url: "s3://#{bucket}/#{raw_key}") end)

    compactor = start_supervised!({Compactor, store(credentials, bucket)})
    _ = :sys.get_state(compactor)

    assert Process.alive?(compactor)
    assert object_status(request, bucket, raw_key) == 200

    refute File.exists?(Path.join(System.get_env("DATA_PATH", System.tmp_dir!()), "w3-compactor"))
  end

  defp canonical_rows(duck, glob) do
    Help.quack(
      duck,
      """
      SELECT year, entity, is_write, timezone
      FROM read_parquet('#{glob}', hive_partitioning = true, union_by_name = true)
      ORDER BY year, entity, is_write
      """
    )
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

  defp object_status(request, bucket, key) do
    Req.head!(request, url: "s3://#{bucket}/#{key}").status
  end

  defp unix_seconds(datetime), do: DateTime.to_unix(datetime, :microsecond) / 1_000_000
  defp store(credentials, bucket), do: Map.put(credentials, :bucket, bucket)
end
