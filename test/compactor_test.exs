defmodule W3.CompactorTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

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

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :start],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        assert :ok = Compactor.compact!(config(credentials, bucket))
      end)

    assert_receive {[:w3, :compact, :start], ^telemetry_ref,
                    %{monotonic_time: monotonic_time, system_time: system_time},
                    %{bucket: ^bucket}}

    assert_receive {[:w3, :compact, :stop], ^telemetry_ref, %{duration: duration},
                    %{bucket: ^bucket}}

    assert is_integer(monotonic_time)
    assert is_integer(system_time)
    assert is_integer(duration)
    assert log =~ "heartbeat compaction complete"
    assert log =~ "#{System.convert_time_unit(duration, :native, :millisecond)}ms"
    assert log =~ bucket

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
    assert :ok = Compactor.compact!(config(credentials, bucket))
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

    assert :ok = Compactor.compact!(config(credentials, bucket))

    refute File.exists?(Path.join(data_path(), "w3-compactor"))
  end

  @tag :minio
  test "runs on the configured interval" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-schedule-#{suffix}"
    raw_key = "raw/scheduled.ndjson.zst"
    canonical_key = "v1/year=2022/heartbeats.parquet"
    local_data_path = Path.join(data_path(), "w3-compact\\'#{suffix}")

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/scheduled.ex")])

    on_exit(fn ->
      Req.delete(request, url: "s3://#{bucket}/#{raw_key}")
      Req.delete(request, url: "s3://#{bucket}/#{canonical_key}")
      File.rm_rf(local_data_path)
    end)

    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :stop]])

    pid =
      start_supervised!(
        {Compactor,
         s3: store(credentials, bucket),
         data_path: local_data_path,
         interval: to_timeout(millisecond: 20)}
      )

    assert_receive {[:w3, :compact, :stop], ^telemetry_ref, %{duration: first_duration},
                    %{bucket: ^bucket}},
                   1_000

    assert_receive {[:w3, :compact, :stop], ^telemetry_ref, %{duration: second_duration},
                    %{bucket: ^bucket}},
                   1_000

    assert first_duration >= 0
    assert second_duration >= 0
    assert Process.alive?(pid)
    assert object_status(request, bucket, raw_key) == 404
    assert object_status(request, bucket, canonical_key) == 200
  end

  @tag :minio
  test "retries after a scheduled failure" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-restart-#{suffix}"
    raw_key = "raw/retry.ndjson.zst"
    canonical_key = "v1/year=2022/heartbeats.parquet"

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    put_raw!(request, bucket, raw_key, [%{"entity" => "synthetic/failure.ex"}])

    on_exit(fn ->
      Req.delete(request, url: "s3://#{bucket}/#{raw_key}")
      Req.delete(request, url: "s3://#{bucket}/#{canonical_key}")
    end)

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :exception],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        pid =
          start_supervised!(
            {Compactor,
             s3: store(credentials, bucket),
             data_path: data_path(),
             interval: to_timeout(millisecond: 100)}
          )

        assert_receive {[:w3, :compact, :exception], ^telemetry_ref, %{duration: _},
                        %{bucket: ^bucket, reason: %DuckNIF.Error{}}},
                       1_000

        assert Process.alive?(pid)

        put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/recovered.ex")])

        assert_receive {[:w3, :compact, :stop], ^telemetry_ref, %{duration: _},
                        %{bucket: ^bucket}},
                       1_000

        assert Process.alive?(pid)
      end)

    assert log =~ "heartbeat compaction failed"
    assert log =~ "heartbeat compaction complete"
    assert object_status(request, bucket, raw_key) == 404
    assert object_status(request, bucket, canonical_key) == 200
  end

  @tag :minio
  test "includes a failed S3 response in the error" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-missing-#{suffix}"
    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise ErlangError, fn ->
          Compactor.compact!(config(credentials, bucket))
        end
      end)

    assert_receive {[:w3, :compact, :exception], ^telemetry_ref, _,
                    %{
                      bucket: ^bucket,
                      kind: :error,
                      reason: {:s3_response, %{status: 404, headers: headers, body: body}}
                    }}

    assert headers != %{}
    assert body != ""
    assert log =~ ":s3_response"
    assert log =~ "status: 404"
    assert log =~ "NoSuchBucket"
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

    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise DuckNIF.Error, fn ->
          Compactor.compact!(config(credentials, bucket))
        end
      end)

    assert_receive {[:w3, :compact, :exception], ^telemetry_ref, %{duration: duration},
                    %{
                      bucket: ^bucket,
                      kind: :error,
                      reason: %DuckNIF.Error{} = reason,
                      stacktrace: stacktrace
                    }}

    assert is_integer(duration)
    assert is_list(stacktrace)
    assert log =~ "heartbeat compaction failed"
    assert log =~ "#{System.convert_time_unit(duration, :native, :millisecond)}ms"
    assert log =~ bucket
    assert log =~ Exception.message(reason)
    assert object_status(request, bucket, raw_key) == 200

    refute File.exists?(Path.join(data_path(), "w3-compactor"))
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
  defp config(credentials, bucket), do: %{s3: store(credentials, bucket), data_path: data_path()}
  defp data_path, do: System.get_env("DATA_PATH", System.tmp_dir!())
  defp store(credentials, bucket), do: Map.put(credentials, :bucket, bucket)
end
