defmodule W3.CompactorTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias W3.Compactor

  @user_agent "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 " <>
                "vscode/1.68.0-insider vscode-wakatime/18.1.5"

  @tag :minio
  test "writes one deterministic fragment per raw object without changing legacy parquet" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-#{suffix}"
    first_raw_key = "raw/first.ndjson.zst"
    second_raw_key = "raw/second.ndjson.zst"
    first_fragment_key = fragment_key(first_raw_key)
    second_fragment_key = fragment_key(second_raw_key)
    legacy_key = "v1/year=2025/heartbeats.parquet"

    :ok = Help.create_bucket(credentials, bucket)

    duck = Help.start_duck(credentials)
    request = Help.s3_req(credentials)

    on_exit(fn -> delete_objects!(request, bucket) end)

    seed_legacy!(duck, bucket)
    legacy_body = object_body(request, bucket, legacy_key)

    first =
      heartbeat(
        time: unix_seconds(~U[2025-06-15 12:00:00Z]),
        entity: "synthetic/first.ex",
        is_write: false
      )
      |> Map.merge(%{
        "ai_cached_input_tokens" => 2,
        "ai_input_tokens" => 3,
        "ai_line_changes" => 4,
        "ai_output_tokens" => 5,
        "ai_prompt_length" => 6,
        "ai_session" => "session-1",
        "ai_subscription_plan" => "plus",
        "human_line_changes" => 7,
        "project_root_count" => 8
      })

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

    put_raw!(request, bucket, first_raw_key, [first, late])
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
    assert object_body(request, bucket, legacy_key) == legacy_body

    assert object_keys(request, bucket, "v1/") ==
             Enum.sort([first_fragment_key, second_fragment_key, legacy_key])

    assert heartbeat_rows(duck, bucket, request) == %{
             "entity" => [
               "synthetic/existing.ex",
               "synthetic/first.ex",
               "synthetic/new.ex",
               "synthetic/new.ex",
               "synthetic/late.ex"
             ],
             "is_write" => [false, false, false, true, true],
             "year" => [2025, 2025, 2025, 2025, 2026]
           }

    first_fragment = "s3://#{bucket}/#{first_fragment_key}"

    assert Help.quack(
             duck,
             """
             SELECT
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata('#{first_fragment}')
             CROSS JOIN parquet_file_metadata('#{first_fragment}')
             """
           ) == %{"format_version" => [2], "non_zstd" => [0]}

    assert Help.quack(
             duck,
             "DESCRIBE SELECT * FROM read_parquet('#{first_fragment}', hive_partitioning = false)"
           )["column_name"] == [
             "time",
             "entity",
             "type",
             "category",
             "project",
             "branch",
             "language",
             "dependencies",
             "lines",
             "lineno",
             "cursorpos",
             "is_write",
             "machine_name",
             "user_agent",
             "ai_line_changes",
             "ai_cached_input_tokens",
             "ai_session",
             "ai_subscription_plan",
             "ai_input_tokens",
             "ai_output_tokens",
             "ai_prompt_length",
             "human_line_changes",
             "project_root_count",
             "year"
           ]

    assert Help.quack(
             duck,
             """
             SELECT
               ai_line_changes,
               ai_cached_input_tokens,
               ai_session,
               ai_subscription_plan,
               ai_input_tokens,
               ai_output_tokens,
               ai_prompt_length,
               human_line_changes,
               project_root_count
             FROM read_parquet('#{first_fragment}')
             WHERE entity = 'synthetic/first.ex'
             """
           ) == %{
             "ai_line_changes" => [4],
             "ai_cached_input_tokens" => [2],
             "ai_session" => ["session-1"],
             "ai_subscription_plan" => ["plus"],
             "ai_input_tokens" => [3],
             "ai_output_tokens" => [5],
             "ai_prompt_length" => [6],
             "human_line_changes" => [7],
             "project_root_count" => [8]
           }

    assert Help.quack(
             duck,
             "SELECT entity, year FROM read_parquet('#{first_fragment}') ORDER BY year"
           ) == %{
             "entity" => ["synthetic/first.ex", "synthetic/late.ex"],
             "year" => [2025, 2026]
           }

    # Replaying an object overwrites its deterministic fragment instead of creating another one.
    put_raw!(request, bucket, first_raw_key, [first, late])
    assert :ok = Compactor.compact!(config(credentials, bucket))
    assert object_status(request, bucket, first_raw_key) == 404

    assert object_keys(request, bucket, "v1/") ==
             Enum.sort([first_fragment_key, second_fragment_key, legacy_key])

    assert object_body(request, bucket, legacy_key) == legacy_body

    assert :ok = Compactor.compact!(config(credentials, bucket))
  end

  @tag :minio
  test "does not read legacy canonical parquet" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-ignore-canonical-#{suffix}"
    raw_key = "raw/ignores-canonical.ndjson.zst"
    legacy_key = "v1/year=1999/heartbeats.parquet"
    legacy_body = "intentionally not parquet"

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    duck = Help.start_duck(credentials)

    on_exit(fn -> delete_objects!(request, bucket) end)

    put_object!(request, bucket, legacy_key, legacy_body)
    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/raw-only.ex")])

    assert :ok = Compactor.compact!(config(credentials, bucket))
    assert object_status(request, bucket, raw_key) == 404
    assert object_body(request, bucket, legacy_key) == legacy_body

    assert fragment_rows(duck, bucket, request) == %{
             "entity" => ["synthetic/raw-only.ex"],
             "year" => [2022]
           }
  end

  @tag :minio
  test "drops raw heartbeats missing required fields" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-invalid-#{suffix}"
    raw_key = "raw/invalid.ndjson.zst"
    fragment_key = fragment_key(raw_key)

    invalid_heartbeats =
      Enum.map(~w(time entity type machine_name), fn field ->
        heartbeat(entity: "synthetic/invalid-#{field}.ex")
        |> Map.delete(field)
      end)

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    duck = Help.start_duck(credentials)
    put_raw!(request, bucket, raw_key, invalid_heartbeats)

    on_exit(fn -> delete_objects!(request, bucket) end)

    assert :ok = Compactor.compact!(config(credentials, bucket))
    assert object_status(request, bucket, raw_key) == 404
    assert object_status(request, bucket, fragment_key) == 404

    put_raw!(request, bucket, raw_key, [
      heartbeat(entity: "synthetic/valid.ex") | invalid_heartbeats
    ])

    assert :ok = Compactor.compact!(config(credentials, bucket))
    assert object_status(request, bucket, raw_key) == 404
    assert object_status(request, bucket, fragment_key) == 200

    assert fragment_rows(duck, bucket, request) == %{
             "entity" => ["synthetic/valid.ex"],
             "year" => [2022]
           }
  end

  @tag :minio
  test "runs on the configured interval" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-schedule-#{suffix}"
    raw_key = "raw/scheduled.ndjson.zst"
    fragment_key = fragment_key(raw_key)
    local_data_path = Path.join(data_path(), "w3-compact\\'#{suffix}")

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/scheduled.ex")])

    on_exit(fn ->
      delete_objects!(request, bucket)
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
    assert object_status(request, bucket, fragment_key) == 200
  end

  @tag :minio
  test "retries after a scheduled failure" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-restart-#{suffix}"
    raw_key = "raw/retry.ndjson.zst"
    fragment_key = fragment_key(raw_key)

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    put_raw!(request, bucket, raw_key, [invalid_heartbeat()])

    on_exit(fn -> delete_objects!(request, bucket) end)

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
             backoff: %{base: 1, max: 10},
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
    assert object_status(request, bucket, fragment_key) == 200
  end

  @tag :minio
  test "includes a failed S3 response in the error" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-missing-#{suffix}"
    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise MatchError, fn ->
          Compactor.compact!(config(credentials, bucket))
        end
      end)

    assert_receive {[:w3, :compact, :exception], ^telemetry_ref, _,
                    %{
                      bucket: ^bucket,
                      kind: :error,
                      reason:
                        {:badmatch, %Req.Response{status: 404, headers: headers, body: body}}
                    }}

    assert headers != %{}
    assert body != ""
    assert log =~ "status: 404"
    assert log =~ "NoSuchBucket"
  end

  @tag :minio
  test "a malformed object is preserved without blocking other raw objects" do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-failure-#{suffix}"
    raw_key = "raw/failure.ndjson.zst"
    valid_raw_key = "raw/valid.ndjson.zst"
    valid_fragment_key = fragment_key(valid_raw_key)
    local_data_path = Path.join(data_path(), "w3-compact-failure-local-#{suffix}")

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    duck = Help.start_duck(credentials)
    put_raw!(request, bucket, raw_key, [invalid_heartbeat()])
    put_raw!(request, bucket, valid_raw_key, [heartbeat(entity: "synthetic/valid.ex")])

    on_exit(fn ->
      delete_objects!(request, bucket)
      File.rm_rf(local_data_path)
    end)

    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise DuckNIF.Error, fn ->
          Compactor.compact!(config(credentials, bucket, local_data_path))
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
    assert object_status(request, bucket, valid_raw_key) == 404
    assert object_status(request, bucket, valid_fragment_key) == 200

    assert fragment_rows(duck, bucket, request) == %{
             "entity" => ["synthetic/valid.ex"],
             "year" => [2022]
           }

    refute File.exists?(Path.join(local_data_path, "w3-compactor"))
  end

  defp heartbeat_rows(duck, bucket, request) do
    legacy =
      request
      |> object_keys(bucket, "v1/year=")
      |> Enum.filter(&String.ends_with?(&1, "/heartbeats.parquet"))

    fragments = object_keys(request, bucket, "v1/fragments/raw-")

    relations =
      [
        parquet_relation(bucket, legacy, true),
        parquet_relation(bucket, fragments, false)
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join("\nUNION ALL BY NAME\n")

    Help.quack(
      duck,
      """
      SELECT year, entity, is_write
      FROM (#{relations})
      ORDER BY year, entity, is_write
      """
    )
  end

  defp fragment_rows(duck, bucket, request) do
    fragments = object_keys(request, bucket, "v1/fragments/raw-")

    Help.quack(
      duck,
      """
      SELECT entity, year
      FROM read_parquet(#{parquet_paths(bucket, fragments)}, hive_partitioning = false)
      ORDER BY year, entity
      """
    )
  end

  defp parquet_relation(_bucket, [], _hive_partitioning), do: nil

  defp parquet_relation(bucket, keys, hive_partitioning) do
    "SELECT * FROM read_parquet(#{parquet_paths(bucket, keys)}, " <>
      "hive_partitioning = #{hive_partitioning}, union_by_name = true)"
  end

  defp parquet_paths(bucket, keys) do
    paths = Enum.map_join(keys, ", ", &sql_quote("s3://#{bucket}/#{&1}"))
    "[#{paths}]"
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
          'ignored'::VARCHAR AS legacy_extra
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
    |> Map.put("user_agent", @user_agent)
  end

  defp invalid_heartbeat do
    heartbeat(entity: "synthetic/failure.ex")
    |> Map.put("time", "not-a-number")
  end

  defp put_raw!(request, bucket, key, heartbeats) do
    body =
      heartbeats
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> :zstd.compress()

    assert %{status: status} = Req.put!(request, url: "s3://#{bucket}/#{key}", body: body)
    assert status in 200..299
  end

  defp put_object!(request, bucket, key, body) do
    assert %{status: status} = Req.put!(request, url: "s3://#{bucket}/#{key}", body: body)
    assert status in 200..299
  end

  defp object_body(request, bucket, key) do
    assert %{status: status, body: body} =
             Req.get!(request, url: "s3://#{bucket}/#{key}", raw: true)

    assert status in 200..299
    IO.iodata_to_binary(body)
  end

  defp object_status(request, bucket, key) do
    Req.head!(request, url: "s3://#{bucket}/#{key}").status
  end

  defp object_keys(request, bucket, prefix) do
    response =
      Req.get!(request,
        url: "s3://#{bucket}",
        params: %{"list-type" => "2", "prefix" => prefix}
      )

    assert response.status in 200..299

    contents = response.body["ListBucketResult"]["Contents"] || []

    contents
    |> Enum.map(&Map.fetch!(&1, "Key"))
    |> Enum.sort()
  end

  defp delete_objects!(request, bucket) do
    for key <- object_keys(request, bucket, "") do
      assert %{status: status} = Req.delete!(request, url: "s3://#{bucket}/#{key}")
      assert status in 200..299
    end
  end

  defp fragment_key(raw_key) do
    source_id = :sha256 |> :crypto.hash(raw_key) |> Base.encode16(case: :lower)
    "v1/fragments/raw-#{source_id}.parquet"
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"

  defp unix_seconds(datetime), do: DateTime.to_unix(datetime, :microsecond) / 1_000_000
  defp config(credentials, bucket), do: config(credentials, bucket, data_path())

  defp config(credentials, bucket, local_data_path) do
    %{s3: store(credentials, bucket), data_path: local_data_path}
  end

  defp data_path, do: System.get_env("DATA_PATH", System.tmp_dir!())
  defp store(credentials, bucket), do: Map.put(credentials, :bucket, bucket)
end
