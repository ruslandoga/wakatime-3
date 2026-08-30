defmodule W3.CompactorTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag :minio
  @moduletag :tmp_dir

  @user_agent "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 " <>
                "vscode/1.68.0-insider vscode-wakatime/18.1.5"

  setup_all do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-#{suffix}"

    :ok = Help.create_bucket(credentials, bucket)

    {:ok, credentials: credentials, bucket: bucket}
  end

  setup %{credentials: credentials, bucket: bucket} do
    request = Help.s3_req(credentials)
    duck = Help.start_duck(credentials)

    on_exit(fn -> delete_objects!(request, bucket) end)

    {:ok, request: request, duck: duck}
  end

  test "writes deterministic year parts for each raw object without changing legacy parquet", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    duck: duck,
    tmp_dir: tmp_dir
  } do
    first_raw_key = "raw/first.ndjson.zst"
    second_raw_key = "raw/second\r.ndjson.zst"
    first_2025_fragment_key = fragment_key(first_raw_key, 2025)
    first_2026_fragment_key = fragment_key(first_raw_key, 2026)
    second_fragment_key = fragment_key(second_raw_key, 2025)
    legacy_key = "v1/year=2025/heartbeats.parquet"

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
      |> Map.merge(%{
        "project" => "<<LAST_PROJECT>>",
        "branch" => "<<LAST_BRANCH>>",
        "language" => "<<LAST_LANGUAGE>>"
      })

    put_raw!(request, bucket, first_raw_key, [first, late])
    put_raw!(request, bucket, second_raw_key, [new, variant])

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :start],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
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
             Enum.sort([
               first_2025_fragment_key,
               first_2026_fragment_key,
               second_fragment_key,
               legacy_key
             ])

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

    first_fragment = "s3://#{bucket}/#{first_2025_fragment_key}"

    assert W3.Duck.query(
             duck,
             """
             SELECT
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata('#{first_fragment}')
             CROSS JOIN parquet_file_metadata('#{first_fragment}')
             """
           ) == %{"format_version" => [2], "non_zstd" => [0]}

    assert W3.Duck.query(
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
             "project_root_count"
           ]

    assert W3.Duck.query(
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

    assert W3.Duck.query(
             duck,
             """
             SELECT entity, year
             FROM read_parquet(
               #{parquet_paths(bucket, [first_2025_fragment_key, first_2026_fragment_key])},
               hive_partitioning = true
             )
             ORDER BY year
             """
           ) == %{
             "entity" => ["synthetic/first.ex", "synthetic/late.ex"],
             "year" => [2025, 2026]
           }

    assert W3.Duck.query(
             duck,
             """
             SELECT project, branch, language
             FROM read_parquet('s3://#{bucket}/#{first_2026_fragment_key}')
             """
           ) == %{
             "project" => ["<<LAST_PROJECT>>"],
             "branch" => ["<<LAST_BRANCH>>"],
             "language" => ["<<LAST_LANGUAGE>>"]
           }

    # Replaying an object overwrites its deterministic year parts instead of creating new ones.
    put_raw!(request, bucket, first_raw_key, [first, late])
    assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
    assert object_status(request, bucket, first_raw_key) == 404

    assert object_keys(request, bucket, "v1/") ==
             Enum.sort([
               first_2025_fragment_key,
               first_2026_fragment_key,
               second_fragment_key,
               legacy_key
             ])

    assert object_body(request, bucket, legacy_key) == legacy_body

    assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
  end

  test "does not read legacy canonical parquet", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    duck: duck,
    tmp_dir: tmp_dir
  } do
    raw_key = "raw/ignores-canonical.ndjson.zst"
    legacy_key = "v1/year=1999/heartbeats.parquet"
    legacy_body = "intentionally not parquet"

    put_object!(request, bucket, legacy_key, legacy_body)
    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/raw-only.ex")])

    assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
    assert object_status(request, bucket, raw_key) == 404
    assert object_body(request, bucket, legacy_key) == legacy_body

    assert fragment_rows(duck, bucket, request) == %{
             "entity" => ["synthetic/raw-only.ex"],
             "year" => [2022]
           }
  end

  test "drops raw heartbeats missing required fields", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    duck: duck,
    tmp_dir: tmp_dir
  } do
    invalid_raw_key = "raw/invalid.ndjson.zst"
    valid_raw_key = "raw/valid.ndjson.zst"
    invalid_fragment_key = fragment_key(invalid_raw_key, 2022)
    valid_fragment_key = fragment_key(valid_raw_key, 2022)

    invalid_heartbeats =
      Enum.map(~w(time entity type machine_name), fn field ->
        heartbeat(entity: "synthetic/invalid-#{field}.ex")
        |> Map.delete(field)
      end)

    put_raw!(request, bucket, invalid_raw_key, invalid_heartbeats)

    assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
    assert object_status(request, bucket, invalid_raw_key) == 404
    assert object_status(request, bucket, invalid_fragment_key) == 404

    put_raw!(request, bucket, valid_raw_key, [
      heartbeat(entity: "synthetic/valid.ex") | invalid_heartbeats
    ])

    assert :ok = W3.compact!(config(credentials, bucket, tmp_dir))
    assert object_status(request, bucket, valid_raw_key) == 404
    assert object_status(request, bucket, valid_fragment_key) == 200

    assert fragment_rows(duck, bucket, request) == %{
             "entity" => ["synthetic/valid.ex"],
             "year" => [2022]
           }
  end

  test "runs on the configured interval", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    tmp_dir: tmp_dir
  } do
    raw_key = "raw/scheduled.ndjson.zst"
    fragment_key = fragment_key(raw_key, 2022)
    local_data_path = Path.join(tmp_dir, "w3-compact\\'data")

    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/scheduled.ex")])

    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :stop]])

    pid =
      start_compactor(
        config(credentials, bucket, local_data_path),
        interval: to_timeout(millisecond: 20)
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

  test "retries after a scheduled failure", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    tmp_dir: tmp_dir
  } do
    raw_key = "raw/retry.ndjson.zst"
    fragment_key = fragment_key(raw_key, 2022)

    put_raw!(request, bucket, raw_key, [invalid_heartbeat()])

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :exception],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        pid =
          start_compactor(
            config(credentials, bucket, tmp_dir),
            backoff: %{base: 1, max: 10},
            interval: to_timeout(millisecond: 100)
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

  test "includes a failed S3 response in the error", %{
    credentials: credentials,
    bucket: bucket,
    tmp_dir: tmp_dir
  } do
    bucket = "#{bucket}-missing"
    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise MatchError, fn ->
          W3.compact!(config(credentials, bucket, tmp_dir))
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

  test "a malformed object preserves the whole raw batch for retry", %{
    credentials: credentials,
    bucket: bucket,
    request: request,
    tmp_dir: tmp_dir
  } do
    raw_key = "raw/failure.ndjson.zst"
    valid_raw_key = "raw/valid.ndjson.zst"
    valid_fragment_key = fragment_key(valid_raw_key, 2022)

    put_raw!(request, bucket, raw_key, [invalid_heartbeat()])
    put_raw!(request, bucket, valid_raw_key, [heartbeat(entity: "synthetic/valid.ex")])

    telemetry_ref = Help.attach_telemetry([[:w3, :compact, :exception]])

    log =
      capture_log(fn ->
        assert_raise DuckNIF.Error, fn ->
          W3.compact!(config(credentials, bucket, tmp_dir))
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
    assert object_status(request, bucket, valid_raw_key) == 200
    assert object_status(request, bucket, valid_fragment_key) == 404
    assert raw_fragment_keys(request, bucket) == []

    refute File.exists?(Path.join(tmp_dir, "w3-compactor"))
  end

  defp heartbeat_rows(duck, bucket, request) do
    parts = parquet_part_keys(request, bucket)

    W3.Duck.query(
      duck,
      """
      SELECT year, entity, is_write
      FROM read_parquet(
        #{parquet_paths(bucket, parts)},
        hive_partitioning = true,
        union_by_name = true
      )
      ORDER BY year, entity, is_write
      """
    )
  end

  defp fragment_rows(duck, bucket, request) do
    fragments = raw_fragment_keys(request, bucket)

    W3.Duck.query(
      duck,
      """
      SELECT entity, year
      FROM read_parquet(#{parquet_paths(bucket, fragments)}, hive_partitioning = true)
      ORDER BY year, entity
      """
    )
  end

  defp parquet_part_keys(request, bucket) do
    request
    |> object_keys(bucket, "v1/year=")
    |> Enum.filter(fn key ->
      filename = Path.basename(key)

      filename == "heartbeats.parquet" or
        (String.starts_with?(filename, "raw-") and String.ends_with?(filename, ".parquet"))
    end)
  end

  defp raw_fragment_keys(request, bucket) do
    request
    |> parquet_part_keys(bucket)
    |> Enum.filter(&(Path.basename(&1) |> String.starts_with?("raw-")))
  end

  defp parquet_paths(bucket, keys) do
    paths = Enum.map_join(keys, ", ", &sql_quote("s3://#{bucket}/#{&1}"))
    "[#{paths}]"
  end

  defp seed_legacy!(duck, bucket) do
    W3.Duck.query(
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

    assert %{status: status} = Req.put!(request, url: object_url(bucket, key), body: body)
    assert status in 200..299
  end

  defp put_object!(request, bucket, key, body) do
    assert %{status: status} = Req.put!(request, url: object_url(bucket, key), body: body)
    assert status in 200..299
  end

  defp object_body(request, bucket, key) do
    assert %{status: status, body: body} =
             Req.get!(request, url: object_url(bucket, key), raw: true)

    assert status in 200..299
    IO.iodata_to_binary(body)
  end

  defp object_status(request, bucket, key) do
    Req.head!(request, url: object_url(bucket, key)).status
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
      assert %{status: status} = Req.delete!(request, url: object_url(bucket, key))
      assert status in 200..299
    end
  end

  defp fragment_key(raw_key, year) do
    source_id = source_id(raw_key)
    "v1/year=#{year}/raw-#{source_id}.parquet"
  end

  defp object_url(bucket, key) do
    key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "s3://#{bucket}/#{key}"
  end

  defp source_id(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower)
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"

  defp unix_seconds(datetime), do: DateTime.to_unix(datetime, :microsecond) / 1_000_000

  defp start_compactor(config, options) do
    options =
      options
      |> Keyword.put_new(:backoff, %{base: 1, max: 1})
      |> Keyword.put(:task, fn -> W3.compact!(config) end)

    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [options]}
    })
  end

  defp config(credentials, bucket, local_data_path) do
    %{s3: Map.put(credentials, :bucket, bucket), data_path: local_data_path}
  end
end
