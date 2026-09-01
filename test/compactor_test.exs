defmodule W3.CompactorTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag :minio
  @moduletag :tmp_dir

  @user_agent "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 " <>
                "vscode/1.68.0-insider vscode-wakatime/18.1.5"

  setup do
    credentials = Help.s3_credentials(:minio)
    suffix = "#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    bucket = "w3-compact-#{suffix}"

    :ok = Help.create_bucket(credentials, bucket)
    request = Help.s3_req(credentials)
    duck = Help.start_duck(credentials)
    s3 = struct!(W3.S3, Map.put(credentials, :bucket, bucket))

    on_exit(fn ->
      delete_objects!(request, bucket)
      assert %{status: status} = Req.delete!(request, url: "s3://#{bucket}")
      assert status in 200..299
    end)

    {:ok, bucket: bucket, request: request, duck: duck, s3: s3}
  end

  test "writes one deterministic, time-sorted Parquet batch", %{
    bucket: bucket,
    request: request,
    duck: duck,
    s3: s3
  } do
    first_raw_key = "raw/first.ndjson.zst"
    second_raw_key = "raw/second\r.ndjson.zst"
    processed_key = processed_key([first_raw_key, second_raw_key])
    legacy_key = "v1/year=2025/heartbeats.parquet"

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

    late =
      heartbeat(
        time: unix_seconds(~U[2026-01-01 00:01:00Z]),
        entity: "synthetic/late.ex",
        project: "<<LAST_PROJECT>>",
        branch: "<<LAST_BRANCH>>",
        language: "<<LAST_LANGUAGE>>"
      )

    put_object!(request, bucket, legacy_key, "legacy stays untouched")
    put_raw!(request, bucket, first_raw_key, [late, first])
    put_raw!(request, bucket, second_raw_key, [new, first])

    raw_bytes =
      byte_size(object_body(request, bucket, first_raw_key)) +
        byte_size(object_body(request, bucket, second_raw_key))

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :start],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)
      end)

    assert_receive {[:w3, :compact, :start], ^telemetry_ref, _, %{bucket: ^bucket}}

    assert_receive {[:w3, :compact, :stop], ^telemetry_ref,
                    %{
                      duration: duration,
                      raw_files: 2,
                      raw_bytes: ^raw_bytes,
                      rows: 3,
                      parquet_bytes: parquet_bytes
                    }, %{bucket: ^bucket}}

    assert is_integer(duration)
    assert parquet_bytes > 0
    assert log =~ "heartbeat compaction complete"
    assert log =~ bucket

    assert object_status(request, bucket, first_raw_key) == 404
    assert object_status(request, bucket, second_raw_key) == 404
    assert object_body(request, bucket, legacy_key) == "legacy stays untouched"
    assert object_keys(request, bucket, "processed/") == [processed_key]

    parquet = "s3://#{bucket}/#{processed_key}"

    assert W3.Duck.query(
             duck,
             """
             SELECT entity, is_write, year(timezone('UTC', time)) AS year
             FROM read_parquet(
               $parquet,
               file_row_number = true,
               hive_partitioning = false
             )
             ORDER BY file_row_number
             """,
             %{"parquet" => parquet}
           ) == %{
             "entity" => ["synthetic/first.ex", "synthetic/new.ex", "synthetic/late.ex"],
             "is_write" => [false, true, false],
             "year" => [2025, 2025, 2026]
           }

    assert W3.Duck.query(
             duck,
             "DESCRIBE SELECT * FROM read_parquet('#{parquet}', hive_partitioning = false)"
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
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata('#{parquet}')
             CROSS JOIN parquet_file_metadata('#{parquet}')
             """
           ) == %{"format_version" => [2], "non_zstd" => [0]}

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
             FROM read_parquet('#{parquet}', hive_partitioning = false)
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

    # Replaying the same source snapshot overwrites its batch instead of duplicating it.
    put_raw!(request, bucket, first_raw_key, [late, first])
    put_raw!(request, bucket, second_raw_key, [new, first])

    assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)
    assert object_keys(request, bucket, "processed/") == [processed_key]
  end

  test "compacts a processed Parquet snapshot into one deduplicated file", %{
    bucket: bucket,
    request: request,
    duck: duck,
    s3: s3
  } do
    ignored_key = "processed/keep.txt"
    legacy_key = "v1/year=2025/heartbeats.parquet"

    put_object!(request, bucket, ignored_key, "ignored")
    put_object!(request, bucket, legacy_key, "legacy")

    assert :noop = W3.Compactor.compact_parquet_files_into_one(s3)

    duplicate =
      heartbeat(
        time: unix_seconds(~U[2025-06-15 12:00:00Z]),
        entity: "synthetic/duplicate.ex",
        is_write: false
      )

    variant = Map.put(duplicate, "is_write", true)

    put_raw!(request, bucket, "raw/first.ndjson.zst", [
      duplicate,
      heartbeat(
        time: unix_seconds(~U[2025-01-01 00:00:00Z]),
        entity: "synthetic/first.ex"
      )
    ])

    assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)

    put_raw!(request, bucket, "raw/second.ndjson.zst", [
      heartbeat(
        time: unix_seconds(~U[2025-12-31 23:59:59Z]),
        entity: "synthetic/last.ex"
      ),
      duplicate,
      variant
    ])

    assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)

    source_keys =
      request
      |> object_keys(bucket, "processed/")
      |> Enum.filter(&String.ends_with?(&1, ".parquet"))

    assert length(source_keys) == 2
    compacted_key = processed_key(source_keys)

    parquet_bytes =
      Enum.sum_by(source_keys, fn key -> byte_size(object_body(request, bucket, key)) end)

    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :compact, :start],
        [:w3, :compact, :stop]
      ])

    log =
      capture_log(fn ->
        assert :ok = W3.Compactor.compact_parquet_files_into_one(s3)
      end)

    assert_receive {[:w3, :compact, :start], ^telemetry_ref, _, %{bucket: ^bucket, job: :parquet}}

    assert_receive {[:w3, :compact, :stop], ^telemetry_ref,
                    %{
                      duration: duration,
                      parquet_files: 2,
                      parquet_bytes: ^parquet_bytes,
                      rows: 4,
                      output_bytes: output_bytes
                    }, %{bucket: ^bucket, job: :parquet}}

    assert is_integer(duration)
    assert output_bytes > 0
    assert log =~ "Parquet compaction complete"
    assert log =~ bucket

    Enum.each(source_keys, fn key -> assert object_status(request, bucket, key) == 404 end)

    assert object_keys(request, bucket, "processed/") == [compacted_key, ignored_key]
    assert object_body(request, bucket, ignored_key) == "ignored"
    assert object_body(request, bucket, legacy_key) == "legacy"

    parquet = "s3://#{bucket}/#{compacted_key}"

    assert W3.Duck.query(
             duck,
             """
             SELECT entity
             FROM read_parquet(
               $parquet,
               file_row_number = true,
               hive_partitioning = false
             )
             ORDER BY file_row_number
             """,
             %{"parquet" => parquet}
           ) == %{
             "entity" => [
               "synthetic/first.ex",
               "synthetic/duplicate.ex",
               "synthetic/duplicate.ex",
               "synthetic/last.ex"
             ]
           }

    assert W3.Duck.query(
             duck,
             """
             SELECT is_write
             FROM read_parquet($parquet, hive_partitioning = false)
             WHERE entity = 'synthetic/duplicate.ex'
             ORDER BY is_write
             """,
             %{"parquet" => parquet}
           ) == %{"is_write" => [false, true]}

    assert W3.Duck.query(
             duck,
             """
             SELECT
               min(format_version) AS format_version,
               count(*) FILTER (WHERE compression <> 'ZSTD') AS non_zstd
             FROM parquet_metadata($parquet)
             CROSS JOIN parquet_file_metadata($parquet)
             """,
             %{"parquet" => parquet}
           ) == %{"format_version" => [2], "non_zstd" => [0]}

    assert :noop = W3.Compactor.compact_parquet_files_into_one(s3)
    assert object_keys(request, bucket, "processed/") == [compacted_key, ignored_key]
  end

  test "preserves a processed Parquet snapshot when merging fails", %{
    bucket: bucket,
    request: request,
    s3: s3
  } do
    keys = ["processed/invalid-a.parquet", "processed/invalid-b.parquet"]
    Enum.each(keys, &put_object!(request, bucket, &1, "not parquet"))

    log =
      capture_log(fn ->
        assert_raise DuckNIF.Error, fn ->
          W3.Compactor.compact_parquet_files_into_one(s3)
        end
      end)

    assert log =~ "Parquet compaction failed"
    assert log =~ bucket
    assert object_keys(request, bucket, "processed/") == keys
  end

  test "unions evolving Parquet schemas by column name", %{tmp_dir: tmp_dir} do
    first_path = Path.join(tmp_dir, "first.parquet")
    second_path = Path.join(tmp_dir, "second.parquet")
    output_path = Path.join(tmp_dir, "output.parquet")

    W3.Duck.with_duck(fn conn ->
      W3.Duck.query(
        conn,
        """
        COPY (
          SELECT
            TIMESTAMPTZ '2025-02-01 00:00:00+00' AS time,
            'synthetic/late.ex'::VARCHAR AS entity,
            'synthetic'::VARCHAR AS project,
            'main'::VARCHAR AS branch,
            'Elixir'::VARCHAR AS language,
            1::BIGINT AS old_value
        ) TO #{W3.Duck.quote(first_path)} (FORMAT PARQUET)
        """
      )

      W3.Duck.query(
        conn,
        """
        COPY (
          SELECT
            'synthetic/early.ex'::VARCHAR AS entity,
            2::BIGINT AS new_value,
            TIMESTAMPTZ '2025-01-01 00:00:00+00' AS time,
            'synthetic'::VARCHAR AS project,
            'main'::VARCHAR AS branch,
            'Elixir'::VARCHAR AS language
        ) TO #{W3.Duck.quote(second_path)} (FORMAT PARQUET)
        """
      )
    end)

    assert 2 = W3.Compactor.copy_parquet_to_parquet([first_path, second_path], output_path)

    W3.Duck.with_duck(fn conn ->
      assert W3.Duck.query(
               conn,
               """
               SELECT entity, old_value, new_value
               FROM read_parquet(
                 #{W3.Duck.quote(output_path)},
                 file_row_number = true,
                 hive_partitioning = false
               )
               ORDER BY file_row_number
               """
             ) == %{
               "entity" => ["synthetic/early.ex", "synthetic/late.ex"],
               "old_value" => [nil, 1],
               "new_value" => [2, nil]
             }
    end)
  end

  test "resolves fallback fields and global heartbeat adjacency across rewrites", %{
    tmp_dir: tmp_dir
  } do
    initial_raw_path = Path.join(tmp_dir, "initial.ndjson.zst")
    initial_path = Path.join(tmp_dir, "initial.parquet")
    first_compacted_path = Path.join(tmp_dir, "first-compacted.parquet")
    late_raw_path = Path.join(tmp_dir, "late.ndjson.zst")
    late_path = Path.join(tmp_dir, "late.parquet")
    output_path = Path.join(tmp_dir, "output.parquet")
    initial_time = 1_700_000_000

    first =
      heartbeat(
        time: initial_time,
        entity: "synthetic/1.ex",
        machine_name: "machine-a",
        project: "alpha",
        branch: "main",
        language: "Elixir"
      )

    third =
      heartbeat(
        time: initial_time + 2,
        entity: "synthetic/3.ex",
        machine_name: "machine-a",
        project: "<<LAST_PROJECT>>",
        branch: "<<LAST_BRANCH>>",
        language: "<<LAST_LANGUAGE>>"
      )

    write_raw!(initial_raw_path, [third, first])
    assert 2 = W3.Compactor.copy_raw_to_parquet([initial_raw_path], initial_path)
    assert 2 = W3.Compactor.copy_parquet_to_parquet([initial_path], first_compacted_path)

    W3.Duck.with_duck(fn conn ->
      assert W3.Duck.query(
               conn,
               """
               SELECT
                 resolved_project,
                 epoch(previous_heartbeat_at)::BIGINT AS previous_epoch,
                 epoch(next_heartbeat_at)::BIGINT AS next_epoch
               FROM read_parquet(#{W3.Duck.quote(first_compacted_path)})
               WHERE entity = 'synthetic/3.ex'
               """
             ) == %{
               "next_epoch" => [nil],
               "previous_epoch" => [initial_time],
               "resolved_project" => ["alpha"]
             }
    end)

    second =
      heartbeat(
        time: initial_time + 1,
        entity: "synthetic/2-concrete.ex",
        machine_name: "machine-b",
        project: "beta",
        branch: "dev",
        language: "Rust"
      )

    same_time_fallback =
      heartbeat(
        time: initial_time + 1,
        entity: "synthetic/2-fallback.ex",
        machine_name: "machine-a",
        project: "<<LAST_PROJECT>>",
        branch: "<<LAST_BRANCH>>",
        language: "<<LAST_LANGUAGE>>"
      )

    fourth =
      heartbeat(
        time: initial_time + 3,
        entity: "synthetic/4.ex",
        machine_name: "machine-a",
        project: "alpha",
        branch: "<<LAST_BRANCH>>",
        language: "<<LAST_LANGUAGE>>"
      )

    write_raw!(late_raw_path, [fourth, same_time_fallback, second, first])
    assert 4 = W3.Compactor.copy_raw_to_parquet([late_raw_path], late_path)

    assert 5 =
             W3.Compactor.copy_parquet_to_parquet(
               [first_compacted_path, late_path],
               output_path
             )

    W3.Duck.with_duck(fn conn ->
      assert W3.Duck.query(
               conn,
               """
               SELECT
                 entity,
                 machine_name,
                 project,
                 branch,
                 language,
                 resolved_project,
                 resolved_branch,
                 resolved_language,
                 epoch(previous_heartbeat_at)::BIGINT AS previous_epoch,
                 epoch(next_heartbeat_at)::BIGINT AS next_epoch
               FROM read_parquet(
                 #{W3.Duck.quote(output_path)},
                 file_row_number = true,
                 hive_partitioning = false
               )
               ORDER BY file_row_number
               """
             ) == %{
               "entity" => [
                 "synthetic/1.ex",
                 "synthetic/2-concrete.ex",
                 "synthetic/2-fallback.ex",
                 "synthetic/3.ex",
                 "synthetic/4.ex"
               ],
               "machine_name" => [
                 "machine-a",
                 "machine-b",
                 "machine-a",
                 "machine-a",
                 "machine-a"
               ],
               "project" => [
                 "alpha",
                 "beta",
                 "<<LAST_PROJECT>>",
                 "<<LAST_PROJECT>>",
                 "alpha"
               ],
               "branch" => [
                 "main",
                 "dev",
                 "<<LAST_BRANCH>>",
                 "<<LAST_BRANCH>>",
                 "<<LAST_BRANCH>>"
               ],
               "language" => [
                 "Elixir",
                 "Rust",
                 "<<LAST_LANGUAGE>>",
                 "<<LAST_LANGUAGE>>",
                 "<<LAST_LANGUAGE>>"
               ],
               "resolved_project" => ["alpha", "beta", "alpha", "beta", "alpha"],
               "resolved_branch" => ["main", "dev", "main", "dev", "main"],
               "resolved_language" => ["Elixir", "Rust", "Elixir", "Rust", "Elixir"],
               "next_epoch" => [
                 initial_time + 1,
                 initial_time + 1,
                 initial_time + 2,
                 initial_time + 3,
                 nil
               ],
               "previous_epoch" => [
                 nil,
                 initial_time,
                 initial_time + 1,
                 initial_time + 1,
                 initial_time + 2
               ]
             }
    end)
  end

  test "uses 8,192-row groups ordered by time", %{tmp_dir: tmp_dir} do
    raw_path = Path.join(tmp_dir, "heartbeats.ndjson.zst")
    parquet_path = Path.join(tmp_dir, "heartbeats.parquet")
    initial_time = 1_700_000_000

    body =
      19_999..0//-1
      |> Enum.map(fn offset ->
        heartbeat(time: initial_time + offset, entity: "synthetic/#{offset}.ex")
      end)
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> :zstd.compress()

    File.write!(raw_path, body)
    W3.Compactor.copy_raw_to_parquet([raw_path], parquet_path)

    W3.Duck.with_duck(fn conn ->
      assert W3.Duck.query(
               conn,
               """
               SELECT row_group_num_rows AS rows
               FROM parquet_metadata('#{parquet_path}')
               WHERE path_in_schema = 'time'
               ORDER BY row_group_id
               """
             ) == %{"rows" => [8192, 8192, 3616]}

      assert W3.Duck.query(
               conn,
               """
               WITH ordered AS (
                 SELECT
                   time,
                   lag(time) OVER (ORDER BY file_row_number) AS previous_time
                 FROM read_parquet(
                   '#{parquet_path}',
                   file_row_number = true,
                   hive_partitioning = false
                 )
               )
               SELECT count(*) FILTER (WHERE time < previous_time) AS out_of_order
               FROM ordered
               """
             ) == %{"out_of_order" => [0]}
    end)
  end

  test "drops rows missing required fields and returns noop with no raw data", %{
    bucket: bucket,
    request: request,
    duck: duck,
    s3: s3
  } do
    raw_key = "raw/mixed.ndjson.zst"

    invalid =
      Enum.map(~w(time entity type machine_name), fn field ->
        heartbeat(entity: "synthetic/missing-#{field}.ex")
        |> Map.delete(field)
      end)

    put_raw!(request, bucket, raw_key, [heartbeat(entity: "synthetic/valid.ex") | invalid])

    assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)
    [processed_key] = object_keys(request, bucket, "processed/")

    assert W3.Duck.query(
             duck,
             "SELECT entity FROM read_parquet('s3://#{bucket}/#{processed_key}')"
           ) == %{"entity" => ["synthetic/valid.ex"]}

    assert :noop = W3.Compactor.compact_raw_files_into_parquet(s3)
    assert object_keys(request, bucket, "processed/") == [processed_key]

    all_invalid_key = "raw/all-invalid.ndjson.zst"
    put_raw!(request, bucket, all_invalid_key, invalid)

    assert :ok = W3.Compactor.compact_raw_files_into_parquet(s3)
    assert object_status(request, bucket, all_invalid_key) == 404
    assert object_keys(request, bucket, "processed/") == [processed_key]
  end

  test "preserves the raw snapshot when conversion fails", %{
    bucket: bucket,
    request: request,
    s3: s3
  } do
    invalid_key = "raw/invalid.ndjson.zst"
    valid_key = "raw/valid.ndjson.zst"

    invalid = heartbeat(entity: "synthetic/invalid.ex") |> Map.put("time", "not-a-number")

    put_raw!(request, bucket, invalid_key, [invalid])
    put_raw!(request, bucket, valid_key, [heartbeat(entity: "synthetic/valid.ex")])

    log =
      capture_log(fn ->
        assert_raise DuckNIF.Error, fn ->
          W3.Compactor.compact_raw_files_into_parquet(s3)
        end
      end)

    assert log =~ "heartbeat compaction failed"
    assert object_status(request, bucket, invalid_key) == 200
    assert object_status(request, bucket, valid_key) == 200
    assert object_keys(request, bucket, "processed/") == []
  end

  test "includes a failed S3 response body in the error", %{s3: s3} do
    missing_bucket = "#{s3.bucket}-missing"
    s3 = %{s3 | bucket: missing_bucket}

    log =
      capture_log(fn ->
        assert_raise RuntimeError,
                     ~r/requested URL returned error: 404.*Response body:.*NoSuchBucket/s,
                     fn ->
                       W3.Compactor.compact_raw_files_into_parquet(s3)
                     end
      end)

    assert log =~ "heartbeat compaction failed"
    assert log =~ missing_bucket
    assert log =~ "requested URL returned error: 404"
    assert log =~ "NoSuchBucket"
  end

  defp heartbeat(options) do
    defaults = %{
      "dependencies" => nil,
      "machine_name" => "synthetic-machine",
      "project" => "synthetic",
      "user_agent" => @user_agent
    }

    overrides = Map.new(options, fn {key, value} -> {to_string(key), value} end)

    Help.heartbeat()
    |> Map.merge(defaults)
    |> Map.merge(overrides)
  end

  defp put_raw!(request, bucket, key, heartbeats) do
    body =
      heartbeats
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> :zstd.compress()

    put_object!(request, bucket, key, body)
  end

  defp write_raw!(path, heartbeats) do
    body =
      heartbeats
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> :zstd.compress()

    File.write!(path, body)
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
    %{status: status, body: %{"ListBucketResult" => result}} =
      Req.get!(request,
        url: "s3://#{bucket}",
        params: %{"list-type" => "2", "prefix" => prefix}
      )

    assert status in 200..299

    result["Contents"]
    |> Kernel.||([])
    |> Enum.map(&Map.fetch!(&1, "Key"))
    |> Enum.sort()
  end

  defp delete_objects!(request, bucket) do
    for key <- object_keys(request, bucket, "") do
      assert %{status: status} = Req.delete!(request, url: object_url(bucket, key))
      assert status in 200..299
    end
  end

  defp processed_key(source_keys) do
    id =
      source_keys
      |> Enum.sort()
      |> JSON.encode_to_iodata!()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    "processed/#{id}.parquet"
  end

  defp object_url(bucket, key) do
    key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "s3://#{bucket}/#{key}"
  end

  defp unix_seconds(datetime), do: DateTime.to_unix(datetime, :microsecond) / 1_000_000
end
