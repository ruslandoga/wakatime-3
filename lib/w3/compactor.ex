defmodule W3.Compactor do
  @moduledoc """
  Converts pending raw heartbeat batches into flat, time-sorted Parquet files.
  """

  alias W3.{S3, Duck}

  @upload_chunk_size 1024 * 1024

  def compact_raw_files_into_parquet(s3) do
    metadata = %{bucket: s3.bucket}

    :telemetry.span([:w3, :compact], metadata, fn ->
      raw_keys =
        s3
        |> S3.list_objects("raw/")
        |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))
        |> Enum.sort()

      result =
        if raw_keys == [] do
          :noop
        else
          compact_raw_files_into_parquet(s3, raw_keys)
        end

      {result, metadata}
    end)
  end

  defp compact_raw_files_into_parquet(s3, raw_keys) do
    base_req = S3.base_req(s3)

    raw_paths =
      raw_keys
      |> Enum.map(fn key ->
        # Plug deletes files when their owner exits, so the parent task owns each download.
        %{key: key, path: Plug.Upload.random_file!("raw")}
      end)
      |> W3.async_map!(
        fn %{key: key, path: path} ->
          response =
            Req.get!(base_req,
              url: S3.object_url(s3, key),
              into: File.stream!(path, [:delayed_write]),
              raw: true
            )

          unless response.status == 200 do
            raise "failed to download #{key}: HTTP #{response.status}"
          end

          path
        end,
        ordered: false,
        timeout: to_timeout(second: 30),
        max_concurrency: System.schedulers_online() * 4
      )

    parquet_path = Plug.Upload.random_file!("parquet")
    row_count = copy_raw_to_parquet(raw_paths, parquet_path)

    if row_count > 0 do
      parquet_size = File.stat!(parquet_path).size
      processed_key = processed_key(raw_keys)

      response =
        Req.put!(
          base_req,
          url: S3.object_url(s3, processed_key),
          body: File.stream!(parquet_path, @upload_chunk_size, read_ahead: @upload_chunk_size),
          headers: %{
            "content-length" => Integer.to_string(parquet_size),
            "content-type" => "application/vnd.apache.parquet"
          }
        )

      unless response.status in 200..299 do
        raise "failed to upload #{processed_key}: HTTP #{response.status}"
      end
    end

    S3.delete_objects!(s3, raw_keys)

    :ok
  end

  defp processed_key(raw_keys) do
    id =
      raw_keys
      |> Enum.sort()
      |> JSON.encode_to_iodata!()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    "processed/batch-#{id}.parquet"
  end

  def copy_raw_to_parquet(raw_paths, parquet_path) do
    Duck.with_duck(fn conn ->
      Duck.query(conn, "SET temp_directory = #{Duck.quote(Path.dirname(parquet_path))}")

      %{"Count" => [row_count]} =
        Duck.query(
          conn,
          """
          COPY (
          WITH raw_input AS (
            SELECT *
            FROM read_ndjson(
              from_json(CAST($raw_paths AS VARCHAR), '["VARCHAR"]'),
              columns = {
                time: 'DOUBLE',
                entity: 'VARCHAR',
                type: 'VARCHAR',
                category: 'VARCHAR',
                project: 'VARCHAR',
                branch: 'VARCHAR',
                language: 'VARCHAR',
                dependencies: 'VARCHAR[]',
                lines: 'BIGINT',
                lineno: 'BIGINT',
                cursorpos: 'BIGINT',
                is_write: 'BOOLEAN',
                machine_name: 'VARCHAR',
                user_agent: 'VARCHAR',
                ai_line_changes: 'BIGINT',
                ai_cached_input_tokens: 'BIGINT',
                ai_session: 'VARCHAR',
                ai_subscription_plan: 'VARCHAR',
                ai_input_tokens: 'BIGINT',
                ai_output_tokens: 'BIGINT',
                ai_prompt_length: 'BIGINT',
                human_line_changes: 'BIGINT',
                project_root_count: 'BIGINT'
              },
              compression = 'zstd',
              format = 'newline_delimited',
              union_by_name = true
            )
          ), event AS (
            SELECT
              to_timestamp(time)::TIMESTAMPTZ AS time,
              entity::VARCHAR AS entity,
              type::VARCHAR AS type,
              category::VARCHAR AS category,
              project::VARCHAR AS project,
              branch::VARCHAR AS branch,
              language::VARCHAR AS language,
              dependencies::VARCHAR[] AS dependencies,
              lines::BIGINT AS lines,
              lineno::BIGINT AS lineno,
              cursorpos::BIGINT AS cursorpos,
              coalesce(is_write::BOOLEAN, false) AS is_write,
              machine_name::VARCHAR AS machine_name,
              max(nullif(user_agent, '')) AS user_agent,
              ai_line_changes::BIGINT AS ai_line_changes,
              ai_cached_input_tokens::BIGINT AS ai_cached_input_tokens,
              ai_session::VARCHAR AS ai_session,
              ai_subscription_plan::VARCHAR AS ai_subscription_plan,
              ai_input_tokens::BIGINT AS ai_input_tokens,
              ai_output_tokens::BIGINT AS ai_output_tokens,
              ai_prompt_length::BIGINT AS ai_prompt_length,
              human_line_changes::BIGINT AS human_line_changes,
              project_root_count::BIGINT AS project_root_count
            FROM raw_input
            WHERE time IS NOT NULL
              AND entity IS NOT NULL
              AND type IS NOT NULL
              AND machine_name IS NOT NULL
            GROUP BY ALL
          )
          SELECT *
          FROM event
          ORDER BY time, entity
          ) TO #{Duck.quote(parquet_path)} (
          FORMAT PARQUET,
          COMPRESSION ZSTD,
          PARQUET_VERSION V2,
          ROW_GROUP_SIZE 8192
          )
          """,
          %{"raw_paths" => JSON.encode!(raw_paths)}
        )

      row_count
    end)
  end
end
