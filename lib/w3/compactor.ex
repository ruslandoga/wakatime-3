defmodule W3.Compactor do
  @moduledoc """
  Converts pending raw heartbeat batches into flat, time-sorted Parquet files
  and periodically consolidates those Parquet files.
  """

  alias W3.{S3, Duck}

  @upload_chunk_size 1024 * 1024

  def compact_raw_files_into_parquet(s3) do
    metadata = %{bucket: s3.bucket, job: :raw}

    :telemetry.span([:w3, :compact], metadata, fn ->
      raw_keys =
        s3
        |> S3.list_objects("raw/")
        |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))
        |> Enum.sort()

      {result, measurements} =
        if raw_keys == [] do
          {:noop, %{raw_files: 0, raw_bytes: 0, rows: 0, parquet_bytes: 0}}
        else
          {:ok, compact_raw_files_into_parquet(s3, raw_keys)}
        end

      {result, measurements, metadata}
    end)
  end

  def compact_parquet_files_into_one(s3) do
    metadata = %{bucket: s3.bucket, job: :parquet}

    :telemetry.span([:w3, :compact], metadata, fn ->
      parquet_keys =
        s3
        |> S3.list_objects("processed/")
        |> Enum.filter(&String.ends_with?(&1, ".parquet"))
        |> Enum.sort()

      {result, measurements} =
        if length(parquet_keys) < 2 do
          {:noop,
           %{parquet_files: length(parquet_keys), parquet_bytes: 0, rows: 0, output_bytes: 0}}
        else
          {:ok, compact_parquet_files_into_one(s3, parquet_keys)}
        end

      {result, measurements, metadata}
    end)
  end

  defp compact_raw_files_into_parquet(s3, raw_keys) do
    base_req = S3.base_req(s3)
    raw_paths = download_objects(base_req, s3, raw_keys, "raw", to_timeout(second: 30))

    raw_bytes = Enum.sum_by(raw_paths, &File.stat!(&1).size)
    parquet_path = Plug.Upload.random_file!("parquet")
    row_count = copy_raw_to_parquet(raw_paths, parquet_path)

    parquet_bytes =
      if row_count > 0 do
        parquet_bytes = File.stat!(parquet_path).size
        processed_key = processed_key(raw_keys)

        upload_parquet!(base_req, s3, processed_key, parquet_path, parquet_bytes)

        parquet_bytes
      else
        0
      end

    S3.delete_objects!(s3, raw_keys)

    %{
      raw_files: length(raw_keys),
      raw_bytes: raw_bytes,
      rows: row_count,
      parquet_bytes: parquet_bytes
    }
  end

  defp compact_parquet_files_into_one(s3, parquet_keys) do
    base_req = S3.base_req(s3)
    parquet_paths = download_objects(base_req, s3, parquet_keys, "parquet", :infinity)
    parquet_bytes = Enum.sum_by(parquet_paths, &File.stat!(&1).size)
    output_path = Plug.Upload.random_file!("parquet")
    row_count = copy_parquet_to_parquet(parquet_paths, output_path)
    output_bytes = File.stat!(output_path).size
    output_key = processed_key(parquet_keys)

    upload_parquet!(base_req, s3, output_key, output_path, output_bytes)
    S3.delete_objects!(s3, parquet_keys)

    %{
      parquet_files: length(parquet_keys),
      parquet_bytes: parquet_bytes,
      rows: row_count,
      output_bytes: output_bytes
    }
  end

  defp download_objects(base_req, s3, keys, name, timeout) do
    keys
    |> Enum.map(fn key ->
      # Plug deletes files when their owner exits, so the parent task owns each download.
      %{key: key, path: Plug.Upload.random_file!(name)}
    end)
    |> W3.async_map!(
      fn %{key: key, path: path} = object ->
        %{status: 200} =
          Req.get!(base_req,
            url: S3.object_url(s3, key),
            into: File.stream!(path, [:delayed_write]),
            raw: true
          )

        object
      end,
      ordered: false,
      timeout: timeout,
      max_concurrency: System.schedulers_online() * 4
    )
    |> Enum.sort_by(& &1.key)
    |> Enum.map(& &1.path)
  end

  defp upload_parquet!(base_req, s3, key, path, bytes) do
    %{status: 200} =
      Req.put!(
        base_req,
        url: S3.object_url(s3, key),
        body: File.stream!(path, @upload_chunk_size, read_ahead: @upload_chunk_size),
        headers: %{
          "content-length" => Integer.to_string(bytes),
          "content-type" => "application/vnd.apache.parquet"
        }
      )
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

  def copy_parquet_to_parquet(parquet_paths, output_path) do
    Duck.with_duck(fn conn ->
      Duck.query(conn, "SET temp_directory = #{Duck.quote(Path.dirname(output_path))}")

      %{"Count" => [row_count]} =
        Duck.query(
          conn,
          """
          COPY (
            SELECT DISTINCT *
            FROM read_parquet(
              from_json(CAST($parquet_paths AS VARCHAR), '["VARCHAR"]'),
              hive_partitioning = false,
              union_by_name = true
            )
            ORDER BY time, entity
          ) TO #{Duck.quote(output_path)} (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            PARQUET_VERSION V2,
            ROW_GROUP_SIZE 8192
          )
          """,
          %{"parquet_paths" => JSON.encode!(parquet_paths)}
        )

      row_count
    end)
  end
end
