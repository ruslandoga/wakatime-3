defmodule W3.Compactor do
  @moduledoc """
  TODO
  """

  alias W3.{S3, Duck}

  def compact_raw_files_into_parquet(s3) do
    raw_keys =
      s3
      |> S3.list_objects("raw/")
      |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))

    if raw_keys == [] do
      :noop
    else
      compact_raw_files_into_parquet(s3, raw_keys)
    end
  end

  defp compact_raw_files_into_parquet(s3, raw_keys) do
    base_req = S3.base_req(s3)

    raw_paths =
      W3.async_map!(
        raw_keys,
        fn key ->
          %{status: 200, body: body} =
            Req.get!(base_req, url: "s3://#{s3.bucket}/#{key}", raw: true)

          tmp_path = Plug.Upload.random_file!("tmp-raw-download")
          File.write!(tmp_path, body)
          tmp_path
        end,
        ordered: false,
        timeout: to_timeout(second: 30),
        max_concurrency: System.schedulers_online() * 4
      )

    parquet_path = Plug.Upload.random_file!("tmp-raw-parquet")
    copy_raw_to_parquet(raw_paths, parquet_path)

    %{status: 200} =
      Req.put!(base_req,
        url: "s3://#{s3.bucket}/processed/#{Path.basename(parquet_path)}",
        headers: %{"content-type" => "application/vnd.apache.parquet"},
        body: File.read!(parquet_path)
      )

    S3.delete_objects!(s3, raw_keys)

    :ok
  end

  def copy_raw_to_parquet(raw_paths, parquet_path) do
    Duck.with_duck(fn conn ->
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
          ORDER BY year, time, entity
        ) TO #{Duck.quote(parquet_path)} (
          FORMAT PARQUET,
          COMPRESSION ZSTD,
          PARQUET_VERSION V2
          ROW_GROUP_SIZE 8192
        )
        """,
        %{"raw_paths" => JSON.encode!(raw_paths)}
      )
    end)
  end
end
