defmodule W3 do
  @moduledoc false
  alias W3.{S3, Duck}

  def config do
    Application.get_all_env(:w3)
  end

  def compact_raw_files_into_parquet(s3, data_path) do
    raw_keys =
      s3
      |> S3.ls("raw/")
      |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))

    if raw_keys == [] do
      :noop
    else
      compact_raw_files_into_parquet(s3, raw_keys, data_path)
    end
  end

  defp compact_raw_files_into_parquet(s3, raw_keys, data_path) do
    raw_directory = Path.join(data_path, "raw")
    output_directory = Path.join(data_path, "output")
    Enum.each([raw_directory, output_directory], &File.mkdir_p!/1)

    try do
      S3.download(s3, raw_keys, raw_directory)
      outputs = copy_raw_to_parquet!(raw_directory, output_directory)
      S3.upload(s3, outputs)
      S3.delete(s3, raw_keys)

      :ok
    after
      File.rm_rf!(raw_directory)
      File.rm_rf!(output_directory)
    end
  end

  def async_map!(enumerable, fun, options \\ []) do
    defaults = [ordered: false, timeout: to_timeout(second: 60)]
    options = Keyword.merge(defaults, options)

    W3.TaskSupervisor
    |> Task.Supervisor.async_stream(enumerable, fun, options)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  def copy_raw_to_parquet(raw_directory, output_directory) do
    W3.Duck.with_duck(fn conn ->
      W3.Duck.query(
        conn,
        """
        COPY (
          WITH raw_input AS (
            SELECT *
            FROM read_ndjson(
              from_json($raw_directory),
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
              project_root_count::BIGINT AS project_root_count,
              year(timezone('UTC', to_timestamp(time)))::INTEGER AS year
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
        ) TO #{sql_quote(output_directory)} (
          FORMAT PARQUET,
          PARTITION_BY (year),
          COMPRESSION ZSTD,
          PARQUET_VERSION V2
        );
        """,
        %{"raw_directory" => raw_directory}
      )
    end)

    output_directory
    |> Path.join("**/*.parquet")
    |> Path.wildcard()
    |> Enum.map(fn path ->
      key = Path.relative_to(path, output_directory)
      %{key: key, path: path, type: "application/vnd.apache.parquet"}
    end)
  end
end
