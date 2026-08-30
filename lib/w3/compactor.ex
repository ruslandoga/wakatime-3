defmodule W3.Compactor do
  @moduledoc """
  Converts raw heartbeat objects into deterministic Parquet fragments.

  Existing Parquet is deliberately not read on this hot path. Object I/O is
  handled by Req, while DuckDB only sees temporary local files.
  """

  def start_link(options) do
    s3 = options |> Keyword.fetch!(:s3) |> Map.new()
    data_path = Keyword.fetch!(options, :data_path)
    interval = Keyword.get(options, :interval, to_timeout(second: 60))

    backoff =
      Keyword.get(
        options,
        :backoff,
        %{base: to_timeout(second: 1), max: to_timeout(second: 60)}
      )

    task = {__MODULE__, :compact!, [%{s3: s3, data_path: data_path}]}

    W3.Periodic.start_link({interval, backoff, task})
  end

  def child_spec(options) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [options]}}
  end

  def compact!(%{s3: s3, data_path: data_path}) do
    metadata = %{bucket: s3.bucket}

    :telemetry.span([:w3, :compact], metadata, fn ->
      {compact_snapshot!(s3, data_path), metadata}
    end)
  end

  defp compact_snapshot!(s3, data_path) do
    directory = Path.join(data_path, "w3-compactor")
    File.rm_rf!(directory)

    raw_keys =
      s3
      |> list_keys("raw/")
      |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))

    if raw_keys == [] do
      :ok
    else
      compact!(s3, raw_keys, directory)
    end
  end

  defp compact!(s3, raw_keys, directory) do
    raw_directory = Path.join(directory, "raw")
    output_directory = Path.join(directory, "output")

    Enum.each([raw_directory, output_directory], &File.mkdir_p!/1)

    try do
      raw_files = download!(s3, raw_keys, raw_directory, ".ndjson.zst")
      {conversions, failures} = convert(raw_files, output_directory)

      async_map!(conversions, &commit!(s3, &1))
      raise_first!(failures)
    after
      File.rm_rf!(directory)
    end
  end

  defp convert(raw_files, output_directory) do
    W3.Duck.with_duck(fn conn ->
      raw_files
      |> Enum.reduce({[], []}, fn raw_file, {conversions, failures} ->
        try do
          conversion = convert_one!(conn, raw_file, output_directory)
          {[conversion | conversions], failures}
        catch
          kind, reason -> {conversions, [{kind, reason, __STACKTRACE__} | failures]}
        end
      end)
      |> then(fn {conversions, failures} ->
        {Enum.reverse(conversions), Enum.reverse(failures)}
      end)
    end)
  end

  defp convert_one!(conn, %{key: raw_key, path: raw_file}, output_directory) do
    source_id = source_id(raw_key)
    filename = "raw-#{source_id}.parquet"
    path = Path.join(output_directory, filename)

    output =
      if copy_raw!(conn, raw_file, path) == 0 do
        nil
      else
        %{key: "v1/fragments/#{filename}", path: path}
      end

    %{raw_key: raw_key, output: output}
  end

  defp copy_raw!(conn, raw_file, output) do
    result =
      W3.Duck.query(
        conn,
        """
        COPY (
          WITH raw_input AS (
            SELECT *
            FROM read_ndjson(
              from_json(CAST($raw_files AS VARCHAR), '["VARCHAR"]'),
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
                timezone: 'VARCHAR',
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
              nullif(regexp_extract(user_agent, '(vscode/[^ ]+)', 1), '')::VARCHAR AS editor,
              nullif(
                regexp_extract(user_agent, '^wakatime/[^ ]+ [(]?([^ )]+)', 1),
                ''
              )::VARCHAR AS operating_system,
              machine_name::VARCHAR AS machine_name,
              max(nullif(timezone, '')) AS timezone,
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
        ) TO #{sql_quote(output)} (
          FORMAT PARQUET,
          COMPRESSION ZSTD,
          PARQUET_VERSION V2,
          ROW_GROUP_SIZE 122880
        );
        """,
        %{"raw_files" => JSON.encode!([raw_file])}
      )

    case for(%{"Count" => counts} <- result, count <- counts, do: count) do
      [] -> raise "DuckDB COPY did not return a row count: #{inspect(result)}"
      counts -> Enum.sum(counts)
    end
  end

  defp req(s3) do
    Req.new(retry: :transient)
    |> ReqS3.attach(
      aws_sigv4: [
        region: s3.region,
        access_key_id: s3.access_key_id,
        secret_access_key: s3.secret_access_key
      ],
      aws_endpoint_url_s3: s3.endpoint_url
    )
  end

  defp list_keys(s3, prefix) do
    list_keys(req(s3), s3.bucket, prefix, nil, [])
  end

  defp list_keys(req, bucket, prefix, continuation_token, pages) do
    params = %{"list-type" => "2", "prefix" => prefix}

    params =
      if continuation_token do
        Map.put(params, "continuation-token", continuation_token)
      else
        params
      end

    response = Req.get!(req, url: "s3://#{bucket}", params: params) |> success!()
    result = response.body["ListBucketResult"]
    page = Enum.map(result["Contents"] || [], &Map.fetch!(&1, "Key"))
    pages = [page | pages]

    if result["IsTruncated"] == "true" do
      list_keys(req, bucket, prefix, Map.fetch!(result, "NextContinuationToken"), pages)
    else
      pages |> Enum.reverse() |> List.flatten()
    end
  end

  defp download!(s3, keys, directory, extension) do
    req = req(s3)

    async_map!(
      Enum.with_index(keys),
      fn {key, index} ->
        response = Req.get!(req, url: "s3://#{s3.bucket}/#{key}", raw: true) |> success!()
        path = Path.join(directory, "#{index}#{extension}")
        File.write!(path, response.body)
        %{key: key, path: path}
      end
    )
  end

  defp upload!(s3, %{key: key, path: path}) do
    Req.put!(req(s3),
      url: "s3://#{s3.bucket}/#{key}",
      headers: %{"content-type" => "application/vnd.apache.parquet"},
      body: File.read!(path)
    )
    |> success!()

    :ok
  end

  defp commit!(s3, %{raw_key: raw_key, output: output}) do
    if output, do: upload!(s3, output)
    delete!(s3, raw_key)
  end

  defp delete!(s3, key) do
    Req.delete!(req(s3), url: "s3://#{s3.bucket}/#{key}") |> success!()
    :ok
  end

  defp success!(%{status: status} = response) when status in 200..299, do: response

  defp success!(response) do
    :erlang.error({:s3_response, Map.take(response, [:status, :headers, :body])})
  end

  defp async_map!(enumerable, fun) do
    W3.TaskSupervisor
    |> Task.Supervisor.async_stream(
      enumerable,
      fun,
      ordered: false,
      timeout: to_timeout(second: 60)
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp raise_first!([]), do: :ok

  defp raise_first!([{kind, reason, stacktrace} | _failures]) do
    :erlang.raise(kind, reason, stacktrace)
  end

  defp source_id(raw_key) do
    :sha256
    |> :crypto.hash(raw_key)
    |> Base.encode16(case: :lower)
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"
end
