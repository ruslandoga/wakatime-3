defmodule W3.Compactor do
  @moduledoc """
  Merges a snapshot of raw heartbeats into the annual Parquet files.

  Object I/O is handled by Req. DuckDB only sees temporary local files, and
  its database and connection exist only while the local merge is running.
  """

  def start_link(options) do
    s3 = options |> Keyword.fetch!(:s3) |> Map.new()
    data_path = Keyword.fetch!(options, :data_path)
    interval = Keyword.get(options, :interval, to_timeout(second: 60))

    backoff =
      Keyword.get(
        options,
        :backoff,
        W3.Backoff.new(base: to_timeout(second: 1), max: to_timeout(second: 60))
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
    canonical_directory = Path.join(directory, "canonical")
    output_directory = Path.join(directory, "output")

    Enum.each([raw_directory, canonical_directory, output_directory], &File.mkdir_p!/1)

    try do
      raw_files = download!(s3, raw_keys, raw_directory, ".ndjson.zst")

      canonical_keys =
        s3
        |> list_keys("v1/year=")
        |> Enum.filter(&String.ends_with?(&1, "/heartbeats.parquet"))

      canonical_files =
        download!(s3, canonical_keys, canonical_directory, ".parquet")

      query!(raw_files, canonical_files, output_directory)

      outputs =
        for partition <- File.ls!(output_directory),
            directory = Path.join(output_directory, partition),
            File.dir?(directory),
            file <- File.ls!(directory),
            String.ends_with?(file, ".parquet") do
          Path.join(directory, file)
        end

      Enum.each(outputs, &upload!(s3, &1))
      Enum.each(raw_keys, &delete!(s3, &1))
      :ok
    after
      File.rm_rf!(directory)
    end
  end

  defp query!([], _canonical_files, _output_directory), do: :ok

  defp query!(raw_files, canonical_files, output_directory) do
    database = DuckNIF.open()

    try do
      connection = DuckNIF.connect(database)

      try do
        # The pinned DuckNIF's direct query error path releases its result before cleanup.
        statement = DuckNIF.prepare(connection, sql(canonical_files, output_directory))

        try do
          bind_files(statement, "raw_files", raw_files)

          if canonical_files != [] do
            bind_files(statement, "canonical_files", canonical_files)
          end

          result = DuckNIF.execute_prepared_dirty_io(statement)

          try do
            :ok
          after
            :ok = DuckNIF.destroy_result(result)
          end
        after
          :ok = DuckNIF.destroy_prepare(statement)
        end
      after
        :ok = DuckNIF.disconnect(connection)
      end
    after
      :ok = DuckNIF.close(database)
    end
  end

  defp bind_files(statement, name, files) do
    index = DuckNIF.bind_parameter_index(statement, name)
    :ok = DuckNIF.bind_varchar(statement, index, JSON.encode!(files))
  end

  defp sql(canonical_files, output_directory) do
    existing =
      if canonical_files == [] do
        ""
      else
        """
        SELECT *
        FROM read_parquet(
          from_json(CAST($canonical_files AS VARCHAR), '["VARCHAR"]'),
          union_by_name = true
        )
        UNION ALL BY NAME
        """
      end

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
            user_agent: 'VARCHAR'
          },
          compression = 'zstd',
          format = 'newline_delimited',
          union_by_name = true
        )
      ), raw AS (
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
          timezone::VARCHAR AS timezone,
          user_agent::VARCHAR AS user_agent
        FROM raw_input
      ), event AS (
        #{existing}
        SELECT *
        FROM raw
      )
      SELECT
        event.time,
        event.entity,
        event.type,
        event.category,
        event.project,
        event.branch,
        event.language,
        event.dependencies,
        event.lines,
        event.lineno,
        event.cursorpos,
        event.is_write,
        event.editor,
        event.operating_system,
        event.machine_name,
        max(nullif(event.timezone, '')) AS timezone,
        max(nullif(event.user_agent, '')) AS user_agent,
        year(timezone('UTC', event.time))::INTEGER AS year
      FROM event
      WHERE event.time IS NOT NULL
        AND event.entity IS NOT NULL
        AND event.type IS NOT NULL
        AND event.machine_name IS NOT NULL
      GROUP BY ALL
      ORDER BY year, time, entity
    ) TO #{sql_quote(output_directory)} (
      FORMAT PARQUET,
      PARTITION_BY (year),
      COMPRESSION ZSTD,
      PARQUET_VERSION V2
    );
    """
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

    W3.TaskSupervisor
    |> Task.Supervisor.async_stream_nolink(
      Enum.with_index(keys),
      fn {key, index} ->
        try do
          response = Req.get!(req, url: "s3://#{s3.bucket}/#{key}", raw: true) |> success!()
          path = Path.join(directory, "#{index}#{extension}")
          File.write!(path, response.body)
          {:ok, path}
        catch
          kind, reason -> {:error, kind, reason, __STACKTRACE__}
        end
      end,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, {:ok, path}} -> path
      {:ok, {:error, kind, reason, stacktrace}} -> :erlang.raise(kind, reason, stacktrace)
      {:exit, reason} -> exit(reason)
    end)
  end

  defp upload!(s3, path) do
    "year=" <> year = path |> Path.dirname() |> Path.basename()

    Req.put!(req(s3),
      url: "s3://#{s3.bucket}/v1/year=#{year}/heartbeats.parquet",
      headers: %{"content-type" => "application/vnd.apache.parquet"},
      body: File.read!(path)
    )
    |> success!()

    :ok
  end

  defp delete!(s3, key) do
    Req.delete!(req(s3), url: "s3://#{s3.bucket}/#{key}") |> success!()
    :ok
  end

  defp success!(%{status: status} = response) when status in 200..299, do: response

  defp success!(response) do
    :erlang.error({:s3_response, Map.take(response, [:status, :headers, :body])})
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"
end
