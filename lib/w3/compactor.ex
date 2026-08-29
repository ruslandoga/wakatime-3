defmodule W3.Compactor do
  use GenServer

  @moduledoc """
  Merges a snapshot of raw heartbeats into the annual Parquet files.

  Object I/O is handled by Req. DuckDB only sees temporary local files, and
  its database and connection exist only while the local merge is running.
  """

  require Logger

  def start_link(s3) do
    GenServer.start_link(__MODULE__, s3, name: __MODULE__)
  end

  @impl true
  def init(s3) do
    send(self(), :compact)
    {:ok, s3}
  end

  @impl true
  def handle_info(:compact, s3) do
    try do
      run!(s3)
      Logger.info("heartbeat compaction complete")
    rescue
      exception -> Logger.error("heartbeat compaction failed: #{Exception.message(exception)}")
    after
      Process.send_after(self(), :compact, :timer.hours(24))
    end

    {:noreply, s3}
  end

  def run!(s3) do
    s3 = store!(s3)
    request = request(s3)
    directory = Path.join(System.get_env("DATA_PATH", System.tmp_dir!()), "w3-compactor")
    File.rm_rf!(directory)

    raw_keys =
      request
      |> list_keys(s3.bucket, "raw/")
      |> Enum.filter(&String.ends_with?(&1, ".ndjson.zst"))

    if raw_keys == [] do
      :ok
    else
      compact!(request, s3.bucket, raw_keys, directory)
    end
  end

  defp compact!(request, bucket, raw_keys, directory) do
    raw_directory = Path.join(directory, "raw")
    canonical_directory = Path.join(directory, "canonical")
    output_directory = Path.join(directory, "output")

    Enum.each([raw_directory, canonical_directory, output_directory], &File.mkdir_p!/1)

    try do
      download!(request, bucket, raw_keys, raw_directory, ".ndjson.zst")

      canonical_keys =
        request
        |> list_keys(bucket, "v1/year=")
        |> Enum.filter(&String.ends_with?(&1, "/heartbeats.parquet"))

      download!(request, bucket, canonical_keys, canonical_directory, ".parquet")
      query!(sql(raw_directory, canonical_directory, output_directory, canonical_keys))

      outputs = Path.wildcard(Path.join(output_directory, "year=*/*.parquet"))

      if outputs == [] do
        raise "compaction produced no Parquet files"
      end

      Enum.each(outputs, &upload!(request, bucket, &1))
      Enum.each(raw_keys, &delete!(request, bucket, &1))
      :ok
    after
      File.rm_rf!(directory)
    end
  end

  defp query!(sql) do
    database = DuckNIF.open()

    try do
      connection = DuckNIF.connect(database)

      try do
        result = DuckNIF.query_dirty_io(connection, sql)

        try do
          :ok
        after
          :ok = DuckNIF.destroy_result(result)
        end
      after
        :ok = DuckNIF.disconnect(connection)
      end
    after
      :ok = DuckNIF.close(database)
    end
  end

  defp sql(raw_directory, canonical_directory, output_directory, canonical_keys) do
    existing =
      if canonical_keys == [] do
        ""
      else
        """
        SELECT *
        FROM read_parquet(
          #{sql_quote(Path.join(canonical_directory, "*.parquet"))},
          union_by_name = true
        )
        UNION ALL
        """
      end

    """
    SET TimeZone = 'UTC';

    COPY (
      WITH raw_input AS (
        SELECT *
        FROM read_ndjson(
          #{sql_quote(Path.join(raw_directory, "*.ndjson.zst"))},
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
          list_extract(
            list_filter(
              string_split(trim(user_agent), ' '),
              lambda token: starts_with(token, 'vscode/')
            ),
            1
          )::VARCHAR AS editor,
          CASE
            WHEN starts_with(string_split(trim(user_agent), ' ')[1], 'wakatime/') THEN
              nullif(
                replace(
                  replace(string_split(trim(user_agent), ' ')[2], '(', ''),
                  ')',
                  ''
                ),
                ''
              )
          END::VARCHAR AS operating_system,
          machine_name::VARCHAR AS machine_name,
          timezone::VARCHAR AS timezone,
          user_agent::VARCHAR AS user_agent
        FROM raw_input
        WHERE CASE
          WHEN time IS NULL OR entity IS NULL OR type IS NULL OR machine_name IS NULL THEN
            error('heartbeat is missing a required field')
          ELSE true
        END
      ), event AS (
        #{existing}
        SELECT *
        FROM raw
      )
      SELECT
        event.*,
        year(timezone('UTC', event.time))::INTEGER AS year
      FROM event
      QUALIFY row_number() OVER (
        PARTITION BY
          time,
          entity,
          type,
          category,
          project,
          branch,
          language,
          dependencies,
          lines,
          lineno,
          cursorpos,
          is_write,
          editor,
          operating_system,
          machine_name
        ORDER BY
          (nullif(timezone, '') IS NOT NULL)::INTEGER +
            (nullif(user_agent, '') IS NOT NULL)::INTEGER DESC,
          timezone DESC NULLS LAST,
          user_agent DESC NULLS LAST
      ) = 1
      ORDER BY year, time, entity
    ) TO #{sql_quote(output_directory)} (
      FORMAT PARQUET,
      PARTITION_BY (year),
      FILENAME_PATTERN 'heartbeats',
      COMPRESSION ZSTD,
      COMPRESSION_LEVEL 3,
      PARQUET_VERSION V2,
      ROW_GROUP_SIZE 122880
    );
    """
  end

  defp request(s3) do
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

  defp list_keys(request, bucket, prefix) do
    list_keys(request, bucket, prefix, nil, [])
  end

  defp list_keys(request, bucket, prefix, continuation_token, pages) do
    params = %{"list-type" => "2", "prefix" => prefix}

    params =
      if continuation_token do
        Map.put(params, "continuation-token", continuation_token)
      else
        params
      end

    response = Req.get!(request, url: "s3://#{bucket}", params: params) |> success!()
    result = response.body["ListBucketResult"]
    page = Enum.map(result["Contents"] || [], &Map.fetch!(&1, "Key"))
    pages = [page | pages]

    if result["IsTruncated"] == "true" do
      list_keys(request, bucket, prefix, Map.fetch!(result, "NextContinuationToken"), pages)
    else
      pages |> Enum.reverse() |> List.flatten()
    end
  end

  defp download!(request, bucket, keys, directory, extension) do
    keys
    |> Enum.with_index()
    |> Enum.each(fn {key, index} ->
      response = Req.get!(request, url: "s3://#{bucket}/#{key}", raw: true) |> success!()
      File.write!(Path.join(directory, "#{index}#{extension}"), response.body)
    end)
  end

  defp upload!(request, bucket, path) do
    "year=" <> year = path |> Path.dirname() |> Path.basename()

    Req.put!(request,
      url: "s3://#{bucket}/v1/year=#{year}/heartbeats.parquet",
      headers: %{"content-type" => "application/vnd.apache.parquet"},
      body: File.read!(path)
    )
    |> success!()

    :ok
  end

  defp delete!(request, bucket, key) do
    Req.delete!(request, url: "s3://#{bucket}/#{key}") |> success!()
    :ok
  end

  defp success!(%{status: status} = response) when status in 200..299, do: response
  defp success!(%{status: status}), do: raise("S3 request failed with status #{status}")

  defp store!(store) do
    store = Map.new(store)

    for key <- [:bucket, :region, :endpoint_url, :access_key_id, :secret_access_key] do
      case Map.fetch(store, key) do
        {:ok, value} when is_binary(value) and value != "" -> :ok
        _ -> raise ArgumentError, "missing compactor store setting: #{key}"
      end
    end

    store
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"
end
