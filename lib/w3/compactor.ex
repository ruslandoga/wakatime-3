defmodule W3.Compactor do
  @moduledoc """
  Appends new raw heartbeats to the canonical Hive-partitioned Parquet dataset.

  Each invocation owns one in-memory DuckDB database and one connection. Both
  are closed before the function returns or raises.
  """

  @columns [
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
    "editor",
    "operating_system",
    "machine_name",
    "timezone",
    "user_agent"
  ]

  @identity Enum.reject(@columns, &(&1 in ["timezone", "user_agent"]))
  @identity_partition Enum.join(@identity, ", ")

  @existing_match Enum.map_join(@identity, " AND\n", fn column ->
                    "raw.#{column} IS NOT DISTINCT FROM existing.#{column}"
                  end)

  @output_columns Enum.map_join(@columns, ",\n", &"raw.#{&1}")

  def run!(raw, canonical, adapter \\ DuckNIF) do
    raw = store!(raw)
    canonical = store!(canonical)
    database = adapter.open()

    try do
      connection = adapter.connect(database)

      try do
        query!(connection, sql(raw, canonical), adapter)
      after
        :ok = adapter.disconnect(connection)
      end
    after
      :ok = adapter.close(database)
    end
  end

  defp query!(connection, sql, adapter) do
    result = adapter.query_dirty_io(connection, sql)

    try do
      :ok
    after
      :ok = adapter.destroy_result(result)
    end
  end

  defp sql(raw, canonical) do
    raw_path = "s3://#{raw.bucket}/raw/*.ndjson.zst"
    canonical_path = "s3://#{canonical.bucket}/v1/year=*/*.parquet"
    canonical_root = "s3://#{canonical.bucket}/v1"

    """
    LOAD httpfs;
    SET enable_global_s3_configuration = false;
    SET TimeZone = 'UTC';
    #{secret_sql("raw_store", raw)}
    #{secret_sql("canonical_store", canonical)}

    COPY (
      WITH raw_input AS (
        SELECT *
        FROM read_ndjson(
          #{sql_quote(raw_path)},
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
      ), normalized AS (
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
          user_agent::VARCHAR AS user_agent,
          year(timezone('UTC', to_timestamp(time)))::INTEGER AS year
        FROM raw_input
      ), raw AS (
        SELECT *
        FROM normalized
        WHERE CASE
          WHEN time IS NULL OR entity IS NULL OR type IS NULL OR machine_name IS NULL THEN
            error('heartbeat is missing a required field')
          ELSE true
        END
        QUALIFY row_number() OVER (
          PARTITION BY #{@identity_partition}
          ORDER BY
            (nullif(timezone, '') IS NOT NULL)::INTEGER +
              (nullif(user_agent, '') IS NOT NULL)::INTEGER DESC,
            timezone DESC NULLS LAST,
            user_agent DESC NULLS LAST
        ) = 1
      ), existing AS (
        SELECT #{Enum.join(@columns, ", ")}
        FROM read_parquet(
          #{sql_quote(canonical_path)},
          hive_partitioning = true,
          union_by_name = true
        )
      )
      SELECT
        #{@output_columns},
        raw.year
      FROM raw
      WHERE NOT EXISTS (
        SELECT 1
        FROM existing
        WHERE #{@existing_match}
      )
      ORDER BY raw.year, raw.time, raw.entity
    ) TO #{sql_quote(canonical_root)} (
      FORMAT PARQUET,
      PARTITION_BY (year),
      APPEND true,
      FILENAME_PATTERN 'heartbeats_{uuid}',
      COMPRESSION ZSTD,
      COMPRESSION_LEVEL 3,
      PARQUET_VERSION V2,
      ROW_GROUP_SIZE 122880
    );
    """
  end

  defp secret_sql(name, store) do
    {endpoint, use_ssl} = endpoint(store.endpoint_url)

    session_token =
      case store.session_token do
        nil -> ""
        value -> ", SESSION_TOKEN #{sql_quote(value)}"
      end

    """
    CREATE SECRET #{name} (
      TYPE s3,
      PROVIDER config,
      KEY_ID #{sql_quote(store.access_key_id)},
      SECRET #{sql_quote(store.secret_access_key)},
      ENDPOINT #{sql_quote(endpoint)},
      REGION #{sql_quote(store.region)},
      URL_STYLE path,
      USE_SSL #{use_ssl},
      SCOPE #{sql_quote("s3://#{store.bucket}/")}
      #{session_token}
    );
    """
  end

  defp store!(store) do
    store = Map.new(store)

    for key <- [:bucket, :region, :endpoint_url, :access_key_id, :secret_access_key] do
      case Map.fetch(store, key) do
        {:ok, value} when is_binary(value) and value != "" -> :ok
        _ -> raise ArgumentError, "missing compactor store setting: #{key}"
      end
    end

    Map.put_new(store, :session_token, nil)
  end

  defp endpoint(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, authority: authority, path: path}
      when scheme in ["http", "https"] and is_binary(authority) ->
        {authority <> String.trim_trailing(path || "", "/"), scheme == "https"}

      _ ->
        raise ArgumentError, "invalid compactor S3 endpoint"
    end
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"
end
