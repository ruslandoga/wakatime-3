defmodule W3.Compactor do
  @moduledoc """
  Converts raw heartbeat objects into deterministic, year-partitioned Parquet
  parts.

  Existing Parquet is deliberately not read on this hot path. Object I/O is
  handled by Req, while DuckDB only sees temporary local files.
  """

  def compact!(s3, data_path) do
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
      outputs = convert!(raw_files, output_directory)

      W3.async_map!(outputs, &upload!(s3, &1))
      delete_objects!(s3, raw_keys)
      :ok
    after
      File.rm_rf!(directory)
    end
  end

  defp convert!(raw_files, output_directory) do
    W3.Duck.with_duck(fn conn ->
      W3.Duck.query(conn, "SET threads = 1")

      Enum.flat_map(raw_files, &convert_one!(conn, &1, output_directory))
    end)
  end

  defp convert_one!(conn, %{key: raw_key, path: raw_file}, output_directory) do
    source_id = source_id(raw_key)
    directory = Path.join(output_directory, source_id)

    if copy_raw!(conn, raw_file, directory) == 0 do
      []
    else
      for "year=" <> year = partition <- File.ls!(directory) do
        partition_directory = Path.join(directory, partition)

        [filename] =
          partition_directory
          |> File.ls!()
          |> Enum.filter(&String.ends_with?(&1, ".parquet"))

        %{
          key: "v1/year=#{year}/raw-#{source_id}.parquet",
          path: Path.join(partition_directory, filename)
        }
      end
    end
  end

  defp copy_raw!(conn, raw_file, output) do
    %{"Count" => [count]} =
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
        ) TO #{sql_quote(output)} (
          FORMAT PARQUET,
          PARTITION_BY (year),
          COMPRESSION ZSTD,
          PARQUET_VERSION V2
        );
        """,
        %{"raw_files" => JSON.encode!([raw_file])}
      )

    count
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

    %{status: 200, body: body} = Req.get!(req, url: "s3://#{bucket}", params: params)
    result = body["ListBucketResult"]
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

    W3.async_map!(
      Enum.with_index(keys),
      fn {key, index} ->
        %{status: 200, body: body} =
          Req.get!(req, url: object_url(s3, key), raw: true)

        path = Path.join(directory, "#{index}#{extension}")
        File.write!(path, body)
        %{key: key, path: path}
      end
    )
  end

  defp upload!(s3, %{key: key, path: path}) do
    %{status: 200} =
      Req.put!(req(s3),
        url: object_url(s3, key),
        headers: %{"content-type" => "application/vnd.apache.parquet"},
        body: File.read!(path)
      )

    :ok
  end

  defp delete_objects!(s3, keys) do
    Enum.each(Enum.chunk_every(keys, 1_000), fn keys ->
      body =
        IO.iodata_to_binary([
          ~s|<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
          Enum.map(keys, fn key ->
            ["<Object><Key>", xml_escape(key), "</Key></Object>"]
          end),
          "<Quiet>true</Quiet></Delete>"
        ])

      %{status: 200, body: response_body} =
        Req.post!(req(s3),
          url: "s3://#{s3.bucket}",
          params: [delete: ""],
          retry: false,
          headers: %{
            "content-md5" => :md5 |> :crypto.hash(body) |> Base.encode64(),
            "content-type" => "application/xml"
          },
          body: body
        )

      case ReqS3.XML.parse_s3(response_body) do
        %{"DeleteResult" => nil} -> :ok
        %{"DeleteResult" => result} -> nil = result["Error"]
      end
    end)
  end

  defp source_id(raw_key) do
    :sha256
    |> :crypto.hash(raw_key)
    |> Base.encode16(case: :lower)
  end

  defp object_url(s3, key) do
    key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "s3://#{s3.bucket}/#{key}"
  end

  defp xml_escape(value) do
    value
    |> Plug.HTML.html_escape()
    |> String.replace("\r", "&#13;")
    |> String.replace("\n", "&#10;")
  end

  defp sql_quote(value), do: "'#{String.replace(value, "'", "''")}'"
end
