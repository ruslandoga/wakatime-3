defmodule W3.LoggerTelemetryHandler do
  @moduledoc false
  require Logger

  alias W3.S3

  def attach do
    config = %{credentials: credentials()}

    :telemetry.attach_many(
      __MODULE__,
      [
        [:w3, :compact, :stop],
        [:w3, :compact, :exception],
        [:w3, :upload, :stop],
        [:w3, :upload, :exception],
        [:w3, :log]
      ],
      &__MODULE__.handle_event/4,
      config
    )
  end

  def detach do
    :telemetry.detach(__MODULE__)
  end

  def handle_event([:w3, :compact, :stop], measurements, metadata, config) do
    write(:info, config, fn ->
      "heartbeat compaction complete in #{duration(measurements)}ms for #{metadata.bucket}"
    end)
  end

  def handle_event([:w3, :compact, :exception], measurements, metadata, config) do
    write(:error, config, fn ->
      "heartbeat compaction failed after #{duration(measurements)}ms for #{metadata.bucket}:\n" <>
        format_error(metadata)
    end)
  end

  def handle_event([:w3, :upload, :stop], measurements, %{result: :ok} = metadata, config) do
    write(:info, config, fn ->
      "uploaded #{measurements.heartbeats} heartbeat(s) " <>
        "(#{measurements.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} in #{duration(measurements)}ms"
    end)
  end

  def handle_event(
        [:w3, :upload, :stop],
        measurements,
        %{result: {:error, reason}} = metadata,
        config
      ) do
    write(:warning, config, fn ->
      "failed to upload #{measurements.heartbeats} heartbeat(s) " <>
        "(#{measurements.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} after #{duration(measurements)}ms: " <>
        format_reason(reason)
    end)
  end

  def handle_event([:w3, :upload, :exception], measurements, metadata, config) do
    write(:error, config, fn ->
      "failed to upload #{metadata.heartbeats} heartbeat(s) " <>
        "(#{metadata.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} after #{duration(measurements)}ms:\n" <>
        format_error(metadata)
    end)
  end

  def handle_event([:w3, :log], _measurements, %{level: level, log: log}, config) do
    Logger.log(level, redact(log, config))
  end

  defp duration(%{duration: duration}), do: duration(duration)

  defp duration(duration) when is_integer(duration) do
    System.convert_time_unit(duration, :native, :millisecond)
  end

  defp write(level, config, message) do
    Logger.log(level, fn -> message.() |> redact(config) end)
  end

  defp format_error(%{
         reason: {:badmatch, %Req.Response{} = response},
         stacktrace: stacktrace
       }) do
    "** (MatchError) S3 request failed: #{S3.response_error(response)}\n" <>
      Exception.format_stacktrace(stacktrace)
  end

  defp format_error(%{
         reason: %MatchError{term: %Req.Response{} = response},
         stacktrace: stacktrace
       }) do
    "** (MatchError) S3 request failed: #{S3.response_error(response)}\n" <>
      Exception.format_stacktrace(stacktrace)
  end

  defp format_error(%{kind: kind, reason: reason, stacktrace: stacktrace}) do
    Exception.format(kind, reason, stacktrace)
  end

  defp format_error(%{kind: kind, reason: reason}) do
    Exception.format(kind, reason)
  end

  defp format_reason(%Req.Response{} = response), do: S3.response_error(response)
  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(reason) when is_exception(reason), do: Exception.format(:error, reason)
  defp format_reason(reason), do: inspect(reason)

  defp credentials do
    config = W3.config()

    case Keyword.get(config, :s3) do
      %S3{} = s3 -> [Keyword.get(config, :api_key), s3.access_key_id, s3.secret_access_key]
      _ -> [Keyword.get(config, :api_key)]
    end
  end

  defp redact(value, config) when is_binary(value) do
    config
    |> Map.get(:credentials, [])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.flat_map(&[&1, masked_credential(&1)])
    |> Enum.reduce(value, &String.replace(&2, &1, "[REDACTED]"))
  end

  defp redact(%_{} = value, config), do: value |> inspect() |> redact(config)

  defp redact(value, config) when is_map(value) do
    Map.new(value, fn {key, value} -> {redact(key, config), redact(value, config)} end)
  end

  defp redact(value, config) when is_list(value), do: Enum.map(value, &redact(&1, config))

  defp redact(value, config) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&redact(&1, config)) |> List.to_tuple()
  end

  defp redact(value, _config), do: value

  defp masked_credential(credential) do
    length = String.length(credential)

    if length < 4 do
      String.duplicate("*", length)
    else
      String.slice(credential, 0, 3) <> String.duplicate("*", length - 3)
    end
  end
end
