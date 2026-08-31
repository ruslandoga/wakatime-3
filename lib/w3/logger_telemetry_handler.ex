defmodule W3.LoggerTelemetryHandler do
  @moduledoc false
  require Logger

  def attach do
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
      _no_config = []
    )
  end

  def detach do
    :telemetry.detach(__MODULE__)
  end

  def handle_event([:w3, :compact, :stop], measurements, metadata, _config) do
    Logger.info(fn ->
      "heartbeat compaction complete in #{duration(measurements)}ms for #{metadata.bucket}"
    end)
  end

  def handle_event([:w3, :compact, :exception], measurements, metadata, _config) do
    Logger.error(fn ->
      "heartbeat compaction failed after #{duration(measurements)}ms for #{metadata.bucket}:\n" <>
        format_error(metadata)
    end)
  end

  def handle_event([:w3, :upload, :stop], measurements, %{result: :ok} = metadata, _config) do
    Logger.info(fn ->
      "uploaded #{measurements.heartbeats} heartbeat(s) " <>
        "(#{measurements.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} in #{duration(measurements)}ms"
    end)
  end

  def handle_event(
        [:w3, :upload, :stop],
        measurements,
        %{result: {:error, reason}} = metadata,
        _config
      ) do
    Logger.warning(fn ->
      "failed to upload #{measurements.heartbeats} heartbeat(s) " <>
        "(#{measurements.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} after #{duration(measurements)}ms: " <>
        format_reason(reason)
    end)
  end

  def handle_event([:w3, :upload, :exception], measurements, metadata, _config) do
    Logger.error(fn ->
      "failed to upload #{metadata.heartbeats} heartbeat(s) " <>
        "(#{metadata.bytes} compressed bytes) to " <>
        "s3://#{metadata.bucket}/#{metadata.key} after #{duration(measurements)}ms:\n" <>
        format_error(metadata)
    end)
  end

  def handle_event([:w3, :log], _measurements, %{level: level, log: log}, _config) do
    Logger.log(level, log)
  end

  defp duration(%{duration: duration}), do: duration(duration)

  defp duration(duration) when is_integer(duration) do
    System.convert_time_unit(duration, :native, :millisecond)
  end

  defp format_error(%{kind: kind, reason: reason, stacktrace: stacktrace}) do
    Exception.format(kind, reason, stacktrace)
  end

  defp format_error(%{kind: kind, reason: reason}) do
    Exception.format(kind, reason)
  end

  defp format_reason({:http_error, status}), do: "HTTP #{status}"
  defp format_reason(reason) when is_exception(reason), do: Exception.format(:error, reason)
  defp format_reason(reason), do: inspect(reason)
end
