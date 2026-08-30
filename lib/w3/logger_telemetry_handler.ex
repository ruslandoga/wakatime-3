defmodule W3.LoggerTelemetryHandler do
  @moduledoc false

  require Logger

  def attach do
    :telemetry.attach_many(
      __MODULE__,
      [
        [:w3, :compactor, :run, :stop],
        [:w3, :compactor, :run, :exception],
        [:w3, :ingester, :upload, :stop],
        [:w3, :ingester, :upload, :exception],
        [:w3, :plugin, :log]
      ],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  def detach do
    :telemetry.detach(__MODULE__)
  end

  def handle_event([:w3, :compactor, :run, :stop], _measurements, metadata, _config) do
    Logger.info("heartbeat compaction complete", bucket: metadata.bucket)
  end

  def handle_event([:w3, :compactor, :run, :exception], _measurements, metadata, _config) do
    Logger.error(
      "heartbeat compaction failed:\n" <>
        Exception.format(metadata.kind, metadata.reason, metadata.stacktrace),
      bucket: metadata.bucket
    )
  end

  def handle_event(
        [:w3, :ingester, :upload, :stop],
        _measurements,
        %{result: {:error, reason}} = metadata,
        _config
      ) do
    Logger.warning("failed to upload heartbeats: #{inspect(reason)}",
      bucket: metadata.bucket,
      key: metadata.key
    )
  end

  def handle_event(
        [:w3, :ingester, :upload, :stop],
        %{heartbeats: heartbeat_count},
        %{result: :ok} = metadata,
        _config
      ) do
    Logger.info("heartbeats ingested: #{heartbeat_count}",
      bucket: metadata.bucket,
      key: metadata.key
    )
  end

  def handle_event([:w3, :ingester, :upload, :exception], _measurements, metadata, _config) do
    Logger.warning(
      "failed to upload heartbeats:\n" <>
        Exception.format(metadata.kind, metadata.reason, metadata.stacktrace),
      bucket: metadata.bucket,
      key: metadata.key
    )
  end

  def handle_event(
        [:w3, :plugin, :log],
        _measurements,
        %{level: level, log: log},
        _config
      ) do
    Logger.log(level, log)
  end
end
