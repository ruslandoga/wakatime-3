defmodule W3.TelemetryTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  test "logs upload exceptions with duration and error details" do
    measurements = %{duration: System.convert_time_unit(37, :millisecond, :native)}

    metadata = %{
      bucket: "test-bucket",
      key: "raw/test.ndjson.zst",
      heartbeats: 2,
      bytes: 42,
      kind: :error,
      reason: RuntimeError.exception("upload exploded"),
      stacktrace: []
    }

    log =
      capture_log(fn ->
        :telemetry.execute([:w3, :upload, :exception], measurements, metadata)
      end)

    assert log =~ "failed to upload 2 heartbeat(s)"
    assert log =~ "42 compressed bytes"
    assert log =~ "s3://test-bucket/raw/test.ndjson.zst"
    assert log =~ "after 37ms"
    assert log =~ "** (RuntimeError) upload exploded"
  end

  test "logs returned upload errors" do
    measurements = %{
      heartbeats: 1,
      bytes: 21,
      duration: System.convert_time_unit(19, :millisecond, :native)
    }

    reason = %Req.TransportError{reason: :econnrefused}

    metadata = %{
      bucket: "test-bucket",
      key: "raw/test.ndjson.zst",
      result: {:error, reason}
    }

    log =
      capture_log(fn ->
        :telemetry.execute([:w3, :upload, :stop], measurements, metadata)
      end)

    assert log =~ "failed to upload 1 heartbeat(s)"
    assert log =~ "21 compressed bytes"
    assert log =~ "s3://test-bucket/raw/test.ndjson.zst"
    assert log =~ "after 19ms"
    assert log =~ Exception.message(reason)
  end
end
