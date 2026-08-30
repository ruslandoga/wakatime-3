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

  test "redacts S3 credentials from logged errors" do
    access_key_id = "ACCESS-KEY-MARKER"
    secret_access_key = "SECRET-KEY-MARKER"
    masked_access_key_id = "ACC" <> String.duplicate("*", String.length(access_key_id) - 3)

    masked_secret_access_key =
      "SEC" <> String.duplicate("*", String.length(secret_access_key) - 3)

    response = %Req.Response{
      status: 403,
      headers: %{"x-amz-request-id" => ["header-request-id"]},
      body: %{
        "Error" => %{
          "Code" => "InvalidAccessKeyId",
          "Message" => "Credential #{access_key_id} was rejected with #{secret_access_key}",
          "AWSAccessKeyId" => access_key_id,
          "RequestId" => "body-request-id"
        }
      }
    }

    metadata = %{
      bucket: "test-bucket",
      key: "raw/test.ndjson.zst",
      result: {:error, response}
    }

    measurements = %{heartbeats: 1, bytes: 21, duration: 0}

    log =
      capture_log(fn ->
        W3.LoggerTelemetryHandler.handle_event(
          [:w3, :upload, :stop],
          measurements,
          metadata,
          %{credentials: [access_key_id, secret_access_key]}
        )

        W3.LoggerTelemetryHandler.handle_event(
          [:w3, :upload, :stop],
          measurements,
          %{
            metadata
            | result:
                {:error,
                 "#{access_key_id} #{secret_access_key} #{masked_access_key_id} #{masked_secret_access_key}"}
          },
          %{credentials: [access_key_id, secret_access_key]}
        )
      end)

    assert log =~ "HTTP 403: InvalidAccessKeyId (request body-request-id)"
    refute log =~ access_key_id
    refute log =~ secret_access_key
    refute log =~ masked_access_key_id
    refute log =~ masked_secret_access_key
  end
end
