defmodule W3.PeriodicTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  test "does not overlap runs" do
    test = self()

    periodic =
      start_periodic(fn ->
        send(test, {:started, self()})

        receive do
          :continue -> :ok
        end
      end)

    assert_receive {:started, ^periodic}, 1_000
    refute_receive {:started, ^periodic}, 30

    send(periodic, :continue)
    assert_receive {:started, ^periodic}, 1_000
  end

  test "backs off after errors, exits, and throws" do
    test = self()
    attempts = start_supervised!({Agent, fn -> [:error, :exit, :throw, :success] end})

    task = fn ->
      attempt = Agent.get_and_update(attempts, fn [attempt | rest] -> {attempt, rest} end)
      send(test, {:attempt, attempt})

      case attempt do
        :error ->
          raise "boom"

        :exit ->
          exit(:boom)

        :throw ->
          throw(:boom)

        :success ->
          receive do
            :stop -> :ok
          end
      end
    end

    log =
      capture_log(fn ->
        periodic = start_periodic(task)

        for attempt <- [:error, :exit, :throw, :success] do
          assert_receive {:attempt, ^attempt}, 1_000
        end

        assert Process.alive?(periodic)
      end)

    assert log =~ "periodic task #{inspect(task)} failed on attempt 1"
    assert log =~ "retrying attempt 2 in 1ms"
    assert log =~ "failed on attempt 3"
    assert log =~ "** (RuntimeError) boom"
    assert log =~ "** (exit) :boom"
    assert log =~ "** (throw) :boom"
  end

  test "reports failures before retrying and resets after success" do
    telemetry_ref =
      Help.attach_telemetry([
        [:w3, :periodic, :start],
        [:w3, :periodic, :stop],
        [:w3, :periodic, :exception]
      ])

    test = self()
    attempts = start_supervised!({Agent, fn -> [:exit, :success, :exit, :recovered] end})

    task = fn ->
      case Agent.get_and_update(attempts, fn [attempt | rest] -> {attempt, rest} end) do
        :exit ->
          exit(:boom)

        :success ->
          :ok

        :recovered ->
          send(test, :recovered)

          receive do
            :stop -> :ok
          end
      end
    end

    task_name = inspect(task)

    capture_log(fn ->
      periodic = start_periodic(task)

      assert_receive {[:w3, :periodic, :start], ^telemetry_ref, _,
                      %{attempt: 1, retry_delay: 1, task: ^task_name}}

      assert_receive {[:w3, :periodic, :exception], ^telemetry_ref, %{duration: duration},
                      %{
                        attempt: 1,
                        kind: :exit,
                        reason: :boom,
                        retry_delay: 1,
                        stacktrace: stacktrace,
                        task: ^task_name
                      }}

      assert is_integer(duration)
      assert is_list(stacktrace)

      assert_receive {[:w3, :periodic, :start], ^telemetry_ref, _,
                      %{attempt: 2, retry_delay: 1, task: ^task_name}}

      assert_receive {[:w3, :periodic, :stop], ^telemetry_ref, _,
                      %{attempt: 2, retry_delay: 1, task: ^task_name}}

      assert_receive {[:w3, :periodic, :start], ^telemetry_ref, _,
                      %{attempt: 1, retry_delay: 1, task: ^task_name}}

      assert_receive {[:w3, :periodic, :exception], ^telemetry_ref, _,
                      %{
                        attempt: 1,
                        kind: :exit,
                        reason: :boom,
                        retry_delay: 1,
                        task: ^task_name
                      }}

      assert_receive {[:w3, :periodic, :start], ^telemetry_ref, _,
                      %{attempt: 2, retry_delay: 1, task: ^task_name}}

      assert_receive :recovered, 1_000
      assert Process.alive?(periodic)
    end)
  end

  test "ignores stray messages" do
    test = self()

    periodic =
      start_periodic(fn ->
        send(test, :started)

        receive do
          :continue -> :ok
        end
      end)

    assert_receive :started, 1_000
    send(periodic, :stray)
    send(periodic, :continue)
    assert_receive :started, 1_000
    assert Process.alive?(periodic)
  end

  defp start_periodic(task) do
    backoff = %{base: 1, max: 1}

    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [[interval: 10, backoff: backoff, task: task]]}
    })
  end
end
