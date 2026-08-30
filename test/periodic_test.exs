defmodule W3.PeriodicTest do
  use ExUnit.Case, async: true

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

  test "backs off after an exception and resets after success" do
    backoff = %{base: 10, max: 25}

    assert {:keep_state_and_data, {:next_event, :internal, {:run, 2}}} =
             W3.Periodic.handle_event({:timeout, :run}, 2, :no_state, :data)

    for {failure_count, upper_bound} <- [{0, 10}, {1, 20}, {2, 25}, {10, 25}] do
      assert {:keep_state_and_data, {{:timeout, :run}, delay, next_failure_count}} =
               W3.Periodic.handle_event(
                 :internal,
                 {:run, failure_count},
                 :no_state,
                 interval: 100,
                 backoff: backoff,
                 task: fn -> raise "failed run" end
               )

      assert delay in 1..upper_bound
      assert next_failure_count == failure_count + 1
    end

    assert {:keep_state_and_data, {{:timeout, :run}, 100, 0}} =
             W3.Periodic.handle_event(
               :internal,
               {:run, 3},
               :no_state,
               interval: 100,
               backoff: backoff,
               task: fn -> :ok end
             )
  end

  test "backs off after errors, exits, and throws" do
    test = self()
    attempts = start_supervised!({Agent, fn -> [:error, :exit, :throw, :success] end})

    periodic =
      start_periodic(fn ->
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
      end)

    for attempt <- [:error, :exit, :throw, :success] do
      assert_receive {:attempt, ^attempt}, 1_000
    end

    assert Process.alive?(periodic)
  end

  test "reports failures before retrying" do
    telemetry_ref = Help.attach_telemetry([[:w3, :periodic_test, :exception]])
    test = self()
    attempts = start_supervised!({Agent, fn -> [:exit, :success] end})

    periodic =
      start_periodic(fn ->
        :telemetry.span([:w3, :periodic_test], %{source: :test}, fn ->
          case Agent.get_and_update(attempts, fn [attempt | rest] -> {attempt, rest} end) do
            :exit ->
              exit(:boom)

            :success ->
              send(test, :recovered)

              receive do
                :stop -> :ok
              end
          end
        end)
      end)

    assert_receive {[:w3, :periodic_test, :exception], ^telemetry_ref, %{duration: duration},
                    %{kind: :exit, reason: :boom, source: :test, stacktrace: stacktrace}}

    assert is_integer(duration)
    assert is_list(stacktrace)
    assert_receive :recovered, 1_000
    assert Process.alive?(periodic)
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
