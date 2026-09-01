defmodule W3.PeriodicTest do
  use ExUnit.Case, async: true

  test "uses and removes a configured child id" do
    task = fn -> :ok end

    assert %{id: :parquet_compactor, start: {W3.Periodic, :start_link, [options]}} =
             W3.Periodic.child_spec(id: :parquet_compactor, interval: 10, task: task)

    refute Keyword.has_key?(options, :id)
    assert Keyword.fetch!(options, :interval) == 10
    assert Keyword.fetch!(options, :task) == task
  end

  test "starts immediately and does not overlap runs" do
    test = self()

    periodic =
      start_periodic(fn ->
        send(test, {:started, self()})

        receive do
          :continue -> :ok
        end
      end)

    assert_receive {:started, first_task}, 1_000
    refute first_task == periodic
    refute_receive {:started, _task}, 30

    send(first_task, :continue)
    assert_receive {:started, second_task}, 1_000
    refute second_task == first_task
    send(second_task, :continue)
  end

  test "retries after errors, exits, and throws" do
    test = self()
    attempts = start_supervised!({Agent, fn -> [:error, :exit, :throw, :success] end})

    task = fn ->
      attempt = Agent.get_and_update(attempts, fn [attempt | rest] -> {attempt, rest} end)
      send(test, {:attempt, attempt})

      case attempt do
        :error -> raise "boom"
        :exit -> exit(:boom)
        :throw -> throw(:boom)
        :success -> :ok
      end
    end

    periodic = start_periodic(task, interval: 1_000)

    for attempt <- [:error, :exit, :throw, :success] do
      assert_receive {:attempt, ^attempt}, 1_000
    end

    assert Process.alive?(periodic)
  end

  test "bounds full-jitter delays and resets failures after success" do
    config = %{interval: 100, backoff: %{base: 10, max: 25}, task: fn -> :ok end}

    for {failure_count, upper_bound} <- [{0, 10}, {1, 20}, {2, 25}, {8, 25}] do
      task_ref = make_ref()

      assert {:next_state, :idle, ^config, {{:timeout, :start}, retry_delay, next_failure_count}} =
               W3.Periodic.handle_event(
                 :info,
                 {:DOWN, task_ref, :process, self(), :boom},
                 {:busy, task_ref, failure_count},
                 config
               )

      assert retry_delay in 1..upper_bound
      assert next_failure_count == failure_count + 1
    end

    task = spawn(fn -> Process.sleep(:infinity) end)
    task_ref = Process.monitor(task)

    assert {:next_state, :idle, ^config, {{:timeout, :start}, 100, 0}} =
             W3.Periodic.handle_event(
               :info,
               {task_ref, :ok},
               {:busy, task_ref, 8},
               config
             )

    Process.exit(task, :kill)
  end

  test "waits for the configured interval after success" do
    test = self()

    _periodic =
      start_periodic(
        fn -> send(test, {:ran, System.monotonic_time(:millisecond)}) end,
        interval: 80
      )

    assert_receive {:ran, first}, 1_000
    refute_receive {:ran, _second}, 40
    assert_receive {:ran, second}, 1_000
    assert second - first >= 70
  end

  test "ignores stray messages" do
    test = self()

    periodic =
      start_periodic(fn ->
        send(test, {:started, self()})

        receive do
          :continue -> :ok
        end
      end)

    assert_receive {:started, task}, 1_000
    send(periodic, :stray)
    send(task, :continue)
    assert_receive {:started, next_task}, 1_000
    assert Process.alive?(periodic)
    send(next_task, :continue)
  end

  defp start_periodic(task, options \\ []) do
    options =
      options
      |> Keyword.put_new(:interval, 10)
      |> Keyword.put_new(:backoff, %{base: 1, max: 1})
      |> Keyword.put(:task, task)

    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [options]}
    })
  end
end
