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

  test "runs repeatedly after an exception" do
    test = self()

    task = fn ->
      run = Process.get(:run, 0) + 1
      Process.put(:run, run)
      send(test, {:run, self(), run})

      if run == 1 do
        raise "failed run"
      end
    end

    periodic = start_periodic(task)

    assert_receive {:run, ^periodic, 1}, 1_000
    assert_receive {:run, ^periodic, 2}, 1_000
    assert Process.alive?(periodic)
  end

  defp start_periodic(task) do
    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [{to_timeout(millisecond: 10), task}]}
    })
  end
end
