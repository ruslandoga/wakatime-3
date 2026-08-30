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
    backoff = W3.Backoff.new(base: 1, max: 1)

    assert {:keep_state_and_data, {:next_event, :internal, {:run, 2}}} =
             W3.Periodic.handle_event({:timeout, :run}, 2, :no_state, :data)

    assert {:keep_state_and_data, {{:timeout, :run}, 1, 3}} =
             W3.Periodic.handle_event(
               :internal,
               {:run, 2},
               :no_state,
               {100, backoff, fn -> raise "failed run" end}
             )

    assert {:keep_state_and_data, {{:timeout, :run}, 100, 0}} =
             W3.Periodic.handle_event(
               :internal,
               {:run, 3},
               :no_state,
               {100, backoff, fn -> :ok end}
             )
  end

  defp start_periodic(task) do
    backoff = W3.Backoff.new(base: 1, max: 1)

    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [{10, backoff, task}]}
    })
  end
end
