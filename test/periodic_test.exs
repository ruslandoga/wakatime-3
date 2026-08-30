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
                 {100, backoff, fn -> raise "failed run" end}
               )

      assert delay in 1..upper_bound
      assert next_failure_count == failure_count + 1
    end

    assert {:keep_state_and_data, {{:timeout, :run}, 100, 0}} =
             W3.Periodic.handle_event(
               :internal,
               {:run, 3},
               :no_state,
               {100, backoff, fn -> :ok end}
             )
  end

  defp start_periodic(task) do
    backoff = %{base: 1, max: 1}

    start_supervised!(%{
      id: make_ref(),
      start: {W3.Periodic, :start_link, [{10, backoff, task}]}
    })
  end
end
