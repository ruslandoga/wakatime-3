defmodule W3.BackoffTest do
  use ExUnit.Case, async: true

  test "uses capped exponential full jitter" do
    backoff = W3.Backoff.new(base: 10, max: 25)

    for {failure_count, upper_bound} <- [{0, 10}, {1, 20}, {2, 25}, {10, 25}] do
      assert W3.Backoff.delay(backoff, failure_count) in 1..upper_bound
    end
  end
end
