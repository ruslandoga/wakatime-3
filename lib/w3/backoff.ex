defmodule W3.Backoff do
  @moduledoc false

  @type t :: %{base: pos_integer(), max: pos_integer()}
  @type option :: {:base, pos_integer()} | {:max, pos_integer()}

  @default_base to_timeout(millisecond: 100)
  @default_max to_timeout(second: 5)

  @spec new([option()]) :: t()
  def new(options \\ []) do
    %{
      base: Keyword.get(options, :base, @default_base),
      max: Keyword.get(options, :max, @default_max)
    }
  end

  @spec delay(t(), non_neg_integer()) :: pos_integer()
  def delay(%{base: base, max: max}, failure_count) do
    max_sleep = min(max, base * Integer.pow(2, failure_count))
    :rand.uniform(max_sleep)
  end
end
