defmodule W3.Periodic do
  @moduledoc """
  Runs a function or MFA repeatedly.

  Successful runs wait for the configured interval. Failures retry with
  full-jitter exponential backoff.
  """

  @behaviour :gen_statem

  def start_link(options) do
    :gen_statem.start_link(__MODULE__, options, [])
  end

  @impl :gen_statem
  def callback_mode do
    :handle_event_function
  end

  @impl :gen_statem
  def init(options) do
    interval = Keyword.fetch!(options, :interval)
    backoff = Keyword.fetch!(options, :backoff)
    task = Keyword.fetch!(options, :task)
    data = [interval: interval, backoff: backoff, task: task]
    {:ok, :nostate, data, schedule(interval, _failure_count = 0)}
  end

  @impl :gen_statem
  def handle_event({:timeout, :run}, failure_count, _state, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:run, failure_count}}}
  end

  def handle_event(:internal, {:run, failure_count}, _state, options) do
    interval = Keyword.fetch!(options, :interval)
    backoff = Keyword.fetch!(options, :backoff)
    task = Keyword.fetch!(options, :task)

    next =
      try do
        run(task)
      catch
        _kind, _reason ->
          delay = backoff_delay(backoff, failure_count)
          schedule(delay, failure_count + 1)
      else
        _result ->
          schedule(interval, _failure_count = 0)
      end

    {:keep_state_and_data, next}
  end

  def handle_event(:info, _message, _state, _data) do
    :keep_state_and_data
  end

  defp schedule(delay, failure_count) do
    {{:timeout, :run}, delay, failure_count}
  end

  defp backoff_delay(%{base: base, max: max}, failure_count) do
    :rand.uniform(min(max, base * Integer.pow(2, failure_count)))
  end

  defp run({module, function, arguments}), do: apply(module, function, arguments)
  defp run(function) when is_function(function, 0), do: function.()
end
