defmodule W3.Periodic do
  @moduledoc """
  Runs a function or MFA repeatedly.

  Successful runs wait for the configured interval. Exceptions retry with
  full-jitter exponential backoff; throws and exits propagate.
  """

  @behaviour :gen_statem

  def start_link(state) do
    :gen_statem.start_link(__MODULE__, state, [])
  end

  @impl :gen_statem
  def callback_mode do
    :handle_event_function
  end

  @impl :gen_statem
  def init({interval, _backoff, _task} = state) do
    {:ok, :nostate, state, schedule(interval, _failure_count = 0)}
  end

  @impl :gen_statem
  def handle_event({:timeout, :run}, failure_count, _state, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:run, failure_count}}}
  end

  def handle_event(:internal, {:run, failure_count}, _state, {interval, backoff, task}) do
    next =
      try do
        run(task)
      rescue
        _exception ->
          delay = backoff_delay(backoff, failure_count)
          schedule(delay, failure_count + 1)
      else
        _result ->
          schedule(interval, _failure_count = 0)
      end

    {:keep_state_and_data, next}
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
