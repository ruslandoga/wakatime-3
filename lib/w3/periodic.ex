defmodule W3.Periodic do
  @moduledoc """
  Runs a function or MFA repeatedly with a fixed delay between completed runs.

  Exceptions do not cancel future runs. Throws and exits propagate.
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
  def init({interval, _task} = state) do
    {:ok, :no_state, state, schedule(interval)}
  end

  @impl :gen_statem
  def handle_event({:timeout, :run}, [], _state, _data) do
    {:keep_state_and_data, {:next_event, :internal, :run}}
  end

  def handle_event(:internal, :run, _state, {interval, task}) do
    try do
      run(task)
    rescue
      _exception -> :ok
    end

    {:keep_state_and_data, schedule(interval)}
  end

  defp schedule(interval), do: {{:timeout, :run}, interval, _no_content = []}
  defp run({module, function, arguments}), do: apply(module, function, arguments)
  defp run(function) when is_function(function, 0), do: function.()
end
