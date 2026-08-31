defmodule W3.Periodic do
  @moduledoc """
  Runs a zero-arity function repeatedly.

  Successful runs wait for the configured interval. Failures retry with
  full-jitter exponential backoff.
  """

  @behaviour :gen_statem

  def start_link(options) do
    :gen_statem.start_link(__MODULE__, options, [])
  end

  def child_spec(options) do
    {id, options} = Keyword.pop(options, :id, __MODULE__)
    %{id: id, start: {__MODULE__, :start_link, [options]}}
  end

  @impl :gen_statem
  def callback_mode do
    :handle_event_function
  end

  @impl :gen_statem
  def init(options) do
    interval = Keyword.fetch!(options, :interval)
    task = Keyword.fetch!(options, :task)

    backoff =
      Keyword.get_lazy(options, :backoff, fn ->
        %{base: to_timeout(millisecond: 250), max: to_timeout(second: 5)}
      end)

    config = %{interval: interval, backoff: backoff, task: task}
    next = {:next_event, :internal, {:start, _failure_count = 0}}

    {:ok, :idle, config, next}
  end

  @impl :gen_statem
  def handle_event(type, content, state, data)

  def handle_event({:timeout, :start}, failure_count, :idle, _config) do
    {:keep_state_and_data, {:next_event, :internal, {:start, failure_count}}}
  end

  def handle_event(:internal, {:start, failure_count}, :idle, config) do
    # A fresh task releases its process memory and temporary-file/stream resources after each run.
    task = Task.Supervisor.async_nolink(W3.task_supervisor(), config.task)
    {:next_state, {:busy, task.ref, failure_count}, config}
  end

  def handle_event(:info, {task_ref, _result}, {:busy, task_ref, _failure_count}, config) do
    Process.demonitor(task_ref, [:flush])
    {:next_state, :idle, config, {{:timeout, :start}, config.interval, _failure_count = 0}}
  end

  def handle_event(
        :info,
        {:DOWN, task_ref, :process, _pid, _reason},
        {:busy, task_ref, failure_count},
        config
      ) do
    retry_delay = backoff_delay(config.backoff, failure_count)
    {:next_state, :idle, config, {{:timeout, :start}, retry_delay, failure_count + 1}}
  end

  def handle_event(:info, _message, _state, _data) do
    :keep_state_and_data
  end

  defp backoff_delay(%{base: base, max: max}, failure_count) do
    :rand.uniform(min(max, base * Integer.pow(2, failure_count)))
  end
end
