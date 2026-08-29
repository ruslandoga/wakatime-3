defmodule W3.Ingester do
  @moduledoc false
  use Supervisor

  def start_link(options) do
    {supervisor_options, options} = Keyword.split(options, [:name])
    Supervisor.start_link(__MODULE__, options, supervisor_options)
  end

  def insert_heartbeats!(ingester, heartbeats, machine_name) do
    ingester
    |> writer()
    |> W3.Ingester.Writer.insert_heartbeats!(heartbeats, machine_name)
  end

  @impl Supervisor
  def init(options) do
    spool_dir = Keyword.fetch!(options, :spool_dir)
    s3 = Keyword.fetch!(options, :s3)
    interval = Keyword.get(options, :interval, to_timeout(second: 30))
    upload_interval = Keyword.get(options, :upload_interval, interval)
    max_buffer_size = Keyword.get(options, :max_buffer_size, 10_000_000)

    children = [
      {W3.Ingester.Uploader,
       spool_dir: spool_dir, s3: s3, interval: upload_interval, name: uploader_name(options)},
      {W3.Ingester.Writer,
       spool_dir: spool_dir,
       interval: interval,
       max_buffer_size: max_buffer_size,
       uploader: uploader_name(options)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp writer(ingester) do
    ingester
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {W3.Ingester.Writer, pid, :worker, _modules} -> pid
      _child -> nil
    end)
  end

  defp uploader_name(options) do
    {:global, {__MODULE__, :uploader, Keyword.fetch!(options, :spool_dir)}}
  end
end
