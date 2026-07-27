defmodule W3.IngesterTest do
  use ExUnit.Case, async: true

  setup do
    spool_dir =
      Path.join(
        System.tmp_dir!(),
        "w3-ingester-#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn -> File.rm_rf!(spool_dir) end)
    {:ok, spool_dir: spool_dir}
  end

  test "rotates an appended batch to a sealed file", %{spool_dir: spool_dir} do
    writer =
      start_supervised!(
        {W3.Ingester.Writer,
         spool_dir: spool_dir,
         interval: to_timeout(hour: 1),
         max_buffer_size: 1,
         uploader: uploader_name(spool_dir)}
      )

    assert :ok =
             W3.Ingester.Writer.insert_heartbeats!(
               writer,
               [Help.heartbeat(project: "disk-backed")],
               "mac3.local"
             )

    assert [sealed_path] = sealed_files(spool_dir)

    assert %{"project" => "disk-backed", "machine_name" => "mac3.local"} =
             sealed_path |> File.read!() |> JSON.decode!()

    assert File.read!(Path.join(spool_dir, "current.ndjson.open")) == ""
  end

  test "recovers a non-empty active file on startup", %{spool_dir: spool_dir} do
    active_path = Path.join(spool_dir, "current.ndjson.open")
    File.mkdir_p!(spool_dir)
    File.write!(active_path, "{\"project\":\"recovered\"}\n")

    start_supervised!(
      {W3.Ingester.Writer,
       spool_dir: spool_dir,
       interval: to_timeout(hour: 1),
       max_buffer_size: 1_000,
       uploader: uploader_name(spool_dir)}
    )

    assert [sealed_path] = sealed_files(spool_dir)
    assert File.read!(sealed_path) == "{\"project\":\"recovered\"}\n"
    assert File.read!(active_path) == ""
  end

  defp sealed_files(spool_dir) do
    spool_dir
    |> Path.join("sealed/**/*.ndjson")
    |> Path.wildcard()
  end

  defp uploader_name(spool_dir) do
    {:global, {__MODULE__, :missing_uploader, spool_dir}}
  end
end
