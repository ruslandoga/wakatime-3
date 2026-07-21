defmodule W3.IngesterTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "insert, flush, read back", %{tmp_dir: tmp_dir} do
    pid = start_supervised!({W3.Ingester, data_path: tmp_dir})

    heartbeats = [
      %{
        "branch" => "add-ingester",
        "category" => "coding",
        "cursorpos" => 1,
        "dependencies" => nil,
        "entity" => "/Users/q/Developer/copycat/w1/lib/w1/endpoint.ex",
        "is_write" => true,
        "language" => "Elixir",
        "lineno" => 31,
        "lines" => 64,
        "project" => "w1",
        "time" => 1_653_576_798.5958169,
        "type" => "file",
        # TODO
        "rubbish" => "is not saved",
        "user_agent" =>
          "wakatime/v1.45.3 (darwin-21.4.0-arm64) go1.18.1 vscode/1.68.0-insider vscode-wakatime/18.1.5"
      }
    ]

    machine_name = "mac3.local"

    :ok = W3.Ingester.insert_heartbeats(pid, heartbeats)
    :ok = W3.Ingester.flush(pid)

    # TODO read back and assert
  end
end
