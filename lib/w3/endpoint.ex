defmodule W3.Endpoint do
  @moduledoc """
  TODO
  """

  use Plug.Router
  require Logger

  @doc """
  TODO
  """
  def start_link(options) do
    scheme = Keyword.get(options, :http_scheme)
    port = Keyword.get(options, :http_port)

    if scheme && port do
      Bandit.start_link(plug: __MODULE__, scheme: scheme, port: port)
    else
      Logger.notice("HTTP endpoint is disabled because no HTTP scheme or port was provided.")
      :ignore
    end
  end

  @doc """
  TODO
  """
  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  plug :match

  plug Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: JSON

  plug :dispatch

  get "/hello/:name" do
    send_resp(conn, 200, "hello #{name}")
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end
