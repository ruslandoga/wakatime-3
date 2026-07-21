defmodule W3.Endpoint do
  @moduledoc false
  use Plug.Router

  def start_link(options) do
    scheme = Keyword.get(options, :scheme, :http)
    port = Keyword.fetch!(options, :port)
    api_key = Keyword.fetch!(options, :api_key)

    Bandit.start_link(
      plug: {__MODULE__, %{api_key: api_key}},
      scheme: scheme,
      port: port
    )
  end

  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  def call(conn, config) do
    %{api_key: api_key} = config

    conn
    |> put_private(:api_key, api_key)
    |> super(_no_opts = [])
  end

  plug :wakatime_auth
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

  @doc false
  def wakatime_auth(conn, _opts) do
    with ["Basic " <> basic] <- get_req_header(conn, "authorization"),
         {:ok, api_key} <- Base.decode64(basic, padding: false),
         true <- Plug.Crypto.secure_compare(api_key, conn.private.api_key) do
      conn
    else
      _ ->
        conn
        |> put_resp_header("www-authenticate", "Basic")
        |> resp(401, "Unauthorized")
        |> halt()
    end
  end
end
