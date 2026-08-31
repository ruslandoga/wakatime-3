defmodule W3.Endpoint do
  @moduledoc false
  use Plug.Router

  def start_link(options) do
    {api_key, options} = Keyword.pop!(options, :api_key)
    {s3, options} = Keyword.pop!(options, :s3)
    Bandit.start_link([plug: {__MODULE__, %{api_key: api_key, s3: s3}}] ++ options)
  end

  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  def call(conn, config) do
    %{api_key: api_key, s3: s3} = config

    conn
    |> put_private(:api_key, api_key)
    |> put_private(:s3, s3)
    |> super(_no_opts = [])
  end

  plug :wakatime_auth
  plug :put_secure_browser_headers
  plug :match

  plug Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: JSON

  plug :dispatch

  post "/users/current/heartbeats.bulk" do
    handle_heartbeats(conn)
  end

  post "/plugins/errors" do
    log_errors(conn)
  end

  match _ do
    conn
    |> put_resp_header("content-type", "text/plain")
    |> send_resp(404, "not found")
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

  @doc false
  def put_secure_browser_headers(conn, _opts) do
    headers = [
      {"referrer-policy", "strict-origin-when-cross-origin"},
      {"content-security-policy", "base-uri 'self'; frame-ancestors 'self';"},
      {"x-content-type-options", "nosniff"},
      {"x-permitted-cross-domain-policies", "none"}
    ]

    Enum.reduce(headers, conn, fn {key, value}, conn ->
      put_resp_header(conn, key, value)
    end)
  end

  @doc false
  def handle_heartbeats(conn) do
    %{body_params: %{"_json" => heartbeats}, private: %{s3: s3}} = conn
    [machine_name] = get_req_header(conn, "x-machine-name")

    :ok = W3.Ingester.insert_heartbeats(s3, heartbeats, URI.decode_www_form(machine_name))

    json = JSON.encode_to_iodata!(%{"responses" => Enum.map(heartbeats, fn _ -> [nil, 201] end)})

    conn
    |> put_resp_header("content-type", "application/json; charset=utf-8")
    |> send_resp(201, json)
  end

  @doc false
  def log_errors(conn) do
    %{"logs" => logs} = conn.body_params

    logs
    |> String.split("\n", trim: true)
    |> Enum.map(&JSON.decode!/1)
    |> Enum.each(fn %{"level" => level} = log ->
      :telemetry.execute([:w3, :log], %{}, %{level: String.to_existing_atom(level), log: log})
    end)

    send_resp(conn, 201, [])
  end
end
