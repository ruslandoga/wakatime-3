defmodule W3.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = children(Application.get_all_env(:w3))

    opts = [strategy: :one_for_one, name: W3.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def children(config) do
    if api_key = Keyword.get(config, :api_key) do
      s3 = Keyword.fetch!(config, :s3)

      if is_nil(Map.get(s3, :bucket)) do
        raise ArgumentError, "S3_BUCKET is required"
      end

      http = Keyword.get(config, :http, [])
      ingester = Keyword.get(config, :ingester, [])
      ingester_name = Keyword.get(ingester, :name, W3.Ingester)

      [
        {W3.Ingester,
         ingester
         |> Keyword.put_new(:name, ingester_name)
         |> Keyword.put(:s3, s3)},
        {W3.Endpoint, [api_key: api_key, ingester: ingester_name] ++ http}
      ]
    else
      []
    end
  end
end
