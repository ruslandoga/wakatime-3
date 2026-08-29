defmodule W3.Compactor.CLI do
  @moduledoc false

  require Logger

  def run! do
    :ok = W3.Compactor.run!(store("RAW"), store("CANONICAL"))
    Logger.info("heartbeat compaction complete")
    :ok
  end

  defp store(prefix) do
    [
      bucket: System.fetch_env!("#{prefix}_S3_BUCKET"),
      region: System.get_env("#{prefix}_S3_REGION", "auto"),
      endpoint_url: System.fetch_env!("#{prefix}_S3_ENDPOINT_URL"),
      access_key_id: System.fetch_env!("#{prefix}_S3_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("#{prefix}_S3_SECRET_ACCESS_KEY"),
      session_token: System.get_env("#{prefix}_S3_SESSION_TOKEN")
    ]
  end
end
