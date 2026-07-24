import Config

config :adbc, :drivers, [:duckdb]

if config_env() == :test do
  import_config "test.exs"
end
