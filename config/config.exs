import Config

config :w3, start_compactor: config_env() != :test
