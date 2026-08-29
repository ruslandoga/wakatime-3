import Config

config :w3,
  api_key: "406fe41f-6d69-4183-a4cc-121e0c524c2b",
  compactor: [initial_delay: :timer.hours(24)],
  http: [
    port: 0
  ],
  s3: [
    bucket: "w3-test",
    access_key_id: "minioadmin",
    secret_access_key: "minioadmin"
  ]
