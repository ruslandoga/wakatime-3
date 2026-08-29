FROM hexpm/elixir:1.20.3-erlang-29.0.5-debian-bookworm-20260824-slim AS build

WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
COPY config/config.exs config/
RUN mix deps.get --only prod && mix deps.compile

COPY config/runtime.exs config/
COPY lib lib
RUN mix compile && mix release

FROM debian:bookworm-20260824-slim AS app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libncurses6 \
        libsctp1 \
        libstdc++6 \
        openssl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --system --uid 999 --home-dir /app w3

WORKDIR /app

COPY --from=build --chown=w3:w3 /app/_build/prod/rel/w3 ./

USER w3

ENV HOME=/app \
    LANG=C.UTF-8

EXPOSE 4000

CMD ["/app/bin/w3", "start"]
