FROM hexpm/elixir:1.20.3-erlang-29.0.5-alpine-3.23.5 AS build

WORKDIR /app

RUN apk add --no-cache git \
    && mix local.hex --force \
    && mix local.rebar --force

ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
COPY config/config.exs config/
RUN mix deps.get --only prod && mix deps.compile

COPY config/runtime.exs config/
COPY lib lib
COPY rel rel
RUN mix compile \
    && W3_MODE=compactor mix run --no-start -e 'database = DuckNIF.open(); connection = DuckNIF.connect(database); try do result = DuckNIF.query_dirty_io(connection, "INSTALL httpfs"); DuckNIF.destroy_result(result) after DuckNIF.disconnect(connection); DuckNIF.close(database) end' \
    && mix release

FROM alpine:3.23.5 AS app

RUN apk add --no-cache \
        ca-certificates \
        libgcc \
        liblksctp \
        libstdc++ \
        ncurses-libs \
        openssl \
    && addgroup --system w3 \
    && adduser --system --disabled-password --uid 999 --ingroup w3 --home /app w3

WORKDIR /app

COPY --from=build --chown=w3:w3 /app/_build/prod/rel/w3 ./
COPY --from=build --chown=w3:w3 /root/.duckdb /app/.duckdb

USER w3

ENV HOME=/app \
    LANG=C.UTF-8

EXPOSE 4000

CMD ["/app/bin/w3", "start"]
