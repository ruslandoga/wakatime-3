#########
# BUILD #
#########

FROM hexpm/elixir:1.20.2-erlang-29.0.3-alpine-3.24.1 AS build

# install build dependencies
RUN apk add --no-cache --update git build-base

# prepare build dir
RUN mkdir /app
WORKDIR /app

# install hex + rebar
RUN mix local.hex --force && mix local.rebar --force

# set build ENV
ENV MIX_ENV=prod

# install mix dependencies
COPY mix.exs mix.lock ./
COPY config/config.exs config/
RUN mix deps.get
RUN mix deps.compile

# build project
COPY lib lib
RUN mix compile
COPY config/runtime.exs config/

# build release
RUN mix release

#######
# APP #
#######

FROM alpine:3.24.1 AS app

RUN adduser -S -H -u 999 -G nogroup wakatime
RUN apk add --no-cache --update openssl libgcc libstdc++ ncurses
RUN mkdir -p /data && chmod ugo+rw -R /data
USER 999
WORKDIR /app
ENV HOME=/app
ENV DATA_PATH=/data
VOLUME /data

CMD ["/app/bin/w3", "start"]
