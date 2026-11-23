# syntax=docker/dockerfile:1
# Образ Go-бэкенда (только API). Админку собирает и раздаёт отдельный образ
# Caddy из web/Dockerfile.

# 1. Сборка бинарника (чистый Go, без CGO благодаря modernc sqlite)
FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/miner ./cmd/miner \
 && mkdir -p /out/data

# 2. Небольшой runtime от непривилегированного пользователя. (Alpine вместо distroless:
#    gcr.io доступен не из всех сетей, а wget даёт нам healthcheck.)
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -H -u 10001 miner
COPY --from=build /out/miner /usr/local/bin/miner
COPY --from=build --chown=miner:miner /out/data /data
USER miner
ENV MINER_ADDR=:8080 \
    MINER_DB=/data/miner.db
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["miner"]
