# syntax=docker/dockerfile:1

# 1. Сборка админки → dist
FROM node:24-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY . .
RUN npm run build

# 2. Caddy: статика + авторизация + прокси на Go-бэкенд
FROM caddy:2.11.9-alpine
COPY Caddyfile /etc/caddy/Caddyfile
COPY caddy-entrypoint.sh /usr/local/bin/caddy-entrypoint.sh
COPY --from=build /src/dist /srv
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["caddy-entrypoint.sh"]
