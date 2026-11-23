#!/bin/sh
# Запуск Caddy: пароль админки приходит из .env в открытом виде, а basic_auth
# в Caddy принимает только bcrypt-хэш. Считаем хэш при старте — так пароль
# хранится в одном месте и меняется правкой .env и перезапуском.
set -eu

: "${MINER_ADMIN_PASSWORD:?задайте MINER_ADMIN_PASSWORD в .env}"
ADMIN_PASSWORD_HASH="$(caddy hash-password --plaintext "$MINER_ADMIN_PASSWORD")"
export ADMIN_PASSWORD_HASH
unset MINER_ADMIN_PASSWORD # самому Caddy открытый пароль не нужен

exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
