#!/bin/sh
set -eu

REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_ENV="${REDIS_ENV:-dev}"
LIST_MAXLEN="${LIST_MAXLEN:-1000000}"

auth_args=""
if [ -n "$REDIS_PASSWORD" ]; then
  auth_args="-a $REDIS_PASSWORD"
fi

# Klucze list (wersjonowane)
INGEST_LIST="${REDIS_ENV}:queue:ingest:v1"
EMBED_LIST="${REDIS_ENV}:queue:embed:v1"
UPSERT_LIST="${REDIS_ENV}:queue:upsert:v1"

# Lista wszystkich list do inicjalizacji
lists="$INGEST_LIST $EMBED_LIST $UPSERT_LIST"

echo "[init] Creating Redis lists (env=$REDIS_ENV, maxlen~$LIST_MAXLEN)..."

for list_key in $lists; do
  dlq_key="${list_key}:dlq"

  # 0) Jeżeli klucz istnieje, upewnij się że jest LIST, w przeciwnym razie przenieś jako backup
  key_type=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    TYPE "$list_key")
  if [ "$key_type" != "none" ] && [ "$key_type" != "list" ]; then
    backup_key="${list_key}:backup:${key_type}:$(date +%s)"
    echo "[init] WARN: $list_key has type=$key_type; renaming to $backup_key"
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
      RENAME "$list_key" "$backup_key" >/dev/null 2>&1 || true
  fi

  # 1) Upewnij się, że lista istnieje (dodaj dummy element i usuń go)
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    LPUSH "$list_key" "_init_dummy" >/dev/null 2>&1 || true
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    LREM "$list_key" 1 "_init_dummy" >/dev/null 2>&1 || true

  # 2) Utwórz DLQ listę (dead letter queue) – upewnij się że to LIST
  dlq_type=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    TYPE "$dlq_key")
  if [ "$dlq_type" != "none" ] && [ "$dlq_type" != "list" ]; then
    dlq_backup_key="${dlq_key}:backup:${dlq_type}:$(date +%s)"
    echo "[init] WARN: $dlq_key has type=$dlq_type; renaming to $dlq_backup_key"
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
      RENAME "$dlq_key" "$dlq_backup_key" >/dev/null 2>&1 || true
  fi
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    LPUSH "$dlq_key" "_init_dummy" >/dev/null 2>&1 || true
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
    LREM "$dlq_key" 1 "_init_dummy" >/dev/null 2>&1 || true

  # 3) Ustaw TTL dla pustych list (opcjonalne - Redis automatycznie usuwa puste listy)
  # redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" $auth_args \
  #   EXPIRE "$list_key" 86400 >/dev/null 2>&1 || true

  echo "[init] ok: $list_key, dlq=$dlq_key"
done

echo "[init] Done. Lists ready for LPUSH/BRPOP operations."