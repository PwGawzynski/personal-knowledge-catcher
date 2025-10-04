#!/usr/bin/env bash
set -euo pipefail

# cd to repo directory (script can be run from anywhere)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# load .env
if [[ -f ".env" ]]; then
  set -a; source ".env"; set +a
else
  echo "Missing .env – copy .env.sample to .env and fill in the values."
  exit 1
fi

# retry and reaper state
: "${RETRY_LIMIT:=5}"
STATE_DIR="/state"
RETRY_FILE="$STATE_DIR/retry_count"
mkdir -p "$STATE_DIR" 2>/dev/null || true

read_retry_count() {
  if [[ -f "$RETRY_FILE" ]]; then
    cat "$RETRY_FILE" 2>/dev/null || echo 0
  else
    echo 0
  fi
}

write_retry_count() {
  echo "$1" > "$RETRY_FILE" 2>/dev/null || true
}

reset_retry_count() {
  : > "$RETRY_FILE" 2>/dev/null || true
}

docker_api_post() {
  local METHOD="$1"; shift
  local PATH="$1"; shift
  curl -s -o /dev/null --unix-socket /var/run/docker.sock -X "$METHOD" "http://localhost$PATH" || true
}

reaper_stop_stack() {
  echo "Max retries reached ($RETRY_LIMIT). Stopping containers 'qdrant' and 'qdrant-init'."
  # try graceful stop first, then kill
  docker_api_post POST "/containers/qdrant/stop?timeout=10"
  docker_api_post POST "/containers/qdrant/kill"
  docker_api_post POST "/containers/qdrant-init/stop?timeout=10"
  docker_api_post POST "/containers/qdrant-init/kill"
}

fail_with_retry() {
  local MSG="$1"
  echo "$MSG"
  local CURRENT
  CURRENT=$(read_retry_count)
  local NEXT=$((CURRENT + 1))
  write_retry_count "$NEXT"
  echo "Attempt $NEXT/$RETRY_LIMIT failed."
  if [[ "$NEXT" -ge "$RETRY_LIMIT" ]]; then
    reaper_stop_stack
  fi
  exit 1
}

# When running inside Docker, localhost points to this container, not Qdrant.
# If QDRANT_HOST is empty or points to localhost, switch to the service name.
if [[ -f "/.dockerenv" ]]; then
  if [[ -z "${QDRANT_HOST:-}" || "$QDRANT_HOST" =~ ^https?://(localhost|127\.0\.0\.1)(:|/) ]]; then
    echo "Adjusting QDRANT_HOST for Docker network → http://qdrant:6333"
    QDRANT_HOST="http://qdrant:6333"
  fi
fi

# sanity
: "${QDRANT_API_KEY:?Missing QDRANT_API_KEY}"
: "${QDRANT_HOST:?Missing QDRANT_HOST}"
: "${COLLECTION_NAME:?Missing COLLECTION_NAME}"
: "${VECTORS_MODE:?Missing VECTORS_MODE}"

# wait until server is up
echo "Waiting for $QDRANT_HOST/readyz ..."
for i in {1..120}; do
  if curl -fsS "$QDRANT_HOST/readyz" >/dev/null; then
    echo "Qdrant ready."
    break
  fi
  sleep 1
  if [[ $i -eq 120 ]]; then
    fail_with_retry "Timeout waiting for Qdrant."
  fi
done

# build collection payload
if [[ "$VECTORS_MODE" == "single" ]]; then
  : "${VECTOR_SIZE:?Missing VECTOR_SIZE}"
  : "${VECTOR_DISTANCE:?Missing VECTOR_DISTANCE}"
  VECTORS_JSON=$(cat <<EOF
{ "size": $VECTOR_SIZE, "distance": "$VECTOR_DISTANCE" }
EOF
)
elif [[ "$VECTORS_MODE" == "named" ]]; then
  : "${NAMED_VECTORS_JSON:?Missing NAMED_VECTORS_JSON}"
  VECTORS_JSON=$(cat <<EOF
$NAMED_VECTORS_JSON
EOF
)
else
  echo "VECTORS_MODE must be 'single' or 'named'."; exit 1
fi

: "${HNSW_M:=16}"
: "${HNSW_EF_CONSTRUCT:=100}"
: "${INDEXING_THRESHOLD:=10000}"

CREATE_PAYLOAD=$(cat <<JSON
{
  "vectors": $VECTORS_JSON,
  "hnsw_config": { "m": $HNSW_M, "ef_construct": $HNSW_EF_CONSTRUCT },
  "optimizers_config": { "indexing_threshold": $INDEXING_THRESHOLD }
}
JSON
)

# ensure collection exists: check first, then create if missing
echo "Ensuring collection '$COLLECTION_NAME' exists..."
CHECK_CODE=$(curl -s -o /tmp/qdrant_check_out.json -w "%{http_code}" \
  -H "api-key: $QDRANT_API_KEY" \
  "$QDRANT_HOST/collections/$COLLECTION_NAME") || CHECK_CODE="000"

if [[ "$CHECK_CODE" == "200" ]]; then
  echo "Collection exists."
elif [[ "$CHECK_CODE" == "404" ]]; then
  echo "Collection not found. Creating..."
  CREATE_CODE=$(curl -s -o /tmp/qdrant_init_out.json -w "%{http_code}" \
    -X PUT "$QDRANT_HOST/collections/$COLLECTION_NAME" \
    -H "Content-Type: application/json" \
    -H "api-key: $QDRANT_API_KEY" \
    -d "$CREATE_PAYLOAD") || CREATE_CODE="000"
  if [[ "$CREATE_CODE" -ge 400 || "$CREATE_CODE" == "000" ]]; then
    echo "Creation error ($CREATE_CODE):"; cat /tmp/qdrant_init_out.json || true
    fail_with_retry "Failed to create collection '$COLLECTION_NAME'."
  fi
  echo "Collection created."
else
  echo "Unexpected response while checking collection ($CHECK_CODE):"; cat /tmp/qdrant_check_out.json || true
  fail_with_retry "Failed to verify collection '$COLLECTION_NAME'."
fi

reset_retry_count
echo "OK. Collection '$COLLECTION_NAME' ready."

# create payload indexes for common metadata fields (idempotent-ish)
create_index() {
  local FIELD_NAME="$1"
  local FIELD_SCHEMA="$2"
  echo "Creating payload index: $FIELD_NAME ($FIELD_SCHEMA)"
  local RESP_CODE
  RESP_CODE=$(curl -s -o /tmp/qdrant_idx_out.json -w "%{http_code}" \
    -X PUT "$QDRANT_HOST/collections/$COLLECTION_NAME/index" \
    -H "Content-Type: application/json" \
    -H "api-key: $QDRANT_API_KEY" \
    -d "{\"field_name\": \"$FIELD_NAME\", \"field_schema\": \"$FIELD_SCHEMA\"}")
  if [[ "$RESP_CODE" -ge 400 ]]; then
    echo "Index creation error for '$FIELD_NAME' ($RESP_CODE):"; cat /tmp/qdrant_idx_out.json || true
  else
    echo "Index '$FIELD_NAME' ensured."
  fi
}

# Keyword-like fields
create_index "user_id" "keyword"
create_index "source" "keyword"
create_index "lang" "keyword"
create_index "model" "keyword"
create_index "source_url" "keyword"
create_index "source_id" "keyword"

# Text field for full-text search
create_index "title" "text"

# Datetime field (ISO8601 strings)
create_index "created_at" "datetime"

# show brief info
echo "Collection summary:"
curl -fsS -H "api-key: $QDRANT_API_KEY" "$QDRANT_HOST/collections/$COLLECTION_NAME" | sed -E 's/","/\n  "/g' || true
