#!/usr/bin/env bash
# =============================================================================
# MaxKB MCP Server — plain docker launcher (no docker compose required)
# -----------------------------------------------------------------------------
# Reads the same .env file as docker-compose.yml, then starts the containers
# with plain `docker run`.
#
#   ./start.sh all      start both services (default)
#   ./start.sh chat     start the agent chat service only
#   ./start.sh kb       start the knowledge base service only
#   ./start.sh stop     stop and remove both containers
#   ./start.sh status   show container status and health endpoints
#
# Prerequisite: images are available locally, either built from source
#   docker build -t maxkb-mcp-chat:2.0.0      maxkb-chat-mcp
#   docker build -t maxkb-mcp-knowledge:2.0.0 maxkb-knowledge-mcp
# or loaded from a prebuilt bundle:
#   gunzip -c maxkb-mcp-chat-2.0.0.tar.gz      | docker load
#   gunzip -c maxkb-mcp-knowledge-2.0.0.tar.gz | docker load
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

IMAGE_CHAT="${IMAGE_CHAT:-maxkb-mcp-chat:2.0.0}"
IMAGE_KB="${IMAGE_KB:-maxkb-mcp-knowledge:2.0.0}"
NAME_CHAT="${NAME_CHAT:-maxkbv2-mcp-app}"
NAME_KB="${NAME_KB:-maxkbv2-knowledge-mcp-app}"

# load the shared .env (same file docker compose uses)
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
CHAT_PORT="${CHAT_PORT:-18010}"
KB_PORT="${KB_PORT:-18011}"

require_image() {
  if ! docker image inspect "$1" >/dev/null 2>&1; then
    echo "ERROR: image $1 not found locally."
    echo "Build it (docker build -t $1 <dir>) or load it first (gunzip -c <tar.gz> | docker load)."
    exit 1
  fi
}

start_chat() {
  require_image "$IMAGE_CHAT"
  docker rm -f "$NAME_CHAT" 2>/dev/null || true
  # variable-name mapping: .env (user facing) -> container env (server code)
  docker run -d --name "$NAME_CHAT" --restart always \
    -p "${CHAT_PORT}:8000" \
    -e BASE_URL="${MAXKB_BASE_URL:-}" \
    -e API_KEY="${CHAT_API_KEY:-}" \
    -e CHAT_AGENTS="${CHAT_AGENTS:-}" \
    -e SERVER_NAME="${CHAT_SERVER_NAME:-}" \
    -e TOOL_NAME="${CHAT_TOOL_NAME:-}" \
    -e TOOL_DESCRIPTION="${CHAT_TOOL_DESCRIPTION:-}" \
    -e MAXKB_CHAT_PREFIX="${MAXKB_CHAT_PREFIX:-}" \
    -e MAXKB_TIMEOUT="${CHAT_TIMEOUT:-}" \
    -e MCP_AUTH_TOKEN="${MCP_AUTH_TOKEN:-}" \
    -e MCP_ALLOWED_HOSTS="${MCP_ALLOWED_HOSTS:-}" \
    "$IMAGE_CHAT"
  echo "chat      -> http://0.0.0.0:${CHAT_PORT}/mcp  (health /health)"
}

start_kb() {
  require_image "$IMAGE_KB"
  docker rm -f "$NAME_KB" 2>/dev/null || true
  mkdir -p ./data
  docker run -d --name "$NAME_KB" --restart always \
    -p "${KB_PORT}:8000" \
    -v "$(pwd)/data:/app/data" \
    -e BASE_URL="${MAXKB_BASE_URL:-}" \
    -e MAXKB_API_KEY="${KB_API_KEY:-}" \
    -e MAXKB_USERNAME="${KB_USERNAME:-}" \
    -e MAXKB_PASSWORD="${KB_PASSWORD:-}" \
    -e MAXKB_WORKSPACE_ID="${KB_WORKSPACE_ID:-}" \
    -e MAXKB_KNOWLEDGE_ID="${KB_KNOWLEDGE_ID:-}" \
    -e SERVER_NAME="${KB_SERVER_NAME:-}" \
    -e MAXKB_TOP_NUMBER="${KB_TOP_NUMBER:-}" \
    -e MAXKB_SIMILARITY="${KB_SIMILARITY:-}" \
    -e MAXKB_SEARCH_MODE="${KB_SEARCH_MODE:-}" \
    -e MAXKB_CONTEXT_NEIGHBORS="${KB_CONTEXT_NEIGHBORS:-}" \
    -e MAXKB_CONTEXT_MAX_CHARS="${KB_CONTEXT_MAX_CHARS:-}" \
    -e MAXKB_ADMIN_PREFIX="${MAXKB_ADMIN_PREFIX:-}" \
    -e MAXKB_TIMEOUT="${KB_TIMEOUT:-}" \
    -e MCP_AUTH_TOKEN="${MCP_AUTH_TOKEN:-}" \
    -e MCP_ALLOWED_HOSTS="${MCP_ALLOWED_HOSTS:-}" \
    "$IMAGE_KB"
  echo "knowledge -> http://0.0.0.0:${KB_PORT}/mcp  (health /health)"
}

stop_all() {
  docker rm -f "$NAME_CHAT" "$NAME_KB" 2>/dev/null || true
  echo "stopped."
}

show_status() {
  docker ps -a --filter "name=${NAME_CHAT}" --filter "name=${NAME_KB}" \
    --format '{{.Names}} | {{.Image}} | {{.Status}} | {{.Ports}}'
  command -v curl >/dev/null 2>&1 || return 0
  for p in "$CHAT_PORT" "$KB_PORT"; do
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 "http://127.0.0.1:${p}/health" || true)
    echo "http://127.0.0.1:${p}/health -> ${code:-no-response}"
  done
}

case "${1:-all}" in
  chat)   start_chat ;;
  kb)     start_kb ;;
  all)    start_chat; start_kb ;;
  stop)   stop_all ;;
  status) show_status ;;
  *)      echo "usage: $0 [all|chat|kb|stop|status]"; exit 1 ;;
esac
