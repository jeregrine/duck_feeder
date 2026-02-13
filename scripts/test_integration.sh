#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/dev/docker-compose.integration.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "--down" ]]; then
  cleanup
  echo "integration stack stopped"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for integration tests"
  exit 1
fi

docker compose -f "$COMPOSE_FILE" up -d meta_postgres source_postgres

echo "waiting for meta_postgres to become healthy..."
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" exec -T meta_postgres pg_isready -U duck -d duckfeeder_meta >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "waiting for source_postgres to become healthy..."
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" exec -T source_postgres pg_isready -U duck -d duckfeeder_source >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

export DUCK_FEEDER_META_DATABASE_URL="postgres://duck:duck@localhost:55432/duckfeeder_meta"
export DUCK_FEEDER_SOURCE_DATABASE_URL="postgres://duck:duck@localhost:55433/duckfeeder_source"

pushd "$ROOT_DIR" >/dev/null
mix test --only integration
popd >/dev/null

echo "integration tests complete"
echo "run scripts/test_integration.sh --down to stop containers"
