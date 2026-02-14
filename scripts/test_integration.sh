#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI is required for integration tests"
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required for integration tests"
  exit 1
fi

pushd "$ROOT_DIR" >/dev/null
mix test --only integration
popd >/dev/null

echo "integration tests complete"