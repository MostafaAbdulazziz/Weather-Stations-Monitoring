#!/bin/bash

BASE_URL="http://localhost:8080/bitcask"
export BASE_URL

PYTHON_BIN="${PYTHON_BIN:-python3}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT_PY="$SCRIPT_DIR/bitcask_client.py"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "Error: python3 not found. Set PYTHON_BIN to a valid interpreter." >&2
    exit 1
fi

exec "$PYTHON_BIN" "$CLIENT_PY" "$@"
