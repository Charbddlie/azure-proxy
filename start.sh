#!/usr/bin/env bash
# start [serving|routing|all]; default: all.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
exec "$PYTHON" -m proxy.manage start "$@"
