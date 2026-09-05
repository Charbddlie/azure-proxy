#!/usr/bin/env bash
# restart [serving|routing|all]; default: routing.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
exec "$PYTHON" -m proxy.manage restart "$@"
