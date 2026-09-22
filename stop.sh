#!/usr/bin/env bash
# Drain serving, then stop routing.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
exec "$PYTHON" -m proxy.manage stop all "$@"
