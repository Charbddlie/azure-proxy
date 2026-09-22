#!/usr/bin/env bash
# Restart routing and wait for serving to accept the new mapping.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
exec "$PYTHON" -m proxy.manage restart routing "$@"
