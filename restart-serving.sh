#!/usr/bin/env bash
# Warm up a new serving worker, switch traffic, and drain the old worker.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
exec "$PYTHON" -m proxy.manage restart serving "$@"
