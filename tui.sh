#!/usr/bin/env bash
# Open the independent dashboard, including while serving is offline.
#
# This starts nothing and stops nothing. It is a read-only view over the
# proxy's own HTTP surface (/healthz, /routes, /events), so it can be opened
# and closed as often as you like without touching the service — and closing
# the terminal it is in takes the dashboard with it and nothing else.
#
# The proxy itself is managed by the other three scripts:
#
#   ./start.sh     start it in the background
#   ./stop.sh      stop it
#   ./restart.sh   restart routing after editing its policy or re-probing
#
# `python -m tui --attach` is the same thing without the checks below, and
# takes --url for another proxy and --scroll-lines N for wheel sensitivity.
set -euo pipefail

cd "$(dirname "$0")"
source ./preflight.sh

INTERVAL=1.0
URL=""
SCROLL_LINES=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)      URL="$2"; shift ;;
        --interval) INTERVAL="$2"; shift ;;
        --scroll-lines) SCROLL_LINES="$2"; shift ;;
        -h|--help)  sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
    shift
done

check_python
require_rich

ARGS=(--interval "$INTERVAL")
if [[ -n $URL ]]; then
    ARGS+=(--url "$URL")
fi
if [[ -n $SCROLL_LINES ]]; then
    ARGS+=(--scroll-lines "$SCROLL_LINES")
fi
exec "$PYTHON" -m tui "${ARGS[@]}"
