#!/usr/bin/env bash
# Stop, then start again in the background. Use after editing settings/ or
# re-running the probe — configuration is read once, at startup.
#
# The dashboard is unaffected: it is a separate process watching over HTTP, so
# one left open will show an unreachable banner for the second or two the proxy
# is down and then pick the new one up by itself.
set -euo pipefail

cd "$(dirname "$0")"
./stop.sh
./start.sh
