#!/usr/bin/env bash
# Start the proxy in the background. Refuses if one is already running.
#
# Refuses rather than reporting success and doing nothing, which is what this
# used to do. "already running, exit 0" reads as "your new settings are live" to
# every script that checks the exit code, and they were not — configuration is
# read once, at startup. Use ./restart.sh to pick up an edit.
#
# To watch it once it is up: ./tui.sh
set -euo pipefail

cd "$(dirname "$0")"
source ./preflight.sh

read_endpoint
refuse_if_online
preflight

# The pidfile is written by the proxy itself (see proxy/server.py::write_pidfile),
# which lets it drop the file again on a clean exit. $! is kept only to notice a
# process that dies before it can write one.
nohup $PYTHON -m proxy >> $LOGFILE 2>&1 &
CHILD=$!

STATUS=0
wait_for_health "$CHILD" || STATUS=$?

if [[ $STATUS == 0 ]]; then
    echo "started (pid $(cat $PIDFILE 2>/dev/null || echo "$CHILD")) on http://$HOST:$PORT"
    curl -fsS "http://$HOST:$PORT/healthz"; echo
    exit 0
fi

if [[ $STATUS == 2 ]]; then
    echo "process died on startup; last lines of $LOGFILE:" >&2
    tail -20 $LOGFILE >&2
else
    echo "did not become healthy within 20s; see $LOGFILE" >&2
fi
rm -f $PIDFILE
exit 1
