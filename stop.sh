#!/usr/bin/env bash
# Stop the proxy. Exits 0 whether or not it was running.
#
# Nothing here knows about the dashboard, and it does not need to: the
# dashboard is a separate read-only process that watches over HTTP. Stopping
# the proxy leaves it running and showing an unreachable banner, which is the
# correct thing for it to show.
set -euo pipefail

cd "$(dirname "$0")"
PIDFILE=.proxy.pid

# `kill -0` is not enough on its own: between exiting and being reaped by its
# parent a process is a zombie, which still has a pid entry, so `kill -0`
# succeeds on it. Waiting on that answer makes stop.sh sit out its whole ten
# seconds and then report that a process which had already exited was
# "ignoring SIGTERM".
still_running() {
    kill -0 "$1" 2>/dev/null || return 1
    local state
    state=$(ps -o state= -p "$1" 2>/dev/null | tr -d ' ')
    [[ $state != Z* ]]
}

if [[ ! -f $PIDFILE ]]; then
    echo "not running (no $PIDFILE)"
    exit 0
fi

PID=$(cat $PIDFILE)
if ! still_running "$PID"; then
    echo "stale pidfile for pid $PID, removing"
    rm -f $PIDFILE
    exit 0
fi

kill "$PID"
for _ in $(seq 20); do
    if ! still_running "$PID"; then
        rm -f $PIDFILE
        echo "stopped (pid $PID)"
        exit 0
    fi
    sleep 0.5
done

echo "pid $PID ignored SIGTERM, sending SIGKILL" >&2
kill -9 "$PID" 2>/dev/null || true
rm -f $PIDFILE
echo "killed (pid $PID)"
