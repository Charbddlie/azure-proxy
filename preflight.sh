#!/usr/bin/env bash
# Shared checks. Sourced by start.sh and start_tui.sh — never run alone.
#
# The two scripts want opposite answers to the same question. start.sh must
# refuse to start a second proxy; start_tui.sh is only an observer and must
# refuse to run when there is nothing to observe. Asking it in one place is
# what keeps the two answers consistent.

# The interpreter lives in the checkout, so it travels with it. Every caller
# has already cd'd to the repo root, which is what makes the relative path
# safe — and what makes moving or copying the whole tree a complete move.
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"
PIDFILE=.proxy.pid
LOGFILE=proxy.log

# Where the proxy will listen, read from the same file it reads.
#
# The two prerequisites come first, and this is the earliest anything needs
# either: every caller reads the endpoint before it does anything else, so a
# fresh clone would otherwise meet `./.venv/bin/python: No such file or
# directory` here — exit 127 from the shell, with none of the messages below
# ever getting a chance to say which command to run.
read_endpoint() {
    require_settings
    check_python
    HOST=$($PYTHON -c "import yaml;print(yaml.safe_load(open('settings/policy.yaml'))['server']['host'])")
    PORT=$($PYTHON -c "import yaml;print(yaml.safe_load(open('settings/policy.yaml'))['server']['port'])")
}

# Is a proxy already up? Two independent tests, because either alone lies.
#
# A live pid in the pidfile is the ordinary case. The health check catches the
# one that matters more: a proxy someone started by hand, or one whose pidfile
# was removed. Starting a second process on a bound port fails with a traceback
# about the socket; starting one that somehow succeeds would mean two proxies
# with two separate load ledgers spending the same quota, each believing it is
# alone. That is the failure this check exists to prevent.
proxy_online() {
    if [[ -f $PIDFILE ]] && kill -0 "$(cat $PIDFILE 2>/dev/null)" 2>/dev/null; then
        ONLINE_WHY="pid $(cat $PIDFILE) from $PIDFILE"
        return 0
    fi
    if curl -fsS -m 2 "http://$HOST:$PORT/healthz" >/dev/null 2>&1; then
        ONLINE_WHY="something is already answering on http://$HOST:$PORT"
        return 0
    fi
    return 1
}

refuse_if_online() {
    if proxy_online; then
        echo "already running — $ONLINE_WHY" >&2
        echo >&2
        echo "  ./stop.sh      then start it again" >&2
        echo "  ./restart.sh   stop and start again in one step" >&2
        exit 1
    fi
    # A pidfile whose process is gone is debris from a crash, not a claim.
    rm -f $PIDFILE
}

# The mirror, for the dashboard. It starts nothing, so an offline proxy is not
# something it can work around — and a dashboard that came up anyway would
# spend its first screen saying "unreachable" about a proxy that was never
# asked to run, which reads like a fault rather than an instruction.
require_online() {
    if proxy_online; then
        return 0
    fi
    echo "the proxy is not running — there is nothing to watch" >&2
    echo "  (checked $PIDFILE and http://$HOST:$PORT/healthz)" >&2
    echo >&2
    echo "  ./start.sh     start it in the background, then run this again" >&2
    exit 1
}

require_rich() {
    if ! $PYTHON -c "import rich" 2>/dev/null; then
        echo "the dashboard needs rich, which is not installed" >&2
        echo "  $PYTHON -m pip install rich" >&2
        exit 1
    fi
}

# The settings the deployer owns. Shipped as templates only — endpoints.yaml
# names your subscriptions, policy.yaml is your tuning — so a fresh clone has
# neither, and the failure without this check is a Python traceback from inside
# an import rather than a sentence saying which file to copy.
require_settings() {
    local missing=0
    for f in endpoints policy; do
        if [[ ! -f settings/$f.yaml ]]; then
            if [[ $missing == 0 ]]; then
                echo "settings/ has not been set up yet" >&2
                echo >&2
            fi
            echo "  cp settings/$f.yaml.template settings/$f.yaml" >&2
            missing=1
        fi
    done
    if [[ $missing == 1 ]]; then
        echo >&2
        echo "then edit settings/endpoints.yaml to name your own Azure resources." >&2
        exit 1
    fi
}

check_python() {
    if [[ ! -x $PYTHON ]]; then
        echo "no interpreter at $PYTHON" >&2
        echo "the virtualenv lives in the checkout; create it with:" >&2
        echo "  python3 -m venv .venv" >&2
        echo "  .venv/bin/pip install -r requirements.txt" >&2
        echo "or point AZURE_PROXY_PYTHON at one you already have" >&2
        exit 1
    fi
    if ! $PYTHON -c "import fastapi, uvicorn, httpx, yaml, azure.identity" 2>/dev/null; then
        echo "$PYTHON is missing dependencies" >&2
        echo "  $PYTHON -m pip install -r requirements.txt" >&2
        exit 1
    fi
}

# The two files the identity actually consists of. Checked by name, before
# anything shells out to `az`, because an empty az-identity/ is the single most
# likely thing to be wrong on a fresh deployment and `az`'s own message for it
# ("Please run 'az login'") does not mention this directory, this proxy, or the
# fact that a personal `az login` will not help.
require_identity() {
    local dir missing
    dir=$($PYTHON -c "import yaml;print(yaml.safe_load(open('settings/policy.yaml'))['auth'].get('az_config_dir') or '')")
    [[ -n $dir ]] || return 0

    missing=""
    for f in azureProfile.json msal_token_cache.json; do
        [[ -f "$dir/$f" ]] || missing="$missing $f"
    done
    if [[ -n $missing ]]; then
        echo "the proxy has no identity to run as: $dir/ is missing$missing" >&2
        echo >&2
        echo "if you are already logged in as that account somewhere:" >&2
        echo "  ./import-identity.sh <account>@example.com" >&2
        echo >&2
        echo "if not, log in first — this writes to $dir/, not to ~/.azure:" >&2
        echo "  ./az.sh login --use-device-code" >&2
        echo >&2
        echo "use the account in auth.expected_account, not your own." >&2
        echo "see $dir/README.md" >&2
        exit 1
    fi
}

preflight() {
    require_settings
    check_python
    require_identity

    # runtime/ is the probe's output and is deliberately not in git, so a fresh
    # clone reaches this line with nothing to route by. Checked before the token
    # check because it costs nothing, while that one shells out to `az`. Without
    # this you get a FileNotFoundError traceback from inside the import — which
    # says which file is missing but not that `probe.py` is the thing that writes it.
    for f in runtime/models.json runtime/sources.json; do
        if [[ ! -f $f ]]; then
            echo "missing $f — the proxy has no deployment table to route by" >&2
            echo "run the probe first: $PYTHON probe/probe.py" >&2
            echo "(it needs the proxy's own az login; see ./az.sh)" >&2
            exit 1
        fi
    done

    # Fail here rather than on the first request if the AD login has lapsed. Note
    # ./az.sh, not `az`: the proxy runs on a borrowed identity kept in its own
    # credential directory, so your personal `az login` cannot swap it out.
    if ! ./az.sh account get-access-token --resource https://cognitiveservices.azure.com \
            -o none 2>/dev/null; then
        echo "no Azure token for the proxy's own credentials" >&2
        echo "run: ./az.sh login --use-device-code" >&2
        echo "     (sign in as the account in auth.expected_account, not as yourself)" >&2
        exit 1
    fi

    # A valid token for the wrong identity is the failure that looks like success:
    # startup is clean and every deployment returns 401 or 403. Say so here, where
    # the fix is one command away. A warning, not an error — a changed
    # expected_account should not keep a working proxy from booting.
    EXPECTED=$($PYTHON -c "import yaml;print(yaml.safe_load(open('settings/policy.yaml'))['auth'].get('expected_account') or '')")
    ACTUAL=$(./az.sh account show --query user.name -o tsv 2>/dev/null || true)
    if [[ -n $EXPECTED && $ACTUAL != "$EXPECTED" ]]; then
        echo "warning: proxy is logged in as '${ACTUAL:-<none>}', expected '$EXPECTED'" >&2
        echo "         the deployments are granted to $EXPECTED; others get 401/403" >&2
        echo "         fix: ./az.sh login --use-device-code" >&2
    else
        echo "azure identity: $ACTUAL"
    fi
}

# Poll until the proxy answers, or until it dies. $1 is a pid to watch, or 0.
wait_for_health() {
    local watch=${1:-0} tries=${2:-40}
    for _ in $(seq "$tries"); do
        if curl -fsS -m 2 "http://$HOST:$PORT/healthz" >/dev/null 2>&1; then
            return 0
        fi
        if [[ $watch != 0 ]] && ! kill -0 "$watch" 2>/dev/null; then
            return 2
        fi
        sleep 0.5
    done
    return 1
}

