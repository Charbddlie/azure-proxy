#!/usr/bin/env bash
# Run the Azure CLI against the proxy's own config directory.
#
# The proxy does not run on your Azure account. It borrows svc-account, the
# identity the deployments in settings/endpoints.yaml are granted to, and keeps
# that login in a directory of its own (auth.az_config_dir in
# settings/policy.yaml). This wrapper is how you reach it. Use it for every az
# command that concerns the proxy; bare `az` talks to your personal ~/.azure and
# will not help.
#
#   ./az.sh login --use-device-code
#   ./az.sh account set --subscription "Advanced Machine Learning"
#   ./az.sh account show --query user.name -o tsv
#
# Logging in here does not disturb your personal `az login`, and your personal
# login does not disturb this one. Sign in as svc-account@example.com, not as
# yourself — see auth.expected_account in settings/policy.yaml.
set -euo pipefail

cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"

if [[ ! -x $PYTHON ]]; then
    # Only needed to read one YAML key; any python with pyyaml will do.
    PYTHON=python3
fi

# Resolved to an absolute path here rather than passed through relative, because
# `az` is exec'd below and inherits AZURE_CONFIG_DIR — but not this script's
# working directory in any way it is obliged to keep. The same rule as
# Config.az_config_dir in proxy/server.py: relative means relative to the repo.
AZURE_CONFIG_DIR=$($PYTHON - <<'EOF'
import os, yaml
policy = yaml.safe_load(open("settings/policy.yaml"))
path = os.path.expanduser(
    policy["auth"].get("az_config_dir") or os.path.join("~", ".azure"))
print(os.path.abspath(path))
EOF
)
export AZURE_CONFIG_DIR

if [[ $# -eq 0 ]]; then
    echo "AZURE_CONFIG_DIR=$AZURE_CONFIG_DIR"
    echo "usage: ./az.sh <az arguments>    e.g. ./az.sh login --use-device-code"
    exit 0
fi

exec az "$@"
