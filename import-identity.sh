#!/usr/bin/env bash
# Copy one account's Azure credentials into this deployment.
#
#   ./import-identity.sh sc-1234567@microsoft.com
#
# Takes the account you name out of a login you already have (~/.azure, or
# $AZURE_CONFIG_DIR) and writes just that account's material into the proxy's
# own config directory. It does not log in for you: if the account is not
# already logged in somewhere on this machine, it says so and stops.
#
#   --from <dir>   take it from a specific config directory
#   --into <dir>   write somewhere other than auth.az_config_dir
#   --force        replace an identity that is already installed
#
# Why this exists rather than `cp ~/.azure/*.json`: one ~/.azure holds every
# account you have ever logged in as, and copying the files whole imports all of
# their refresh tokens into a directory that everyone operating this proxy can
# read. See the module docstring in tools/import_identity.py.
set -euo pipefail

cd "$(dirname "$0")"
PYTHON="${AZURE_PROXY_PYTHON:-./.venv/bin/python}"

if [[ ! -x $PYTHON ]]; then
    # Only needs the stdlib plus pyyaml, so an interpreter that predates the
    # virtualenv will do — this is a setup step, and needing the environment
    # before you can set up the environment would be a poor ordering.
    PYTHON=python3
fi

exec "$PYTHON" tools/import_identity.py "$@"
