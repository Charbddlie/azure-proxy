# .az-identity/

**The proxy will not start until you put two files in here.**

This is the Azure CLI configuration directory the proxy runs as. It is not your
personal `~/.azure`, and it must not be: the proxy runs as whatever principal
the endpoints in `settings/endpoints.yaml` are granted to, and your own
`az login` must not be able to swap that identity out from under it — nor the
other way round.

## The two files

Measured, not guessed: with exactly these two present and nothing else, `az
account get-access-token` succeeds; with either one missing it fails.

| file | what it is | without it |
|---|---|---|
| `azureProfile.json` | which account, tenant and subscription are active | `ERROR: Please run 'az login' to setup account.` |
| `msal_token_cache.json` | **the refresh token** | ``ERROR: User '…' does not exist in MSAL token cache. Run `az login`.`` |

Everything else the Azure CLI leaves in a config directory — `az.sess`,
`commandIndex.json`, `telemetry/`, `logs/`, `versionCheck.json`,
`msal_http_cache.bin`, `cliextensions/` — is cache, telemetry and command
metadata. The CLI regenerates all of it on first run. Do not bother copying it,
and do not worry when it appears.

## Getting them here

If the account is already logged in somewhere on this machine — your own
`~/.azure`, or anywhere `$AZURE_CONFIG_DIR` points — take it from there:

```bash
./import-identity.sh sc-1234567@microsoft.com
```

That copies **only that account's** material in and proves it can mint a token
before it returns. Do not `cp` the files across by hand; see "one login is
often several identities" below for what that gets you.

Otherwise, log in directly. This writes both files here rather than to
`~/.azure`:

```bash
./az.sh login --use-device-code
```

`az.sh` is a one-line wrapper that points `AZURE_CONFIG_DIR` at this directory.
Use it for every `az` command that concerns the proxy. **Bare `az` will not
work** — it reads and writes your personal `~/.azure` and will report a
perfectly healthy login that this proxy cannot see.

Sign in as the service principal or account the deployments are granted to, not
as yourself. `auth.expected_account` in `settings/policy.yaml` records who that
should be; `./start.sh` warns loudly at boot if the two disagree, because a
valid token for the wrong identity is the failure that looks like success —
startup is clean and every single request comes back 401 or 403.

## One login is often several identities

`msal_token_cache.json` is indexed by `home_account_id`, and a `~/.azure` that
has been used for more than one `az login` holds a refresh token for each of
them. The one this project was first set up from held two.

So copying the two files wholesale imports live credentials for every account
the source had ever logged in as, into a directory that everyone operating the
proxy can read — and the result is indistinguishable from a single clean login.
Nothing warns you. `./import-identity.sh` filters by `home_account_id` instead,
and reports which accounts it left behind.

## This directory must stay writable

The CLI writes its caches here on every invocation, and MSAL rewrites
`msal_token_cache.json` itself whenever the refresh token is exchanged. A
read-only mount, or a directory owned by someone other than the account running
the proxy, produces token failures that look like network problems.

## What is in here is a credential

`msal_token_cache.json` holds a refresh token. Anyone who can read this
directory can mint access tokens for that identity **anywhere** — not only
through this proxy, and not only for the endpoints it happens to route to. That
is the whole security boundary of this project.

`.gitignore` keeps everything in here out of the repository except this README.
That rule is the only thing standing between a refresh token and your git
history, so do not relax it and do not `git add -f` anything in this directory.

If several people operate one deployment, all of them can read this file. There
is no arrangement where sharing operation does not also share the identity — so
choose the identity accordingly, and prefer one whose blast radius you are
comfortable with.

## Rotating or revoking

Delete both files and run `./az.sh login --use-device-code` again. Nothing else
in the tree caches the credential: the proxy holds an access token in memory
with a background refresh, so a restart is enough to pick up a new identity.
