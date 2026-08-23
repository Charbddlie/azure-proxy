"""Copy one account's Azure CLI credentials out of a login you already have.

    ./import-identity.sh sc-1234567@microsoft.com

The proxy runs as a specific principal — the one the deployments in
settings/endpoints.yaml are granted to — and needs that principal's refresh
token in its own config directory. The obvious way to get it there is to copy
`~/.azure/azureProfile.json` and `~/.azure/msal_token_cache.json` across. Do not
do that, and this script exists because of what it does:

    a real ~/.azure on this machine held refresh tokens for TWO accounts,
    other-account and svc-account. The proxy uses one of them.

A whole-file copy therefore imports a live credential for an account the proxy
will never use, into a directory that has to be readable by everyone who
operates the proxy. Nothing announces this; both files look like one login.

So the copy is a filter, not a copy. Every section of the MSAL cache is keyed by
`home_account_id`, which makes the filter exact rather than approximate.

Two details that are easy to get wrong by hand, and are the other half of why
this is a script:

  * `azureProfile.json` is written by the CLI with a UTF-8 BOM. Read it as
    plain utf-8 and json.load raises; write it back without one and you have
    changed a file the CLI wrote.
  * Access tokens are deliberately NOT carried over. They are valid for about an
    hour, so importing one means the first `az account get-access-token`
    succeeds from cache without ever exercising the refresh token — and a
    refresh token that does not work looks fine until the hour is up. Dropping
    them makes the very next call the real test.
"""

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PROFILE = "azureProfile.json"
TOKENS = "msal_token_cache.json"

# Sections of the MSAL cache that are per-account and must be filtered.
# AppMetadata is keyed by client_id/environment and holds no secret, so it is
# carried over whole; anything unrecognised is dropped rather than copied
# blindly, because an unknown section is exactly where a second account's
# secret would survive the filter.
ACCOUNT_SECTIONS = ("Account", "RefreshToken", "IdToken")
COPY_WHOLE = ("AppMetadata",)
DROP = ("AccessToken",)


def read_json(path, bom=False):
    with open(path, encoding="utf-8-sig" if bom else "utf-8") as f:
        return json.load(f)


def write_json(path, data, bom=False):
    """Write, then tighten the mode. Created at 0600 before anything is in it."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8-sig" if bom else "utf-8") as f:
        json.dump(data, f, indent=4)
    os.chmod(path, 0o600)


def candidate_sources(explicit=None):
    """Where a login for this account might already be sitting."""
    if explicit:
        return [os.path.abspath(os.path.expanduser(explicit))]
    seen, out = set(), []
    for path in (os.environ.get("AZURE_CONFIG_DIR"),
                 os.path.expanduser("~/.azure")):
        if not path:
            continue
        path = os.path.abspath(os.path.expanduser(path))
        if path not in seen:
            seen.add(path)
            out.append(path)
    return out


def accounts_in(directory):
    """{username: home_account_id} for every login in this config directory."""
    path = os.path.join(directory, TOKENS)
    if not os.path.isfile(path):
        return {}
    try:
        cache = read_json(path)
    except (ValueError, OSError):
        return {}
    return {entry.get("username", "").lower(): entry.get("home_account_id")
            for entry in (cache.get("Account") or {}).values()
            if entry.get("username")}


def filter_tokens(cache, home_account_id):
    """The MSAL cache with everything but one account's material removed."""
    out = {}
    for section in ACCOUNT_SECTIONS:
        out[section] = {
            key: entry for key, entry in (cache.get(section) or {}).items()
            if entry.get("home_account_id") == home_account_id}
    for section in COPY_WHOLE:
        if section in cache:
            out[section] = cache[section]
    return out


def filter_profile(profile, username):
    """The subscription list with only this account's, one marked default.

    The CLI refuses to work with a profile that has no default subscription, and
    the one that was default in the source may have belonged to the account
    being filtered out. Falling back to the first surviving subscription is
    arbitrary but always valid, and `az account set` can change it afterwards.
    """
    subs = [s for s in (profile.get("subscriptions") or [])
            if (s.get("user") or {}).get("name", "").lower() == username.lower()]
    if subs and not any(s.get("isDefault") for s in subs):
        subs[0]["isDefault"] = True
    out = dict(profile)
    out["subscriptions"] = subs
    return out, subs


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="./import-identity.sh",
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("account",
                        help="the account to import, e.g. sc-1234567@microsoft.com")
    parser.add_argument("--from", dest="source", default=None,
                        help="config directory to take it from "
                             "(default: $AZURE_CONFIG_DIR, then ~/.azure)")
    parser.add_argument("--into", default=None,
                        help="destination (default: auth.az_config_dir from "
                             "settings/policy.yaml)")
    parser.add_argument("--force", action="store_true",
                        help="overwrite an identity that is already there")
    args = parser.parse_args(argv)

    wanted = args.account.strip()
    destination = args.into or default_destination()
    destination = os.path.join(ROOT, destination) if not os.path.isabs(destination) \
        else destination

    # -- find it ----------------------------------------------------------
    source = None
    for candidate in candidate_sources(args.source):
        found = accounts_in(candidate)
        if wanted.lower() in found:
            source, home_account_id = candidate, found[wanted.lower()]
            break

    if source is None:
        print("no login for {} found".format(wanted), file=sys.stderr)
        for candidate in candidate_sources(args.source):
            known = sorted(accounts_in(candidate))
            print("  {}: {}".format(
                candidate, ", ".join(known) if known else "no logins"),
                file=sys.stderr)
        print("\nlog in as that account first, then run this again:",
              file=sys.stderr)
        print("  az login --use-device-code", file=sys.stderr)
        return 1

    if os.path.abspath(source) == os.path.abspath(destination):
        print("pruning {} in place".format(destination))
    print("found {} in {}".format(wanted, source))
    # Everything below can fail to stderr, and the two streams do not interleave
    # in order unless this one is pushed out first — which made the refusal
    # below read as though it came before the line explaining what was found.
    sys.stdout.flush()

    # -- refuse to clobber silently ---------------------------------------
    existing = os.path.join(destination, TOKENS)
    if os.path.isfile(existing) and not args.force \
            and os.path.abspath(source) != os.path.abspath(destination):
        current = sorted(accounts_in(destination))
        print("\n{} already holds an identity: {}".format(
            destination, ", ".join(current) or "unreadable"), file=sys.stderr)
        print("re-run with --force to replace it", file=sys.stderr)
        return 1

    # -- filter -----------------------------------------------------------
    cache = read_json(os.path.join(source, TOKENS))
    profile = read_json(os.path.join(source, PROFILE), bom=True)

    dropped = sorted(u for u in accounts_in(source) if u != wanted.lower())
    filtered_cache = filter_tokens(cache, home_account_id)
    filtered_profile, subs = filter_profile(profile, wanted)

    if not subs:
        print("\n{} has no subscriptions in {}".format(wanted, PROFILE),
              file=sys.stderr)
        print("the login exists but is not usable; try `az login` again",
              file=sys.stderr)
        return 1

    # -- write ------------------------------------------------------------
    os.makedirs(destination, mode=0o700, exist_ok=True)
    os.chmod(destination, 0o700)
    write_json(os.path.join(destination, TOKENS), filtered_cache)
    write_json(os.path.join(destination, PROFILE), filtered_profile, bom=True)

    print("wrote {}/".format(destination))
    print("  {:<24} {} subscription(s), default {}".format(
        PROFILE, len(subs),
        next((s["name"] for s in subs if s.get("isDefault")), "?")))
    print("  {:<24} {} refresh token(s)".format(
        TOKENS, len(filtered_cache.get("RefreshToken") or {})))
    for section in DROP:
        n = len(cache.get(section) or {})
        if n:
            print("  dropped {} {}(s) — the next call will mint a fresh one"
                  .format(n, section))
    if dropped:
        print("  left behind: {} — not this proxy's identity"
              .format(", ".join(dropped)))

    return verify(destination, wanted)


def default_destination():
    """auth.az_config_dir, read from the same file the proxy reads."""
    try:
        import yaml
        with open(os.path.join(ROOT, "settings", "policy.yaml")) as f:
            policy = yaml.safe_load(f) or {}
        return (policy.get("auth") or {}).get("az_config_dir") or ".az-identity"
    except Exception:
        return ".az-identity"


def verify(destination, wanted):
    """Prove the imported identity can actually mint a token.

    Worth the round trip: everything above is a file operation, and a filter
    that produced a well-formed but unusable cache would otherwise be found by
    whoever next tried to start the proxy.
    """
    if shutil.which("az") is None:
        print("\naz not on PATH; skipping verification")
        return 0
    env = dict(os.environ, AZURE_CONFIG_DIR=os.path.abspath(destination))
    try:
        out = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "https://cognitiveservices.azure.com",
             "--query", "expiresOn", "-o", "tsv"],
            env=env, capture_output=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as e:
        print("\ncould not verify: {}".format(e), file=sys.stderr)
        return 1
    if out.returncode != 0:
        print("\nimported, but it cannot get a token:", file=sys.stderr)
        print(out.stderr.decode("utf-8", "replace").strip(), file=sys.stderr)
        return 1
    print("\nverified: token for {} good until {}".format(
        wanted, out.stdout.decode().strip()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
