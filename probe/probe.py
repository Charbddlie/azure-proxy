#!/usr/bin/env python3
"""Discover which Azure OpenAI deployments are actually reachable.

Reads  settings/endpoints.yaml   candidate endpoints
Writes runtime/sources.json      per-endpoint status and live deployments
       runtime/models.json       model name -> routes, in failover order

The deployment list comes from ARM, not from a list of names to try. The data
plane has no discovery call, but the management plane does, and it answers three
questions the data plane cannot:

  * which deployments exist, so nothing has to be guessed at;
  * which MODEL each one serves, which is not the deployment's name — endpoint-b
    has a deployment called `gpt-4o-mini` that serves gpt-4.1-mini;
  * what quota it holds, per deployment, in the same units the runtime later
    reads off x-ratelimit-limit-*.

ARM says what exists; the data plane still says what works. Every deployment ARM
reports is probed exactly as before — the principal may hold ARM read and no
data action at all.

Two faces are probed independently. A deployment that answers on
chat/completions may still be unusable through the Responses API: Azure gates
that behind a separate data action (`OpenAI/responses/write`), and older
api-versions do not serve it at all.

    python probe/probe.py
    python probe/probe.py --only endpoint-a
"""

import argparse
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SETTINGS = os.path.join(ROOT, "settings")
RUNTIME = os.path.join(ROOT, "runtime")

MAX_WORKERS = 12
HTTP_TIMEOUT = 60
RETRY_429 = 2

# ARM's deployment list. Old enough to be served everywhere, new enough to carry
# `properties.rateLimits` and `properties.capabilities`.
ARM_RESOURCE = "https://management.azure.com"
ARM_API_VERSION = "2024-10-01"

# 256 rather than a token or two. The probe only needs a 200, but a reasoning
# model spends its budget on reasoning before it emits anything, and a ceiling
# too low to reach the first visible token comes back as an error that reads
# like a dead deployment. gpt-5-pro and gpt-5.4-pro are the ones this is for.
PROBE_BODY = {"messages": [{"role": "user", "content": "hi"}]}
PROBE_MAX_TOKENS = 256
RESPONSES_PROBE_BODY = {"input": "hi", "max_output_tokens": PROBE_MAX_TOKENS}

# Tried in order; the first one an endpoint accepts is recorded and reused.
# The deployment goes in the body's `model` for both, never in the path.
RESPONSES_PATHS = ["openai/v1/responses",
                   "openai/responses?api-version={api_version}"]

_ctx = ssl.create_default_context()
TOKEN = None
KEYS = {}


def az_env():
    """The proxy's own Azure CLI directory, not the shared ~/.azure.

    This box is a shared account: whoever ran `az login` last owns the default
    directory. settings/policy.yaml points the proxy elsewhere so the two cannot
    displace each other, and the probe has to look in the same place or it would
    report on a different principal's access than the proxy actually has.
    """
    with open(os.path.join(SETTINGS, "policy.yaml")) as f:
        auth = yaml.safe_load(f).get("auth") or {}
    config_dir = auth.get("az_config_dir")
    if not config_dir:
        return dict(os.environ)
    return dict(os.environ, AZURE_CONFIG_DIR=os.path.expanduser(config_dir))


def get_cli_token(resource="https://cognitiveservices.azure.com"):
    try:
        out = subprocess.check_output(
            ["az", "account", "get-access-token",
             "--resource", resource, "-o", "json"],
            stderr=subprocess.DEVNULL, env=az_env())
    except (subprocess.CalledProcessError, FileNotFoundError):
        sys.exit("no Azure token for the proxy's own credentials.\n"
                 "run: ./az.sh login --use-device-code")
    return json.loads(out)["accessToken"]


def request(url, cand, body):
    """POST one request. Never raises; returns (status, error_code, message)."""
    headers = {"Content-Type": "application/json"}
    if cand["auth"] == "cli":
        headers["Authorization"] = "Bearer " + TOKEN
    else:
        headers["api-key"] = KEYS.get(cand["name"], "")

    for attempt in range(RETRY_429 + 1):
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=_ctx) as r:
                r.read()
                return r.status, None, None
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
            code, msg = None, raw[:300]
            try:
                err = json.loads(raw).get("error") or {}
                code, msg = err.get("code"), (err.get("message") or "")[:300]
            except Exception:
                pass
            if e.code == 429 and attempt < RETRY_429:
                time.sleep(2 ** attempt)
                continue
            return e.code, code, msg
        except Exception as e:
            return 0, type(e).__name__, str(e)[:300]


def call(cand, deployment, body):
    """One chat/completions probe against a deployment."""
    url = "{}openai/deployments/{}/chat/completions?api-version={}".format(
        cand["url"], deployment, cand["api_version"])
    return request(url, cand, body)


# --------------------------------------------------------------------------
# ARM: what is deployed, what it serves, and how much quota it holds
# --------------------------------------------------------------------------


def arm_get(url, token):
    """GET one ARM page. Returns the decoded body, or raises."""
    req = urllib.request.Request(
        url, headers={"Authorization": "Bearer " + token}, method="GET")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=_ctx) as r:
        return json.loads(r.read().decode())


def arm_deployments(cand, token):
    """Every deployment ARM knows about on this account, or None.

    None means "could not ask" — no coordinates in endpoints.yaml, no ARM
    permission, no network. It is not the same answer as an empty list, and the
    caller has to tell them apart: one falls back to the guess list, the other
    means the account really is empty.
    """
    sub, group = cand.get("subscription"), cand.get("resource_group")
    if not sub or not group:
        return None
    url = ("{}/subscriptions/{}/resourceGroups/{}/providers/"
           "Microsoft.CognitiveServices/accounts/{}/deployments"
           "?api-version={}").format(ARM_RESOURCE, sub, group, cand["name"],
                                     ARM_API_VERSION)
    items = []
    while url:
        try:
            page = arm_get(url, token)
        except Exception as e:
            print("  {:<28} ARM list failed: {}".format(
                cand["name"], str(e)[:120]))
            return None
        items.extend(page.get("value") or [])
        url = page.get("nextLink")
    return items


def _capacity(item):
    """(requests per minute, tokens per minute) for one ARM deployment.

    `properties.rateLimits` is the direct answer and is quoted in exactly the
    units x-ratelimit-limit-requests / -tokens use at runtime, which is what
    makes it usable as a cold-start prior without conversion. Verified on
    2026-08-20 against endpoint-a/gpt-5.6-sol: ARM says 333 and 333000, the
    response headers say 333 and 333000.

    Older deployments carry a null there and only have `sku.capacity`. In every
    sample where both were present, sku.capacity equalled the request ceiling
    and a thousand times it was the token ceiling, so that is the fallback.
    """
    props = item.get("properties") or {}
    limits = {}
    for entry in (props.get("rateLimits") or []):
        count = entry.get("count")
        if entry.get("key") in ("request", "token") and count:
            limits[entry["key"]] = float(count)
    if limits:
        return limits.get("request"), limits.get("token")

    sku_capacity = (item.get("sku") or {}).get("capacity")
    if not sku_capacity:
        return None, None
    return float(sku_capacity), float(sku_capacity) * 1000


def plan_deployments(items):
    """ARM's deployment list -> the ones worth probing, with what they serve.

    Three exclusions, all of them things that would fail the data-plane probe
    anyway — done here to save the request rather than to hide anything:

      * not `Succeeded`: still being created, or failed to.
      * a Batch SKU: served only through the batch API. `gpt-4.1-batch`,
        `gpt-4o-data`, and endpoint-b's `o4-mini` are all this.
      * no chat or responses capability: image and embedding deployments. The
        proxy offers neither face, so dall-e-3 and gpt-image-* are not its
        business.
    """
    out = []
    for item in items:
        props = item.get("properties") or {}
        if props.get("provisioningState") != "Succeeded":
            continue
        sku = (item.get("sku") or {}).get("name") or ""
        if "batch" in sku.lower():
            continue
        caps = props.get("capabilities") or {}
        faces = ("chatCompletion", "responses")
        if not any(str(caps.get(f)).lower() == "true" for f in faces):
            continue

        model = props.get("model") or {}
        name = model.get("name") or item.get("name")
        requests_per_minute, tokens_per_minute = _capacity(item)
        out.append({
            "deployment": item.get("name"),
            # The model the deployment actually serves, which is the name the
            # proxy will publish. It is not reliably the deployment's own name
            # and nothing downstream should assume it is.
            "model": name,
            "model_version": model.get("version"),
            "sku": sku or None,
            "capacity_requests": requests_per_minute,
            "capacity_tokens": tokens_per_minute,
        })
    out.sort(key=lambda d: (d["model"], d["deployment"]))
    return out


def guessed_deployments(names):
    """The fallback plan: names to try, each assumed to serve its own model.

    That assumption is the reason ARM is preferred. Nothing here can be checked
    — the data plane will confirm the deployment answers and say nothing at all
    about what is behind it.
    """
    return [{"deployment": n, "model": n, "model_version": None, "sku": None,
             "capacity_requests": None, "capacity_tokens": None}
            for n in names]


def route_sort_key(route):
    """Failover order for one model's routes.

    Endpoint priority first — that is what endpoints.yaml's order means and it
    still decides. Capacity only breaks ties WITHIN an endpoint, which is where
    the question is new: endpoint-a serves gpt-5.5 from two deployments, one at
    5000 and one at 15000, and there is no reason to reach for the small one
    first. Deployment name last, so the order does not wobble between runs.

    proxy/server.py sorts by the same key after loading. Kept in both places on
    purpose: the file should be readable in the order it will be used, and the
    server must not depend on a JSON array's order surviving anything.
    """
    capacity = route.get("capacity_tokens") or route.get("capacity_requests") or 0
    return (route.get("priority", 0), -capacity, route.get("deployment") or "")


def order_routes(routes):
    return sorted(routes, key=route_sort_key)


def call_responses(cand, path, deployment):
    """One Responses API probe. The deployment name rides in the body."""
    url = cand["url"] + path.format(api_version=cand["api_version"])
    body = dict(RESPONSES_PROBE_BODY, model=deployment)
    return request(url, cand, body)



def probe_pair(cand, deployment):
    """Is this deployment live, and which token-limit parameter does it want?

    GPT-5.x and o-series require max_completion_tokens; GPT-4.x requires
    max_tokens. Trying both here costs nothing and settles the question.
    """
    result = (0, None, None)
    for limit_param in ("max_completion_tokens", "max_tokens"):
        body = dict(PROBE_BODY, **{limit_param: PROBE_MAX_TOKENS})
        status, code, msg = call(cand, deployment, body)
        result = (status, code, msg)
        if status == 200:
            return {"live": True, "limit_param": limit_param, "result": result}
        if "max_tokens" not in (msg or "").lower():
            break
    return {"live": False, "limit_param": None, "result": result}


def classify(results):
    """One status per endpoint, from all its deployment attempts."""
    if any(r[0] == 200 for r in results):
        return "ok", None
    msgs = " ".join((r[2] or "") for r in results).lower()
    codes = set(r[1] or "" for r in results)
    statuses = set(r[0] for r in results)

    if "name or service not known" in msgs or "nodename nor servname" in msgs:
        return "dns_nxdomain", "hostname does not resolve"
    if "public access is disabled" in msgs:
        return "public_access_disabled", "resource requires a private endpoint"
    if "AuthenticationTypeDisabled" in codes:
        return "key_auth_disabled", "api-key auth is off; try auth: cli"
    if 401 in statuses:
        return "auth_denied", "principal lacks the chat/completions data action"
    if 403 in statuses:
        return "forbidden", "authenticated but not authorised"
    if statuses == {404}:
        return "no_known_deployments", "no probed deployment answered here"
    return "unreachable", ",".join(sorted(str(s) for s in statuses))


def probe_responses_path(cand, deployment):
    """Which Responses URL shape does this endpoint answer on, if any?

    Returns (path or None, last result). A 404 means the shape is not served
    here, so the next one is worth trying. Any other status means the endpoint
    answered — the shape is right, and the second shape would only repeat the
    same answer.
    """
    result = (0, None, None)
    for path in RESPONSES_PATHS:
        result = call_responses(cand, path, deployment)
        if result[0] == 200:
            return path, result
        if result[0] != 404:
            return (path if result[0] != 0 else None), result
    return None, result


def classify_responses(results):
    """Responses-face status, kept separate from the chat one.

    Azure gates the Responses API behind its own data action, so a principal
    with working chat/completions access can still be refused here. Saying
    `auth_denied` rather than folding it into a vague failure is the difference
    between a five minute fix and an afternoon.
    """
    if not results:
        return "not_probed", None
    if any(r[0] == 200 for r in results):
        return "ok", None
    msgs = " ".join((r[2] or "") for r in results).lower()
    statuses = set(r[0] for r in results)

    if "responses/write" in msgs or "responses/action" in msgs:
        return "auth_denied", "principal lacks the OpenAI/responses data action"
    if 401 in statuses or 403 in statuses:
        return "auth_denied", "principal lacks the OpenAI/responses data action"
    if statuses == {404}:
        return "unsupported", "no Responses API on this endpoint or api-version"
    return "unreachable", ",".join(sorted(str(s) for s in statuses))



def write_json(name, doc):
    os.makedirs(RUNTIME, exist_ok=True)
    with open(os.path.join(RUNTIME, name), "w") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write("\n")


def probe_responses(candidates, plans, live):
    """Second pass: which deployments answer on the Responses API.

    Every planned deployment is tried, not only the ones that answered on chat.
    Some models are Responses-only and reply to chat/completions with a flat
    `400 The requested operation is unsupported.` — measured 2026-08-21 for
    gpt-5-pro, gpt-5.4-pro, gpt-5.1-codex, gpt-5.1-codex-max and gpt-5.3-codex,
    all of which then return 200 on the responses face. Gating this pass on the
    chat one made every one of them invisible.

    The URL shape is settled once per endpoint and then reused. It is settled
    against a chat-live deployment where there is one: a 404 from a shape that
    is wrong and a 404 from a deployment that is not there look identical, and
    only the first should end the endpoint's pass.
    """
    paths, attempts, alive = {}, {c["name"]: [] for c in candidates}, set()

    for c in candidates:
        deps = sorted(d["deployment"] for d in plans[c["name"]][0])
        if not deps:
            continue
        known = sorted(d for (ep, d) in live if ep == c["name"])
        first = known[0] if known else deps[0]

        path, result = probe_responses_path(c, first)
        attempts[c["name"]].append(result)
        if result[0] == 200:
            alive.add((c["name"], first))
        if path is None or result[0] not in (200, 429):
            continue        # nothing here answers; do not spend the rest
        paths[c["name"]] = path

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futs = {pool.submit(call_responses, c, path, d): d
                    for d in deps if d != first}
            for fut in as_completed(futs):
                res = fut.result()
                attempts[c["name"]].append(res)
                if res[0] == 200:
                    alive.add((c["name"], futs[fut]))

    return paths, attempts, alive


def discover(candidates, fallback_names, arm_token):
    """Per endpoint: what to probe, and where the list came from.

    Returns {endpoint: (plan, "arm"|"list")}. The source is recorded rather than
    inferred later because the two lists carry different confidence: an ARM plan
    knows which model each deployment serves and what quota it holds, a guessed
    one knows neither and merely hopes the name means what it says.
    """
    plans = {}
    for c in candidates:
        items = arm_deployments(c, arm_token) if arm_token else None
        if items is None:
            plans[c["name"]] = (guessed_deployments(fallback_names), "list")
        else:
            plans[c["name"]] = (plan_deployments(items), "arm")
    return plans


def main():
    global TOKEN, KEYS
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="probe a single endpoint by name")
    ap.add_argument("--no-responses", action="store_true",
                    help="skip the Responses API pass")
    ap.add_argument("--no-arm", action="store_true",
                    help="skip ARM discovery and guess from the name list")
    args = ap.parse_args()

    with open(os.path.join(SETTINGS, "endpoints.yaml")) as f:
        inv = yaml.safe_load(f)
    candidates = inv["candidates"]
    if args.only:
        candidates = [c for c in candidates if c["name"] == args.only]
        if not candidates:
            sys.exit("no candidate named " + args.only)

    TOKEN = get_cli_token()
    keys_path = os.path.join(HERE, "keys.json")
    if os.path.exists(keys_path):
        with open(keys_path) as f:
            KEYS = json.load(f)

    arm_token = None if args.no_arm else get_cli_token(ARM_RESOURCE)
    plans = discover(candidates, inv["deployments"], arm_token)
    pairs = [(c, d) for c in candidates for d in plans[c["name"]][0]]
    print("{} endpoints, {} deployments to probe ({})".format(
        len(candidates), len(pairs),
        ", ".join("{}:{}".format(name, source)
                  for name, (_plan, source) in plans.items())))

    attempts = {c["name"]: [] for c in candidates}
    live = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(probe_pair, c, d["deployment"]): (c, d)
                for c, d in pairs}
        for fut in as_completed(futs):
            c, d = futs[fut]
            res = fut.result()
            attempts[c["name"]].append(res["result"])
            if res["live"]:
                live[(c["name"], d["deployment"])] = dict(
                    d, cand=c, limit_param=res["limit_param"])

    if args.no_responses:
        r_paths, r_attempts, r_live = {}, {c["name"]: [] for c in candidates}, set()
    else:
        print("responses face: {} deployments to probe".format(len(pairs)))
        r_paths, r_attempts, r_live = probe_responses(candidates, plans, live)

    # A deployment is usable if EITHER face answered. The chat pass is what
    # settles limit_param, so a Responses-only deployment has no measured
    # answer for it — it is given the modern one, which is what every model in
    # that category wants and which the chat face will never ask it for anyway,
    # there being no chat route to ask.
    usable = dict(live)
    for c in candidates:
        for spec in plans[c["name"]][0]:
            key = (c["name"], spec["deployment"])
            if key in r_live and key not in usable:
                usable[key] = dict(spec, cand=c,
                                   limit_param="max_completion_tokens")

    stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    header = {"_generated_by": "probe/probe.py", "_generated_at": stamp,
              "_do_not_edit": "Generated from live probes. Re-run probe/probe.py."}

    endpoints, models = [], {}
    # Candidate order in endpoints.yaml IS failover priority: the first listed
    # endpoint that serves a model is tried first. Routes are appended in that
    # order, and each carries its index so the server does not have to rely on
    # the ordering surviving a JSON round trip.
    for priority, c in enumerate(candidates):
        status, reason = classify(attempts[c["name"]])
        r_status, r_reason = classify_responses(r_attempts[c["name"]])
        entry = {"name": c["name"], "url": c["url"],
                 "api_version": c["api_version"], "auth": c["auth"],
                 "priority": priority, "status": status,
                 "discovery": plans[c["name"]][1],
                 "responses_status": r_status}
        if reason:
            entry["reason"] = reason
        if r_reason:
            entry["responses_reason"] = r_reason
        if c["name"] in r_paths:
            entry["responses_path"] = r_paths[c["name"]].format(
                api_version=c["api_version"])
        # Either face being up is enough to have routes here. An endpoint whose
        # deployments are all Responses-only is a working endpoint with a dead
        # chat face, not a dead endpoint.
        if "ok" in (status, r_status):
            entry["deployments"] = []
            for (ep, dep), info in sorted(usable.items()):
                if ep != c["name"]:
                    continue
                faces = []
                if (ep, dep) in live:
                    faces.append("chat")
                if (ep, dep) in r_live:
                    faces.append("responses")
                shared = {"model": info["model"],
                          "model_version": info["model_version"],
                          "capacity_requests": info["capacity_requests"],
                          "capacity_tokens": info["capacity_tokens"]}
                entry["deployments"].append(
                    dict(shared, name=dep, sku=info["sku"],
                         limit_param=info["limit_param"], faces=faces))
                # Keyed by the MODEL, not the deployment. One endpoint can hold
                # several deployments of one model — a second one bought under a
                # different SKU is a second, independent quota — and they are
                # separate routes to the same name, not a collision.
                models.setdefault(info["model"], []).append(
                    dict(shared, endpoint=ep, deployment=dep,
                         priority=priority,
                         limit_param=info["limit_param"], faces=faces))
        endpoints.append(entry)
        print("  {:<28} {:<22} {:<16} {}".format(
            c["name"], status, "responses:" + r_status,
            "{} deployments, {} on responses".format(
                len(entry.get("deployments", [])),
                sum(1 for (ep, _d) in r_live if ep == c["name"]))
            if "ok" in (status, r_status) else (reason or "")))

    write_json("sources.json", dict(header, endpoints=endpoints))
    write_json("models.json", dict(
        header, models={k: {"routes": order_routes(v)}
                        for k, v in sorted(models.items())}))
    responses_only = sorted(dep for (_ep, dep), info in usable.items()
                            if (_ep, dep) not in live)
    if responses_only:
        # Worth a line: these have no chat route at all, and a caller on the
        # chat face gets "model not found" for a model that is plainly listed.
        print("  note: responses-only, no chat route: {}".format(
            ", ".join(sorted(set(responses_only)))))
    for model, routes in sorted(models.items()):
        versions = sorted({r["model_version"] for r in routes
                           if r["model_version"]})
        if len(versions) > 1:
            # Not an error and not silently swallowed either. The routes serve
            # the same model name at different vintages, so which one a request
            # lands on is now a routing decision — worth knowing about before it
            # shows up as a behaviour difference between two identical calls.
            print("  note: {} spans versions {}".format(
                model, ", ".join(versions)))
    print("\n{} live pairs ({} on responses), {} models -> runtime/".format(
        len(live), len(r_live), len(models)))



if __name__ == "__main__":
    main()
