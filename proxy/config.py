"""Configuration and route descriptors shared by the two processes.

Importing this module performs no file I/O and starts no services.
"""

import json
import hashlib
import math
import os
from typing import Dict, List, Optional, Tuple

import yaml
from urllib.parse import urlsplit, urlunsplit

ROOT = os.environ.get("AZURE_PROXY_HOME",
                      os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SETTINGS = os.path.join(ROOT, "settings")
RUNTIME = os.path.join(ROOT, "runtime")
BALANCE_MODES = ("strict_priority", "priority_threshold", "capacity")
BALANCE_ALIASES = {"priority": "strict_priority", "weighted": "capacity"}
FACES = ("chat", "chat_stream", "responses", "responses_stream", "image")
TABLES = ("routes", "responses_routes", "image_routes", "image_edit_routes")


def endpoint_identity(address, scope):
    url = urlsplit(address)
    if url.username or url.password or url.query or url.fragment:
        raise ValueError("endpoint URL must not contain credentials, query or fragment")
    normalized = urlunsplit((url.scheme.lower(), url.netloc.lower(), url.path.rstrip("/"), "", ""))
    return hashlib.sha256((normalized + "\0" + scope).encode()).hexdigest()

def _as_number(value) -> Optional[float]:
    """Header value -> float, or None. Azure has been consistent about sending
    plain integers here, but a weight calculation is not the place to find out
    what happens the day it is not."""
    if value is None:
        return None
    try:
        number = float(str(value).strip())
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _route_sort_key(route: "Route"):
    """Failover order for one model's routes: priority, then size, then name."""
    capacity = route.capacity_tokens or route.capacity_requests or 0
    return (route.priority, -capacity, route.deployment)


class Route:
    """One (endpoint, deployment) pair that can serve a model.

    A model can have more than one of these on the SAME endpoint: a second
    deployment of the same model, bought under a different SKU, is a second
    quota and a genuinely separate destination. Everything downstream keys on
    the pair, never on the endpoint alone.
    """

    __slots__ = ("endpoint", "url", "api_version", "deployment",
                 "limit_param", "priority", "responses_path",
                 "model_version", "capacity_requests", "capacity_tokens",
                 "image_edits", "scope")

    def __init__(self, endpoint, url, api_version, deployment, limit_param,
                 priority, responses_path=None, model_version=None,
                 capacity_requests=None, capacity_tokens=None,
                 image_edits=False, scope=None):
        self.endpoint = endpoint
        self.url = url
        self.api_version = api_version
        self.deployment = deployment
        self.limit_param = limit_param
        self.priority = priority
        # None when this endpoint does not serve the Responses API. Which of
        # the two URL shapes it wants is settled by the probe.
        self.responses_path = responses_path
        # Which vintage of the model this deployment serves. Carried for
        # reporting; the proxy does not route on it.
        self.model_version = model_version
        # The quota ARM says this deployment holds, in the units the runtime
        # later reads off x-ratelimit-limit-*. None when the probe could not ask
        # ARM — an older runtime/, or an endpoint discovered by guesswork.
        # Image deployments carry a request ceiling and no token one, which the
        # load calculation already handles: it takes whichever ceiling it has.
        self.capacity_requests = capacity_requests
        self.capacity_tokens = capacity_tokens
        # Whether this image deployment also serves /images/edits. From ARM's
        # capability flag, never measured — an edits probe would have to carry a
        # real image.
        # The data-plane audience this endpoint's token must be minted for.
        # None means the default scope (cognitiveservices.azure.com); the
        # AI-Foundry project endpoint sets ai.azure.com, and the same resource
        # returns 401 for a token minted for the other one. See TOKENS registry.
        self.image_edits = image_edits
        self.scope = scope

    def chat_target(self) -> str:
        return "{}openai/deployments/{}/chat/completions?api-version={}".format(
            self.url, self.deployment, self.api_version)

    def responses_target(self) -> str:
        # No deployment in the path here: the Responses API takes it from the
        # body's `model`, which _forward has already rewritten.
        return self.url + self.responses_path

    def image_target(self, kind: str) -> str:
        """generations or edits.

        Azure endpoints put the deployment in the path; an AI-Foundry project
        endpoint (openai/v1, deployment in the body) has no api-version segment
        and takes the deployment from `model`, exactly as its Responses face
        does. `responses_path` being an openai/v1 shape is what tells them
        apart — the same signal the probe settled the endpoint on.
        """
        if "/api/projects/" in self.url or \
                (self.scope or "").startswith("https://ai.azure.com"):
            return "{}openai/v1/images/{}".format(self.url, kind)
        return "{}openai/deployments/{}/images/{}?api-version={}".format(
            self.url, self.deployment, kind, self.api_version)

    def __repr__(self):
        return "{}/{}".format(self.endpoint, self.deployment)


class Config:
    """With load_routes=False, parse serving startup settings only."""

    def __init__(self, load_routes=True):
        with open(os.path.join(SETTINGS, "policy.yaml")) as f:
            policy = yaml.safe_load(f)
        sources, models = {"endpoints": []}, {"models": {}}
        if load_routes:
            with open(os.path.join(RUNTIME, "sources.json")) as f:
                sources = json.load(f)
            with open(os.path.join(RUNTIME, "models.json")) as f:
                models = json.load(f)

        self.policy = policy
        self.host = policy["server"]["host"]
        self.port = policy["server"]["port"]
        self.log_level = policy["server"].get("log_level", "info")

        # Diagnostics. Off unless a directory is named, and it has to stay that
        # way: these files contain prompts. See _capture_stream.
        self.capture_dir = policy["server"].get("capture_dir") or None
        self.capture_mode = policy["server"].get("capture_mode", "suspicious")
        self.capture_limit = int(policy["server"].get("capture_limit", 40))

        r = policy["routing"]
        self.retry_on_status = set(r["retry_on_status"])
        self.retry_on_transport_error = r["retry_on_transport_error"]
        self.retry_on_timeout = r["retry_on_timeout"]
        self.timeout = r["request_timeout_seconds"]
        self.max_attempts = r["max_attempts_per_request"]
        self.backoff_initial = r["backoff_initial_seconds"]
        self.backoff_multiplier = r["backoff_multiplier"]
        self.backoff_jitter = r["backoff_jitter_seconds"]

        s = r.get("stream_probe") or {}
        self.probe_seconds = float(s.get("hold_seconds", 0.5))
        self.probe_bytes = int(s.get("hold_bytes", 16384))
        # Encoded once, at boot: this list is consulted per chunk of every
        # stream, and str.encode() on the hot path for a constant is waste.
        self.stream_retry_markers = [
            str(code).encode() for code in
            (s.get("retry_on_codes") or ["rate_limit_exceeded"])]

        # The image face retries differently, because it fails differently.
        # Image quota is granted in requests per minute and granted meanly —
        # 2 RPM for gpt-image-2, 30 for gpt-image-1.5 — while one call takes
        # 10-25 seconds, so a 429 is the ordinary case rather than the sign of a
        # storm. Two consequences, both of them settings rather than code:
        #
        #   * `max_attempts` here is a number of ATTEMPTS, not of routes. Most
        #     image models have exactly one deployment, so the text faces' rule
        #     — one attempt per route, never the same route twice — would hand
        #     the first 429 straight back to the caller. Here the attempt list
        #     cycles through the routes instead.
        #   * the wait between them is Azure's own Retry-After rather than a
        #     blind exponential, because a per-minute ceiling refills on a
        #     schedule Azure knows and the proxy does not.
        #
        # Clients are the reason this belongs here rather than in them: the
        # OpenAI SDK's image calls are routinely made with maxRetries: 0, and
        # one 429 is then a failed page rather than a slow one.
        i = r.get("image") or {}
        self.image_attempts = int(i.get("max_attempts", 3))
        self.image_max_wait = float(i.get("max_wait_seconds", 30))

        a = r.get("session_affinity") or {}
        self.affinity_keys = list(a.get("keys") or [
            "header:session-id", "body:prompt_cache_key",
            "body:client_metadata.session_id", "header:x-session-id"])
        self.affinity_ttl = float(a.get("ttl_seconds", 172800))
        if not math.isfinite(self.affinity_ttl) or self.affinity_ttl <= 0:
            raise ValueError("session affinity TTL must be positive and finite")
        self.affinity_active_window = float(a.get("active_window_seconds", 300))
        if not math.isfinite(self.affinity_active_window) or self.affinity_active_window <= 0:
            raise ValueError("session affinity active window must be positive and finite")
        self.affinity_attempts = int(a.get("wait_attempts", 4))
        self.affinity_max_wait = float(a.get("max_wait_seconds", 30))
        if (self.affinity_attempts < 0 or not math.isfinite(self.affinity_max_wait)
                or self.affinity_max_wait < 0):
            raise ValueError("invalid session affinity retry budget")
        # Legacy mode switches are ignored during migration. Endpoint binding
        # and full encrypted-state preservation are unconditional.

        self.forward_headers = policy["request"]["forward_headers"]
        self.responses_compat = policy["request"].get("responses_compat", True)
        self.scope = policy["auth"]["scope"]
        self.refresh_margin = policy["auth"]["refresh_margin_seconds"]
        self.expected_account = policy["auth"].get("expected_account")

        # Every distinct data-plane scope in play. The default from policy is
        # always present; an endpoint may pin its own (the AI-Foundry project
        # path uses ai.azure.com). The token layer holds one cache per scope.
        self.scopes = {self.scope} | {
            e["scope"] for e in sources["endpoints"] if e.get("scope")}
        self.endpoint_identities = {
            e["name"]: endpoint_identity(e["url"], e.get("scope") or self.scope)
            for e in sources["endpoints"]}

        # Give the Azure CLI its own directory before anything shells out to it.
        # AzureCliCredential spawns `az` with a copy of os.environ, so setting
        # it here is enough for the credential, the background refresher and the
        # startup account check alike.
        self.az_config_dir = policy["auth"].get("az_config_dir")
        if self.az_config_dir:
            self.az_config_dir = os.path.expanduser(self.az_config_dir)
            # A relative path is relative to the repo, not to whatever directory
            # the proxy happened to be started from. That is what lets the
            # credential directory travel with the checkout: the whole tree can
            # be moved to another path, or handed to another account, without a
            # setting that points back at where it used to live.
            if not os.path.isabs(self.az_config_dir):
                self.az_config_dir = os.path.join(ROOT, self.az_config_dir)
            os.environ["AZURE_CONFIG_DIR"] = self.az_config_dir

        self.routes: Dict[str, List[Route]] = {}
        self.responses_routes: Dict[str, List[Route]] = {}
        self.image_routes: Dict[str, List[Route]] = {}
        self.image_edit_routes: Dict[str, List[Route]] = {}
        self.image_deployments: Dict[str, str] = {}
        self.routing_report = {}
        if not load_routes:
            return

        # Routing owns algorithm settings and deployment discovery. Serving
        # receives their results in snapshots and can boot without parsing them.
        self.balance_configured = r.get("balance", "priority_threshold")
        self.balance = BALANCE_ALIASES.get(self.balance_configured,
                                          self.balance_configured)
        if self.balance not in BALANCE_MODES:
            self.balance = "strict_priority"

        b = r.get("balancing") or {}
        self.static_weights = b.get("static_weights") or {}
        self.headroom_high_water = float(b.get("headroom_high_water", 0.5))
        self.weight_floor = float(b.get("weight_floor", 0.05))
        self.observation_ttl = float(b.get("observation_ttl_seconds", 120))
        self.demote_multiplier = float(b.get("demote_multiplier", 0.25))
        self.demote_seconds = float(b.get("demote_seconds", 30))
        self.demote_halflife = float(
            b.get("demote_recovery_halflife_seconds", 30))
        self.spill_threshold = float(b.get("spill_threshold", 0.70))
        self.load_window = float(b.get("load_window_seconds", 60))
        self.rpm_window = max(1.0, float(b.get(
            "rpm_window_seconds", b.get("qpm_window_seconds", 60.0))))
        for name in ("load_window", "rpm_window", "spill_threshold"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError("invalid routing window/threshold: " + name)
        capacity_file = b.get("capacity_state_file", "runtime/capacity.json")
        self.capacity_state_file = os.path.expanduser(str(capacity_file))
        if not os.path.isabs(self.capacity_state_file):
            self.capacity_state_file = os.path.join(ROOT, self.capacity_state_file)
        self.chars_per_token = float(b.get("assumed_chars_per_token", 4)) or 4.0

        f = b.get("foreign_load") or {}
        self.foreign_enabled = bool(f.get("enabled", True))
        self.foreign_reclaim = float(f.get("reclaim_per_minute", 0.1))

        # An endpoint counts as usable if any face is up. They are gated
        # separately by Azure and they fail separately: a resource whose chat
        # face is refused can still serve the Responses API, and dropping it
        # entirely would take working routes down with the broken one.
        meta = {e["name"]: e for e in sources["endpoints"]
                if "ok" in (e["status"], e.get("responses_status"),
                            e.get("image_status"))}
        self.endpoints = [(e["name"], e["status"], e.get("responses_status", "?"),
                           e.get("image_status", "?"))
                          for e in sources["endpoints"]]
        for name, spec in models["models"].items():
            built, responses, images = [], [], []
            for hop in spec["routes"]:
                ep = meta.get(hop["endpoint"])
                if ep is None:
                    continue        # endpoint went unhealthy since the last probe
                route = Route(
                    endpoint=hop["endpoint"],
                    url=ep["url"],
                    api_version=ep["api_version"],
                    deployment=hop["deployment"],
                    limit_param=hop["limit_param"],
                    priority=hop.get("priority", ep.get("priority", 0)),
                    responses_path=ep.get("responses_path"),
                    model_version=hop.get("model_version"),
                    capacity_requests=hop.get("capacity_requests"),
                    capacity_tokens=hop.get("capacity_tokens"),
                    image_edits=bool(hop.get("image_edits")),
                    scope=ep.get("scope"),
                )
                # Absent `faces` means a runtime/ written before the Responses
                # API existed here. Default it to chat only, so a stale probe
                # leaves the old face working and merely reports no routes on
                # the new one.
                faces = hop.get("faces", ["chat"])
                # Not every deployment has a chat face. gpt-5-pro and the codex
                # models answer chat/completions with a flat 400 and serve the
                # Responses API only, so listing them as chat routes would offer
                # a destination that cannot work. gpt-image-* have neither.
                if "chat" in faces:
                    built.append(route)
                if "responses" in faces and route.responses_path:
                    responses.append(route)
                if "image" in faces:
                    images.append(route)
            # Candidate order in endpoints.yaml is failover priority, and
            # capacity breaks ties within one endpoint — a model served twice by
            # the same resource should be reached for at its larger deployment
            # first. Sorted here rather than trusted from the file, which is the
            # same key probe/probe.py:route_sort_key writes it in.
            built.sort(key=_route_sort_key)
            responses.sort(key=_route_sort_key)
            images.sort(key=_route_sort_key)
            if built:
                self.routes[name] = built
            if responses:
                self.responses_routes[name] = responses
            if images:
                self.image_routes[name] = images
                edits = [r for r in images if r.image_edits]
                if edits:
                    self.image_edit_routes[name] = edits

        # Which image deployment to name on an endpoint, for the Responses
        # API's built-in image_generation tool: Azure will not pick one itself
        # and refuses the call without the header. Largest ceiling first, so the
        # default is the one with room rather than whichever sorted first.
        best: Dict[str, Tuple[float, str]] = {}
        for routes in self.image_routes.values():
            for route in routes:
                size = route.capacity_requests or 0.0
                if size > best.get(route.endpoint, (-1.0, ""))[0]:
                    best[route.endpoint] = (size, route.deployment)
        self.image_deployments: Dict[str, str] = {
            endpoint: deployment for endpoint, (_size, deployment) in best.items()}

        self.generated_at = models.get("_generated_at")
