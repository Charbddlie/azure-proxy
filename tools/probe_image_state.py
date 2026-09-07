#!/usr/bin/env python3
"""One synthetic Images API request; retain only response metadata and hashes."""

import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from proxy.config import Config
from probe_deployment_cipher import encrypted_values


def main():
    cfg = Config()
    route = next(r for r in cfg.image_routes["gpt-image-2"] if r.endpoint == "gpt4v-swc")
    scope = route.scope or cfg.scope
    env = dict(os.environ, AZURE_CONFIG_DIR=cfg.az_config_dir)
    auth = subprocess.run(["az", "account", "get-access-token", "--resource",
                           scope.removesuffix("/.default"), "-o", "json"],
                          env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=60)
    if auth.returncode:
        raise SystemExit("Isolated Azure identity could not obtain a token")
    token = json.loads(auth.stdout)["accessToken"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result = {"model": "gpt-image-2", "endpoint": route.endpoint,
              "deployment": route.deployment, "model_version": route.model_version,
              "started_at": stamp, "direct_upstream": True,
              "purpose": "Inspect Images API state fields; no same-endpoint deployment pair exists",
              "requests": []}
    body = {"model": route.deployment, "prompt": "A small black square centered on a plain white background.",
            "size": "1024x1024", "quality": "low", "n": 1}
    for attempt in range(2):
        request = urllib.request.Request(route.image_target("generations"),
            data=json.dumps(body).encode(), method="POST",
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        started = time.monotonic()
        headers, response = {}, {}
        try:
            with urllib.request.urlopen(request, timeout=60) as reply:
                status = reply.status
                headers = dict(reply.headers.items())
                response = json.load(reply)
        except urllib.error.HTTPError as error:
            status = error.code
            headers = dict(error.headers.items())
            try:
                response = json.loads(error.read())
            except ValueError:
                response = {}
        except Exception as error:
            result["requests"].append({"transport_error": type(error).__name__})
            break
        quota = {k.lower(): v for k, v in headers.items()
                 if k.lower() == "retry-after" or k.lower().startswith("x-ratelimit-")}
        record = {"http_status": status, "elapsed_seconds": round(time.monotonic() - started, 3),
                  "top_level_fields": sorted(response), "error_code": (response.get("error") or {}).get("code"),
                  "quota_headers": quota, "encrypted_content_count": len(list(encrypted_values(response))),
                  "usage": response.get("usage"), "images": []}
        for entry in response.get("data") or []:
            item = {"fields": sorted(entry)}
            if entry.get("b64_json"):
                image = base64.b64decode(entry["b64_json"], validate=True)
                item.update(bytes=len(image), sha256=hashlib.sha256(image).hexdigest(),
                            png_signature=image.startswith(b"\x89PNG\r\n\x1a\n"))
            record["images"].append(item)
        result["requests"].append(record)
        print(json.dumps(record), flush=True)
        if status != 429 or attempt:
            break
        try:
            delay = float(quota.get("retry-after", 1))
        except ValueError:
            break
        if not 0 <= delay <= 30:
            break
        time.sleep(delay + 0.2)
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    path = ROOT / "test/results" / ("image-state-gpt-image-2-gpt4v-swc-" + stamp + ".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n")
    print("sanitized_results=" + str(path), flush=True)


if __name__ == "__main__":
    main()
