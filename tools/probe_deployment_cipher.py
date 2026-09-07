#!/usr/bin/env python3
"""Bounded, synthetic Responses encrypted-state compatibility experiment.

Calls Azure directly using the proxy's existing isolated CLI identity. Results
contain metadata and hashes only; access tokens, ciphertext and model output
remain in memory. No proxy configuration or service state is modified.
"""

import argparse
import copy
import datetime as dt
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

import yaml


ROOT = Path(__file__).resolve().parents[1]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--deployments", nargs=2, required=True, metavar=("A", "B"))
    parser.add_argument("--max-requests", type=int, default=16, choices=range(1, 17))
    parser.add_argument("--repeats", type=int, default=2, choices=(1, 2))
    args = parser.parse_args()
    if args.deployments[0] == args.deployments[1]:
        parser.error("Two distinct deployments are required")
    return args


def retry_delay(headers):
    """Respect the larger of Azure's seconds and milliseconds hints."""
    delays = []
    if headers.get("retry-after-ms"):
        try:
            delays.append(float(headers["retry-after-ms"]) / 1000)
        except ValueError:
            pass
    if headers.get("retry-after"):
        value = headers["retry-after"]
        try:
            delays.append(float(value))
        except ValueError:
            try:
                when = parsedate_to_datetime(value)
                delays.append(when.timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                pass
    return max([0.0] + delays) if delays else 1.0


def encrypted_values(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "encrypted_content" and isinstance(child, str) and child:
                yield child
            else:
                yield from encrypted_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from encrypted_values(child)


def cipher_metadata(value):
    return [{"length": len(cipher),
             "sha256": hashlib.sha256(cipher.encode()).hexdigest()}
            for cipher in encrypted_values(value)]


def corrupt_first_cipher(value):
    """Change one encoded character while preserving ciphertext length."""
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "encrypted_content" and isinstance(child, str) and child:
                pos = len(child) // 2
                replacement = "A" if child[pos] != "A" else "B"
                value[key] = child[:pos] + replacement + child[pos + 1:]
                return True
            if corrupt_first_cipher(child):
                return True
    elif isinstance(value, list):
        for child in value:
            if corrupt_first_cipher(child):
                return True
    return False


def output_text(response):
    return "".join(part.get("text", "")
                   for item in response.get("output", [])
                   for part in item.get("content", [])
                   if part.get("type") == "output_text").strip()


def main():
    args = arguments()
    os.chdir(ROOT)
    policy = yaml.safe_load((ROOT / "settings/policy.yaml").read_text())
    sources = json.loads((ROOT / "runtime/sources.json").read_text())
    models = json.loads((ROOT / "runtime/models.json").read_text())
    endpoint = next(e for e in sources["endpoints"] if e["name"] == args.endpoint)
    routes = [r for r in models["models"][args.model]["routes"]
              if r["endpoint"] == args.endpoint and r["deployment"] in args.deployments]
    assert (len(routes) == 2 and endpoint["auth"] == "cli"
            and all("responses" in r["faces"] for r in routes))
    url = endpoint["url"].rstrip("/") + "/" + endpoint["responses_path"]
    scope = endpoint.get("scope") or policy["auth"]["scope"]
    env = dict(os.environ)
    config_dir = policy["auth"].get("az_config_dir")
    if config_dir:
        env["AZURE_CONFIG_DIR"] = str(Path(config_dir).expanduser().resolve())
    token_process = subprocess.run(
        ["az", "account", "get-access-token", "--resource",
         scope.removesuffix("/.default"), "-o", "json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=60)
    if token_process.returncode:
        raise SystemExit("Existing isolated Azure identity could not obtain a token.")
    token = json.loads(token_process.stdout)["accessToken"]
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    identity = re.sub(r"[^a-zA-Z0-9_.-]", "_", args.model + "-" + args.endpoint)
    result_path = ROOT / "test/results" / f"deployment-cipher-{identity}-{stamp}.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "started_at": stamp, "model": args.model, "endpoint": args.endpoint,
        "url": url, "scope": scope,
        "routes": [{k: r[k] for k in ("deployment", "model", "model_version")}
                   for r in routes],
        "protocol": {"direct_upstream": True, "store": False,
                     "include": ["reasoning.encrypted_content"],
                     "reasoning_effort": "low", "max_output_tokens": 512,
                     "concurrency": 1, "request_limit": args.max_requests,
                     "repeats": args.repeats, "deployment_order": args.deployments,
                     "retry_limit": 2, "max_wait_seconds": 30,
                     "replay": "original input + all unmodified output items + new user turn",
                     "negative_control": "one ciphertext character changed in a separate request"},
        "requests": [],
    }
    throttles = {}
    rate_limit_counts = {}
    retries = 0

    def save():
        result_path.write_text(json.dumps(report, indent=2) + "\n")

    def call_once(label, deployment, history, expected):
        if len(report["requests"]) >= args.max_requests:
            raise RuntimeError("Request limit exceeded")
        body = {"model": deployment, "store": False, "stream": False,
                "include": ["reasoning.encrypted_content"],
                "reasoning": {"effort": "low"}, "max_output_tokens": 512,
                "input": history}
        request = urllib.request.Request(
            url, data=json.dumps(body).encode(), method="POST",
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"})
        started = time.monotonic()
        record = {"label": label, "deployment": deployment,
                  "input_ciphers": cipher_metadata(history),
                  "input_history_sha256": hashlib.sha256(json.dumps(
                      history, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                  "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()}
        response = {}
        response_headers = {}
        try:
            with urllib.request.urlopen(request, timeout=60) as reply:
                record["http_status"] = reply.status
                response_headers = dict(reply.headers.items())
                response = json.loads(reply.read())
        except urllib.error.HTTPError as error:
            record["http_status"] = error.code
            response_headers = dict(error.headers.items())
            try:
                response = json.loads(error.read())
            except (ValueError, UnicodeDecodeError):
                record["parse_error"] = True
        except Exception as error:
            record["http_status"] = None
            record["transport_error_type"] = type(error).__name__
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        allowed_headers = {"retry-after", "retry-after-ms", "x-ratelimit-limit-requests",
                           "x-ratelimit-limit-tokens", "x-ratelimit-remaining-requests",
                           "x-ratelimit-remaining-tokens", "x-ratelimit-reset-requests",
                           "x-ratelimit-reset-tokens"}
        record["quota_headers"] = {k.lower(): v for k, v in response_headers.items()
                                   if k.lower() in allowed_headers}
        error = response.get("error") or {}
        if not isinstance(error, dict):
            error = {}
        record.update({"response_status": response.get("status"),
                       "response_model": response.get("model"),
                       "error_code": error.get("code"),
                       "error_type": error.get("type"),
                       "incomplete_reason": (response.get("incomplete_details") or {}).get("reason"),
                       "usage": response.get("usage"),
                       "output_ciphers": cipher_metadata(response.get("output", [])),
                       "answer_matches_expected": output_text(response) == expected})
        record["completed"] = (record["http_status"] == 200
                               and record["response_status"] == "completed"
                               and not error)
        record["outcome"] = (
            "encrypted_content_rejected" if record["error_code"] == "invalid_encrypted_content"
            else "completed" if record["completed"]
            else "rate_limited" if record["http_status"] == 429
            else "upstream_server_error" if (record["http_status"] or 0) >= 500
            else "other_error_or_incomplete")
        report["requests"].append(record)
        save()
        print(json.dumps({k: record[k] for k in (
            "label", "deployment", "http_status", "response_status", "error_code",
            "outcome", "elapsed_seconds", "answer_matches_expected")}), flush=True)
        if record["outcome"] == "rate_limited":
            rate_limit_counts[deployment] = rate_limit_counts.get(deployment, 0) + 1
            delay = retry_delay(record["quota_headers"])
            throttles[deployment] = time.monotonic() + delay
        elif record["outcome"] == "upstream_server_error":
            raise RuntimeError("Experiment stopped on upstream server error")
        return response, record

    def call(label, deployment, history, expected):
        nonlocal retries

        def wait_or_skip():
            delay = max(0, throttles.get(deployment, 0) - time.monotonic())
            if delay > 30 or rate_limit_counts.get(deployment, 0) >= 2:
                report.setdefault("skipped_checks", []).append(
                    {"label": label, "deployment": deployment,
                     "reason": "deployment throttled beyond bounded retry policy"})
                save()
                return False
            if delay:
                wait_record = {"label": label, "deployment": deployment,
                               "wait_seconds": round(delay, 3)}
                report.setdefault("waits", []).append(wait_record)
                save()
                print(json.dumps(wait_record), flush=True)
                time.sleep(delay)
            return True

        if not wait_or_skip():
            return {}, {"completed": False, "output_ciphers": [],
                        "outcome": "skipped_throttled", "error_code": None}
        response, record = call_once(label, deployment, history, expected)
        if (record["outcome"] == "rate_limited" and retries < 2
                and len(report["requests"]) < args.max_requests and wait_or_skip()):
            retries += 1
            response, record = call_once(label + ":retry", deployment, history, expected)
        if all(rate_limit_counts.get(d, 0) >= 2 for d in args.deployments):
            raise RuntimeError("Experiment stopped: both deployments persistently rate limited")
        return response, record

    try:
        for repeat in range(1, args.repeats + 1):
            for source in args.deployments:
                other = next(d for d in args.deployments if d != source)
                prefix = f"round{repeat}:{source}"
                increment = repeat + 18
                answer = 37 * 43 + increment
                history = [{"role": "user", "content":
                            f"Compute 37 times 43, then add {increment}. "
                            "Work it out carefully and reply only with the final integer."}]
                minted, mint_record = call(prefix + ":mint", source, history, str(answer))
                if not mint_record["completed"] or not mint_record["output_ciphers"]:
                    report.setdefault("skipped_rounds", []).append(
                        {"round": prefix, "reason": "mint not completed or no encrypted content"})
                    save()
                    continue
                replay = history + minted["output"] + [
                    {"role": "user", "content":
                     "Add 3 to that result. Reply only with the resulting integer."}]
                expected_ciphers = cipher_metadata(replay)
                assert expected_ciphers == mint_record["output_ciphers"]
                targets = (source, other) if repeat == 1 else (other, source)
                for target in targets:
                    assert cipher_metadata(replay) == expected_ciphers
                    kind = "self" if target == source else "cross_deployment"
                    _, replay_record = call(prefix + ":" + kind, target, replay, str(answer + 3))
                    if kind == "self" and not replay_record["completed"]:
                        raise RuntimeError("Experiment stopped: same-deployment positive control failed")
                if repeat == 1:
                    corrupted = copy.deepcopy(replay)
                    assert corrupt_first_cipher(corrupted)
                    assert cipher_metadata(corrupted) != expected_ciphers
                    _, negative_record = call(prefix + ":tampered_self", source, corrupted, str(answer + 3))
                    if negative_record["error_code"] != "invalid_encrypted_content":
                        raise RuntimeError("Experiment stopped: tampered-cipher negative control inconclusive")
    except RuntimeError as error:
        report["stopped_reason"] = str(error)
    finally:
        report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        report["request_count"] = len(report["requests"])
        report["retry_count"] = retries
        save()
        print("sanitized_results=" + str(result_path), flush=True)


if __name__ == "__main__":
    main()
