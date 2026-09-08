#!/usr/bin/env python3
"""Inspect Codex's ephemeral-fork identifiers against a synthetic local upstream.

Usage: .venv/bin/python tools/probe_codex_affinity.py /absolute/path/to/codex
No model calls or user history are needed. Only request shape is printed.
"""

import collections
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "test"))
from fake_azure import Behaviour, FakeAzure


def events():
    items = [
        dict(type="reasoning", id="rs_probe", summary=[], encrypted_content="probe-cipher"),
        dict(type="message", id="msg_probe", role="assistant", status="completed",
             phase="final_answer", content=[dict(type="output_text", text="OK", annotations=[])]),
    ]
    return [dict(type="response.created", response=dict(id="resp_probe", status="in_progress")),
            *[dict(type="response.output_item.done", output_index=i, item=item)
              for i, item in enumerate(items)],
            dict(type="response.completed", response=dict(id="resp_probe", status="completed",
                 output=items, usage=dict(input_tokens=1, output_tokens=1, total_tokens=2)))]


def probe(executable):
    with tempfile.TemporaryDirectory(prefix="codex-affinity-") as root:
        codex_dir = os.path.join(root, "codex")
        workspace = os.path.join(root, "workspace")
        os.mkdir(codex_dir)
        os.mkdir(workspace)
        upstream = FakeAzure("probe", [Behaviour(events=events())]).start()
        env = {k: v for k, v in os.environ.items() if not k.startswith("CODEX_")}
        env["CODEX_HOME"] = codex_dir
        command = [executable, "app-server"]
        config = {
            "model": '"gpt-6-astra-azure"',
            "model_provider": '"affinity-probe"',
            "model_providers.affinity-probe": '{name="Probe",base_url="' + upstream.url.rstrip("/")
                + '/v1",wire_api="responses",requires_openai_auth=false}',
            "features.enable_request_compression": "false",
            "features.multi_agent": "false",
            "check_for_update_on_startup": "false",
        }
        for key, value in config.items():
            command.extend(["-c", key + "=" + value])
        messages = queue.Queue()
        errors = collections.deque(maxlen=10)
        process = subprocess.Popen(command, env=env, cwd=workspace, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        def read_stdout():
            for line in process.stdout:
                messages.put(json.loads(line))
            messages.put(None)

        def read_stderr():
            for line in process.stderr:
                errors.append(line.rstrip())

        threading.Thread(target=read_stdout, daemon=True).start()
        threading.Thread(target=read_stderr, daemon=True).start()

        def wait(predicate):
            deadline = time.monotonic() + 40
            while True:
                message = messages.get(timeout=max(.01, deadline - time.monotonic()))
                if message is None:
                    raise RuntimeError(list(errors))
                if predicate(message):
                    return message
                if time.monotonic() >= deadline:
                    raise TimeoutError(list(errors))

        request_id = 0

        def rpc(method, params):
            nonlocal request_id
            request_id += 1
            process.stdin.write(json.dumps(dict(id=request_id, method=method, params=params)) + "\n")
            process.stdin.flush()
            result = wait(lambda message: message.get("id") == request_id)
            if "error" in result:
                raise RuntimeError(result["error"])
            return result["result"]

        def turn(thread):
            rpc("turn/start", dict(threadId=thread, input=[dict(type="text", text="Say OK.", text_elements=[])]))
            result = wait(lambda message: message.get("method") == "turn/completed"
                          and message["params"]["threadId"] == thread)
            if result["params"]["turn"].get("error"):
                raise RuntimeError(result["params"]["turn"]["error"])

        try:
            rpc("initialize", dict(clientInfo=dict(name="codex-affinity-probe", version="1"),
                                   capabilities=dict(experimentalApi=True)))
            parent = rpc("thread/start", dict(cwd=workspace, model="gpt-6-astra-azure",
                         approvalPolicy="never", sandbox="read-only"))["thread"]["id"]
            turn(parent)
            child = rpc("thread/fork", dict(threadId=parent, ephemeral=True,
                        threadSource="cli_side", excludeTurns=True))["thread"]["id"]
            turn(child)
            print(json.dumps(dict(parent=parent, child=child), indent=2))
            for request in upstream.requests:
                body = request["body"]
                headers = {k: v for k, v in request["headers"].items()
                           if any(part in k.lower() for part in ("session", "thread", "subagent", "metadata"))}
                print(json.dumps(dict(headers=headers, prompt_cache_key=body.get("prompt_cache_key"),
                     client_metadata=body.get("client_metadata"),
                     previous_response_id=body.get("previous_response_id"),
                     has_encrypted="encrypted_content" in json.dumps(body.get("input")),
                     body_keys=sorted(body)), indent=2))
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            upstream.stop()


if __name__ == "__main__":
    probe(os.path.abspath(sys.argv[1]))
