# Fork affinity and active-session window

## Behavior

- Codex 0.153.4's ephemeral `thread/fork` creates a new `session-id` and
  `prompt_cache_key`. It includes `forked_from_thread_id` in the JSON string
  carried by both `x-codex-turn-metadata` and
  `client_metadata["x-codex-turn-metadata"]`.
- The installed copilot-api provider path preserves the body copy. Serving
  accepts either carrier and atomically inherits the parent's valid endpoint
  descriptor on the fork's first request. No gateway modification is required.
- Selection uses identifiers and route capabilities. Tests cover the
  `gpt-7-azure` and `arbitrary-model-alias` names on `renamed-resource`.
- Forks retain independent activity and TTL. Existing fork bindings survive
  serving restarts, parent expiration, and routing removal of the original
  endpoint. Unknown encrypted state continues to fail before dispatch.
- `routing.session_affinity.active_window_seconds` defaults to 300. All TUI
  binding counts include recent or in-flight sessions. `retained_sessions`
  includes all unexpired bindings; the 172800-second TTL is unchanged.

## Verification

- Synthetic Codex probe: `tools/probe_codex_affinity.py <codex-binary>`.
  Uses a temporary Codex home and fake upstream; prints request shape only.
- Unit/integration discovery: 165 tests passed. The final affinity suite's
  31 tests passed, including a regression proving that an existing fork no
  longer consults its parent.
- Protocol and routing regression: `test/run_tests.py`, 133/133 passed.
- Live serving upgrade: worker 1927729 accepted traffic after 1.145 seconds
  of warmup; socket switch took approximately 0.001 seconds. The supervisor
  remained 1059128 and the old worker continued draining.
- Live test through `http://127.0.0.1:6768/v1/responses`, using synthetic
  arithmetic prompts and `gpt-6-astra-azure`: parent and fork returned HTTP
  200. One encrypted reasoning item was replayed. The deployments were
  `gpt-6-astra` and `gpt-6-astra-2`, both at `yifanyang-foundry-eastus2`.
- Following upgrade, health reported a 300-second active window, healthy
  persistence, and 114 retained bindings, of which 3 were active at that
  instant. The live synthetic test subsequently added two temporary sessions.

Official behavior reference:
[Codex side chats](https://developers.openai.com/codex/cli/slash-commands/).
