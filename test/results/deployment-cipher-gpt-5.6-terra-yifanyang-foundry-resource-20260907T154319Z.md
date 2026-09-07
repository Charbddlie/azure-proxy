# GPT-5.6 Terra deployment encrypted-state probe

## Result

On 2026-09-07 15:43:19–15:43:47 UTC, encrypted reasoning produced by either `gpt-5.6-terra` or `gpt-5.6-terra-dz` was accepted by the other deployment at the same `yifanyang-foundry-resource` endpoint. Both directions passed twice. Same-deployment positive controls passed, and ciphertext-tampering negative controls returned HTTP 400 `invalid_encrypted_content` on both deployments.

This establishes observed API-level encrypted-content compatibility for this deployment pair at this time. Runtime discovery reports `model_version: null` for both deployments, so the deployed model versions remain unknown. Response `model` values echo the requested deployment name. This experiment does not establish compatibility with other endpoints, other deployment pairs, future model revisions, or equivalent reasoning performance.

## Target and method

- Endpoint: `yifanyang-foundry-resource`.
- URL: `https://yifanyang-foundry-resource.services.ai.azure.com/api/projects/yifanyang-foundry/openai/v1/responses`.
- Deployments: `gpt-5.6-terra` and `gpt-5.6-terra-dz`; discovered model `gpt-5.6-terra`; both model versions unknown.
- Existing isolated Azure CLI identity; resource scope `https://ai.azure.com/.default`.
- Direct upstream requests, `store: false`, `stream: false`, `include: ["reasoning.encrypted_content"]`, `reasoning.effort: low`, `max_output_tokens: 512`.
- Synthetic arithmetic: compute `37 * 43 + increment`; next user turn asks to add 3. Replays preserve original input, every output item, and the new user turn. Cipher and full-history hashes are recorded.
- Two independent minted histories in each direction. Self/cross ordering reverses in the second round. Each deployment receives one separate tampered-cipher negative-control request; one ciphertext character is changed while preserving its length.
- All four mints returned one non-empty encrypted item of 1,060 characters. All 12 valid requests completed and returned the expected arithmetic answer.
- 14 requests total, serial execution, zero retries, zero 429 responses, zero upstream 5xx responses, zero incomplete responses. Reported usage totals: 870 input tokens and 223 output tokens, including 143 reasoning tokens.

| Check | Result |
| --- | --- |
| `gpt-5.6-terra` mint | 2/2 HTTP 200 completed, non-empty cipher |
| `gpt-5.6-terra-dz` mint | 2/2 HTTP 200 completed, non-empty cipher |
| `gpt-5.6-terra` → itself | 2/2 HTTP 200 completed |
| `gpt-5.6-terra` → `gpt-5.6-terra-dz` | 2/2 HTTP 200 completed |
| `gpt-5.6-terra-dz` → itself | 2/2 HTTP 200 completed |
| `gpt-5.6-terra-dz` → `gpt-5.6-terra` | 2/2 HTTP 200 completed |
| Tampered cipher → `gpt-5.6-terra` | HTTP 400 `invalid_encrypted_content` |
| Tampered cipher → `gpt-5.6-terra-dz` | HTTP 400 `invalid_encrypted_content` |

## Evidence and reproduction

Sanitized request-level evidence: [JSON](deployment-cipher-gpt-5.6-terra-yifanyang-foundry-resource-20260907T154319Z.json). Tokens, ciphertext, and response text were kept in process memory only. Production configuration and service processes were unchanged.

```bash
./.venv/bin/python tools/probe_deployment_cipher.py \
  --endpoint yifanyang-foundry-resource \
  --model gpt-5.6-terra \
  --deployments gpt-5.6-terra gpt-5.6-terra-dz \
  --max-requests 16 --repeats 2
```

OpenAI Docs informed the complete-history replay and status checks. No official search tool was available, so the official pages were fetched directly before the experiment:

- [Reasoning guide](https://developers.openai.com/api/docs/guides/reasoning): preserve every output item for stateless history; distinguish `incomplete` from `completed`.
- [GPT-5.6 Terra model](https://developers.openai.com/api/docs/models/gpt-5.6-terra): Responses support and the supported `low` reasoning effort.

The official pages provide API usage guidance; the deployment-pair compatibility result above comes from this experiment.
