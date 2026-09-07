# GPT-5.5 同 endpoint 跨 deployment 加密状态实验

实测结论：在 `gpt4v-scus` 的 `gpt-5.5` 与 `gpt-5.5-2` 之间，两个方向的原样加密状态复用均成功。就本次测试的组合而言，加密状态可以跨这两个 deployment 使用。

两者运行时登记的模型均为 `gpt-5.5`，版本均为 `2026-04-24`。实验直接请求 `https://gpt4v-scus.openai.azure.com/openai/v1/responses`，使用现有隔离 Azure CLI 身份，scope 为 `https://cognitiveservices.azure.com/.default`。

## 结果

2026-09-07 15:43:07–15:43:40 UTC，共 14 个串行请求，无限流、重试或 5xx。A 为 `gpt-5.5`，B 为 `gpt-5.5-2`。

| 测试 | 次数 | 结果 |
| --- | ---: | --- |
| A、B 各生成两份新密文 | 4 | 全部 HTTP 200、`status=completed`，每份含一个非空加密段 |
| A → A | 2 | 全部 HTTP 200、`status=completed` |
| A → B | 2 | 全部 HTTP 200、`status=completed` |
| B → B | 2 | 全部 HTTP 200、`status=completed` |
| B → A | 2 | 全部 HTTP 200、`status=completed` |
| A、B 各自篡改一份密文后回传 | 2 | 全部 HTTP 400、`invalid_encrypted_content` |

12 个正常完成请求的算术回答全部正确。上游返回的用量合计 980 tokens，其中 input 568、output 412；错误请求没有返回 usage。

## 对照完整性

- 使用合成算术问题，`store:false`、`stream:false`、`include:["reasoning.encrypted_content"]`、`reasoning.effort:low`、`max_output_tokens:512`。
- 每次 mint 完成且取得非空密文后，使用完整原输入、全部未修改的 output items、后续用户问题组成 replay。
- 每轮同 deployment 与跨 deployment 请求只改变请求体的 `model`。已核验二者完整 input 的 SHA-256 相同，密文 SHA-256 相同且等于该轮 mint 输出的密文 SHA-256。
- 四份密文长度分别为 1036、1016、1016、1016 字符。篡改对照在独立请求中更改一个编码字符，保留长度，并核验 SHA-256 确实改变。
- 第二轮反转同 deployment 与跨 deployment 对照的调用顺序。
- 未保存令牌、原始密文、原始模型输出或真实用户会话内容。

## 结论边界

这组结果支持上述 endpoint、模型版本和 deployment 组合在实验时点可以双向复用加密状态。篡改负对照表明上游确实验证收到的密文。

本实验没有测量模型性能变化，也不能证明其他模型、跨 endpoint、部署重新创建或升级后的兼容性；此前 `gpt-6-astra` 实验因 429 未取得密文，其结论仍为不确定。

线上配置、会话绑定策略和服务状态均未修改，未重启服务。

## 复现与产物

```bash
./.venv/bin/python tools/probe_deployment_cipher.py \
  --endpoint gpt4v-scus --model gpt-5.5 \
  --deployments gpt-5.5 gpt-5.5-2 --max-requests 16 --repeats 2
```

脱敏 JSON：`deployment-cipher-gpt-5.5-gpt4v-scus-20260907T154307Z.json`。

官方文档用于核对无状态密文重放结构，Azure deployment 兼容性以本次实验为依据：

- [OpenAI reasoning guide](https://developers.openai.com/api/docs/guides/reasoning)
- [GPT-5.5 model](https://developers.openai.com/api/docs/models/gpt-5.5)
