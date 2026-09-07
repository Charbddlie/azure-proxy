# 同 endpoint 跨 deployment 加密状态复用实验

时间：2026-09-07 15:35–15:37 UTC。

结论：本次结果不确定。两个 deployment 的合成首请求均受到 token 配额限制，未取得任何加密状态，因此没有执行原样复用或篡改密文对照。429 不能作为跨 deployment 不兼容的证据。

## 实验对象与方法

- endpoint：`yifanyang-foundry-eastus2`。
- 直接上游 URL：`https://yifanyang-foundry-eastus2.cognitiveservices.azure.com/openai/v1/responses`。
- deployment：`gpt-6-astra`、`gpt-6-astra-2`，本地运行时记录的 model 均为 `gpt-6-astra`、model_version 均为 `2026-09-03`。
- Azure scope：`https://cognitiveservices.azure.com/.default`，使用代理现有隔离 Azure CLI 身份。
- 仅使用合成算术问题；`store:false`、`stream:false`、`include:["reasoning.encrypted_content"]`、`reasoning.effort:low`、`max_output_tokens:512`。
- 计划每个来源做两轮同 deployment 与跨 deployment 原样 replay，保留所有 output items；每个来源做一次独立篡改密文负对照。结果记录密文长度和 SHA-256，不保存密文、令牌或原始模型输出。
- 串行请求；两个 deployment 均限流后立即停止；实际上合计发送 3 个请求。

## 实际结果

| 时间 UTC | deployment | 请求 | HTTP / error_code | 剩余 token | 剩余请求 | Retry-After |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 15:35:45 | gpt-6-astra | 合成首请求 | 429 / rate_limit_exceeded | 未记录 | 未记录 | 未记录 |
| 15:36:56 | gpt-6-astra-2 | 合成首请求 | 429 / rate_limit_exceeded | -78861 | 332 | 14s |
| 15:37:00 | gpt-6-astra | 合成首请求 | 429 / rate_limit_exceeded | -55205 | 999 | 3s |

两次记录到配额头的请求均显示剩余请求数充足、剩余 token 为负。没有 `invalid_encrypted_content` 错误，也没有成功完成的 response。

## 边界

此结果不能判定会话是否必须固定 deployment。需要在两个 deployment 的 token 配额可用时重新执行对照。即使后续同版本部署之间的双向复用成功，也只能证明当时该 endpoint 和该版本组合的实测行为，不能证明跨 endpoint、跨模型版本或部署升级后始终兼容。

未修改线上配置、绑定策略或服务代码，未重启服务。新增实验脚本通过 `py_compile` 语法检查。

原始脱敏结果：

- `deployment-cipher-20260907T153545Z.json`
- `deployment-cipher-20260907T153656Z.json`

请求结构参考：[OpenAI reasoning guide](https://developers.openai.com/api/docs/guides/reasoning)。该文档说明无状态 encrypted_content 的保留与重放方法；Azure deployment 之间的兼容性仍需实验确定。
