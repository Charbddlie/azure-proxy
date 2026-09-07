# GPT Image 2 状态字段实验

时间：2026-09-07 15:45 UTC。

在 `gpt4v-swc` 的 `gpt-image-2` deployment 上直接调用 Images API，发送一条合成的黑色方块提示词，`quality: low`、`size: 1024x1024`、`n: 1`。

- HTTP 200，耗时 11.239 秒；共 1 个请求，无重试。
- 返回 1 张有效 PNG，118,353 字节；只保留 SHA-256 和长度，图片内容未落盘。
- 响应顶层字段为 `background`、`created`、`data`、`output_format`、`quality`、`size`、`usage`。
- 图片条目只有 `b64_json`，整个响应中没有 `encrypted_content`。
- 总用量 213 tokens。

本机登记的 GPT Image 2 位于三个不同 endpoint，每个 endpoint 只有一个 deployment，因此当前没有同 endpoint 双 deployment 的对照条件。这次 Images API 请求没有产生可用于密文重放的状态；该结果不推断 Responses 图像工具或 `previous_response_id` 的跨 endpoint 兼容性。

请求参数参照 [OpenAI 图像生成指南](https://developers.openai.com/api/docs/guides/image-generation) 和 [GPT Image 2 模型页](https://developers.openai.com/api/docs/models/gpt-image-2)。模型运行时记录版本为 `2026-04-21`。

脱敏证据：[JSON](image-state-gpt-image-2-gpt4v-swc-20260907T154539Z.json)。复现脚本：`tools/probe_image_state.py`。

使用现有隔离 Azure 身份，未修改线上配置或重启服务。
