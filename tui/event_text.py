"""Chinese descriptions of the current structured event types."""

import re

KIND_LABELS = {
    "route_refresh": "路由更新",
    "boot": "服务启动", "request": "收到请求", "response": "请求结果",
    "capacity": "容量更新", "throttle": "上游限流", "demote": "上游异常",
    "foreign": "外部用量", "failover": "等待重试", "pin": "会话绑定",
    "upstream_error": "上游错误", "timeout": "请求超时",
    "exhausted": "重试耗尽", "image_tool": "图像工具", "token": "访问凭据",
}


def number(value, default="未知"):
    if value is None:
        return default
    try:
        return "{:g}".format(float(value))
    except (ValueError, TypeError):
        return str(value)


def retry_action(event):
    source, target = event.get("route"), event.get("to_route")
    if source and target:
        if source == target:
            return "原地重试", "重试同一部署"
        if source.partition("/")[0] == target.partition("/")[0]:
            return "换部署重试", "在同一源内更换部署重试"
        return "换源重试", "更换源后重试"
    return "等待重试", "重试请求"


def kind_label(event):
    kind = event.get("kind")
    if kind == "failover":
        return retry_action(event)[0]
    if kind == "response" and event.get("broke"):
        return "流连接中断"
    if kind == "response" and event.get("status") == 429:
        return "返回限流"
    return KIND_LABELS.get(kind, "其他事件")


def reason_text(value):
    reason = str(value or "")
    if "200+throttled" in reason:
        return "上游限流（HTTP 200）"
    if (re.search(r"\b429\b", reason) or "rate_limit" in reason
            or "too_many_requests" in reason):
        return "上游限流"
    if "timeout" in reason.lower():
        return "上游请求超时"
    if "transport" in reason.lower() or "ConnectError" in reason:
        return "上游连接异常"
    status = re.search(r"\b([45]\d\d)\b", reason)
    if status:
        return "上游返回 HTTP " + status.group(1)
    return "上游请求异常"


def message(event):
    """Describe the observation/action without inferring a successful model turn."""
    kind = event.get("kind")
    if kind == "route_refresh":
        return "路由已更新：新增 {}，移除 {}，当前 {} 个部署".format(
            number(event.get("added")), number(event.get("removed")), number(event.get("total")))
    seconds = number(event.get("seconds"))
    if kind == "failover":
        return "{}；{} 秒后{}".format(reason_text(event.get("reason")),
                                     number(event.get("wait_seconds")), retry_action(event)[1])
    if kind == "demote":
        return reason_text(event.get("reason"))
    if kind == "throttle":
        if event.get("inband") and event.get("header") is not None:
            return "上游返回 HTTP 200，要求等待 {} 秒；代理判为限流".format(number(event["header"]))
        if event.get("last_route"):
            return "重试机会已用完，将上游限流响应返回客户端"
        if event.get("in_stream"):
            return "响应流中报告限流"
        if event.get("reason"):
            return reason_text(event["reason"])
        return "上游触发限流"
    if kind == "timeout":
        operation = {"ReadTimeout": "读取上游响应", "ConnectTimeout": "连接上游",
                     "WriteTimeout": "发送上游请求", "PoolTimeout": "等待可用连接"}.get(
                         event.get("timeout_type"), "上游请求")
        return "{}超时：限制 {} 秒，已耗时 {} 秒".format(operation, number(event.get("timeout_seconds")), seconds)
    if kind == "response":
        status = event.get("status")
        if event.get("broke"):
            return "响应流连接中断，已传输 {} 字节，耗时 {} 秒".format(number(event.get("bytes")), seconds)
        if status == 429:
            return "向客户端返回上游限流（HTTP 429），耗时 {} 秒".format(seconds)
        if isinstance(status, int) and status >= 400:
            return "向客户端返回 HTTP {}，耗时 {} 秒".format(status, seconds)
        if event.get("stream"):
            return "数据流结束，HTTP {}，{} 字节，耗时 {} 秒".format(number(status), number(event.get("bytes")), seconds)
        return "请求返回 HTTP {}，耗时 {} 秒".format(number(status), seconds)
    if kind == "upstream_error":
        code = event.get("error_code")
        description = {"invalid_encrypted_content": "上游无法验证或解密加密内容",
                       "truncated": "上游提前结束响应流，缺少完成事件",
                       "server_error": "上游内部错误",
                       "rate_limit_exceeded": "响应流中报告限流",
                       "too_many_requests": "上游请求过多，触发限流"}.get(code)
        return description or "上游请求失败，错误代码：{}".format(code or "未提供")
    if kind == "exhausted":
        return "已尝试 {} 次，全部失败；返回 HTTP {}。最后原因：{}".format(
            number(event.get("attempts")), event.get("status", 503), reason_text(event.get("error")))
    if kind == "pin":
        return "会话绑定到当前源"
    if kind == "foreign":
        return "估计外部用量 {} RPM；本代理用量 {} RPM，已验证最大值 {} RPM".format(
            number(event.get("other_rpm")), number(event.get("our_rpm")), number(event.get("capacity_rpm")))
    if kind == "capacity":
        return "已验证的最大安全速率从 {} 提高到 {} RPM".format(
            number(event.get("capacity_before")), number(event.get("capacity_after")))
    if kind == "token":
        if event.get("ok") is False:
            if event.get("expires_in_seconds", 0) > 0:
                return "凭据刷新失败，现有凭据剩余有效期 {} 秒".format(number(event["expires_in_seconds"]))
            return "没有可用的上游访问凭据，需要检查登录状态"
        return "访问凭据已刷新，剩余有效期 {} 秒".format(number(event.get("expires_in_seconds")))
    if kind == "image_tool":
        if event.get("ok") is False:
            return "可用源缺少图像部署，图像工具请求已原样转发"
        return "图像工具请求限定到含图像部署的 {} 个候选路由".format(number(event.get("routes")))
    if kind == "boot":
        return "服务已启动：对话模型 {} 个，响应模型 {} 个，图像模型 {} 个".format(
            number(event.get("chat_models")), number(event.get("responses_models")), number(event.get("image_models")))
    if kind == "request":
        return "收到{}请求：{}".format("流式" if event.get("stream") else "非流式", event.get("face", "接口未知"))
    return "其他事件：" + str(event.get("message", "未提供说明"))
