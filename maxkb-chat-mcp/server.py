#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MaxKB 智能体对话 MCP 服务器（对接 MaxKB v2.x）

功能:
  - 通过 MaxKB 应用对话 API 与指定智能体对话（/chat/api/*）
  - 同一进程同时提供两种传输协议:
      * MCP Streamable HTTP:  http://<host>:<port>/mcp
      * MCP SSE:               http://<host>:<port>/sse  (消息回传 /messages/)
      * 健康检查:              http://<host>:<port>/health

对接的 MaxKB 接口（均使用 Authorization: Bearer <应用API Key> 认证）:
  - GET  {BASE_URL}/chat/api/open                        打开会话, 返回 chat_id
  - POST {BASE_URL}/chat/api/chat_message/{chat_id}      发送消息
  - POST {BASE_URL}/chat/api/application/{app_id}/chat/completions  (备用, OpenAI 兼容)

环境变量:
  BASE_URL          MaxKB 服务根地址, 如 http://<MAXKB_HOST>:8080        (必填)
                    注意: 请勿填应用专属地址 http://host/chat/api/<应用ID>。
                    服务会自动剥离 /chat|/admin 及其后的路径做归一化,
                    但建议在 compose 中直接填根地址以免歧义。
  API_KEY           应用 API Key, 如 agent-xxxxxxxx / application-xxx   (必填)
  SERVER_NAME       MCP 服务器名称(展示用)
  TOOL_NAME         对外暴露的工具名, 默认 maxkb_chat
  TOOL_DESCRIPTION  工具描述(会展示给调用方 AI)
  MAXKB_CHAT_PREFIX MaxKB 聊天 API 前缀, 默认 /chat
  MAXKB_TIMEOUT     调用 MaxKB 的超时时间(秒), 默认 180
  HOST / PORT       监听地址与端口, 默认 0.0.0.0:8000
  MCP_AUTH_TOKEN    可选, 设置后调用 /mcp、/sse、/messages 需携带
                    Authorization: Bearer <MCP_AUTH_TOKEN>
  MCP_ALLOWED_HOSTS 允许的 Host 头白名单, 逗号分隔; 默认 * 表示不校验
                    (例如: your-host:18010,localhost:8000)
"""
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from urllib.parse import urlsplit

import httpx
import uvicorn
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

log = logging.getLogger("maxkb-chat-mcp")


# ----------------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------------
def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    return v if v not in (None, "") else default


def _normalize_base_url(raw: str) -> str:
    """把 BASE_URL 归一化成 MaxKB 服务根地址。

    MaxKB 应用设置页常给出形如
        http://host:8080/chat/api/<application_id>
    的「应用专属地址」; 若把它直接当 BASE_URL, 本服务再拼 /chat/api/open 就会变成
        .../chat/api/<id>/chat/api/open  -> 404 HTTP_404_NOT_FOUND
    这里统一剥离 /chat、/admin 及其后的 API/UI 路径, 只保留可配置的反向代理前缀。
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "http://" + raw
    parts = urlsplit(raw)
    path = parts.path or ""
    cut = len(path)
    for marker in ("/chat/api", "/admin/api", "/chat", "/admin"):
        idx = path.lower().find(marker)
        if idx != -1:
            cut = min(cut, idx)
    path = path[:cut].rstrip("/")
    return f"{parts.scheme}://{parts.netloc}{path}"


BASE_URL_RAW = (_env("BASE_URL") or "").rstrip("/")
BASE_URL = _normalize_base_url(BASE_URL_RAW)
API_KEY = _env("API_KEY", "")
CHAT_PREFIX = (_env("MAXKB_CHAT_PREFIX", "/chat")).rstrip("/")
SERVER_NAME = _env("SERVER_NAME", "MaxKB 智能体对话 MCP 服务器")
TOOL_NAME = _env("TOOL_NAME", "maxkb_chat")
TOOL_DESCRIPTION = _env(
    "TOOL_DESCRIPTION",
    "发送消息给 MaxKB 智能体并获取回答",
)
MAXKB_TIMEOUT = float(_env("MAXKB_TIMEOUT", "180"))
HOST = _env("HOST", "0.0.0.0")
PORT = int(_env("PORT", "8000"))
MCP_AUTH_TOKEN = _env("MCP_AUTH_TOKEN")
ALLOWED_HOSTS_RAW = _env("MCP_ALLOWED_HOSTS", "*")


# ----------------------------------------------------------------------------
# MaxKB 客户端
# ----------------------------------------------------------------------------
_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=MAXKB_TIMEOUT)
    return _client


def auth_headers() -> dict:
    return {"Authorization": f"Bearer {API_KEY}"}


def parse_payload(resp: httpx.Response) -> dict:
    """解析 MaxKB 响应。MaxKB 的业务码包装在 HTTP 200 里: {"code":200,"message":..,"data":..}"""
    try:
        payload = resp.json()
    except Exception:
        raise ToolError(f"MaxKB 返回了非 JSON 内容 (HTTP {resp.status_code}): {resp.text[:300]}")
    if not isinstance(payload, dict):
        return {"data": payload}
    if resp.status_code == 401 or payload.get("code") in (1002, 1003):
        raise ToolError(
            f"MaxKB 认证失败 ({payload.get('code', resp.status_code)}): "
            f"{payload.get('message', '请检查 API_KEY 是否正确')} "
            f"[HTTP {resp.status_code}] {resp.text[:200]}"
        )
    code = payload.get("code")
    if code is not None and code != 200:
        hint = ""
        if code == 404 or resp.status_code == 404:
            hint = (
                " | 404 通常是 BASE_URL 填错: 应填 MaxKB 根地址(如 http://host:8080), "
                "不要填应用设置页复制的 /chat/api/<应用ID> 专属地址。"
                f"当前生效地址: {BASE_URL}, 请求: {resp.request.url}"
            )
        raise ToolError(
            f"MaxKB 调用失败 [{code}]: {payload.get('message', '')} "
            f"[HTTP {resp.status_code}]{hint}"
        )
    return payload


def extract_answer(payload: Any) -> str:
    """从 chat_message / chat/completions 的响应中提取回答文本(兼容多种返回结构)。"""
    data = payload.get("data", payload) if isinstance(payload, dict) else payload

    candidates: list[Any] = []

    def _collect(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        candidates.append(obj.get("content"))
        candidates.append(obj.get("answer_text"))
        atl = obj.get("answer_text_list")
        if isinstance(atl, list) and atl and isinstance(atl[0], dict):
            candidates.append(atl[0].get("content"))
        choices = obj.get("choices")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            msg = choices[0].get("message")
            if isinstance(msg, dict):
                candidates.append(msg.get("content"))

    _collect(data)
    _collect(payload)  # 兼容没有 data 包装的情况

    # 流式分片聚合格式: {"data": [{"content": "x"}, ...]} 或顶层即为分片列表
    if isinstance(data, list):
        joined = "".join(
            c.get("content", "") for c in data if isinstance(c, dict) and c.get("content")
        )
        if joined:
            return joined

    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c

    raise ToolError("无法从 MaxKB 响应中解析回答内容，原始返回: " + json.dumps(payload, ensure_ascii=False)[:1500])


# ----------------------------------------------------------------------------
# 会话管理: 复用 chat_id 实现多轮上下文
# ----------------------------------------------------------------------------
_state: dict = {"chat_id": None}


async def _open_chat() -> str:
    resp = await get_client().get(f"{BASE_URL}{CHAT_PREFIX}/api/open", headers=auth_headers())
    payload = parse_payload(resp)
    data = payload.get("data")
    if isinstance(data, str) and data:
        return data
    if isinstance(data, dict):
        for k in ("chat_id", "id"):
            if data.get(k):
                return str(data[k])
    raise ToolError("打开会话失败(未返回 chat_id): " + json.dumps(payload, ensure_ascii=False)[:300])


async def _send_message(chat_id: str, message: str) -> dict:
    resp = await get_client().post(
        f"{BASE_URL}{CHAT_PREFIX}/api/chat_message/{chat_id}",
        headers={**auth_headers(), "Content-Type": "application/json"},
        json={"message": message, "stream": False, "re_chat": False},
    )
    return parse_payload(resp)


async def maxkb_chat(message: str, new_chat: bool = False, chat_id: str = "") -> str:
    """发送消息给 MaxKB 智能体并获取回答。"""
    if not BASE_URL:
        raise ToolError("未配置 BASE_URL，请在环境变量中设置 MaxKB 服务地址")
    if not API_KEY:
        raise ToolError("未配置 API_KEY，请在环境变量中设置应用 API Key (application-xxx)")

    try:
        cid = (chat_id or "").strip() or None
        if cid is None:
            if new_chat or not _state["chat_id"]:
                _state["chat_id"] = await _open_chat()
            cid = _state["chat_id"]

        try:
            payload = await _send_message(cid, message)
        except ToolError as e:
            msg = str(e)
            # 会话不存在/已过期时自动重开一个会话并重试一次
            if "会话" in msg or "conversation" in msg.lower():
                _state["chat_id"] = await _open_chat()
                cid = _state["chat_id"]
                payload = await _send_message(cid, message)
            else:
                raise

        answer = extract_answer(payload)
        _state["chat_id"] = cid
        return f"{answer}\n\n[chat_id: {cid}]"
    except httpx.TimeoutException:
        raise ToolError(f"MaxKB 请求超时({MAXKB_TIMEOUT}s)，可在环境变量 MAXKB_TIMEOUT 中调大")
    except httpx.HTTPError as e:
        raise ToolError(f"无法连接 MaxKB ({BASE_URL}): {e}")
    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"调用 MaxKB 对话接口出错: {type(e).__name__}: {e}")


# ----------------------------------------------------------------------------
# 组装 MCP 服务器(双协议)
# ----------------------------------------------------------------------------
mcp = MCPServer(SERVER_NAME)

_FULL_DESCRIPTION = (
    TOOL_DESCRIPTION
    + "\n\n参数说明: message=要发送的消息内容; new_chat=true 时新开一个对话(默认 false, "
      "自动延续服务器内保存的最近一次对话上下文); chat_id=传入已有对话 id 以在该对话中继续。"
)

mcp.add_tool(maxkb_chat, name=TOOL_NAME, description=_FULL_DESCRIPTION)


def _build_security() -> TransportSecuritySettings:
    hosts = [h.strip() for h in ALLOWED_HOSTS_RAW.split(",") if h.strip()]
    if not hosts or hosts == ["*"]:
        # 内网/默认场景: 关闭 DNS 重绑定保护, 任意 Host 均可访问
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=[],  # 非浏览器客户端不携带 Origin 头, 默认放行
    )


_security = _build_security()
_streamable_app = mcp.streamable_http_app(transport_security=_security, host=HOST)
_sse_app = mcp.sse_app(transport_security=_security, host=HOST)


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "server": SERVER_NAME,
            "tool": TOOL_NAME,
            "transports": {
                "streamable-http": "/mcp",
                "sse": "/sse (POST /messages/)",
            },
            "maxkb_base_url": BASE_URL or "(未配置)",
            "maxkb_base_url_raw": BASE_URL_RAW or "(未配置)",
            "maxkb_open_url": f"{BASE_URL}{CHAT_PREFIX}/api/open" if BASE_URL else "(未配置)",
            "api_key_set": bool(API_KEY),
            "current_chat_id": _state["chat_id"],
        }
    )


@asynccontextmanager
async def lifespan(app):
    # 挂载的子应用 lifespan 不会执行, 必须由宿主应用启动 session_manager
    async with mcp.session_manager.run():
        log.info("MCP session manager started")
        yield


# 将两个子应用(streamable-http 与 sse)的路由直接嫁接到宿主应用。
# 注意: 不能用 Mount("/mcp", ...), 因为 Starlette 的 Mount 不匹配不带尾斜杠的裸路径,
# 而标准 MCP 客户端连接的就是 http://host:port/mcp。
app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        *_streamable_app.routes,
        *_sse_app.routes,
    ],
    lifespan=lifespan,
)


# ----------------------------------------------------------------------------
# 可选的访问令牌校验(纯 ASGI 中间件)
# ----------------------------------------------------------------------------
class BearerAuthMiddleware:
    PROTECTED_PREFIXES = ("/mcp", "/sse", "/messages")

    def __init__(self, asgi_app, token: str):
        self.asgi_app = asgi_app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            path = scope.get("path", "")
            if any(path == p or path.startswith(p + "/") for p in self.PROTECTED_PREFIXES):
                auth = b""
                for k, v in scope.get("headers", []):
                    if k.lower() == b"authorization":
                        auth = v
                        break
                if auth.lower() != f"bearer {self.token}".lower().encode():
                    response = JSONResponse({"error": "unauthorized, 请携带 Authorization: Bearer <MCP_AUTH_TOKEN>"}, status_code=401)
                    await response(scope, receive, send)
                    return
        await self.asgi_app(scope, receive, send)


if MCP_AUTH_TOKEN:
    app = BearerAuthMiddleware(app, MCP_AUTH_TOKEN)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.info("=" * 60)
    log.info("MaxKB 智能体对话 MCP 服务器启动中")
    log.info("  对外协议:  MCP(Streamable HTTP) http://%s:%s/mcp", HOST, PORT)
    log.info("             SSE                   http://%s:%s/sse", HOST, PORT)
    log.info("  健康检查:  http://%s:%s/health", HOST, PORT)
    log.info("  MaxKB:     %s (API_KEY %s)", BASE_URL or "未配置", "已配置" if API_KEY else "未配置!!")
    log.info("  工具:      %s", TOOL_NAME)
    if not BASE_URL or not API_KEY:
        log.error("  !! BASE_URL / API_KEY 未配置, 工具调用将报错, 请检查环境变量")
    log.info("=" * 60)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
