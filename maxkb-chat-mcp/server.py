#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MaxKB agent chat MCP server (MaxKB v2.x).

Features:
  - Chat with a MaxKB application (agent) through the app chat API (/chat/api/*)
  - One process serves both transports:
      * MCP Streamable HTTP:  http://<host>:<port>/mcp
      * MCP SSE:               http://<host>:<port>/sse  (messages via /messages/)
      * Health check:          http://<host>:<port>/health

MaxKB endpoints used (all with Authorization: Bearer <application API key>):
  - GET  {BASE_URL}/chat/api/open                        open a session, returns chat_id
  - POST {BASE_URL}/chat/api/chat_message/{chat_id}      send a message
  - POST {BASE_URL}/chat/api/application/{app_id}/chat/completions  (fallback, OpenAI-compatible)

Environment variables:
  BASE_URL          MaxKB service root, e.g. http://<MAXKB_HOST>:8080        (required)
                    Do NOT use the per-app address http://host/chat/api/<app-id>.
                    The service strips /chat|/admin suffixes automatically,
                    but setting the bare root in compose avoids ambiguity.
  API_KEY           [single agent] application API key, e.g. agent-xxxxxxxx
                    (either this or CHAT_AGENTS)
  CHAT_AGENTS       [multi agent] register several chat tools in one process:
                      tool_name:app_api_key[:description]|tool_name2:key2[:desc2]
                    - entries separated by |; description optional (default
                      text used), may contain colons (only the first two fields
                      are split); do not use | inside a description.
                    - tool names should be ASCII (letters/digits/_/-/.) for
                      client-side classification and must not duplicate the
                      TOOL_NAME produced by API_KEY.
                    - each agent keeps its own chat_id; contexts never mix.
                    Example:
                      CHAT_AGENTS=ops_chat:agent-aaa:ops expert for VMware questions|crm_chat:agent-bbb
                    Both variables may be set together (API_KEY yields the
                    TOOL_NAME tool, CHAT_AGENTS appends the rest).
  SERVER_NAME       MCP server display name
  TOOL_NAME         tool name in single-agent mode, default maxkb_chat
  TOOL_DESCRIPTION  tool description shown to the calling AI
  MAXKB_CHAT_PREFIX MaxKB chat API prefix, default /chat
  MAXKB_TIMEOUT     MaxKB call timeout (seconds), default 180
  HOST / PORT       bind address and port, default 0.0.0.0:8000
  MCP_AUTH_TOKEN    optional; when set, /mcp, /sse, /messages require
                    Authorization: Bearer <MCP_AUTH_TOKEN>
  MCP_ALLOWED_HOSTS Host header allow-list, comma separated; default * (no check)
                    (e.g. your-host:18010,localhost:8000)
"""
import json
import logging
import os
import re
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
# Configuration
# ----------------------------------------------------------------------------
def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    return v if v not in (None, "") else default


def _normalize_base_url(raw: str) -> str:
    """Normalize BASE_URL to the MaxKB service root.

    The MaxKB app settings page often shows a per-app address like
        http://host:8080/chat/api/<application_id>
    If that is used as BASE_URL, appending /chat/api/open again yields
        .../chat/api/<id>/chat/api/open  -> 404 HTTP_404_NOT_FOUND
    Here we strip /chat, /admin and everything after them, keeping only an
    optional reverse-proxy prefix.
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
SERVER_NAME = _env("SERVER_NAME", "MaxKB Agent Chat MCP Server")
TOOL_NAME = _env("TOOL_NAME", "maxkb_chat")
TOOL_DESCRIPTION = _env(
    "TOOL_DESCRIPTION",
    "Ask and get answers from the MaxKB agent.",
)
CHAT_AGENTS_RAW = _env("CHAT_AGENTS", "")
MAXKB_TIMEOUT = float(_env("MAXKB_TIMEOUT", "180"))
HOST = _env("HOST", "0.0.0.0")
PORT = int(_env("PORT", "8000"))
MCP_AUTH_TOKEN = _env("MCP_AUTH_TOKEN")
ALLOWED_HOSTS_RAW = _env("MCP_ALLOWED_HOSTS", "*")

DEFAULT_AGENT_DESC = "Ask and get answers from the MaxKB agent."
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")


class AgentConfig:
    """One agent = one chat tool: tool name + application API key + description."""

    __slots__ = ("tool_name", "api_key", "description", "chat_id")

    def __init__(self, tool_name: str, api_key: str, description: str):
        self.tool_name = tool_name
        self.api_key = api_key
        self.description = description
        self.chat_id: str | None = None  # each agent keeps its own session


def _parse_agents(raw: str) -> list[AgentConfig]:
    """Parse CHAT_AGENTS=name:key[:desc]|name2:key2; bad config fails at startup."""
    agents: list[AgentConfig] = []
    for item in (raw or "").split("|"):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":", 2)
        if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
            raise RuntimeError(
                f"Invalid CHAT_AGENTS entry (expected tool_name:app_api_key[:description]): {item!r}"
            )
        name, key = parts[0].strip(), parts[1].strip()
        desc = parts[2].strip() if len(parts) > 2 and parts[2].strip() else DEFAULT_AGENT_DESC
        if not _TOOL_NAME_RE.match(name):
            raise RuntimeError(
                f"CHAT_AGENTS tool name may only contain letters/digits/underscore/hyphen/dot (ASCII recommended): {name!r}"
            )
        agents.append(AgentConfig(name, key, desc))
    return agents


AGENT_LIST: list[AgentConfig] = []
_seen_names: set[str] = set()
if API_KEY:
    ag = AgentConfig(TOOL_NAME, API_KEY, TOOL_DESCRIPTION)
    AGENT_LIST.append(ag)
    _seen_names.add(TOOL_NAME)
for ag in _parse_agents(CHAT_AGENTS_RAW):
    if ag.tool_name in _seen_names:
        raise RuntimeError(f"Duplicate tool name: {ag.tool_name} (check API_KEY/TOOL_NAME vs CHAT_AGENTS)")
    _seen_names.add(ag.tool_name)
    AGENT_LIST.append(ag)


# ----------------------------------------------------------------------------
# MaxKB client
# ----------------------------------------------------------------------------
_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=MAXKB_TIMEOUT)
    return _client


def auth_headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


def parse_payload(resp: httpx.Response) -> dict:
    """Parse a MaxKB response. Business codes are wrapped in HTTP 200: {"code":200,...}"""
    try:
        payload = resp.json()
    except Exception:
        raise ToolError(f"MaxKB returned non-JSON content (HTTP {resp.status_code}): {resp.text[:300]}")
    if not isinstance(payload, dict):
        return {"data": payload}
    if resp.status_code == 401 or payload.get("code") in (1002, 1003):
        raise ToolError(
            f"MaxKB authentication failed ({payload.get('code', resp.status_code)}): "
            f"{payload.get('message', 'check whether API_KEY is correct')} "
            f"[HTTP {resp.status_code}] {resp.text[:200]}"
        )
    code = payload.get("code")
    if code is not None and code != 200:
        hint = ""
        if code == 404 or resp.status_code == 404:
            hint = (
                " | 404 usually means BASE_URL is wrong: use the MaxKB root "
                "(e.g. http://host:8080), not the per-app address "
                "/chat/api/<app-id> copied from the app settings page. "
                f"Effective base: {BASE_URL}, request: {resp.request.url}"
            )
        raise ToolError(
            f"MaxKB call failed [{code}]: {payload.get('message', '')} "
            f"[HTTP {resp.status_code}]{hint}"
        )
    return payload


def extract_answer(payload: Any) -> str:
    """Extract the answer text from chat_message / chat/completions responses."""
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
    _collect(payload)  # tolerate responses without the data wrapper

    # streamed chunk aggregation: {"data": [{"content": "x"}, ...]} or a bare list
    if isinstance(data, list):
        joined = "".join(
            c.get("content", "") for c in data if isinstance(c, dict) and c.get("content")
        )
        if joined:
            return joined

    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c

    raise ToolError("Could not parse an answer from the MaxKB response, raw: " + json.dumps(payload, ensure_ascii=False)[:1500])


# ----------------------------------------------------------------------------
# Session management: each agent keeps its own chat_id, contexts never mix
# ----------------------------------------------------------------------------
async def _open_chat(api_key: str) -> str:
    resp = await get_client().get(
        f"{BASE_URL}{CHAT_PREFIX}/api/open", headers=auth_headers(api_key)
    )
    payload = parse_payload(resp)
    data = payload.get("data")
    if isinstance(data, str) and data:
        return data
    if isinstance(data, dict):
        for k in ("chat_id", "id"):
            if data.get(k):
                return str(data[k])
    raise ToolError("Failed to open a session (no chat_id returned): " + json.dumps(payload, ensure_ascii=False)[:300])


async def _send_message(api_key: str, chat_id: str, message: str) -> dict:
    resp = await get_client().post(
        f"{BASE_URL}{CHAT_PREFIX}/api/chat_message/{chat_id}",
        headers={**auth_headers(api_key), "Content-Type": "application/json"},
        json={"message": message, "stream": False, "re_chat": False},
    )
    return parse_payload(resp)


def _make_chat_tool(agent: AgentConfig):
    """Build the chat tool function for one agent (closure isolates its session)."""

    async def chat(message: str, new_chat: bool = False, chat_id: str = "") -> str:
        """Send a message to the MaxKB agent and return its answer."""
        if not BASE_URL:
            raise ToolError("BASE_URL is not configured; set the MaxKB service address in the environment")
        if not agent.api_key:
            raise ToolError(f"Agent {agent.tool_name} has no application API key configured")
        try:
            cid = (chat_id or "").strip() or None
            if cid is None:
                if new_chat or not agent.chat_id:
                    agent.chat_id = await _open_chat(agent.api_key)
                cid = agent.chat_id

            try:
                payload = await _send_message(agent.api_key, cid, message)
            except ToolError as e:
                msg = str(e)
                # session missing/expired: open a new one and retry once.
                # MaxKB may phrase this in Chinese or English, match both.
                low = msg.lower()
                if "会话" in msg or "session" in low or "conversation" in low:
                    agent.chat_id = await _open_chat(agent.api_key)
                    cid = agent.chat_id
                    payload = await _send_message(agent.api_key, cid, message)
                else:
                    raise

            answer = extract_answer(payload)
            agent.chat_id = cid
            return f"{answer}\n\n[chat_id: {cid}]"
        except httpx.TimeoutException:
            raise ToolError(f"MaxKB request timed out ({MAXKB_TIMEOUT}s); raise MAXKB_TIMEOUT if the agent is slow")
        except httpx.HTTPError as e:
            raise ToolError(f"Cannot connect to MaxKB ({BASE_URL}): {e}")
        except ToolError:
            raise
        except Exception as e:
            raise ToolError(f"Error calling the MaxKB chat API: {type(e).__name__}: {e}")

    chat.__name__ = agent.tool_name
    return chat


# Backwards-compatible alias: single-agent mode still exposes maxkb_chat
maxkb_chat = _make_chat_tool(AGENT_LIST[0]) if AGENT_LIST else None


# ----------------------------------------------------------------------------
# Assemble the MCP server (dual protocol)
# ----------------------------------------------------------------------------
mcp = MCPServer(SERVER_NAME)

_PARAM_HINT = (
    "\n\nArguments: message = the text to send; new_chat = true starts a fresh "
    "conversation (default false, this agent's latest conversation is reused); "
    "chat_id = continue an existing conversation by id."
)

if not AGENT_LIST:
    log.warning("No agents configured (API_KEY and CHAT_AGENTS are both empty); chat tools unavailable")

for _agent in AGENT_LIST:
    _fn = _make_chat_tool(_agent)
    mcp.add_tool(_fn, name=_agent.tool_name, description=_agent.description + _PARAM_HINT)


def _build_security() -> TransportSecuritySettings:
    hosts = [h.strip() for h in ALLOWED_HOSTS_RAW.split(",") if h.strip()]
    if not hosts or hosts == ["*"]:
        # internal/default scenario: disable DNS rebinding protection, any Host allowed
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=[],  # non-browser clients send no Origin header; allow by default
    )


_security = _build_security()
_streamable_app = mcp.streamable_http_app(transport_security=_security, host=HOST)
_sse_app = mcp.sse_app(transport_security=_security, host=HOST)


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "server": SERVER_NAME,
            "tools": [a.tool_name for a in AGENT_LIST],
            "agent_count": len(AGENT_LIST),
            "transports": {
                "streamable-http": "/mcp",
                "sse": "/sse (POST /messages/)",
            },
            "maxkb_base_url": BASE_URL or "(unset)",
            "maxkb_base_url_raw": BASE_URL_RAW or "(unset)",
            "maxkb_open_url": f"{BASE_URL}{CHAT_PREFIX}/api/open" if BASE_URL else "(unset)",
            # only echo whether keys are set and current session ids, never the keys
            "agents": [
                {"tool": a.tool_name, "api_key_set": bool(a.api_key), "chat_id": a.chat_id}
                for a in AGENT_LIST
            ],
        }
    )


@asynccontextmanager
async def lifespan(app):
    # mounted sub-app lifespans never run; the host app must start session_manager
    async with mcp.session_manager.run():
        log.info("MCP session manager started")
        yield


# Mount both sub-apps (streamable-http and sse) routes directly onto the host
# app. Mount("/mcp", ...) cannot be used: Starlette's Mount does not match the
# bare path without a trailing slash, while standard MCP clients connect to
# http://host:port/mcp.
app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        *_streamable_app.routes,
        *_sse_app.routes,
    ],
    lifespan=lifespan,
)


# ----------------------------------------------------------------------------
# Optional access token check (plain ASGI middleware)
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
                    response = JSONResponse({"error": "unauthorized, send Authorization: Bearer <MCP_AUTH_TOKEN>"}, status_code=401)
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
    log.info("MaxKB Agent Chat MCP server starting")
    log.info("  protocols: MCP(Streamable HTTP) http://%s:%s/mcp", HOST, PORT)
    log.info("             SSE                   http://%s:%s/sse", HOST, PORT)
    log.info("  health:    http://%s:%s/health", HOST, PORT)
    log.info("  MaxKB:     %s", BASE_URL or "unset")
    log.info("  agents:    %d -> %s", len(AGENT_LIST), " / ".join(a.tool_name for a in AGENT_LIST) or "(none configured!!)")
    if not BASE_URL:
        log.error("  !! BASE_URL is not configured; tool calls will fail, check the environment")
    if not AGENT_LIST:
        log.error("  !! Neither API_KEY nor CHAT_AGENTS is configured; no chat tools available")
    log.info("=" * 60)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
