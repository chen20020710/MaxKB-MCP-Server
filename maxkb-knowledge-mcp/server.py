#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MaxKB 知识库管理 MCP 服务器（对接 MaxKB v2.x）

功能:
  1. 查询智能体(应用)列表 / 知识库列表
  2. 上传文件到知识库, 两种方式:
     a. add_segment      —— 直接创建分段(传入文本内容), 可新建文档或追加到已有文档
     b. upload_document  —— 上传整个文件:
        - 普通知识库: 调用 document/split 自动分段 + batch_create 创建文档
        - 工作流知识库(type=4): 先上传源文件, 再通过 upload_document 交给
          知识库工作流做智能分段(异步处理)
  3. search_knowledge   —— 直接检索知识库(命中测试), 获取知识切片
  4. list_documents     —— 查询知识库下的文档列表

传输协议(同一进程):
  * MCP Streamable HTTP:  http://<host>:<port>/mcp
  * MCP SSE:               http://<host>:<port>/sse  (消息回传 /messages/)
  * 健康检查:              http://<host>:<port>/health

对接的 MaxKB 管理端接口(前缀 {BASE_URL}/admin/api, 认证 Authorization: Bearer <token>):
  - POST /user/login                                    账号密码登录获取 token
  - GET  /user/profile                                  获取用户信息(含 workspace_list)
  - GET  /workspace/{wid}/application/{page}/{size}     智能体列表(分页)
  - GET  /workspace/{wid}/knowledge/{page}/{size}       知识库列表(分页)
  - GET  /workspace/{wid}/knowledge/{kid}               知识库详情
  - POST /workspace/{wid}/knowledge/{kid}/document                      创建文档(可内嵌分段)
  - POST /workspace/{wid}/knowledge/{kid}/document/{did}/paragraph      向已有文档添加分段
  - POST /workspace/{wid}/knowledge/{kid}/document/split                文件分段(返回分段结果)
  - POST /workspace/{wid}/knowledge/{kid}/document/batch_create         批量创建文档
  - POST /workspace/{wid}/knowledge/{kid}/upload_document               工作流方式上传文档
  - POST /workspace/{wid}/knowledge/{kid}/hit_test                      命中测试/检索
  - GET  /workspace/{wid}/knowledge/{kid}/document/{page}/{size}       文档列表(分页)
  - POST /oss/file                                      上传源文件(工作流方式需要)

环境变量:
  BASE_URL             MaxKB 服务地址, 如 http://<MAXKB_HOST>:8080                (必填)
  MAXKB_API_KEY        用户级 API Key(推荐, MaxKB 页面右上角"API Key 管理"创建) (二选一)
  MAXKB_USERNAME       登录用户名                                            (二选一)
  MAXKB_PASSWORD       登录密码
  MAXKB_WORKSPACE_ID   工作空间 id, 默认自动从用户 profile 获取(社区版为 default)
  MAXKB_KNOWLEDGE_ID   默认知识库 id, 工具未显式指定时使用
  MAXKB_ADMIN_PREFIX   管理端 API 前缀, 默认 /admin
  SERVER_NAME          MCP 服务器名称
  MAXKB_TIMEOUT        调用超时(秒), 默认 120
  MAXKB_TOP_NUMBER     检索默认返回条数, 默认 5
  MAXKB_SIMILARITY     检索默认相似度阈值, 默认 0.6
  MAXKB_SEARCH_MODE    检索默认模式 embedding|keywords|blend, 默认 blend
                    (工具入参支持中文/别名自动归一化, 非法值回退默认)
  HOST / PORT          监听地址与端口, 默认 0.0.0.0:8000
  MCP_AUTH_TOKEN       可选, 设置后调用 MCP 端点需要 Bearer 令牌
  MCP_ALLOWED_HOSTS    Host 白名单, 默认 * 不校验
"""
import base64
import json
import logging
import mimetypes
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from urllib.parse import quote as _urlquote
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

log = logging.getLogger("maxkb-knowledge-mcp")


# ----------------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------------
def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    return v if v not in (None, "") else default


def _normalize_base_url(raw: str) -> str:
    """把 BASE_URL 归一化成 MaxKB 服务根地址(剥离误填的 /chat|/admin 应用专属路径)。"""
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
MAXKB_API_KEY = _env("MAXKB_API_KEY", "")
MAXKB_USERNAME = _env("MAXKB_USERNAME", "")
MAXKB_PASSWORD = _env("MAXKB_PASSWORD", "")
MAXKB_WORKSPACE_ID = _env("MAXKB_WORKSPACE_ID", "")
MAXKB_KNOWLEDGE_ID = _env("MAXKB_KNOWLEDGE_ID", "")
ADMIN_PREFIX = (_env("MAXKB_ADMIN_PREFIX", "/admin")).rstrip("/")
SERVER_NAME = _env("SERVER_NAME", "MaxKB 知识库管理 MCP 服务器")
MAXKB_TIMEOUT = float(_env("MAXKB_TIMEOUT", "120"))
DEFAULT_TOP_NUMBER = int(_env("MAXKB_TOP_NUMBER", "5"))
DEFAULT_SIMILARITY = float(_env("MAXKB_SIMILARITY", "0.6"))
DEFAULT_SEARCH_MODE = _env("MAXKB_SEARCH_MODE", "blend")

# MaxKB v2.x HitTest 只接受 embedding|keywords|blend (报错文案的中文只是 i18n 翻译)。
# LLM 常会传中文/别名, 这里统一归一化, 非法值回退默认, 绝不再透传给 MaxKB 触发 500。
_SEARCH_MODE_MAP = {
    "embedding": "embedding", "embed": "embedding", "vector": "embedding",
    "semantic": "embedding", "语义": "embedding", "向量": "embedding", "嵌入": "embedding",
    "keywords": "keywords", "keyword": "keywords", "fulltext": "keywords",
    "关键字": "keywords", "关键词": "keywords", "全文": "keywords",
    "blend": "blend", "hybrid": "blend", "mixed": "blend", "mix": "blend",
    "混合": "blend", "混合检索": "blend",
}


def _normalize_search_mode(value: str) -> str:
    for candidate in ((value or "").strip().lower(), (DEFAULT_SEARCH_MODE or "").strip().lower()):
        if candidate in _SEARCH_MODE_MAP:
            return _SEARCH_MODE_MAP[candidate]
    return "blend"


HOST = _env("HOST", "0.0.0.0")
PORT = int(_env("PORT", "8000"))
MCP_AUTH_TOKEN = _env("MCP_AUTH_TOKEN")
ALLOWED_HOSTS_RAW = _env("MCP_ALLOWED_HOSTS", "*")


# ----------------------------------------------------------------------------
# 认证与 HTTP 客户端
# ----------------------------------------------------------------------------
_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=MAXKB_TIMEOUT)
    return _client


class _AuthState:
    jwt: str | None = None


_auth = _AuthState()


async def _login() -> None:
    if not (MAXKB_USERNAME and MAXKB_PASSWORD):
        raise ToolError("未配置凭据: 请设置 MAXKB_API_KEY, 或设置 MAXKB_USERNAME/MAXKB_PASSWORD")
    resp = await get_client().post(
        f"{BASE_URL}{ADMIN_PREFIX}/api/user/login",
        headers={"Content-Type": "application/json"},
        json={"username": MAXKB_USERNAME, "password": MAXKB_PASSWORD},
    )
    payload = parse_payload(resp)
    data = payload.get("data")
    token = data.get("token") if isinstance(data, dict) else data
    if not token:
        raise ToolError("登录成功但未返回 token: " + json.dumps(payload, ensure_ascii=False)[:300])
    _auth.jwt = str(token)


async def _auth_headers(force_new: bool = False) -> dict:
    # 优先使用 API Key; 否则账号密码登录获取 JWT
    if MAXKB_API_KEY and not force_new:
        return {"Authorization": f"Bearer {MAXKB_API_KEY}"}
    if not _auth.jwt or force_new:
        await _login()
    return {"Authorization": f"Bearer {_auth.jwt}"}


def parse_payload(resp: httpx.Response) -> dict:
    """解析 MaxKB 响应。HTTP 200 时业务码在 body.code 中, 200 才算成功。"""
    try:
        payload = resp.json()
    except Exception:
        raise ToolError(f"MaxKB 返回了非 JSON 内容 (HTTP {resp.status_code}): {resp.text[:300]}")
    if not isinstance(payload, dict):
        return {"data": payload}
    if resp.status_code == 401 or payload.get("code") in (1002, 1003):
        raise ToolError(
            f"MaxKB 认证失败 ({payload.get('code', resp.status_code)}): "
            f"{payload.get('message', '请检查凭据是否正确/是否过期')}"
        )
    code = payload.get("code")
    if code is not None and code != 200:
        raise ToolError(f"MaxKB 调用失败 [{code}]: {payload.get('message', '')} [HTTP {resp.status_code}]")
    return payload


async def maxkb_request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: Any = None,
    files: list | None = None,
    data: Any = None,
    content: bytes | None = None,
    content_type: str | None = None,
    retried: bool = False,
) -> dict:
    """调用 MaxKB 管理端接口; 遇到 401 时刷新凭据重试一次。"""
    try:
        headers = await _auth_headers()
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        if content_type is not None:
            headers["Content-Type"] = content_type
        resp = await get_client().request(
            method,
            f"{BASE_URL}{ADMIN_PREFIX}/api{path}",
            headers=headers,
            params=params,
            json=json_body,
            files=files,
            data=data,
            content=content,
        )
    except httpx.TimeoutException:
        raise ToolError(f"MaxKB 请求超时({MAXKB_TIMEOUT}s): {method} {path}")
    except httpx.HTTPError as e:
        raise ToolError(f"无法连接 MaxKB ({BASE_URL}): {e}")
    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"调用 MaxKB 接口出错 ({method} {path}): {type(e).__name__}: {e}")
    if resp.status_code == 401 and not retried:
        return await maxkb_request(
            method, path, params=params, json_body=json_body, files=files, data=data,
            content=content, content_type=content_type, retried=True,
        )
    return parse_payload(resp)


# ----------------------------------------------------------------------------
# 工作空间 / 知识库 解析
# ----------------------------------------------------------------------------
_workspace: dict = {"id": None}


async def get_workspace_id() -> str:
    if MAXKB_WORKSPACE_ID:
        return MAXKB_WORKSPACE_ID
    if _workspace["id"]:
        return _workspace["id"]
    try:
        payload = await maxkb_request("GET", "/user/profile")
        ws_list = (payload.get("data") or {}).get("workspace_list") or []
        _workspace["id"] = (ws_list[0].get("id") if ws_list else None) or "default"
    except ToolError as e:
        log.warning("自动获取工作空间失败, 回退为 default: %s", e)
        _workspace["id"] = "default"
    return _workspace["id"]


async def resolve_kb(knowledge_base_id: str) -> str:
    kb = (knowledge_base_id or "").strip() or MAXKB_KNOWLEDGE_ID
    if not kb:
        raise ToolError(
            "未指定知识库: 请传入 knowledge_base_id 参数, "
            "或在环境变量 MAXKB_KNOWLEDGE_ID 中配置默认知识库 (可先用 list_knowledge_bases 查询)"
        )
    return kb


def page_items(payload: dict) -> tuple[list, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        items = data.get("records") or data.get("list") or data.get("data") or []
        total = data.get("total", len(items) if isinstance(items, list) else 0)
        return items, total
    if isinstance(data, list):
        return data, len(data)
    return [], 0


KB_TYPE_NAMES = {
    0: "通用知识库",
    1: "Web 知识库",
    2: "QA 知识库",
    3: "表格知识库",
    4: "工作流知识库",
}


def _kb_type_name(t: Any) -> str:
    if isinstance(t, int) and t in KB_TYPE_NAMES:
        return f"{t}({KB_TYPE_NAMES[t]})"
    return str(t)


def _fmt_row(it: dict, keys: list[str]) -> str:
    parts = []
    for k in keys:
        v = it.get(k)
        if v not in (None, ""):
            if k == "type":
                v = _kb_type_name(v)
            parts.append(f"{k}: {v}")
    return " | ".join(parts) if parts else json.dumps(it, ensure_ascii=False)[:200]


# ----------------------------------------------------------------------------
# 工具实现
# ----------------------------------------------------------------------------
async def list_agents(name: str = "", current_page: int = 1, page_size: int = 20) -> str:
    """查询 MaxKB 智能体(应用)列表。

    参数 name 可按名称模糊过滤; current_page/page_size 分页。
    返回每个智能体的 id、名称、类型、创建时间等信息。
    """
    ws = await get_workspace_id()
    params = {"name": name} if name else None
    try:
        payload = await maxkb_request(
            "GET", f"/workspace/{ws}/application/{current_page}/{page_size}", params=params
        )
        items, total = page_items(payload)
    except ToolError:
        # 兼容不支持分页接口的版本, 回退到非分页列表
        payload = await maxkb_request("GET", f"/workspace/{ws}/application", params=params)
        items, total = page_items(payload)

    lines = [f"智能体列表 (工作空间 {ws}, 共 {total} 个, 第 {current_page} 页):"]
    if not items:
        lines.append("  (空)")
    for it in items:
        if isinstance(it, dict):
            lines.append("- " + _fmt_row(it, ["id", "name", "type", "create_time", "desc"]))
        else:
            lines.append(f"- {it}")
    return "\n".join(lines)


async def list_knowledge_bases(name: str = "", current_page: int = 1, page_size: int = 20) -> str:
    """查询 MaxKB 知识库列表。

    参数 name 可按名称模糊过滤; current_page/page_size 分页。
    返回每个知识库的 id、名称、类型、描述等信息, 其中 id 可用于其他工具的 knowledge_base_id 参数。
    """
    ws = await get_workspace_id()
    params = {"name": name} if name else None
    try:
        payload = await maxkb_request(
            "GET", f"/workspace/{ws}/knowledge/{current_page}/{page_size}", params=params
        )
        items, total = page_items(payload)
    except ToolError:
        payload = await maxkb_request("GET", f"/workspace/{ws}/knowledge", params=params)
        items, total = page_items(payload)

    lines = [f"知识库列表 (工作空间 {ws}, 共 {total} 个, 第 {current_page} 页):"]
    if not items:
        lines.append("  (空)")
    for it in items:
        if isinstance(it, dict):
            lines.append("- " + _fmt_row(it, ["id", "name", "type", "desc", "create_time"]))
        else:
            lines.append(f"- {it}")
    return "\n".join(lines)


async def list_documents(
    knowledge_base_id: str = "", name: str = "", current_page: int = 1, page_size: int = 20
) -> str:
    """查询指定知识库下的文档列表。

    参数 knowledge_base_id 不传时使用默认知识库; name 可按文档名过滤。
    返回文档 id、名称、状态等, 其中文档 id 可用于 add_segment 的 document_id 参数(向已有文档追加分段)。
    """
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)
    params = {"name": name} if name else None
    try:
        payload = await maxkb_request(
            "GET", f"/workspace/{ws}/knowledge/{kb}/document/{current_page}/{page_size}", params=params
        )
        items, total = page_items(payload)
    except ToolError:
        payload = await maxkb_request("GET", f"/workspace/{ws}/knowledge/{kb}/document", params=params)
        items, total = page_items(payload)

    lines = [f"文档列表 (知识库 {kb}, 共 {total} 个, 第 {current_page} 页):"]
    if not items:
        lines.append("  (空)")
    for it in items:
        if isinstance(it, dict):
            lines.append("- " + _fmt_row(it, ["id", "name", "is_active", "hit_handling_method", "create_time"]))
        else:
            lines.append(f"- {it}")
    return "\n".join(lines)


def _problem_list(problems: list[str] | None) -> list[dict]:
    return [{"content": p} for p in (problems or []) if p and p.strip()]


async def add_segment(
    content: str,
    title: str = "",
    document_name: str = "",
    document_id: str = "",
    knowledge_base_id: str = "",
    problems: list[str] | None = None,
) -> str:
    """向知识库添加一条自定义分段(方式一: 直接传入文本内容创建分段)。

    content 为分段正文(必填); title 为分段标题;
    - 指定 document_id 时: 追加到该已有文档;
    - 未指定 document_id 时: 新建一个文档(名称用 document_name, 缺省用 title 或自动生成)并写入该分段。
    problems 为可选的同义问题列表(提高命中检索率)。
    """
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)

    paragraph: dict[str, Any] = {"content": content}
    if title:
        paragraph["title"] = title
    pl = _problem_list(problems)
    if pl:
        paragraph["problem_list"] = pl

    if document_id:
        payload = await maxkb_request(
            "POST",
            f"/workspace/{ws}/knowledge/{kb}/document/{document_id}/paragraph",
            json_body=paragraph,
        )
        created = payload.get("data")
        ids = []
        if isinstance(created, list):
            ids = [str(p.get("id")) for p in created if isinstance(p, dict) and p.get("id")]
        elif isinstance(created, dict) and created.get("id"):
            ids = [str(created.get("id"))]
        return (
            f"分段添加成功: 已追加到文档 {document_id} "
            f"(知识库 {kb}, 段落id: {', '.join(ids) or '未返回'})"
        )
    else:
        doc_name = (
            document_name or title[:40] or f"分段_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        payload = await maxkb_request(
            "POST",
            f"/workspace/{ws}/knowledge/{kb}/document",
            json_body={"name": doc_name, "paragraphs": [paragraph]},
        )
        created = payload.get("data")
        doc_id = None
        if isinstance(created, dict):
            doc_id = created.get("id") or created.get("document_id")
        elif isinstance(created, list) and created and isinstance(created[0], dict):
            doc_id = created[0].get("id") or created[0].get("document_id")
        return (
            f"分段添加成功: 已创建新文档「{doc_name}」并写入 1 条分段 "
            f"(知识库 {kb}, 文档id: {doc_id or '未返回'})"
        )


async def _is_workflow_kb(ws: str, kb: str) -> bool:
    try:
        payload = await maxkb_request("GET", f"/workspace/{ws}/knowledge/{kb}")
        return (payload.get("data") or {}).get("type") == 4
    except ToolError as e:
        log.warning("查询知识库类型失败, 按普通知识库处理: %s", e)
        return False


def _read_upload_source(file_path: str, file_name: str, content_base64: str) -> tuple[str, bytes]:
    if file_path:
        path = file_path
        if not os.path.isfile(path):
            raise ToolError(f"文件不存在: {path} (容器内路径, 建议挂载目录后使用, 如 /app/data/xx.pdf)")
        with open(path, "rb") as f:
            data = f.read()
        name = file_name or os.path.basename(path)
        return name, data
    if content_base64:
        try:
            data = base64.b64decode(content_base64)
        except Exception as e:
            raise ToolError(f"content_base64 解码失败: {e}")
        if not (file_name):
            raise ToolError("使用 content_base64 上传时必须同时提供 file_name")
        return file_name, data
    raise ToolError("请提供 file_path(服务器本地路径) 或 content_base64+file_name 之一")


def _guess_content_type(file_name: str) -> str:
    ctype, _ = mimetypes.guess_type(file_name)
    return ctype or "application/octet-stream"


def _encode_multipart(
    fields: list[tuple[str, str]],
    file_field: str,
    file_name: str,
    file_data: bytes,
    content_type: str | None = None,
) -> tuple[bytes, str]:
    """手动构造 multipart/form-data 请求体。

    不使用 httpx 的 files+data 混合编码: 当 data 为 (k,v) 元组列表时 httpx 会产生损坏的请求体,
    而分段接口的 patterns 字段需要重复键, 只能用元组列表表达。此处同时支持中文文件名
    (filename + RFC5987 filename* 双写, 与浏览器行为一致)。
    """
    boundary = f"----MaxKBMCP{os.urandom(16).hex()}"
    parts: list[bytes] = []
    for name, value in fields:
        safe_name = name.replace('"', "'").replace("\r", "").replace("\n", "")
        parts.append(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{safe_name}"\r\n\r\n{value}\r\n'
            ).encode("utf-8")
        )
    safe_file_name = file_name.replace('"', "'").replace("\\", "").replace("\r", "").replace("\n", "")
    parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{safe_file_name}"; filename*=UTF-8\'\'{_urlquote(file_name, safe="")}\r\n'
            f"Content-Type: {content_type or _guess_content_type(file_name)}\r\n\r\n"
        ).encode("utf-8")
    )
    parts.append(file_data)
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


async def _upload_via_workflow(ws: str, kb: str, name: str, data: bytes) -> str:
    # 1. 上传源文件到 MaxKB (返回 ./oss/file/<file_id>)
    body, ctype = _encode_multipart(
        [("source_id", kb), ("source_type", "KNOWLEDGE")], "file", name, data
    )
    payload = await maxkb_request("POST", "/oss/file", content=body, content_type=ctype)
    file_url = payload.get("data")
    if isinstance(file_url, dict):
        file_url = file_url.get("url") or file_url.get("file_id")
    file_id = str(file_url).rstrip("/").split("/")[-1]

    # 2. 提交知识库工作流处理
    payload = await maxkb_request(
        "POST",
        f"/workspace/{ws}/knowledge/{kb}/upload_document",
        json_body={
            "data_source": {
                "file_list": [
                    {"name": name, "file_id": file_id, "url": str(file_url)}
                ]
            }
        },
    )
    action = payload.get("data") or {}
    action_id = action.get("id")
    state = action.get("state", "STARTED")
    return (
        f"已通过知识库工作流提交文档「{name}」(file_id: {file_id}, "
        f"处理任务id: {action_id or '未返回'}, 状态: {state})。"
        f"工作流处理为异步执行, 可稍后用 list_documents 查看结果或在 MaxKB 页面查看处理进度。"
    )


async def _upload_via_split(
    ws: str, kb: str, name: str, data: bytes, limit: int, patterns: list[str] | None, with_filter: bool
) -> str:
    # 1. 分段预览
    fields: list[tuple[str, str]] = [("with_filter", "true" if with_filter else "false")]
    if limit and limit > 0:
        fields.append(("limit", str(limit)))
    for p in patterns or []:
        fields.append(("patterns", str(p)))
    body, ctype = _encode_multipart(fields, "file", name, data)
    payload = await maxkb_request(
        "POST",
        f"/workspace/{ws}/knowledge/{kb}/document/split",
        content=body,
        content_type=ctype,
    )
    segments = payload.get("data")
    if not isinstance(segments, list) or not segments:
        raise ToolError("文件分段失败, 未返回分段结果: " + json.dumps(payload, ensure_ascii=False)[:300])

    # 2. 组装段落并创建文档 (分段结果 -> paragraphs)
    paragraphs = []
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        para: dict[str, Any] = {"content": seg.get("content", "")}
        if seg.get("title"):
            para["title"] = seg.get("title")
        if isinstance(seg.get("problem_list"), list) and seg["problem_list"]:
            para["problem_list"] = seg["problem_list"]
        paragraphs.append(para)

    payload = await maxkb_request(
        "POST",
        f"/workspace/{ws}/knowledge/{kb}/document/batch_create",
        json_body=[{"name": name, "paragraphs": paragraphs}],
    )
    return (
        f"文档「{name}」上传成功: 按知识库分段规则切分为 {len(paragraphs)} 个分段, "
        f"已创建文档(向量化在后台异步进行)。"
    )


async def upload_document(
    file_path: str = "",
    file_name: str = "",
    content_base64: str = "",
    knowledge_base_id: str = "",
    mode: str = "auto",
    limit: int = 0,
    patterns: list[str] | None = None,
    with_filter: bool = True,
) -> str:
    """上传整个文件到知识库并自动智能分段(方式二)。

    文件来源二选一: file_path(服务器上的文件路径, 推荐 docker 部署时挂载目录) 或
    content_base64 + file_name(直接传文件内容, 适合小文件)。

    mode 取值:
      auto     自动判断: 工作流知识库走工作流智能分段, 普通知识库走规则分段
      workflow 强制走知识库工作流(要求知识库为已发布的"工作流知识库")
      split    强制走规则分段(可配合 limit=分段长度, patterns=分段正则, with_filter=清除特殊字符)
    """
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)
    name, data = _read_upload_source(file_path, file_name, content_base64)
    if len(data) > 100 * 1024 * 1024:
        raise ToolError(f"文件过大({len(data) // 1024 // 1024}MB), 上传接口限制 100MB 以内")

    if mode not in ("auto", "workflow", "split"):
        raise ToolError("mode 仅支持 auto / workflow / split")

    use_workflow = {"workflow": True, "split": False}.get(mode)
    if use_workflow is None:
        use_workflow = await _is_workflow_kb(ws, kb)

    if use_workflow:
        try:
            return await _upload_via_workflow(ws, kb, name, data)
        except ToolError as e:
            raise ToolError(
                f"工作流方式上传失败: {e}\n"
                f"提示: 该方式要求知识库是已发布的「工作流知识库」; "
                f"普通知识库请使用 mode=\"split\""
            )
    return await _upload_via_split(ws, kb, name, data, limit, patterns, with_filter)


async def search_knowledge(
    query: str,
    knowledge_base_id: str = "",
    top_number: int = 0,
    similarity: float = 0.0,
    search_mode: str = "",
) -> str:
    """直接检索 MaxKB 知识库(命中测试), 返回命中的知识切片。

    query 为检索文本; top_number 返回条数(默认 5); similarity 相似度阈值(默认 0.6);
    search_mode 检索模式, 仅支持 embedding(嵌入/向量)、keywords(关键字)、blend(混合, 默认);
    传入中文或别名会自动归一化, 无法识别的值自动回退 blend。
    返回每个命中切片的相似度得分、标题、内容(以及所属文档)。
    """
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)
    body = {
        "query_text": query,
        "top_number": top_number or DEFAULT_TOP_NUMBER,
        "similarity": similarity if similarity > 0 else DEFAULT_SIMILARITY,
        "search_mode": _normalize_search_mode(search_mode),
    }
    payload = await maxkb_request(
        "POST", f"/workspace/{ws}/knowledge/{kb}/hit_test", json_body=body
    )
    hits = payload.get("data")
    if isinstance(hits, dict):
        hits = hits.get("records") or hits.get("list") or []
    if not isinstance(hits, list):
        hits = []

    if not hits:
        return f"未检索到相关内容 (知识库 {kb}, query: {query}, 阈值 {body['similarity']})"

    lines = [f"检索到 {len(hits)} 条知识切片 (知识库 {kb}):"]
    for i, h in enumerate(hits, 1):
        if not isinstance(h, dict):
            lines.append(f"{i}. {h}")
            continue
        score = h.get("similarity", h.get("comprehensive_score", h.get("score", "")))
        title = h.get("title", "")
        doc = h.get("document_name", h.get("name", ""))
        content = str(h.get("content", ""))[:800]
        head = f"{i}. [得分 {score}]"
        if title:
            head += f" {title}"
        if doc:
            head += f" (文档: {doc})"
        lines.append(head)
        lines.append("   " + content.replace("\n", "\n   "))
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# 组装 MCP 服务器(双协议)
# ----------------------------------------------------------------------------
mcp = MCPServer(SERVER_NAME)

mcp.add_tool(list_agents, name="list_agents")
mcp.add_tool(list_knowledge_bases, name="list_knowledge_bases")
mcp.add_tool(list_documents, name="list_documents")
mcp.add_tool(add_segment, name="add_segment")
mcp.add_tool(upload_document, name="upload_document")
mcp.add_tool(search_knowledge, name="search_knowledge")


def _build_security() -> TransportSecuritySettings:
    hosts = [h.strip() for h in ALLOWED_HOSTS_RAW.split(",") if h.strip()]
    if not hosts or hosts == ["*"]:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(enable_dns_rebinding_protection=True, allowed_hosts=hosts)


_security = _build_security()
_streamable_app = mcp.streamable_http_app(transport_security=_security, host=HOST)
_sse_app = mcp.sse_app(transport_security=_security, host=HOST)


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "server": SERVER_NAME,
            "tools": [
                "list_agents",
                "list_knowledge_bases",
                "list_documents",
                "add_segment",
                "upload_document",
                "search_knowledge",
            ],
            "transports": {
                "streamable-http": "/mcp",
                "sse": "/sse (POST /messages/)",
            },
            "maxkb_base_url": BASE_URL or "(未配置)",
            "maxkb_base_url_raw": BASE_URL_RAW or "(未配置)",
            "credential": (
                "api_key" if MAXKB_API_KEY else ("password" if MAXKB_USERNAME else "未配置!!")
            ),
            "workspace_id": MAXKB_WORKSPACE_ID or "(auto)",
            "default_knowledge_id": MAXKB_KNOWLEDGE_ID or "(未设置)",
        }
    )


@asynccontextmanager
async def lifespan(app):
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
                    response = JSONResponse(
                        {"error": "unauthorized, 请携带 Authorization: Bearer <MCP_AUTH_TOKEN>"},
                        status_code=401,
                    )
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
    log.info("MaxKB 知识库管理 MCP 服务器启动中")
    log.info("  对外协议:  MCP(Streamable HTTP) http://%s:%s/mcp", HOST, PORT)
    log.info("             SSE                   http://%s:%s/sse", HOST, PORT)
    log.info("  健康检查:  http://%s:%s/health", HOST, PORT)
    log.info("  MaxKB:     %s", BASE_URL or "未配置")
    log.info("  凭据方式:  %s", "API Key" if MAXKB_API_KEY else ("账号密码" if MAXKB_USERNAME else "未配置!!"))
    log.info("  工具:      list_agents / list_knowledge_bases / list_documents / add_segment / upload_document / search_knowledge")
    if not BASE_URL or not (MAXKB_API_KEY or MAXKB_USERNAME):
        log.error("  !! BASE_URL 或凭据未配置, 工具调用将报错, 请检查环境变量")
    log.info("=" * 60)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
