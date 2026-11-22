#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MaxKB knowledge base management MCP server (MaxKB v2.x).

Features:
  1. list_knowledge_bases  - list knowledge bases
  2. upload documents, two ways:
     a. add_segment      - create a segment from raw text (new document or append)
     b. upload_document  - upload a whole file:
        - regular knowledge base: document/split + batch_create
        - workflow knowledge base (type=4): upload source file, then hand it to
          the knowledge workflow for smart splitting (async)
  3. search_knowledge    - direct retrieval (hit test), returns matched segments
  4. get_segment_context - read segments around a hit (context expansion) or the
                           full document text
  5. list_documents      - list documents of a knowledge base

Transports (same process):
  * MCP Streamable HTTP:  http://<host>:<port>/mcp
  * MCP SSE:               http://<host>:<port>/sse  (messages via /messages/)
  * Health check:          http://<host>:<port>/health

MaxKB admin endpoints used (prefix {BASE_URL}/admin/api, Authorization: Bearer <token>):
  - POST /user/login                                    login with username/password
  - GET  /user/profile                                  user info (workspace_list)
  - GET  /workspace/{wid}/knowledge/{page}/{size}       knowledge base list (paged)
  - GET  /workspace/{wid}/knowledge/{kid}               knowledge base details
  - POST /workspace/{wid}/knowledge/{kid}/document                      create document (inline segments)
  - POST /workspace/{wid}/knowledge/{kid}/document/{did}/paragraph      append segment to a document
  - POST /workspace/{wid}/knowledge/{kid}/document/split                split a file (preview)
  - POST /workspace/{wid}/knowledge/{kid}/document/batch_create         batch create documents
  - POST /workspace/{wid}/knowledge/{kid}/upload_document               workflow upload
  - POST /workspace/{wid}/knowledge/{kid}/hit_test                      retrieval / hit test
  - GET  /workspace/{wid}/knowledge/{kid}/document/{page}/{size}       document list (paged)
  - GET  /workspace/{wid}/knowledge/{kid}/document/{did}/paragraph      all segments (unpaged, full text)
  - GET  /workspace/{wid}/knowledge/{kid}/document/{did}/paragraph/{p}/{s} segments (paged)
  - GET  /workspace/{wid}/knowledge/{kid}/document/{did}/download_source_file original file (manual uploads only)
  - POST /oss/file                                      upload source file (workflow mode)

Environment variables:
  BASE_URL             MaxKB service address, e.g. http://<MAXKB_HOST>:8080   (required)
  MAXKB_API_KEY        user-level API key (recommended)                       (either)
  MAXKB_USERNAME       login username                                         (either)
  MAXKB_PASSWORD       login password
  MAXKB_WORKSPACE_ID   workspace id, auto-detected from profile when empty
  MAXKB_KNOWLEDGE_ID   default knowledge base id used when a tool omits it
  MAXKB_ADMIN_PREFIX   admin API prefix, default /admin
  SERVER_NAME          MCP server display name
  MAXKB_TIMEOUT        request timeout (seconds), default 120
  MAXKB_TOP_NUMBER     default retrieval result count, default 5
  MAXKB_SIMILARITY     default similarity threshold, default 0.6
  MAXKB_SEARCH_MODE    default retrieval mode embedding|keywords|blend, default blend
                       (tool arguments accept aliases and are normalized)
  MAXKB_CONTEXT_NEIGHBORS get_segment_context default segments per side, default 1
  MAXKB_CONTEXT_MAX_CHARS get_segment_context default character cap, default 6000
  HOST / PORT          bind address and port, default 0.0.0.0:8000
  MCP_AUTH_TOKEN       optional; when set, MCP endpoints require a Bearer token
  MCP_ALLOWED_HOSTS    Host header allow-list, default * (no check)
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
# Configuration
# ----------------------------------------------------------------------------
def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    return v if v not in (None, "") else default


def _normalize_base_url(raw: str) -> str:
    """Normalize BASE_URL to the MaxKB service root (strip /chat|/admin app paths)."""
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
SERVER_NAME = _env("SERVER_NAME", "MaxKB Knowledge Base MCP Server")
MAXKB_TIMEOUT = float(_env("MAXKB_TIMEOUT", "120"))
DEFAULT_TOP_NUMBER = int(_env("MAXKB_TOP_NUMBER", "5"))
DEFAULT_SIMILARITY = float(_env("MAXKB_SIMILARITY", "0.6"))
DEFAULT_SEARCH_MODE = _env("MAXKB_SEARCH_MODE", "blend")
# get_segment_context defaults: segments per side and total character cap
DEFAULT_CONTEXT_NEIGHBORS = int(_env("MAXKB_CONTEXT_NEIGHBORS", "1"))
DEFAULT_CONTEXT_MAX_CHARS = int(_env("MAXKB_CONTEXT_MAX_CHARS", "6000"))

# MaxKB v2.x HitTest only accepts embedding|keywords|blend. The Chinese keys
# below are INPUT normalization (LLMs often pass Chinese/aliases), not output.
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
# Auth & HTTP client
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
        raise ToolError("No credentials configured: set MAXKB_API_KEY, or MAXKB_USERNAME/MAXKB_PASSWORD")
    resp = await get_client().post(
        f"{BASE_URL}{ADMIN_PREFIX}/api/user/login",
        headers={"Content-Type": "application/json"},
        json={"username": MAXKB_USERNAME, "password": MAXKB_PASSWORD},
    )
    payload = parse_payload(resp)
    data = payload.get("data")
    token = data.get("token") if isinstance(data, dict) else data
    if not token:
        raise ToolError("Login succeeded but no token returned: " + json.dumps(payload, ensure_ascii=False)[:300])
    _auth.jwt = str(token)


async def _auth_headers(force_new: bool = False) -> dict:
    # Prefer the API key; otherwise log in with username/password for a JWT
    if MAXKB_API_KEY and not force_new:
        return {"Authorization": f"Bearer {MAXKB_API_KEY}"}
    if not _auth.jwt or force_new:
        await _login()
    return {"Authorization": f"Bearer {_auth.jwt}"}


def parse_payload(resp: httpx.Response) -> dict:
    """Parse a MaxKB response. Business code lives in body.code under HTTP 200."""
    try:
        payload = resp.json()
    except Exception:
        raise ToolError(f"MaxKB returned non-JSON content (HTTP {resp.status_code}): {resp.text[:300]}")
    if not isinstance(payload, dict):
        return {"data": payload}
    if resp.status_code == 401 or payload.get("code") in (1002, 1003):
        raise ToolError(
            f"MaxKB authentication failed ({payload.get('code', resp.status_code)}): "
            f"{payload.get('message', 'check whether the credentials are correct or expired')}"
        )
    code = payload.get("code")
    if code is not None and code != 200:
        raise ToolError(f"MaxKB call failed [{code}]: {payload.get('message', '')} [HTTP {resp.status_code}]")
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
    """Call a MaxKB admin endpoint; refresh credentials and retry once on 401."""
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
        raise ToolError(f"MaxKB request timed out ({MAXKB_TIMEOUT}s): {method} {path}")
    except httpx.HTTPError as e:
        raise ToolError(f"Cannot connect to MaxKB ({BASE_URL}): {e}")
    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"Error calling MaxKB ({method} {path}): {type(e).__name__}: {e}")
    if resp.status_code == 401 and not retried:
        return await maxkb_request(
            method, path, params=params, json_body=json_body, files=files, data=data,
            content=content, content_type=content_type, retried=True,
        )
    return parse_payload(resp)


# ----------------------------------------------------------------------------
# Workspace / knowledge base resolution
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
        log.warning("Auto-detecting workspace failed, falling back to default: %s", e)
        _workspace["id"] = "default"
    return _workspace["id"]


async def resolve_kb(knowledge_base_id: str) -> str:
    kb = (knowledge_base_id or "").strip() or MAXKB_KNOWLEDGE_ID
    if not kb:
        raise ToolError(
            "No knowledge base specified: pass knowledge_base_id, "
            "or set MAXKB_KNOWLEDGE_ID (use list_knowledge_bases to find ids)"
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
    0: "general",
    1: "web",
    2: "qa",
    3: "table",
    4: "workflow",
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
# Tools
# ----------------------------------------------------------------------------
async def list_knowledge_bases(name: str = "", current_page: int = 1, page_size: int = 20) -> str:
    """List all knowledge bases.

    Filter by name (fuzzy) with `name`; paginate with current_page/page_size.
    Returns id, name, type and description of each knowledge base; the id can be
    passed to other tools as knowledge_base_id.
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

    lines = [f"Knowledge bases (workspace {ws}, total {total}, page {current_page}):"]
    if not items:
        lines.append("  (empty)")
    for it in items:
        if isinstance(it, dict):
            lines.append("- " + _fmt_row(it, ["id", "name", "type", "desc", "create_time"]))
        else:
            lines.append(f"- {it}")
    return "\n".join(lines)


async def list_documents(
    knowledge_base_id: str = "", name: str = "", current_page: int = 1, page_size: int = 20
) -> str:
    """List all documents in a knowledge base.

    Uses the default knowledge base when knowledge_base_id is omitted; filter by
    document name with `name`. Returned document ids can be used as document_id
    in add_segment (append segments to an existing document).
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

    lines = [f"Documents (knowledge base {kb}, total {total}, page {current_page}):"]
    if not items:
        lines.append("  (empty)")
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
    """Create a custom text segment in a knowledge base.

    `content` is the segment body (required); `title` the segment title.
    - with document_id: append the segment to that existing document;
    - without document_id: create a new document (named by document_name,
      falling back to title or an auto-generated name) holding this segment.
    `problems` is an optional list of synonymous questions that improve recall.
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
            f"Segment added: appended to document {document_id} "
            f"(knowledge base {kb}, paragraph ids: {', '.join(ids) or 'none returned'})"
        )
    else:
        doc_name = (
            document_name or title[:40] or f"segment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
            f"Segment added: created document '{doc_name}' with 1 segment "
            f"(knowledge base {kb}, document id: {doc_id or 'none returned'})"
        )


async def _is_workflow_kb(ws: str, kb: str) -> bool:
    try:
        payload = await maxkb_request("GET", f"/workspace/{ws}/knowledge/{kb}")
        return (payload.get("data") or {}).get("type") == 4
    except ToolError as e:
        log.warning("Failed to query knowledge base type, treating as regular: %s", e)
        return False


def _read_upload_source(file_path: str, file_name: str, content_base64: str) -> tuple[str, bytes]:
    if file_path:
        path = file_path
        if not os.path.isfile(path):
            raise ToolError(f"File not found: {path} (container path; mount a directory first, e.g. /app/data/xx.pdf)")
        with open(path, "rb") as f:
            data = f.read()
        name = file_name or os.path.basename(path)
        return name, data
    if content_base64:
        try:
            data = base64.b64decode(content_base64)
        except Exception as e:
            raise ToolError(f"content_base64 decode failed: {e}")
        if not (file_name):
            raise ToolError("content_base64 uploads also require file_name")
        return file_name, data
    raise ToolError("Provide either file_path (server-local path) or content_base64+file_name")


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
    """Build a multipart/form-data body by hand.

    httpx's files+data mixed encoding produces a broken body when data is a list
    of (k, v) tuples, while the split endpoint needs repeated `patterns` keys.
    Non-ASCII file names are supported via filename + RFC5987 filename* (same as
    browsers).
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
    # 1. upload the source file to MaxKB (returns ./oss/file/<file_id>)
    body, ctype = _encode_multipart(
        [("source_id", kb), ("source_type", "KNOWLEDGE")], "file", name, data
    )
    payload = await maxkb_request("POST", "/oss/file", content=body, content_type=ctype)
    file_url = payload.get("data")
    if isinstance(file_url, dict):
        file_url = file_url.get("url") or file_url.get("file_id")
    file_id = str(file_url).rstrip("/").split("/")[-1]

    # 2. submit the knowledge workflow job
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
        f"Document '{name}' submitted via the knowledge workflow (file_id: {file_id}, "
        f"task id: {action_id or 'none returned'}, state: {state}). "
        f"The workflow runs asynchronously; check progress later with list_documents or in the MaxKB UI."
    )


async def _upload_via_split(
    ws: str, kb: str, name: str, data: bytes, limit: int, patterns: list[str] | None, with_filter: bool
) -> str:
    # 1. split preview
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
        raise ToolError("File split failed, no segments returned: " + json.dumps(payload, ensure_ascii=False)[:300])

    # 2. assemble paragraphs and create the document
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
        f"Document '{name}' uploaded: split into {len(paragraphs)} segments by the "
        f"knowledge base rules, document created (vectorization runs in background)."
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
    """Upload a document file into the knowledge base and auto-split it into segments.

    File source, either: file_path (path on the server; use the mounted directory
    with docker) or content_base64 + file_name (inline content, good for small files).

    mode values:
      auto     decide automatically: workflow knowledge bases use the workflow
               smart split, regular ones use rule-based splitting
      workflow force the knowledge workflow (requires a published workflow KB)
      split    force rule-based splitting (limit = segment length,
               patterns = split regex, with_filter = strip special characters)
    """
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)
    name, data = _read_upload_source(file_path, file_name, content_base64)
    if len(data) > 100 * 1024 * 1024:
        raise ToolError(f"File too large ({len(data) // 1024 // 1024}MB), upload limit is 100MB")

    if mode not in ("auto", "workflow", "split"):
        raise ToolError("mode must be one of auto / workflow / split")

    use_workflow = {"workflow": True, "split": False}.get(mode)
    if use_workflow is None:
        use_workflow = await _is_workflow_kb(ws, kb)

    if use_workflow:
        try:
            return await _upload_via_workflow(ws, kb, name, data)
        except ToolError as e:
            raise ToolError(
                f"Workflow upload failed: {e}\n"
                f"Hint: this mode requires a published workflow knowledge base; "
                f'use mode="split" for regular knowledge bases'
            )
    return await _upload_via_split(ws, kb, name, data, limit, patterns, with_filter)


async def search_knowledge(
    query: str,
    knowledge_base_id: str = "",
    top_number: int = 0,
    similarity: float = 0.0,
    search_mode: str = "",
) -> str:
    """Search the knowledge base directly (hit test) and return matched segments.

    `query` is the search text; top_number = result count (default 5);
    similarity = threshold (default 0.6); search_mode is one of
    embedding (vector), keywords, blend (hybrid, default). Other spellings are
    normalized automatically; unrecognized values fall back to blend.
    Returns similarity score, title, content and source document per hit.
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
        return f"No matching segments (knowledge base {kb}, query: {query}, threshold {body['similarity']})"

    lines = [f"Found {len(hits)} segments (knowledge base {kb}):"]
    for i, h in enumerate(hits, 1):
        if not isinstance(h, dict):
            lines.append(f"{i}. {h}")
            continue
        score = h.get("similarity", h.get("comprehensive_score", h.get("score", "")))
        title = h.get("title", "")
        doc = h.get("document_name", h.get("name", ""))
        content = str(h.get("content", ""))[:800]
        head = f"{i}. [score {score}]"
        if title:
            head += f" {title}"
        if doc:
            head += f" (document: {doc})"
        lines.append(head)
        lines.append("   " + content.replace("\n", "\n   "))
        pid, did = h.get("id"), h.get("document_id")
        if pid and did:
            lines.append(
                f"   [paragraph_id: {pid} | document_id: {did} | position: {h.get('position', '')}]"
            )
    lines.append(
        "Tip: if a slice lacks context, call get_segment_context to read the "
        "neighboring segments or the full document."
    )
    return "\n".join(lines)


_doc_names: dict[str, str] = {}


async def _get_doc_name(ws: str, kb: str, doc_id: str) -> str:
    """Resolve a document name. MaxKB's paragraph endpoint does not return
    document_name and the document detail endpoint has no GET, so match by id
    against the document list (cached)."""
    if not doc_id:
        return ""
    ck = f"{kb}:{doc_id}"
    if ck in _doc_names:
        return _doc_names[ck]
    name = ""
    try:
        payload = await maxkb_request("GET", f"/workspace/{ws}/knowledge/{kb}/document/1/200")
        docs, _ = page_items(payload)
        name = next(
            (str(d.get("name") or "") for d in docs if isinstance(d, dict) and str(d.get("id")) == doc_id),
            "",
        )
    except ToolError as e:
        log.info("Failed to resolve document name (non-fatal): %s", e)
    _doc_names[ck] = name
    return name


async def _fetch_paragraphs(ws: str, kb: str, doc_id: str) -> list[dict]:
    """Read all segments of a document (full content), ordered by position.

    MaxKB endpoint: GET /workspace/{ws}/knowledge/{kb}/document/{doc}/paragraph
    (unpaged). Falls back to the paged endpoint for old versions or huge docs.
    """
    try:
        payload = await maxkb_request(
            "GET", f"/workspace/{ws}/knowledge/{kb}/document/{doc_id}/paragraph"
        )
        rows, _ = page_items(payload)
        rows = [r for r in rows if isinstance(r, dict)]
        if rows:
            rows.sort(key=lambda r: (r.get("position") or 0, str(r.get("create_time") or "")))
            return rows
    except ToolError as e:
        log.info("Unpaged paragraph endpoint unavailable, falling back to paging: %s", e)

    rows: list[dict] = []
    page, size, guard = 1, 100, 0
    while guard < 60:  # hard cap 6000 segments, avoid infinite loops
        guard += 1
        try:
            payload = await maxkb_request(
                "GET",
                f"/workspace/{ws}/knowledge/{kb}/document/{doc_id}/paragraph/{page}/{size}",
            )
        except ToolError:
            break
        batch, total = page_items(payload)
        batch = [b for b in batch if isinstance(b, dict)]
        if not batch:
            break
        rows.extend(batch)
        if len(rows) >= int(total or 0) or len(batch) < size:
            break
        page += 1
    rows.sort(key=lambda r: (r.get("position") or 0, str(r.get("create_time") or "")))
    return rows


async def _locate_by_paragraph_id(ws: str, kb: str, paragraph_id: str) -> tuple[str, list[dict], int] | None:
    """With only a paragraph id, scan the knowledge base documents (up to 200)
    to find which document and position it belongs to."""
    try:
        payload = await maxkb_request(
            "GET", f"/workspace/{ws}/knowledge/{kb}/document/1/200"
        )
        docs, _ = page_items(payload)
    except ToolError:
        docs = []
    for d in docs:
        if not isinstance(d, dict) or not d.get("id"):
            continue
        doc_id = str(d["id"])
        if d.get("name"):
            _doc_names[f"{kb}:{doc_id}"] = str(d["name"])
        rows = await _fetch_paragraphs(ws, kb, doc_id)
        for idx, r in enumerate(rows):
            if str(r.get("id")) == paragraph_id:
                return doc_id, rows, idx
    return None


def _build_context_window(
    rows: list[dict], center_idx: int, neighbors: int, max_chars: int
) -> tuple[list[tuple[int, dict]], int, int]:
    """Take `neighbors` segments on each side of center_idx and apply the
    character cap.

    Returns (selected [(index, paragraph)], window size, truncated 1/0).
    Truncation keeps the matched segment whole, then expands nearest-first and
    stops once max_chars would be exceeded.
    """
    n = len(rows)
    lo = max(0, center_idx - max(0, neighbors))
    hi = min(n - 1, center_idx + max(0, neighbors))
    window = list(range(lo, hi + 1))

    order = sorted(window, key=lambda i: abs(i - center_idx))  # hit -> near -> far
    picked: set[int] = set()
    used = 0
    truncated = 0
    for i in order:
        size = len(str(rows[i].get("content") or "")) + len(str(rows[i].get("title") or ""))
        if used + size > max_chars and picked:
            truncated = 1
            break
        picked.add(i)
        used += size
    return [(i, rows[i]) for i in sorted(picked)], len(window), truncated


async def get_segment_context(
    paragraph_id: str = "",
    document_id: str = "",
    knowledge_base_id: str = "",
    query: str = "",
    neighbors: int = -1,
    max_chars: int = 0,
    full_document: bool = False,
) -> str:
    """Read neighboring segments around a retrieved hit, or the full document text.

    Use it when a search_knowledge slice is not enough to answer: expand the
    context around the hit instead of re-answering from a single slice.
    Locating the segment (pick one, the first is recommended):
      1. pass paragraph_id + document_id as annotated in search_knowledge output;
      2. pass paragraph_id only (the service scans documents, slightly slower);
      3. pass document_id + full_document=true to read the whole document;
      4. pass query: the service searches first, then expands the top hit.
    neighbors = segments to include on each side (-1 = env default, suggest 1~3,
    max 20); max_chars = total character cap (0 = env default) to avoid token
    blow-ups; full_document=true reads from the matched segment to the end of
    the document, still capped by max_chars.
    Only call this when the retrieved slices are insufficient; do not expand
    automatically after every search.
    """
    paragraph_id = (paragraph_id or "").strip()
    document_id = (document_id or "").strip()
    # pure argument validation first, avoid pointless network round-trips
    if not paragraph_id and not document_id and not (query or "").strip():
        raise ToolError(
            "Missing arguments: provide at least one of paragraph_id (from "
            "search_knowledge output), document_id (with full_document), or "
            "query (search first, then expand)."
        )
    ws = await get_workspace_id()
    kb = await resolve_kb(knowledge_base_id)
    nb = DEFAULT_CONTEXT_NEIGHBORS if neighbors < 0 else min(max(neighbors, 0), 20)
    cap = max_chars if max_chars > 0 else DEFAULT_CONTEXT_MAX_CHARS

    rows: list[dict] = []
    center_idx = 0
    doc_name = ""
    hit_note = ""

    # --- locate via query / document_id / paragraph_id ---
    if not document_id and query and not paragraph_id:
        body = {
            "query_text": query,
            "top_number": 1,
            "similarity": DEFAULT_SIMILARITY,
            "search_mode": _normalize_search_mode(""),
        }
        payload = await maxkb_request(
            "POST", f"/workspace/{ws}/knowledge/{kb}/hit_test", json_body=body
        )
        hits = payload.get("data") or []
        if isinstance(hits, dict):
            hits = hits.get("records") or hits.get("list") or []
        if not hits or not isinstance(hits[0], dict):
            return f"No segments matched the query (knowledge base {kb}); cannot expand context."
        top = hits[0]
        document_id = str(top.get("document_id") or "")
        paragraph_id = str(top.get("id") or "")
        doc_name = str(top.get("document_name") or "")
        hit_note = f"located the top-scoring segment via query (score {top.get('similarity', top.get('comprehensive_score', ''))})"

    if document_id:
        rows = await _fetch_paragraphs(ws, kb, document_id)
        if not rows:
            return (
                f"Document has no readable segments (document_id {document_id}, knowledge base {kb}). "
                f"It may have been deleted, or knowledge_base_id does not match the document."
            )
        if not doc_name:
            doc_name = await _get_doc_name(ws, kb, document_id)
        # locate the center segment
        if paragraph_id:
            center_idx = next(
                (i for i, r in enumerate(rows) if str(r.get("id")) == paragraph_id), -1
            )
            if center_idx < 0:
                center_idx = 0
                hit_note = (hit_note + " | " if hit_note else "") + "paragraph_id not found in this document; using the first segment"
        else:
            center_idx = 0
            if not full_document:
                hit_note = (hit_note + " | " if hit_note else "") + "no paragraph_id given; reading from segment 1"
    elif paragraph_id:
        found = await _locate_by_paragraph_id(ws, kb, paragraph_id)
        if not found:
            return (
                f"Paragraph {paragraph_id} not found in any document of knowledge base {kb} "
                f"(scanned up to 200 documents). "
                f"Pass document_id (from search_knowledge output) or list documents first."
            )
        document_id, rows, center_idx = found
        doc_name = await _get_doc_name(ws, kb, document_id)
    else:
        # query matched but the response carried no document_id
        return (
            f"Located a matching segment but could not determine its document (knowledge base {kb}). "
            f"Retry with paragraph_id + document_id from the search_knowledge output."
        )

    # --- assemble output ---
    if full_document:
        # read from the hit to the end of the document (not from segment 1)
        start = center_idx if paragraph_id else 0
        window = list(enumerate(rows))[start:]
        total_in_window = len(window)
        truncated = 0
    else:
        window, total_in_window, truncated = _build_context_window(rows, center_idx, nb, cap)

    out: list[str] = [
        f"Context expansion (knowledge base {kb} | document: {doc_name or document_id or 'unknown'}"
        f" | {len(rows)} segments total{'' if full_document else f' | hit at segment {center_idx + 1}'}):"
    ]
    if hit_note:
        out.append(f"  ({hit_note})")

    used = 0
    cut = False
    for i, r in window:
        title = str(r.get("title") or "").strip()
        content = str(r.get("content") or "")
        if not r.get("is_active", True):
            tag = "[inactive]"
        else:
            tag = ""
        marker = ">>> MATCHED SEGMENT <<<" if (i == center_idx and not full_document) else f"segment {i + 1}/{len(rows)}"
        head = f"\n--- {marker}{(' ' + title) if title else ''}{(' ' + tag) if tag else ''} ---"
        remain = cap - used
        if remain <= 0:
            cut = True
            break
        if len(content) > remain:
            content = content[:remain] + " ...(truncated)"
            cut = True
        used += len(content) + len(title)
        out.append(head)
        out.append(content)

    if cut or truncated:
        if full_document:
            out.append(
                f"\n(document has {len(rows)} segments; body cut off by max_chars={cap}. "
                f"Raise max_chars, or use a neighbors window for the part you need.)"
            )
        else:
            extra = (
                "some segment bodies were truncated" if (cut and len(window) == total_in_window)
                else f"only {len(window)} of {total_in_window} window segments emitted"
            )
            out.append(
                f"\n(incomplete: {extra}; character cap {cap} reached. "
                f"Raise max_chars or lower neighbors and retry.)"
            )
    if not full_document and center_idx + nb >= len(rows) - 1:
        out.append("(note: reached the end of the document; no further segments.)")
    return "\n".join(out)



# ----------------------------------------------------------------------------
# Assemble the MCP server (dual protocol)
# ----------------------------------------------------------------------------
mcp = MCPServer(SERVER_NAME)

mcp.add_tool(list_knowledge_bases, name="list_knowledge_bases")
mcp.add_tool(list_documents, name="list_documents")
mcp.add_tool(add_segment, name="add_segment")
mcp.add_tool(upload_document, name="upload_document")
mcp.add_tool(search_knowledge, name="search_knowledge")
mcp.add_tool(get_segment_context, name="get_segment_context")


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
                "list_knowledge_bases",
                "list_documents",
                "add_segment",
                "upload_document",
                "search_knowledge",
                "get_segment_context",
            ],
            "transports": {
                "streamable-http": "/mcp",
                "sse": "/sse (POST /messages/)",
            },
            "maxkb_base_url": BASE_URL or "(unset)",
            "maxkb_base_url_raw": BASE_URL_RAW or "(unset)",
            "credential": (
                "api_key" if MAXKB_API_KEY else ("password" if MAXKB_USERNAME else "not configured!!")
            ),
            "workspace_id": MAXKB_WORKSPACE_ID or "(auto)",
            "default_knowledge_id": MAXKB_KNOWLEDGE_ID or "(unset)",
        }
    )


@asynccontextmanager
async def lifespan(app):
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
                        {"error": "unauthorized, send Authorization: Bearer <MCP_AUTH_TOKEN>"},
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
    log.info("MaxKB Knowledge Base MCP server starting")
    log.info("  protocols: MCP(Streamable HTTP) http://%s:%s/mcp", HOST, PORT)
    log.info("             SSE                   http://%s:%s/sse", HOST, PORT)
    log.info("  health:    http://%s:%s/health", HOST, PORT)
    log.info("  MaxKB:     %s", BASE_URL or "unset")
    log.info("  credential: %s", "API Key" if MAXKB_API_KEY else ("password" if MAXKB_USERNAME else "not configured!!"))
    log.info("  tools:     list_knowledge_bases / list_documents / add_segment / upload_document / search_knowledge / get_segment_context")
    if not BASE_URL or not (MAXKB_API_KEY or MAXKB_USERNAME):
        log.error("  !! BASE_URL or credentials missing; tool calls will fail, check the environment")
    log.info("=" * 60)
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
