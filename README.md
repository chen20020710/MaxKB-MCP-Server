# MaxKB MCP Server

MCP tools for [MaxKB](https://github.com/1Panel-dev/MaxKB) v2.x — agent chat and knowledge base management. Plug them into any MCP client (WorkBuddy, LiteLLM, Claude Desktop, Cursor, ...).

Both services speak **MCP Streamable HTTP** and **SSE** over plain HTTP, run as two containers via Docker Compose, and take all credentials from `.env` — nothing is baked into images or code.

## Services

| Service | Directory | Port | Tools |
|---|---|---|---|
| Agent chat | `maxkb-chat-mcp/` | `18010` | `maxkb_chat` — plus one tool per extra agent via `CHAT_AGENTS` |
| Knowledge base | `maxkb-knowledge-mcp/` | `18011` | `list_knowledge_bases`, `list_documents`, `add_segment`, `upload_document`, `search_knowledge`, `get_segment_context` |

Each service exposes `/mcp` (Streamable HTTP, recommended), `/sse` (legacy SSE) and `/health` (config overview, never returns secrets).

## Prerequisites

- Docker + Docker Compose v2
- A reachable MaxKB v2.x instance
- Two credentials:
  - Chat: MaxKB **application API key** (App → Publish/Settings → API Key; prefix `agent-` on v2.10+, `application-` on older builds)
  - Knowledge: MaxKB **user-level API key** (avatar → API Keys) **or** admin username/password

## Deploy

```bash
git clone https://github.com/chen20020710/MaxKB-MCP-Server.git
cd MaxKB-MCP-Server
cp .env.example .env            # Windows: copy .env.example .env
# edit .env — minimum: MAXKB_BASE_URL, CHAT_API_KEY, and KB_API_KEY or KB_USERNAME/KB_PASSWORD
docker compose up -d --build
curl http://127.0.0.1:18010/health
curl http://127.0.0.1:18011/health
```

> ⚠️ **`MAXKB_BASE_URL` must be the bare base URL, no app id.**
> Correct: `http://host:8080` — Wrong: `http://host:8080/chat/api/<app-id>` (that is the web embed URL; using it makes every call 404).
> Which agent you talk to is decided entirely by `CHAT_API_KEY` — one key = one agent, never by the URL.

Every other variable is optional; each one is documented inline in `.env.example`.

## Use

Add to your MCP client (replace `<HOST>`):

```json
{
  "mcpServers": {
    "maxkb-chat":      { "url": "http://<HOST>:18010/mcp" },
    "maxkb-knowledge": { "url": "http://<HOST>:18011/mcp" }
  }
}
```

If `MCP_AUTH_TOKEN` is set, add the header `Authorization: Bearer <MCP_AUTH_TOKEN>` on the client side.

### Tools

| Tool | What it does |
|---|---|
| `maxkb_chat` | Ask the agent; conversation context is kept automatically |
| `list_knowledge_bases` / `list_documents` | Discover knowledge base / document ids |
| `add_segment` | Create a text segment directly |
| `upload_document` | Upload a whole file, auto-split into segments |
| `search_knowledge` | Retrieval (embedding / keywords / blend); returns matched segments with paragraph & document ids |
| `get_segment_context` | On demand: read the segments before/after a hit, or the full document (capped by `max_chars` to keep token use under control) |

Multiple agents in **one** process: set `CHAT_AGENTS=name:appApiKey[:description]|name2:key2` — each agent becomes its own tool with its own session.

## Operations

```bash
docker compose ps                  # status
docker compose logs -f chat        # logs (chat / knowledge)
docker compose up -d               # apply .env changes
docker compose restart
docker compose down
docker compose up -d --build       # after code changes
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `404 HTTP_404_NOT_FOUND` | `MAXKB_BASE_URL` contains an app path — use the bare base URL (see note above) |
| Wrong agent replies / switch agent | Change `CHAT_API_KEY`; for several agents at once use `CHAT_AGENTS` |
| `未指定知识库` (knowledge base not specified) | Set `KB_KNOWLEDGE_ID` in `.env`, or pass `knowledge_base_id` per call |
| Auth errors (1002 / 1003) | Key wrong or expired — regenerate it; for the knowledge service you can fall back to `KB_USERNAME`/`KB_PASSWORD` |
| Client cannot connect | Open the ports; check `/mcp` vs `/sse`; send the Bearer token if `MCP_AUTH_TOKEN` is set |

## Security

- `.env` is git-ignored — never commit real credentials
- Set `MCP_AUTH_TOKEN` in production and keep the deployment inside a trusted network
- Prefer revocable user-level API keys over admin passwords

## Layout

```
MaxKB-MCP-Server/
├── .env.example            # config template with inline docs (safe to commit)
├── .gitignore
├── docker-compose.yml      # single entrypoint for both services
├── maxkb-chat-mcp/         # server.py + Dockerfile
└── maxkb-knowledge-mcp/    # server.py + Dockerfile
```

Python 3.12 · MCP Python SDK 2.3.0 (Streamable HTTP + SSE) · httpx · uvicorn/Starlette · Docker Compose v2
