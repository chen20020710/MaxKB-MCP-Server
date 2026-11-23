# MaxKB MCP Server

MCP tools for [MaxKB](https://github.com/1Panel-dev/MaxKB) v2.x — agent chat and knowledge base management. Plug them into any MCP client (WorkBuddy, LiteLLM, Claude Desktop, Cursor, ...).

Both services speak **MCP Streamable HTTP** and **SSE** over plain HTTP, take all credentials from `.env`, and run either via Docker Compose or with plain `docker run` — no compose required.

Current version: **2.0.0** (image tags `maxkb-mcp-chat:2.0.0`, `maxkb-mcp-knowledge:2.0.0`).

## Services

| Service | Directory | Port | Tools |
|---|---|---|---|
| Agent chat | `maxkb-chat-mcp/` | `18010` | `maxkb_chat` — plus one tool per extra agent via `CHAT_AGENTS` |
| Knowledge base | `maxkb-knowledge-mcp/` | `18011` | `list_knowledge_bases`, `list_documents`, `add_segment`, `upload_document`, `search_knowledge`, `get_segment_context` |

Each service exposes `/mcp` (Streamable HTTP, recommended), `/sse` (legacy SSE) and `/health` (config overview, never returns secrets).

## Prerequisites

- Docker (Compose v2 is optional — see *Run with plain docker* below)
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

## Run with plain docker (no compose)

`start.sh` reads the same `.env` file and launches the containers with plain `docker run` — nothing but Docker is required.

Build the images once (source needed), then start:

```bash
docker build -t maxkb-mcp-chat:2.0.0      maxkb-chat-mcp
docker build -t maxkb-mcp-knowledge:2.0.0 maxkb-knowledge-mcp
./start.sh all                            # or: chat | kb | stop | status
```

No source, no internet at all? Load prebuilt images from the release bundle instead of building:

```bash
gunzip -c maxkb-mcp-chat-2.0.0.tar.gz      | docker load
gunzip -c maxkb-mcp-knowledge-2.0.0.tar.gz | docker load
./start.sh all
```

The two commands `start.sh` runs for you, in full — paste them directly if you prefer:

```bash
docker run -d --name maxkbv2-mcp-app --restart always -p 18010:8000 \
  -e BASE_URL="http://<maxkb-host>:8080" \
  -e API_KEY="<app..." \
  maxkb-mcp-chat:2.0.0

docker run -d --name maxkbv2-knowledge-mcp-app --restart always -p 18011:8000 \
  -v "$(pwd)/data:/app/data" \
  -e BASE_URL="http://<maxkb-host>:8080" \
  -e MAXKB_USERNAME="admin" -e MAXKB_PASSWORD="***" \
  -e MAXKB_KNOWLEDGE_ID="<knowledge-base-id>" \
  maxkb-mcp-knowledge:2.0.0
```

Note the environment variable names in `docker run` are the container-side names (`BASE_URL`, `API_KEY`, `MAXKB_*`); `start.sh` maps them from the user-facing `.env` names (`MAXKB_BASE_URL`, `CHAT_API_KEY`, `KB_*`) for you. Only the variables you actually set matter — the rest have defaults.

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

Plain docker mode (without compose):

```bash
./start.sh status                  # containers + health
./start.sh all                     # (re)start both from current .env
./start.sh stop                    # remove containers (./data kept)
docker logs -f maxkbv2-mcp-app     # logs
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
├── docker-compose.yml      # compose entrypoint for both services
├── start.sh                # plain `docker run` launcher (no compose needed)
├── maxkb-chat-mcp/         # server.py + Dockerfile
└── maxkb-knowledge-mcp/    # server.py + Dockerfile
```

Python 3.12 · MCP Python SDK 2.3.0 (Streamable HTTP + SSE) · httpx · uvicorn/Starlette · Docker Compose v2
