# MaxKB MCP Server

把 [MaxKB](https://github.com/1Panel-dev/MaxKB) v2.x 的智能体对话与知识库能力封装成标准 MCP 工具，供 WorkBuddy、OpenClaw、DeepSeek Harness、LiteLLM、Claude Desktop、Cursor 等支持 MCP 的智能体 / 客户端直接调用。

同一进程同时提供 **MCP Streamable HTTP** 与 **MCP SSE** 两种传输协议，全部通过 Docker Compose 一键部署，凭据通过 `.env` 注入，不写死在镜像或代码里。

---

## 一、包含两个服务

| 服务 | 目录 | 默认端口 | 提供的工具 | 主要用途 |
|---|---|---|---|---|
| **智能体对话** | `maxkb-chat-mcp/` | `18010` | `maxkb_chat` | 与指定 MaxKB 应用（智能体）对话，支持多轮上下文 |
| **知识库管理** | `maxkb-knowledge-mcp/` | `18011` | `list_knowledge_bases`、`list_documents`、`add_segment`、`upload_document`、`search_knowledge`、`get_segment_context` | 查询列表、上传文档、创建分段、直接检索知识切片、按命中片段扩展前后上下文或读取全文 |

两个服务相互独立，可以一起启动，也可以只启动其中一个。

```
客户端 (WorkBuddy / LiteLLM / Claude Desktop / ...)
   │  MCP over HTTP
   ├── :18010  maxkb-chat-mcp       ──> MaxKB /chat/api/*
   └── :18011  maxkb-knowledge-mcp  ──> MaxKB /admin/api/*
                                        └──> 同一个 MaxKB 服务
```

每个服务都暴露三个端点：

| 端点 | 协议 | 说明 |
|---|---|---|
| `/mcp` | Streamable HTTP | **推荐**，新版本客户端优先用这个 |
| `/sse` | SSE（消息回传 `/messages/`） | 兼容只支持 SSE 的旧客户端 |
| `/health` | HTTP GET | 健康检查，返回配置概览（不含密钥） |

---

## 二、前置条件

1. 一台安装了 **Docker** 与 **Docker Compose v2**（`docker compose` 命令）的主机；
2. 已部署并可访问的 **MaxKB v2.x** 服务；
3. 部署主机与 MCP 客户端都能访问到该主机的 `18010` / `18011` 端口；
4. 两个凭据（后面会填到 `.env`）：
   - 对话服务：MaxKB **应用 API Key**（应用 → 设置/发布 → API Key）；
   - 知识库服务：MaxKB **用户级 API Key**（右上角头像 → API Key 管理）**或**管理员账号密码。

> 不需要在部署主机上安装 Python，所有依赖都在镜像里构建。

---

## 三、Docker 部署（Step by Step）

### 第 1 步：拉取代码

```bash
git clone https://github.com/chen20020710/MaxKB-MCP-Server.git
cd MaxKB-MCP-Server
```

### 第 2 步：创建配置文件

```bash
cp .env.example .env          # Windows CMD: copy .env.example .env
```

> `.env` 已被 `.gitignore` 忽略，**永远不会**被提交。真实密码只写在这个文件里。

### 第 3 步：编辑 `.env`，填入真实值

最少只需要改两处（其余保持默认即可）：

```env
MAXKB_BASE_URL=http://192.168.1.100:8080        # 改成你的 MaxKB 根地址
CHAT_API_KEY=agent-你的应用APIKey                 # 改成你的应用 API Key
```

知识库服务还需要补上凭据，**二选一**：

```env
# 方式 A（推荐）：用户级 API Key
KB_API_KEY=你的用户级APIKey

# 方式 B：管理员账号密码（KB_API_KEY 留空时生效）
KB_USERNAME=admin
KB_PASSWORD=REPLACE_WITH_YOUR_PASSWORD
```

> 建议再填上 `KB_KNOWLEDGE_ID`（默认知识库 ID），否则直接调用 `search_knowledge` 时会返回「未指定知识库」。ID 可在启动后通过 `list_knowledge_bases` 工具查询。

完整变量说明见 [第四节](#四环境变量说明) 与 `.env.example` 内的逐项注释。

### 💡 常见问题：我要调用哪个智能体（应用）？需要填应用 ID 吗？

**不需要。** `MAXKB_BASE_URL` 只是 MaxKB 的服务根地址；**具体调用哪个智能体，完全由 `CHAT_API_KEY` 决定**。

MaxKB 是「一应用一密钥」的设计，服务端的处理链路是：

```text
GET /chat/api/open
  └─ Authorization: Bearer <CHAT_API_KEY>
       └─ ChatTokenAuth → ApplicationKey handler
            用 secret_key = token 查 application_api_key 表
            → 得到 application_id，写入 request.auth
       └─ OpenView 用 request.auth.application_id 创建会话，返回 chat_id
POST /chat/api/chat_message/{chat_id}   （同样带这个 Key）
```

应用 ID 是服务端从 Key 反查出来的，配置里根本不需要出现它。

**那个 `http://host:8080/chat/api/<应用ID>` 是什么？**
是 MaxKB 应用设置页里给**前端嵌入 / 匿名网页访问**用的公开地址（给浏览器打开的），不是 API 基地址。把它填进 `MAXKB_BASE_URL`，服务会再拼一次 `/chat/api/open`，变成 `/chat/api/<应用ID>/chat/api/open` → 404。

**怎么拿到指定智能体的 Key：**

| 步骤 | 操作 |
|---|---|
| 1 | MaxKB 控制台 →「应用」列表，进入你要对接的那个应用 |
| 2 | 打开「设置 / 发布」页，开启 API 访问（部分版本叫「API Key / 访问凭据」） |
| 3 | 复制该应用的 API Key（v2.10+ 多为 `agent-` 前缀，旧版 `application-`） |
| 4 | 填入 `.env` 的 `CHAT_API_KEY`，执行 `docker compose up -d` 生效 |

> 注意区分：`CHAT_API_KEY` 是**应用级**密钥（决定智能体）；`KB_API_KEY` 是**用户级**密钥（用于知识库管理接口），两者不能混用。

**想切换智能体**：换 `CHAT_API_KEY` 即可。

**想同时对接多个智能体**（推荐，无需多个服务）：用 `CHAT_AGENTS` 在一个进程里注册多个对话工具，格式 `工具名:应用APIKey[:描述]`，多个用 `|` 分隔：

```env
# .env
CHAT_AGENTS=ops_chat:agent-xxxxxxxx:运维专家, 回答 VMware/机房/ZStack 问题|crm_chat:agent-yyyyyyyy:客服助手, 回答产品与工单问题
```

重启后 `http://<HOST>:18010/mcp` 这一个端点上就同时有 `ops_chat`、`crm_chat` 两个工具（各自独立维护会话 `chat_id`，上下文互不干扰；`CHAT_API_KEY` 单智能体配置仍可与之并存）。工具描述里写清楚各智能体擅长什么，调用方 AI 才能选对工具。

<details>
<summary>备选方案：复制多个 chat 服务（各配独立 Key / 端口）</summary>

```yaml
  chat-expert:
    build: { context: ./maxkb-chat-mcp }
    image: maxkb-mcp-chat:1.0.0
    container_name: maxkbv2-mcp-expert
    ports: ["18012:8000"]
    environment:
      BASE_URL: ${MAXKB_BASE_URL:-}
      API_KEY: ${CHAT_EXPERT_API_KEY:-}     # 另一个应用的 Key
      TOOL_NAME: maxkb_expert_chat          # 工具名必须不同, 否则客户端会冲突
      SERVER_NAME: "资深运维专家"
    restart: always
```

适合想让不同智能体暴露在不同 MCP 端点（做权限隔离 / 分组）的场景；一般场景用 `CHAT_AGENTS` 即可。
</details>

### 第 4 步：构建并启动

```bash
docker compose up -d --build
```

只启动其中一个服务：

```bash
docker compose up -d --build chat          # 仅对话服务
docker compose up -d --build knowledge     # 仅知识库服务
```

首次构建会拉取 `python:3.12-slim` 基础镜像并安装依赖，国内网络默认走清华 pip 源；如可直连 PyPI，可在 `.env` 中把 `PIP_INDEX_URL` 改为 `https://pypi.org/simple`。

### 第 5 步：确认部署成功

```bash
docker compose ps                          # 两个服务应为 Up / healthy
curl http://127.0.0.1:18010/health         # 对话服务
curl http://127.0.0.1:18011/health         # 知识库服务
```

`/health` 会返回服务器名、工具列表、协议端点，以及 `maxkb_base_url` / `maxkb_base_url_raw` / `maxkb_open_url`（便于核对实际请求地址），**不会返回任何密钥**。

用 MCP 协议做一次最简握手（返回 `200` 且含 `serverInfo` 即正常）：

```bash
curl -X POST http://127.0.0.1:18010/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}'
```

---

## 四、环境变量说明

所有变量都写在项目根目录的 `.env` 中，由 `docker-compose.yml` 通过 `${变量名}` 传入容器。**修改 `.env` 后执行 `docker compose up -d` 即可生效**（改了 `PIP_INDEX_URL` 才需要重新 `--build`）。

### 4.1 全局 / 公共

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `MAXKB_BASE_URL` | ✅ | 无 | MaxKB **根地址**，只填到端口，如 `http://192.168.1.100:8080`。**不要**填应用专属的 `.../chat/api/<应用ID>` 地址，否则会 404 |
| `PIP_INDEX_URL` | ❌ | 清华源 | 构建镜像时的 pip 镜像源 |
| `MCP_AUTH_TOKEN` | ❌ | 空（不校验） | 设置后，访问 `/mcp`、`/sse`、`/messages` 需带 `Authorization: Bearer <该值>`；两个服务共用 |
| `MCP_ALLOWED_HOSTS` | ❌ | `*`（不校验） | Host 头白名单，逗号分隔，用于防 DNS rebinding |

### 4.2 对话服务

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `CHAT_PORT` | ❌ | `18010` | 宿主机端口（容器内固定 8000） |
| `CHAT_API_KEY` | ✅（与 `CHAT_AGENTS` 二选一） | 无 | 单智能体：应用 API Key。v2.10+ 多为 `agent-` 前缀，旧版为 `application-`，以页面实际复制为准 |
| `CHAT_AGENTS` | ❌ | 空 | **多智能体**：`工具名:应用APIKey[:描述]`，多个用 `\|` 分隔；一个进程注册多个对话工具，各自独立会话 |
| `CHAT_SERVER_NAME` | ❌ | `MaxKB 智能体对话 MCP 服务器` | 展示给客户端的服务器名 |
| `CHAT_TOOL_NAME` | ❌ | `maxkb_chat` | 单智能体方式下的工具名 |
| `CHAT_TOOL_DESCRIPTION` | ❌ | 内置描述 | 工具描述，决定调用方 AI 何时使用它 |
| `CHAT_TIMEOUT` | ❌ | `180` | 调用 MaxKB 对话接口超时（秒） |
| `MAXKB_CHAT_PREFIX` | ❌ | `/chat` | 聊天 API 前缀，一般无需修改 |

### 4.3 知识库服务

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `KB_PORT` | ❌ | `18011` | 宿主机端口（容器内固定 8000） |
| `KB_API_KEY` | ✅（二选一） | 空 | 用户级 API Key，优先级最高 |
| `KB_USERNAME` / `KB_PASSWORD` | ✅（二选一） | 空 | 管理员账号密码，服务自动登录换取 JWT 并在 401 时重登 |
| `KB_KNOWLEDGE_ID` | 建议 | 空 | 默认知识库 ID；不填则 `search_knowledge` 必须每次显式传 `knowledge_base_id` |
| `KB_WORKSPACE_ID` | ❌ | 自动获取 | 工作空间 ID，社区版通常自动得到 `default` |
| `KB_SERVER_NAME` | ❌ | `MaxKB 知识库管理 MCP 服务器` | 展示名 |
| `KB_TOP_NUMBER` | ❌ | `5` | 检索默认返回条数 |
| `KB_SIMILARITY` | ❌ | `0.6` | 检索默认相似度阈值 |
| `KB_SEARCH_MODE` | ❌ | `blend` | 只能是 `embedding` / `keywords` / `blend` 之一 |
| `KB_CONTEXT_NEIGHBORS` | ❌ | `1` | `get_segment_context` 默认向前/向后各扩展的分段数（建议 1~3） |
| `KB_CONTEXT_MAX_CHARS` | ❌ | `6000` | `get_segment_context` 单次返回字符上限，防止上下文扩展导致 token 暴增 |
| `KB_TIMEOUT` | ❌ | `120` | 管理端接口超时（秒） |
| `MAXKB_ADMIN_PREFIX` | ❌ | `/admin` | 管理端 API 前缀，一般无需修改 |

> 值中含空格或中文标点时，请在 `.env` 里用双引号包裹，例如 `CHAT_TOOL_DESCRIPTION="..."`。

---

## 五、部署完成后如何使用

### 5.1 在 MCP 客户端里添加服务器

把下面的 `<HOST>` 换成部署主机地址。**支持 Streamable HTTP 的客户端用 `/mcp`；只支持 SSE 的老客户端用 `/sse`。**

**WorkBuddy**（`~/.workbuddy/mcp.json`）：

```json
{
  "mcpServers": {
    "maxkb-chat": {
      "url": "http://<HOST>:18010/mcp"
    },
    "maxkb-knowledge": {
      "url": "http://<HOST>:18011/mcp"
    }
  }
}
```

**Claude Desktop / Cursor / Cline 等**（部分客户端需要 `mcp-remote` 桥接 HTTP 服务）：

```json
{
  "mcpServers": {
    "maxkb-chat": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://<HOST>:18010/mcp"]
    }
  }
}
```

**LiteLLM**：在管理后台添加 MCP Server，URL 填 `http://<HOST>:18010/mcp`（Transport 选 Streamable HTTP；选 SSE 则填 `/sse`），两个服务分别添加即可。

启用 `MCP_AUTH_TOKEN` 时，在客户端的请求头中加上：

```
Authorization: Bearer <MCP_AUTH_TOKEN 的值>
```

添加完成后，在客户端执行「列出工具 / refresh」，应能看到对话工具（单智能体 1 个；配置 `CHAT_AGENTS` 后为 1 + N 个）+ 知识库 6 个工具。

### 5.2 工具清单与参数

#### 对话服务

| 工具 | 参数 | 说明 |
|---|---|---|
| `maxkb_chat` | `message`（必填，问题内容）<br>`new_chat`（可选，默认 `false`）<br>`chat_id`（可选） | 发消息并返回智能体回答。默认自动延续服务内保存的最近一次会话上下文；`new_chat=true` 新开会话；`chat_id` 可指定已有会话继续。会话失效会自动重开并重试 |

#### 知识库服务

| 工具 | 关键参数 | 说明 |
|---|---|---|
| `list_knowledge_bases` | `name`、`current_page`、`page_size` | 查询知识库列表，返回的 `id` 可用于其他工具 |
| `list_documents` | `knowledge_base_id`、`name`、`current_page`、`page_size` | 查询知识库下文档列表，返回的文档 `id` 可用于 `add_segment` |
| `add_segment` | `content`（必填）、`title`、`document_name`、`document_id`、`knowledge_base_id`、`problems` | **方式一：创建分段**。直接传文本内容新建一条分段；指定 `document_id` 时追加到已有文档；`problems` 为同义问题列表，可提高检索命中率 |
| `upload_document` | `file_path` 或 `content_base64`+`file_name`（二选一）、`knowledge_base_id`、`mode`、`limit`、`patterns`、`with_filter` | **方式二：上传整文件自动分段**。`mode=auto`（默认）自动判断：工作流知识库走工作流智能分段，普通知识库走规则分段；也可强制 `workflow` / `split`。`file_path` 需以 `/app/data` 开头（对应宿主机 `./data` 目录）；文件上限 100MB |
| `search_knowledge` | `query`（必填）、`knowledge_base_id`、`top_number`、`similarity`、`search_mode` | 直接检索知识库并返回命中的知识切片（含相似度、标题、内容、所属文档，以及供后续扩窗用的**段落ID / 文档ID 标注**）。`search_mode` 支持中文/别名自动归一化，非法值回退 `blend` |
| `get_segment_context` | `paragraph_id`、`document_id`、`knowledge_base_id`、`query`（定位方式任选其一）、`neighbors`、`max_chars`、`full_document` | **上下文扩展（按需调用，不要在每次检索后自动触发）**：读取命中片段的前后相邻分段（MaxKB 检索命中的是整段「大块」，其前/后段按同文档 `position` 顺序取回），或 `full_document=true` 读取整篇文档全文。`neighbors` 控制前后各取几段（默认 1，建议 1~3）；`max_chars` 控制总字符上限（默认 6000，超限会明确提示内容不完整而不是静默截断）。典型用法：`search_knowledge` 回答不满意 → 用结果里标注的段落ID+文档ID 调本工具扩窗 → 仍不足再 `full_document=true` |

未显式传 `knowledge_base_id` 的工具会使用 `.env` 里的 `KB_KNOWLEDGE_ID`。

### 5.3 典型使用流程

1. 调用 `list_knowledge_bases` 拿到目标知识库 `id` → 填进 `.env` 的 `KB_KNOWLEDGE_ID` → `docker compose up -d` 重启知识库服务；
2. 让 AI 调用 `search_knowledge({"query": "某个问题"})` 直接拿到知识切片（结果会标注每条切片的段落ID / 文档ID）；
3. 切片内容不足以回答时，**按需**扩窗（不要每次检索后都自动调用，避免 token 暴增）：
   `get_segment_context({"paragraph_id": "<命中段落ID>", "document_id": "<文档ID>", "neighbors": 2})`
   仍不够时再 `"full_document": true` 从命中段读到文档结尾（受 `max_chars` 保护）；
4. 需要补充资料时：把文件放到宿主机 `./data/` 目录，调用
   `upload_document({"file_path": "/app/data/手册.pdf", "mode": "auto"})`；
   或只补一小段内容时调用
   `add_segment({"content": "...", "title": "..."})`；
5. 需要与智能体对话时调用 `maxkb_chat({"message": "..."})`，连续追问会自动带上上下文。

### 5.4 常用运维命令

```bash
docker compose ps                       # 查看状态
docker compose logs -f chat             # 查看对话服务日志
docker compose logs -f knowledge        # 查看知识库服务日志
docker compose up -d                    # 改完 .env 后应用配置
docker compose restart                  # 重启
docker compose down                     # 停止并移除容器（保留 ./data）
docker compose up -d --build            # 代码/依赖变更后重新构建
```

---

## 六、故障排查

| 现象 | 原因与处理 |
|---|---|
| `MaxKB 调用失败 [404]: HTTP_404_NOT_FOUND` | `MAXKB_BASE_URL` 填成了应用专属地址 `.../chat/api/<应用ID>`。必须只填到端口的根地址。服务虽会自动归一化，但仍建议直接改对；用 `/health` 里的 `maxkb_open_url` 核对 |
| 调用的不是我想要的智能体 / 想换应用 | 应用由 `CHAT_API_KEY` 决定（一应用一密钥，**不需要填应用 ID**）。去目标应用的「设置 / 发布」页复制它自己的 API Key 填进 `.env`，`docker compose up -d`；多个智能体并存见第三节的多服务示例 |
| `搜索模式:类型仅支持嵌入\|关键字\|混合` | `KB_SEARCH_MODE` 或工具入参传了非法值。默认值只能用英文 `embedding` / `keywords` / `blend`；工具入参的中文/别名会自动归一化，无法识别会回退 `blend` |
| `未指定知识库` | 未配置 `KB_KNOWLEDGE_ID` 且调用时未传 `knowledge_base_id`。用 `list_knowledge_bases` 查到 ID 后填入 `.env` |
| `MaxKB 认证失败 (1002/1003)` | 应用 API Key 或管理端凭据错误/过期；社区版用户级 API Key 可能不可用，改用管理员账密 |
| LiteLLM 后台工具显示「Other / 未分类」 | LiteLLM 前端按工具名/描述中的**英文 CRUD 动词**自动分组，纯中文描述匹配不到就归为 Other。只是 UI 分组，**不影响调用**；在 `CHAT_TOOL_DESCRIPTION` 开头加 `search` / `get` 等英文动词即可归入 Read 组 |
| `get_segment_context` 返回「参数不足」 | 定位方式至少要传一种：`paragraph_id`+`document_id`（取自 `search_knowledge` 结果标注，最准）、单独 `paragraph_id`（会在知识库内逐文档查找，稍慢）、`document_id`（配合 `full_document`）或 `query`（先检索再扩窗） |
| `get_segment_context` 返回「未找到段落ID」 | 段落 ID 所属知识库与 `knowledge_base_id`/`KB_KNOWLEDGE_ID` 不一致，或文档超过扫描上限（200 个）。补传 `document_id` 即可精确定位 |
| 上下文扩展后内容仍提示「不完整」 | 属预期保护行为：达到 `max_chars`（默认 6000）即停止。确需更多再调大 `KB_CONTEXT_MAX_CHARS` 或单次传 `max_chars`，不建议长期放大 |
| 智能体回答慢导致超时 | 调大 `CHAT_TIMEOUT` / `KB_TIMEOUT` |
| 客户端连不上 | 确认防火墙放行 `CHAT_PORT` / `KB_PORT`；确认 URL 用的是 `/mcp`（新客户端）还是 `/sse`（旧客户端）；若设置了 `MCP_AUTH_TOKEN`，确认请求头带了 Bearer 令牌 |
| 构建时 pip 下载慢/失败 | 修改 `PIP_INDEX_URL` 后重新 `docker compose up -d --build` |

---

## 七、安全建议

1. **绝不提交 `.env`**：仓库已通过 `.gitignore` 忽略 `.env`、`.env.*`（仅放行 `.env.example`）与 `*.env`，请确认 `git status` 中没有它；
2. 生产环境建议设置 `MCP_AUTH_TOKEN`，并按需收紧 `MCP_ALLOWED_HOSTS`；
3. 不要用 root 之外的共享账号随意扩散凭据；知识库服务优先使用可单独吊销的 **用户级 API Key**；
4. 若服务暴露到公网，请置于网关 / 反向代理之后并启用 TLS；
5. 上传目录 `./data` 已在 `.gitignore` 中忽略，其中的业务文件不会被提交。

---

## 八、目录结构

```
MaxKB-MCP-Server/
├── .env.example              # 环境变量模板（含逐项说明，可提交）
├── .gitignore                # 忽略 .env / data / 缓存
├── README.md
├── docker-compose.yml        # 顶层统一编排（从 .env 读取所有配置）
├── maxkb-chat-mcp/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── .dockerignore
│   └── server.py             # 对话服务源码
└── maxkb-knowledge-mcp/
    ├── Dockerfile
    ├── requirements.txt
    ├── .dockerignore
    └── server.py             # 知识库服务源码
```

仓库只保留部署与运行所必需的文件，不含任何测试脚本、临时运维脚本或凭据文件。

---

## 九、技术栈

Python 3.12 · [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) 2.3.0（Streamable HTTP + SSE 双协议）· httpx · uvicorn/Starlette · Docker Compose v2
