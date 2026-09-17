# TDL 管理

`TDL 管理` 是基于 [iyear/tdl](https://github.com/iyear/tdl) `v0.20.4` 的 Telegram 下载管理服务。它将上游 tdl 的登录与下载能力封装为 Web 管理台，并提供 Telegram Bot 和表情触发入口。

项目面向自托管：应用、PostgreSQL、Telegram 会话、下载历史和最终文件均由部署者持有。默认 Compose 仅监听宿主机回环地址；公网访问应始终经由你自己的 HTTPS 反向代理。

## 功能

- 管理员 Web 登录：Argon2id 密码哈希、HttpOnly 会话 Cookie、登录限流和来源校验；
- Telegram 多账户：二维码登录、两步验证、当前账户切换、会话健康检查与失效提示；
- 消息下载：消息链接、相册/媒体组、暂停/恢复/取消/重试、实时文件进度与速率；
- 会话下载：频道和群组历史下载、消息监听、可恢复的索引进度与会话级控制；
- 下载规则：HTTP/HTTPS/SOCKS5/SOCKS5H 代理、并发/连接池、文件体积与类型筛选、临时和最终命名模板；
- 文件安全：自动建目录、Linux 文件名清理、超长名称收缩、最终文件原子落盘且不覆盖已有文件；
- PostgreSQL 永久去重：按 Telegram 对话与消息记录下载状态。自动清理不删除下载历史、去重记录或最终文件；
- Bot 控制：多位授权用户、`/help`、`/tasks`、`/chats`、`/status`、`/config`、`/restart`、任务详情与通知；
- 表情触发：监听所有已授权 Telegram 账户本人作出的匹配 Unicode 表情，支持私聊、群组、频道和收藏消息；
- 仪表盘、结构化应用日志、数据库健康检查；文件进度通过 SSE 内存推送，不高频写 PostgreSQL。


## 数据与目录

以下目录是持久化数据，不要随意删除：

| 目录 | 内容 | 影响 |
| --- | --- | --- |
| `data/` | 管理员密码哈希、Telegram 会话、Bot 游标、设置和运行状态 | 删除后需要重新初始化和登录 |
| `postgres/` | 下载历史、去重记录、会话索引和任务状态 | 删除后会失去历史和去重能力 |
| `downloads/` | 最终文件和上游临时下载文件 | 按你的存储策略管理 |

请同时备份 `data/`、`postgres/` 和 `downloads/`。仅备份其中一个无法完整恢复服务。

## 部署前准备

- Docker Engine 26+ 与 Docker Compose v2；
- 首次构建和运行建议至少预留 2 GB 内存；
- 一个用于保存下载文件的宿主机目录；
- 首次 Telegram 登录需要能够访问 Telegram；需要时可在 Web 的“配置管理”中设置代理；
- 公网访问需要域名和现有 HTTPS 反向代理。

首次启动会下载 Go、Node、PostgreSQL 和上游 tdl 依赖；后续更新会使用构建缓存。

## 方式一：Docker Compose（推荐）

适用于 OrbStack、Docker Desktop、命令行和 NAS 的 Stack / Compose 项目功能。获取源码后进入项目目录；仓库内已提供相同的 `compose.yaml` 与 `.env.example`。

**`.env` 完整示例：**

```dotenv
TDL_ADMIN_USERNAME=admin
TDL_ADMIN_INITIAL_PASSWORD=admin

TDL_DATA_DIR=/data
TDL_DOWNLOAD_DIR=/downloads
TDL_LISTEN_ADDR=:8080

TDL_POSTGRES_DB=tdl
TDL_POSTGRES_USER=tdl
TDL_POSTGRES_PASSWORD=替换为长随机数据库密码
TDL_DATABASE_URL=postgres://tdl:替换为长随机数据库密码@postgres:5432/tdl?sslmode=disable

# 本地访问保持 false；HTTPS 反向代理部署时改为 true。
TDL_COOKIE_SECURE=false
TDL_TRUSTED_ORIGINS=
TDL_TRUST_PROXY=false
```

`TDL_POSTGRES_PASSWORD` 与连接串中的密码必须完全相同；如果密码含有 `@`、`:`、`/`、`?`、`#` 等 URL 特殊字符，需要先做 URL 编码。

**`compose.yaml` 完整示例：**

```yaml
services:
  permissions:
    image: alpine:3.21
    user: "0:0"
    command:
      - /bin/sh
      - -ec
      - |
        mkdir -p /data /downloads
        chown -R 65532:65532 /data
        chown 65532:65532 /downloads
    volumes:
      - ./data:/data
      - ./downloads:/downloads
    networks: [internal]
    restart: "no"

  postgres:
    image: postgres:17-alpine
    env_file: .env
    environment:
      POSTGRES_DB: ${TDL_POSTGRES_DB:?set TDL_POSTGRES_DB in .env}
      POSTGRES_USER: ${TDL_POSTGRES_USER:?set TDL_POSTGRES_USER in .env}
      POSTGRES_PASSWORD: ${TDL_POSTGRES_PASSWORD:?set TDL_POSTGRES_PASSWORD in .env}
    volumes:
      - ./postgres:/var/lib/postgresql/data
    networks: [internal]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 5s
      timeout: 3s
      retries: 12
    restart: unless-stopped

  app:
    build: .
    env_file: .env
    ports:
      - "127.0.0.1:8080:8080"
    environment:
      TDL_DATA_DIR: /data
      TDL_DOWNLOAD_DIR: /downloads
      TDL_LISTEN_ADDR: :8080
      TDL_DATABASE_URL: ${TDL_DATABASE_URL:?set TDL_DATABASE_URL in .env}
    volumes:
      - ./data:/data
      - ./downloads:/downloads
    networks: [internal]
    depends_on:
      permissions:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
    restart: unless-stopped

networks:
  internal:
    driver: bridge
```

启动和更新：

```bash
# 首次：复制并编辑 .env 后启动
docker compose up -d --build

# 查看状态、日志与健康状态
docker compose ps
docker compose logs -f app
curl -fsS http://127.0.0.1:8080/api/health

# 更新源码后的重建
git pull
docker compose up -d --build
```

访问 `http://127.0.0.1:8080`，默认管理员账号和首次密码均为 `admin`。`TDL_ADMIN_INITIAL_PASSWORD` 仅在 `data/admin.json` 不存在时用于创建管理员；账户创建后，Web 中保存的密码始终优先于 `.env` 的该值。

`permissions` 是一次性权限初始化服务：它只将 `data/` 与 `downloads/` 交给应用容器的非 root 用户。这样在 Linux、NAS 和 macOS 上都不会因为宿主机挂载权限导致首次启动失败；`.env` 不会被容器修改。

NAS 若不支持相对挂载，直接把 `./data`、`./postgres`、`./downloads` 改为绝对路径，例如 `/volume1/docker/tdl/data:/data`、`/volume1/docker/tdl/postgres:/var/lib/postgresql/data`、`/volume1/downloads/telegram:/downloads`。容器内的 `/data`、`/downloads` 和 PostgreSQL 目标路径不要改。

不要执行 `docker compose down -v`，也不要删除 `data/`、`postgres/` 或 `downloads/`，除非你明确希望丢弃对应数据。

## 方式二：Docker 命令

适用于不使用 Compose 的环境。以下示例创建私有网络和三个持久化卷：

```bash
docker network create tdl_internal
docker volume create tdl_postgres
docker volume create tdl_data
docker volume create tdl_downloads

docker build -t tdl-manager:1.0.0 .

docker run -d --name tdl-postgres \
  --network tdl_internal --restart unless-stopped \
  -e POSTGRES_DB=tdl -e POSTGRES_USER=tdl \
  -e POSTGRES_PASSWORD='替换为长随机数据库密码' \
  -v tdl_postgres:/var/lib/postgresql/data postgres:17-alpine

docker run -d --name tdl-app \
  --network tdl_internal --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  -e TDL_DATA_DIR=/data -e TDL_DOWNLOAD_DIR=/downloads -e TDL_LISTEN_ADDR=:8080 \
  -e TDL_ADMIN_USERNAME=admin \
  -e TDL_ADMIN_INITIAL_PASSWORD='替换为强管理员密码' \
  -e 'TDL_DATABASE_URL=postgres://tdl:替换为长随机数据库密码@tdl-postgres:5432/tdl?sslmode=disable' \
  -v tdl_data:/data -v tdl_downloads:/downloads tdl-manager:1.0.0
```

若需保存到宿主机指定目录，将最后两个卷替换为明确的路径挂载，例如 `-v /volume1/downloads/telegram:/downloads`。

## HTTPS 反向代理与公网访问

默认端口绑定 `127.0.0.1:8080:8080`，只允许本机或同机反向代理连接。这是预期安全设置；不要直接把应用端口暴露到公网。

在 `.env` 中设置：

```dotenv
TDL_COOKIE_SECURE=true
TDL_TRUSTED_ORIGINS=https://tdl.example.com
# 仅当可信反向代理转发真实客户端 IP 时启用：
TDL_TRUST_PROXY=true
```

让反向代理将 HTTPS 域名转发至 `http://127.0.0.1:8080`，并保留 `Host`、`X-Forwarded-For` 与 `X-Forwarded-Proto`。WebSocket 不是必需的，但 SSE 连接需要允许保持较长时间。

若反向代理在另一台机器或另一个 Docker 网络，不能使用 `127.0.0.1` 绑定。仅在可信私有网络中改成反向代理可达的地址，并继续让 HTTPS 反向代理作为唯一公网入口。

## 环境变量参考

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `TDL_ADMIN_USERNAME` | 是 | `admin` | 首次创建的管理员名称；之后改此值不会改已有账户 |
| `TDL_ADMIN_INITIAL_PASSWORD` | 否 | `admin` | 仅在首次创建管理员时使用；创建后由 Web 已保存密码接管 |
| `TDL_DATA_DIR` | 是 | `/data` | 会话、配置和管理员信息目录 |
| `TDL_DOWNLOAD_DIR` | 是 | `/downloads` | 最终下载文件目录 |
| `TDL_LISTEN_ADDR` | 否 | `:8080` | HTTP 监听地址 |
| `TDL_POSTGRES_DB` | Compose 是 | `tdl` | PostgreSQL 数据库名 |
| `TDL_POSTGRES_USER` | Compose 是 | `tdl` | PostgreSQL 用户名 |
| `TDL_POSTGRES_PASSWORD` | Compose 是 | 无 | PostgreSQL 密码 |
| `TDL_DATABASE_URL` | 是 | 无 | PostgreSQL 连接串，必须与数据库配置一致 |
| `TDL_COOKIE_SECURE` | HTTPS 是 | `false` | HTTPS 部署设为 `true` |
| `TDL_TRUSTED_ORIGINS` | HTTPS 是 | 空 | 写操作允许的页面来源；多个值以逗号分隔 |
| `TDL_TRUST_PROXY` | 视情况 | `false` | 仅信任自己的反向代理时设为 `true`，用于登录限流真实 IP |

## 健康检查与故障排查

```bash
curl -fsS http://127.0.0.1:8080/api/health
docker compose logs --tail=200 app
docker compose logs --tail=200 postgres
```

- 返回 `{"status":"ok"}` 代表应用与 PostgreSQL 已连接；
- 启动失败时，先检查 `.env` 的管理员密码、PostgreSQL 密码和 `TDL_DATABASE_URL` 是否完整且一致；
- Telegram 登录失败时，先确认网络连通性，再在“配置管理”设置代理；
- 出现“请求来源校验失败”时，检查 `TDL_TRUSTED_ORIGINS` 是否与浏览器地址完全一致（含 `https://`，不含结尾 `/`）；
- 迁移或升级失败时不要删除数据库。保留日志和 `postgres/`，回退代码或镜像后再处理。

## 开发

安装 VS Code Dev Containers 扩展后，在仓库目录执行 **Dev Containers: Reopen in Container**。开发容器包含 Go、Node 和宿主机 Docker 访问能力。

先启动 PostgreSQL 与后端：

```bash
cp .env.example .env
# 编辑 .env 后：
docker compose up -d postgres
go run ./cmd/tdl
```

另开一个终端运行前端：

```bash
cd web
npm install
npm run dev
```

访问 `http://localhost:5173`。Vite 会把 `/api` 代理到 `http://localhost:8080`。

发布前常用检查：

```bash
go test ./...
cd web && npm run build
docker compose config --quiet
```

涉及 PostgreSQL 的集成测试默认跳过。要显式执行，设置 `TDL_TEST_POSTGRES_URL` 指向专用测试数据库，绝不要指向生产数据库。

## 许可证

本项目依赖 AGPL-3.0 的上游 tdl。请在分发、修改或对外提供网络服务前阅读并遵守 [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.html)。
