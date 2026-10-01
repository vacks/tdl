# TDL 管理

[![Release](https://img.shields.io/github/v/release/vacks/tdl)](https://github.com/vacks/tdl/releases)
[![Image](https://img.shields.io/badge/image-ghcr.io%2Fvacks%2Ftdl-blue?logo=docker&logoColor=white)](https://github.com/vacks/tdl/pkgs/container/tdl)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Vue](https://img.shields.io/badge/Vue-3-4FC08D?logo=vuedotjs&logoColor=white)](https://vuejs.org)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](https://www.gnu.org/licenses/agpl-3.0.html)

基于 [iyear/tdl](https://github.com/iyear/tdl) `v0.20.4` 的 Telegram 下载管理服务。

它把 TDL 封装成一个常驻服务：网页管理台负责账号、任务和配置，Bot 负责在手机上随手丢链接，表情监听负责"点个赞就开始下载"。所有下载历史、去重记录和任务状态落在 PostgreSQL 里，按能承载千万级记录的量级设计。

---

## 亮点功能

### 🖥️ 网页管理台

Vue 3 + Element Plus 构建，服务端通过 SSE 推送合并后的进度事件，页面无需轮询。

- **仪表盘**：CPU、内存、网络吞吐与下载状态的实时概览
- **账户**：扫码登录、多账号切换、会话有效性自动检查
- **下载**：任务列表分页、逐条进度、暂停 / 恢复 / 取消 / 重试
- **设置**：代理、并发、筛选、命名模板、Bot 与表情监听

### 📥 四种发起下载的方式

| 方式 | 说明 |
| --- | --- |
| **消息链接** | 粘贴单条消息链接即可下载；相册自动展开 |
| **会话下载** | 下载频道 / 群组的全部历史，并持续监听后续新消息 |
| **收藏夹** | `/saved all` 下载本人收藏夹历史，`/saved listen` 持续监听新消息 |
| **表情触发** | 给消息加一个指定的表情，自动开始下载 |

### 💬 评论与回复

开启后，频道帖子的评论区回复和群组内的关联回复会随原帖一起下载，并通过命名模板里的 `.IsComment`、`.OriginMessageID` 与原帖关联。断线期间遗漏的消息会在重连后自动补齐。

### 🔒 真正的去重

以 PostgreSQL 中的 `(dialog_key, message_id)` 所有权记录为准，一个媒体身份在同一时刻只属于一个任务。同一链接、同一会话、表情触发和 Bot 并发提交，都不会产生重复文件；下载历史同时是"哪些文件已经下过"的唯一依据。

### 🎛️ 精细控制

- **类型白名单**：图片、视频、GIF、音乐、语音、贴纸、文档，可多选（空选即全部不下载）
- **体积区间**：`minFileSizeMB` / `maxFileSizeMB`，`0` 表示不限制
- **并发模型**：上传线程数、单任务文件并发、任务并发、连接池、请求间隔分别可调
- **命名模板**：Go template 语法，支持条件块与十余个变量
- **代理**：支持 `http(s)://` 与 `socks5(h)://`

### 🤖 Bot 控制

私聊机器人即可操作，任务卡片跟随进度刷新：

```
/help                            查看帮助
/tasks [状态]                    任务列表，可按状态筛选
/chats [链接]                    查看或创建会话下载
/saved all|listen|stop|status    收藏夹下载与监听
/status                          当前状态
/event                           监听的待处理事件
/config                          当前配置
/restart                         重启所有服务
```

直接发送消息链接同样可以创建下载任务。Bot 只在私聊中响应，且仅限已授权的用户 ID；任务完成、部分完成或失败时可按需推送通知。

### 📈 面向大数据量

数据库按至少 5000 万条记录的规模设计：队列调度使用贴合查询的部分索引，列表分页走游标 seek 而非 `OFFSET`，媒体认领采用批量 set-based 写入。每个 Telegram 账号长期复用一条连接，空闲状态下服务几乎不产生数据库查询。

---

## 快速开始

需要 Docker 与 Docker Compose，推荐至少 2 GB 可用内存。

```bash
git clone https://github.com/vacks/tdl.git
cd tdl

# 改掉默认管理员密码后再启动：
# 编辑 compose.yaml 中的 TDL_ADMIN_INITIAL_PASSWORD（以及 POSTGRES_PASSWORD）
docker compose up -d
```

启动完成后访问 <http://127.0.0.1:8080>，使用 `admin` / 你设置的密码登录。

> **务必修改默认密码。** `compose.yaml` 里的示例密码是公开的，直接部署等于对外提供一个已知的管理员登录。`TDL_ADMIN_INITIAL_PASSWORD` 只在管理员账号首次创建时生效，之后请在网页里修改密码。

服务默认只监听 `127.0.0.1:8080`，不会直接暴露到公网；需要外部访问时请走反向代理。

### 查看状态

```bash
docker compose ps
docker compose logs -f app
curl -s http://127.0.0.1:8080/api/health
```

### 更新

```bash
git pull
docker compose pull
docker compose up -d
```

`data/`、`postgres/`、`downloads/` 都是宿主机目录挂载，更新不会影响已有数据；数据库结构变更由服务启动时自动迁移。

### 停止

```bash
docker compose down          # 停止服务，保留全部数据
```

---

## 基本使用

**1. 登录 Telegram 账号**

进入「账户」页添加账号，用手机 Telegram 的「设置 → 设备 → 扫描二维码」扫码。启用了两步验证的账号会继续提示输入密码。可以添加多个账号并随时切换，服务会定期检查会话是否仍然有效。

**2. 创建下载任务**

在「下载」页粘贴消息链接；或者在同一页输入频道 / 群组链接做整会话下载，勾选"监听新消息"后新帖子会被持续下载。

**3. 调整下载设置**

进入「设置」页配置代理、并发、文件类型与体积筛选、命名模板。开启 Bot 后填入 BotFather 申请的 Token 和授权用户 ID；开启表情监听后选择触发用的表情。

**4. 用 Bot 操作**

把机器人拉进私聊，发送 `/help` 查看全部命令，发送链接直接建任务。

---

## 配置

### 环境变量

部署参数写在 [compose.yaml](compose.yaml) 的 `environment` 中，修改后 `docker compose up -d` 生效。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TDL_DATABASE_URL` | 无（**必填**） | PostgreSQL 连接串 |
| `TDL_LISTEN_ADDR` | `:8080` | HTTP 监听地址 |
| `TDL_DATA_DIR` | `./data` | 管理账号、Telegram 会话与设置 |
| `TDL_DOWNLOAD_DIR` | `./downloads` | 下载文件目录 |
| `TDL_ADMIN_USERNAME` | `admin` | 管理台用户名 |
| `TDL_ADMIN_INITIAL_PASSWORD` | `admin` | 首次创建管理员账号时使用的密码，**请务必修改** |
| `TDL_TRUSTED_PROXIES` | 空 | **本容器看到的**反向代理地址（IP 或 CIDR，逗号分隔），见[反向代理与 HTTPS](#反向代理与-https) |

### 命名模板

默认模板：

```
{{ .OriginDialogName }}/{{ .OriginMessageID }}_{{ if .IsComment }}c_{{ end }}{{ .MessageID }}{{ if .MessageText }}_{{ .MessageText }}{{ end }}{{ .FileExt }}
```

可用变量：`.DialogID`、`.DialogName`、`.MessageID`、`.GroupedID`、`.OriginDialogName`、`.OriginMessageID`、`.IsComment`、`.MessageText`、`.FileName`、`.FileExt`、`.DownloadDate`。

`{{ if .MessageText }}…{{ end }}` 是一个完整的条件区块：正文为空时整段不输出，也可以写成 `{{ if … }}…{{ else }}…{{ end }}`。

---

## 反向代理与 HTTPS

公网部署时由你自己的 HTTPS 反向代理转发至 tdl。反向代理有两条硬性要求：

- **保留原始 `Host`。** 改写 `Host` 会让服务端算出的来源与浏览器发来的 `Origin` 对不上。
- **传递 `X-Forwarded-Proto: $scheme`。** Caddy、Traefik 默认会加，**Nginx 不会**，必须显式配置。

```nginx
location / {
    proxy_pass http://tdl:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

`X-Forwarded-Proto` 只在请求来自受信任地址时才被采信，所以还要把**本容器看到的反代地址**填进 `TDL_TRUSTED_PROXIES`：

| 反向代理的位置 | 应填写的值 |
| --- | --- |
| 与 tdl 在同一个容器网络 | 反代容器在该网络中的地址，或该网络的网段 |
| 宿主机上，经 `127.0.0.1:8080` 发布端口访问 | compose 网络的网关地址 |

**填 `127.0.0.1` 是无效的**：经发布端口或容器网络到达的连接，源地址不可能是回环地址。实际值可以直接查：

```bash
docker network inspect tdl_internal --format '{{range .IPAM.Config}}{{.Subnet}}{{end}}'
```

不设置（或填错）时该头被忽略——这是刻意的安全默认值，因为任何客户端都能伪造它，而它决定会话 Cookie 是否标记为 `Secure`，以及同源校验用哪个协议去比对。

**忽略它的直接后果是：所有写操作被拒绝，包括登录**，返回 `请求来源无效`。同时服务端会打一条 `request_origin_rejected` 的 WARN 日志，带着 `origin`、`host`、`scheme`、`forwarded_proto`、`peer` 五个值——照着比对就能判断是缺了这个头，还是 `Host` 被改写了：

```bash
docker compose logs app | grep request_origin_rejected
```

直接暴露 8080、不经过反向代理时不需要设置。

---

## 数据与备份

| 目录 | 内容 | 是否必须备份 |
| --- | --- | --- |
| `postgres/` | 下载历史、去重记录、任务与事件 | **必须** |
| `data/` | 管理账号、Telegram 会话、设置 | **必须** |
| `downloads/` | 已下载的文件 | 按需 |

`postgres/` 是永久下载历史，也是去重的唯一依据：文件记录不会被保留策略清理，删库会同时失去"哪些文件已经下过"的判断，重新下载时所有文件都会被当作新文件。请定期备份，不要只备份 `downloads/`：

```bash
docker compose exec postgres pg_dump -U tdl -d tdl | gzip > tdl-$(date +%F).sql.gz
```

数据量大时建议改用 `pg_dump -Fc`（自定义格式，支持可选表恢复与并行恢复），并配合宿主机层面的快照。

### 使用外置 PostgreSQL

把 `TDL_DATABASE_URL` 指向你的数据库，然后只启动应用：

```bash
docker compose up -d --no-deps app
```

---

## 本地开发

使用当前源码构建镜像（不会从 ghcr.io 拉取）：

```bash
docker compose -f compose.yaml -f compose.dev.yaml up -d --build
```

也可以在 devcontainer（Go 1.25 + Node 22）里开发：后端位于 `internal/`，前端位于 `web/`。涉及数据库的测试需要真实 PostgreSQL，通过 `TDL_TEST_POSTGRES_URL` 开启，未设置时自动跳过。

---

## 许可证

本项目依赖 AGPL-3.0 的上游 [iyear/tdl](https://github.com/iyear/tdl)；分发或对外提供服务前请遵守 [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.html)。
