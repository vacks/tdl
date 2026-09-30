# TDL 管理

基于 [iyear/tdl](https://github.com/iyear/tdl) `v0.20.4` 的 Telegram 下载管理服务，提供 Web 管理、Bot 控制和表情触发下载。

## 功能

- Telegram 多账户登录、切换和会话检查
- 消息、相册、频道/群组历史及新消息下载
- 评论/回复下载、暂停、恢复、取消、重试和去重
- 文件体积/类型筛选、代理、下载并发与命名模板
- Bot 控制、表情触发、实时进度和 PostgreSQL 下载历史

## Docker Compose 部署

需要 Docker Compose。完整配置见 [compose.yaml](compose.yaml)。

```bash
git clone https://github.com/vacks/tdl.git
cd tdl
docker compose up -d
```

默认管理台账号和首次密码都是 `admin`。部署参数直接写在 [compose.yaml](compose.yaml) 的 `environment` 中；需要调整时编辑该文件后重新启动服务。

访问：<http://127.0.0.1:8080>

需要改下载目录时，编辑 [compose.yaml](compose.yaml) 中的 `./downloads:/downloads`；NAS 可替换为绝对路径。

本机开发使用当前源码构建：

```bash
docker compose -f compose.yaml -f compose.dev.yaml up -d --build
```

## 常用操作

```bash
# 查看状态和日志
docker compose ps
docker compose logs -f app

# 更新项目
git pull
docker compose pull
docker compose up -d

# 停止服务（保留数据）
docker compose down
```

持久化数据：`data/`（管理账号、Telegram 会话和设置）、`postgres/`（任务历史和去重记录）、`downloads/`（下载文件）。请按需备份，删除目录会丢失对应数据。

使用外置 PostgreSQL 时，将应用的 `TDL_DATABASE_URL` 改为外置数据库 URL，然后启动应用：`docker compose up -d --no-deps app`。

## HTTPS 反向代理

公网部署时由你自己的 HTTPS 反向代理转发至 `127.0.0.1:8080`。反向代理应保留原始 `Host`，并传递 `X-Forwarded-Proto`；Caddy、Nginx 和 Traefik 的标准反代配置都会自动处理。

`X-Forwarded-Proto` 只在请求来自受信任地址时才被采信，因此使用反向代理时需要在 `compose.yaml` 中设置 `TDL_TRUSTED_PROXIES`，列出代理的地址（单个 IP 或 CIDR，逗号分隔），例如 `TDL_TRUSTED_PROXIES: 127.0.0.1` 或 `172.16.0.0/12`。不设置时该头被忽略，这是安全默认值——任何客户端都能伪造这个头，而它决定会话 Cookie 是否标记为 Secure。直接暴露 8080 时不需要设置。

## 备份

`postgres/` 是永久下载历史，也是去重的唯一依据：文件记录不会被保留策略清理，删库会同时失去"哪些文件已经下过"的判断，重新下载时全部文件都会被当作新文件。请定期备份，不要只备份 `downloads/`：

```bash
docker compose exec postgres pg_dump -U tdl -d tdl | gzip > tdl-$(date +%F).sql.gz
```

数据量大时建议改用 `pg_dump -Fc`（自定义格式，可选表恢复、并行恢复），并配合宿主机层面的快照。`data/` 存放管理账号和 Telegram 会话，同样需要备份；`downloads/` 是已下载文件本身，可按需取舍。

## 许可证

本项目依赖 AGPL-3.0 的上游 tdl；分发或对外提供服务前请遵守 [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.html)。
