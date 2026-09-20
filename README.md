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

公网部署时由你自己的 HTTPS 反向代理转发至 `127.0.0.1:8080` 即可，无需额外设置环境变量。反向代理应保留原始 `Host`，并传递 `X-Forwarded-Proto`；Caddy、Nginx 和 Traefik 的标准反代配置都会自动处理。

## 许可证

本项目依赖 AGPL-3.0 的上游 tdl；分发或对外提供服务前请遵守 [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.html)。
