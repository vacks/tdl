# tdl Web

基于 [iyear/tdl](https://github.com/iyear/tdl) `v0.20.4` 构建的 Telegram Web 管理服务。

## 已实现功能

- Vue 3 + TypeScript + Element Plus 管理界面；
- WebUI 管理员账号密码登录，使用 Argon2id 密码哈希和服务端会话 Cookie；
- 多个 Telegram 账号的二维码登录、两步验证、当前账号切换与删除；
- HTTP、HTTPS、SOCKS5、SOCKS5H 代理配置，支持 `user:password@host:port` 形式；
- tdl 下载默认参数：线程数、任务限制、连接池、下载间隔与文件命名模板；
- 下载管理：粘贴 Telegram 消息链接后调用上游 tdl 下载，支持相册/媒体组；任务、媒体记录与创建时绑定的 Telegram 账号持久保存在 PostgreSQL；
- PostgreSQL 永久下载记录：以 Telegram 的 `DialogID + MessageID` 唯一约束登记每个媒体消息，重复提交同一消息或相册成员会被拒绝。下载历史与去重记录不会因自动清理而删除；
- 两层文件命名：临时文件模板仅使用上游 tdl 支持的变量和函数；最终文件模板由本项目处理，支持 `DialogID`、`DialogName`、`MessageID`、`MessageText`、`FileName` 和 `FileExt`（含点），且变量值会自动按 Linux 规则清理。下载先写入临时目录，再按最终模板落到下载目录；`MessageText` 按 Telegram 客户端视觉相册规则计算：媒体组中只有一条非空正文时使用该正文；零条或多条正文时为空；
- 下载队列：可配置多个任务并行执行，支持暂停、恢复、取消和失败后的手动重试；重启时运行中的任务会重新入队，并复用上游 tdl 的恢复记录和临时文件。完成项目不会再次下载；
- 文件实时进度与速率：通过 SSE 推送内存中的分块字节进度，不会高频写入 PostgreSQL。该能力由 `patches/tdl-progress-0.20.4.patch` 为上游 `tdl v0.20.4` 增加的可选回调提供；构建脚本会校验版本和受影响源码文件的 SHA-256，不匹配即中止构建；
- 安全最终落盘：自动创建临时与最终目录，拒绝绝对路径、`..` 路径段和跳出下载目录的符号链接；最终文件使用不覆盖的原子发布方式；
- Telegram 会话健康检查：每 30 分钟以一个轻量 `Self` 请求检查已授权账号，避免与下载并行使用同一会话；失效账号会在界面标记为需要重新登录；
- Bot 控制：多控制用户、任务列表与详情、状态/配置查询、链接创建任务、生命周期通知和服务重启；
- 表情实时监听：仅处理当前已登录账号自己作出的匹配 Unicode 表情，支持私聊、群组、频道与收藏消息；
- 辅助历史清理：可配置保留天数并分批清理状态事件、请求和表情触发收件箱；任务历史与去重记录永久保留；
- 仅本机绑定的 Docker Compose 配置，生产环境可由 NAS 的现有反向代理提供 HTTPS。

## 当前限制

- 重试为管理员手动触发，不会对 Telegram 错误进行盲目自动重试；
- 暂不提供下载记录导出；
- 自定义 Premium 表情不能作为文本配置的触发表情。

## 开发

复制 `.env.example` 为 `.env`，设置强管理员密码与 PostgreSQL 密码，然后在 VS Code 中执行 **Dev Containers: Reopen in Container**。

容器中分别运行：

```bash
go run ./cmd/tdl
cd web && npm run dev
```

## 本地 Compose

```bash
cp .env.example .env
docker compose up --build
```

默认通过 `http://localhost:8080` 访问。生产部署时可由 NAS 现有的反向代理提供域名和 HTTPS。

PostgreSQL 仅加入 Compose 内部网络，不映射宿主机端口。请在 `.env` 中保持
`TDL_POSTGRES_DB`、`TDL_POSTGRES_USER`、`TDL_POSTGRES_PASSWORD` 与
`TDL_DATABASE_URL` 一致；数据库文件存储于项目目录的 `postgres/`。

生产环境请在 `.env` 增加 `TDL_COOKIE_SECURE=true`，并设置
`TDL_TRUSTED_ORIGINS=https://你的域名`。这会启用 HTTPS 专用会话 Cookie，
并只接受该站点发出的管理操作请求。若应用只经由受信任的反向代理访问，可再设
`TDL_TRUST_PROXY=true`，让登录限流按转发的真实客户端 IP 生效；不要在应用端口
直接暴露到公网时启用它。

## 许可证

本项目依赖 AGPL-3.0 的上游 tdl；发布和对外提供网络服务时，须遵守 AGPL-3.0 的源代码提供义务。
