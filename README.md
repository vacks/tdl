# tdl Web

基于 [iyear/tdl](https://github.com/iyear/tdl) `v0.20.4` 构建的 Telegram Web 管理服务。

## 已实现功能

- Vue 3 + TypeScript + Element Plus 管理界面；
- WebUI 管理员账号密码登录，使用 Argon2id 密码哈希和服务端会话 Cookie；
- 多个 Telegram 账号的二维码登录、两步验证、当前账号切换与删除；
- HTTP、HTTPS、SOCKS5、SOCKS5H 代理配置，支持 `user:password@host:port` 形式；
- tdl 下载默认参数：线程数、任务限制、连接池、下载间隔与文件命名模板；
- 下载管理：粘贴 Telegram 消息链接后调用上游 tdl 下载，支持相册/媒体组；任务、媒体记录与创建时绑定的 Telegram 账号持久保存在 SQLite；
- SQLite 下载记录（`data/tdl.db`）：以 Telegram 的 `DialogID + MessageID` 唯一约束登记每个媒体消息，重复提交同一消息或相册成员会被拒绝；
- 两层文件命名：临时文件模板仅使用上游 tdl 支持的变量和函数；最终文件模板由本项目处理，支持 `DialogID`、`DialogName`、`MessageID`、`MessageText`、`FileName` 和 `FileExt`（含点），且变量值会自动按 Linux 规则清理。下载先写入临时目录，再按最终模板落到下载目录；`MessageText` 按 Telegram 客户端视觉相册规则计算：媒体组中只有一条非空正文时使用该正文；零条或多条正文时为空；
- 下载队列：单个上游下载任务顺序执行，支持暂停、恢复、取消和失败后的手动重试；重启时运行中的任务会重新入队，并复用上游 tdl 的恢复记录和临时文件。完成项目不会再次下载；暂停/取消后不会把临时文件落盘到最终目录；
- 文件实时进度与速率：通过 SSE 推送内存中的分块字节进度，不会高频写入 SQLite。该能力由 `patches/tdl-progress-0.20.4.patch` 为上游 `tdl v0.20.4` 增加的可选回调提供；构建脚本会校验版本和受影响源码文件的 SHA-256，不匹配即中止构建；
- 安全最终落盘：自动创建临时与最终目录，拒绝绝对路径、`..` 路径段和跳出下载目录的符号链接；最终文件使用不覆盖的原子发布方式；
- Telegram 会话健康检查：每 30 分钟以一个轻量 `Self` 请求检查已授权账号，避免与下载并行使用同一会话；失效账号会在界面标记为需要重新登录；
- 仅本机绑定的 Docker Compose 配置，生产环境可由 NAS 的现有反向代理提供 HTTPS。

## 当前限制

- 界面显示的是可靠的“已完成文件数/总文件数”，不是字节级实时速度或百分比；上游 `tdl v0.20.4` 的嵌入式 API 没有暴露安全可订阅的字节进度回调。文件级续传由上游恢复记录处理；
- 重试为管理员手动触发，不会对 Telegram 错误进行盲目自动重试；
- Bot 控制与表情实时监听；
- 下载记录的手动清理与导出。

## 开发

复制 `.env.example` 为 `.env`，设置一个强管理员密码，然后在 VS Code 中执行 **Dev Containers: Reopen in Container**。

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

## 许可证

本项目依赖 AGPL-3.0 的上游 tdl；发布和对外提供网络服务时，须遵守 AGPL-3.0 的源代码提供义务。
