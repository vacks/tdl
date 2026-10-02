# 已知未做清单

这份清单是**有意不做的决定**，不是待办堆积。每一条都记下为什么现在不做、以及将来要动的时候
需要知道什么。改动相关代码时请回来对照。

---

## 一、上游 TDL 解耦过程中挂起

### 1. Bot 迁移到 MTProto-as-bot（挂起，非拒绝）

现状：`internal/bot` 走 Bot API 的 HTTP（`api.telegram.org`），只用了 7 个方法
（`getUpdates` / `sendMessage` / `editMessageText` / `answerCallbackQuery` / `getMe` /
`setMyCommands` / `setChatMenuButton`），出站已被 `awaitOutbound` 限到 8 req/s，
并且处理了 429 的 `retry_after`。

**为什么现在不迁**：Bot API 是官方接口，限额有文档、长轮询是它设计的用法，而我们的发送量极低。
风控压力真正大的地方是**用户账号的 MTProto 侧**（每条新消息重读一次历史、按 id 取消息逐条发、
下载池不在限流闸门里），那里才是本轮的整改重点。

**将来要迁时的迁移要点**（gotd 支持 `Auth().Bot(token)` → `auth.importBotAuthorization`）：

| 现在 | 换成 |
|---|---|
| `getUpdates` 长轮询 | MTProto update 流（要自己管 pts/qts 与断点） |
| `sendMessage` + `parse_mode: HTML` | `messages.sendMessage` + 自建 `MessageEntity`（或改发纯文本） |
| `editMessageText` + `reply_markup` | `messages.editMessage` + `ReplyInlineMarkup` |
| `answerCallbackQuery`（含 toast / loading 态） | `messages.setBotCallbackAnswer`（语义不同，toast 要另想办法） |
| `getMe` | `users.getFullUser` 或 client `Self` |
| `setMyCommands` / `setChatMenuButton` | `bots.setBotCommands` 等 |
| 回调查询 `callback_query` | `updateBotCallbackQuery` |

**顺带的好处**：Bot 侧能拿到 access hash，`forward_origin` 那条 `t.me` 链接绕行
（v1.11.3 立的规矩）以及"Bot 会话里 Bot API 与 MTProto 两套 message id 不能互用"的坑都会消失。

### 2. 频道历史的两趟扫描

`scanChatStream`（`chat.go`，`messages.search` 带媒体过滤器，**页少**）与
`scanChatReplyCandidates`（`chat.go`，`messages.getHistory` 走**全量**，只挑带 `replies` 的帖子
去拉评论区）在同一个区间上各走一趟。

**看起来能合并，实际可能是净亏**：`search` 每页返回 100 条**匹配**的消息，可能跨越很长的区间；
`getHistory` 每页只覆盖 100 条消息。合并成 `getHistory` 一趟，会把媒体流从"少页"变成"全量页"。
两趟还各有独立的 `chat_download_streams` 游标，合并会改变可恢复性语义。

**真正的省法是换个角度**：不要扫频道找"有评论的帖子"，而是扫**关联讨论组**里的评论
（`reconcileDiscussionGap` 已经是这个思路）。需要单独评估。

### 3. 限流白名单的边界

`internal/telegram/accounts.go` 的 `pacedRequest` 是**白名单**（黑名单会死锁：client 自身启动
要走同一个 invoker 发 `initConnection`）。

当前**不在**白名单里、且需要评估的：

- `updates.getState` / `updates.getDifference`：update 流自身的请求。它们由 gotd 的连接管理发起，
  跟着长轮询的节奏走，人为加令牌可能拖慢 update 投递并造成积压。
- `auth.*`（sendCode / signIn / password）：只发生在登录时。
- 连接内部请求（`help.getConfig`、MTProto ping、DC 迁移）。
- `messages.getChats`：**已确认应该补进白名单**——gotd 的 peers 解析基本群时会发它，
  现在没过闸门。见 `internal/telegram/accounts.go`。

### 4. `upstreamVersion` 这个字段

`/api/dashboard`、`/api/config` 与 Bot 的 `/help`、`/status` 仍在显示"上游 TDL v0.20.4"。

它是**来源说明**，没有任何代码读它做判断（`internal/buildinfo` 的 `UpstreamVersion`）。
等到下载引擎完全自建（P3）之后，就没有"上游版本"可指了，届时把这个字段连同 UI 文案一起删掉。

---

## 二、更早就明确不做的

- **T16**：Bot `getUpdates` 超时 25s→50s、卡片按进度刷新。用户明确不做。
- **#14 链接域名校验**：用户明确不做。
- **不回修历史脏数据**：v1.11.2 的 `incompleteFileError` 只防新增。
  历史上 `status='completed' AND started_at=''` 的行（一个字节没写却报完成）不回改。
- **`reconcileChatClaims` 仍是逐行查询**（有界，任务里的 item 数）。
- **`RunCurrent` 没有任何调用点**（死代码，暂时保留）。
- **不给 `core` 打补丁**：这条随解耦方案作废——`core` 会被整个吸收进仓库。
- **旧仪表盘删掉两张卡片后，`.overview-grid` / `.overview-card` / `accent-*` 的 CSS 仍留在
  `web/src/style.css` 里**（有意保留）。

---

## 三、解耦过程中的临时状态（会随阶段推进消失）

- `internal/adapter/upstream`：**已删除**（v1.11.5）。它自称是"唯一可以 import 上游的层"，
  实际每个上游 import 都在别处，包本身只为提供 `upstream.Version` 一个字符串，
  却把 `app/login`（连同 bbolt、校验器三件套）拖进二进制。
- `internal/buildinfo.UpstreamVersion`：临时保留，见上文第 4 条。
- `patches/`、`.upstream/`、`scripts/prepare-upstream-progress.sh`、`go.work` 的 replace、
  `Dockerfile` 里的 3 行 COPY：**仍在**，随下载引擎自建（P3）一并删除。
