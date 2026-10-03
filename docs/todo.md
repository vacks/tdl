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
- ~~`messages.getChats`~~：**已补进白名单**（v1.11.13）。gotd 的 peers 解析基本群时会发它。

**闸门与用户名缓存的顺序（v1.11.13 订正）**：链序现在是 `usernameCache → gate → tally`。
原来 gate 在缓存外侧，一次命中缓存的解析照样要花一个令牌——账户处在 FLOOD_WAIT 冷却里时
（可达数小时），一个纯内存查表会被一直压到冷却结束。

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

## 三、解耦进度（已完成的部分）

上游 TDL 的依赖已经**完全移除**（v1.11.9）：

- `internal/adapter/upstream`：已删除（v1.11.5）。
- `internal/tgclient`（客户端构造 / 连接池 / 中间件）、`internal/kv`（存储契约）、
  `internal/tmedia`、`internal/tmsg`、`internal/download/transfer`（下载引擎）：
  都是本仓库自己的实现，见各包顶部的注释。
- `patches/`、`.upstream/`、`scripts/`、`go.work` 的 replace、`Dockerfile` 里的 3 行 COPY：
  **已全部删除**。构建闭包的第三方模块从 69 降到 30。
- `go.mod` 里不再有 `github.com/iyear/tdl`（含 `/core`）。
  **`github.com/iyear/connectproxy` 保留**：`golang.org/x/net/proxy` 只认 socks5，
  `http` / `https` 代理靠它注册，且它不是上游应用的一部分。

仍待处理：

- **字节级断点续传（未实现，历史上也从未有过）**：暂停/取消后重下从 0 开始
  （`os.Create` 会截断），所以临时目录里没有可续的进度 —— 这也是 v1.11.10
  删掉这些目录依据。`upload.getFile` 支持 offset，因此理论上能做，但需要处理
  分片乱序写入导致的"文件大小 ≠ 进度"、按 part 边界截断、以及恢复时
  `file_reference` 可能已过期。**用户 2026-10-03 提出过，尚未决定是否要做。**
- `internal/buildinfo.UpstreamVersion` 与其 UI 文案：**已删除**（v1.11.10）。
- 临时目录残留：**已修复**（v1.11.10）。任务无论成败都清理自己的目录，
  外加启动时与每日的兜底扫描。
- 监听路径每条新消息重读一次历史：**已修复**（v1.11.11）。事件现在携带消息本身。
- 引擎每批次重建 `peers.Manager`：**已修复**（v1.11.12）。而且原因和当初的猜测不同——
  `peers.Options` 不传 `Cache` 会拿到 `peers.NoopCache`，它的 `Find*` 一律报告未命中，
  所以那样建的 manager **每次查询都要问一次** Telegram，不是每个 manager 一次；
  光把 manager 挂到账号会话上共用解决不了任何问题。两处都要改：`Manager.Peers`
  现在每账号一个 manager **且**带 `&peers.InmemoryCache{}`。实测（开发实例，
  温暖会话，单文件链接）：重复的 `channels.getChannels` 消失，再下同一个频道
  连那 1 次也没有。

---

## 四、P4：两套状态机合一（v1.11.13–v1.11.18 完成）

**动因不是"代码重复不好看"，而是修复只落在一遍上。** v1.11.13 之前一遍通读就找出 7 处分叉，
其中 2 处是真机上会丢文件的高危（完成写入一边原子一边两条语句；父状态读失败一边按"仍在跑"
一边按"不可发布"）。所以 P4 的目标是让下一处修复**只有一个落点**。

六块各自独立提交 + 发布，全部变异验证：

| 块 | 合并了什么 | 判据 |
|---|---|---|
| 1（v1.11.14） | 认领规则：三份实现 → `claimMediaOn` 一份，`*database`/`*databaseTx` 走同一个 `querier` | 摄取一页 64 个媒体：**129 → 3 条语句** |
| 2（v1.11.15） | 状态写入：`setItem`/`setChatItem` → 一个写入器 + `itemTable` 描述符 | 两个描述符差异各一个变异变红 |
| 3（v1.11.16） | 发布：`publishItem`/`publishChatItem` → 一个 `publish`，锁与可运行判定作为参数 | 停掉的任务不发布（两类各一测） |
| 4（v1.11.16） | 控制的**语句与状态集**（不是函数本身，见下） | 暂停列表去掉 `waiting` → 两张表同时变红 |
| 5（v1.11.18） | inbox 的重试状态机（逐字节相同的 40 行 ×2）+ peer 构造 | 两张表的三种结局 |
| 6（v1.11.17） | 四处"消息 → 文件记录"的字段映射 | — |

**两块刻意没有按计划书做，理由记在这里：**

1. **控制函数（Pause/Resume/Retry/Cancel）没有合成一个泛型。** 变异成本算过：描述差异的参数表
   比函数体还长，而唯一**应该**不同的地方（`恢复` 只收回暂停拿走的，`重试` 收回所有未完成的）
   会被藏进参数里。合并的是语句文本与状态列表——那才是 M1 那个 bug 的产地。
2. **inbox 的准入/认领/完成 SQL 仍分两份**，因为两张表的列不同（`chat_message_inbox` 没有
   `job_id`，`reaction_inbox` 有）。合并它们要加的是开关而不是共享。

**顺带修掉的两个真缺陷**（都在合并过程中暴露）：
- 消息侧"暂停/取消"原来在事务**之外**释放认领，进程在那中间死掉就留下一批永远无人释放的
  认领，等待它的任务要等一分钟的兜底扫描。现在跟会话侧一样在事务内。
- 释放认领原来用调用方传进来的 owner，"行本身不带任务"的表没有 owner 可传 —— 那个删除会
  一条都不删却报告成功。现在用语句 `RETURNING` 读回来的真实 owner。

**不变量（全程未动）**：表结构；状态字符串；`reconcile` 的 WHERE；列表/分页游标；
认领的 256/块与行值谓词 `(?,?::integer)`；迁移版本仍是 21。

## 五、P6：大规模执行计划复核（v1.11.14 已完成，5M 档）

临时库 `tdl_scale`，真实 schema + 合成数据：`download_items` 500 万、
`downloaded_media` 500 万、`chat_download_items` 300 万、`download_jobs` 2500 个（其中 2000 个排队）。
`EXPLAIN (ANALYZE, BUFFERS)` 结果：

| 语句 | 计划 | 耗时 | Buffers |
|---|---|---|---|
| `claimMediaBatch` 认领（256 个身份，全部命中已有行） | `Values Scan` → `Insert`，冲突仲裁走主键 | **4.7 ms** | 1039 |
| 同上（256 个全部新插入） | 同上 | 15 ms | — |
| `claimMediaChunk` 败者读取（256 个身份） | `BitmapOr` over 256 次主键 probe | 3.6 ms | 949 |
| `messageTaskRows`（v1.11.14 新增，256 个身份） | `BitmapOr` over 主键 | 2.9 ms | 768 |
| `setItem` 完成 CTE（单行） | `CTE Scan` → `Insert ... ON CONFLICT DO UPDATE` | **2.0 ms** | 49 |
| `ItemsPage`（**分散**任务：每 500 条一条，横跨整个 id 空间） | `Index Scan using download_items_job_id_id` | **0.34 ms** | 107 |
| 同上，换个计划作对照（`enable_indexscan=off` → 排序） | `Sort` | **114 ms** | 9863 read |
| `applyItemFailures` 的重试写回 | `Index Scan using download_items_dialog_key_message_id_key` | 0.024 ms | 3 |
| `nextQueued`（2000 个排队任务） | `Nested Loop Anti Join` | 0.062 ms | 55 |
| 会话取批（`status='queued' ORDER BY message_id DESC LIMIT 64`） | `Index Scan Backward using chat_download_items_work` | 0.28 ms | 67 |
| `reconcileChatClaims` 等待项轮转 | `Index Only Scan using chat_download_items_waiting_rotation` | 0.47 ms | 261 |

**结论：全部走索引，无 Seq Scan，无全表排序。**

⚠️ **只测到 500 万，没测 5000 万**（本机磁盘余量约 20G，加载 5000 万不现实）。
判断依据是：这些计划里**没有任何节点的代价随表增大而线性增长** —— `ItemsPage` 是
`(job_id, id)` 上的 seek（107 buffers）、认领是 `VALUES` 驱动的 256 次主键 probe、
调度是 anti join。真正会随规模翻转的是"优化器选了全表扫描或全表排序"，
上面每一个都能直接排除。若生产上出现翻转变慢，第一个要看的是这几条的 `EXPLAIN`。

⚠️ 造数三个坑：`docker exec` 必须带 `-i` 否则 heredoc 不进 psql；`download_items.id` 是序列，
晚插入的行自然落在末尾，想造"分散"必须显式指定 id；灌数据前要
`ALTER TABLE ... DISABLE TRIGGER USER` 并去掉 `download_item_stats` 的外键。

## 六、第二轮 DB / RPC 审查（v1.11.32–v1.11.39）

一次性库 `tdl_scale`（真实 schema + 合成数据：`download_items` 500 万、`downloaded_media`
500 万、`chat_download_items` 300 万、两张 inbox 各 30 万），`EXPLAIN (ANALYZE, BUFFERS)`。
本轮**已修**的记在各自的提交里，下面是**查出来但有意不做**的三条。

### 1. inbox 的"该重试了吗"谓词不可索引（不做，留此备查）

```sql
WHERE status = 'pending' AND next_attempt_at::timestamptz <= ?::timestamptz ORDER BY next_attempt_at, id LIMIT ?
```

索引是**文本列** `(status, next_attempt_at, id)`（`reaction_inbox_ready` /
`chat_message_inbox_ready`），加了这个 cast 之后 `next_attempt_at` 就只是扫描后的
`Filter`，不是 Index Cond，所以**扫描无法在"第一个未到期"处停下**。实测：30 万个未到期的
pending 行（退避中的积压就是这形状），**没有任何一条到期**时探测查询是 21 ms / 11907 buffers，
取批查询 11.7 ms / 11964 buffers，而且**每次轮询都重来一遍**——reaction 侧 1 秒一次、
会话侧 3 秒一次。代价随**开启态积压**线性增长，与 5000 万行目标冲突。

**cast 是对的、不能删**：`time.RFC3339Nano` 会裁掉小数秒末尾的 0，于是 `T00:00:00Z` 与
`T00:00:00.5Z` 的**文本序与时间序相反**，直接比文本会把同一秒内到期的行判成没到期。

想两头都要只能动存储，两条路都带迁移 + 回填：

- 列改 `timestamptz`（干净，但 `updated_at` 等列同理，要一起评估）；
- 写入时用**定宽**小数秒（如永远 9 位）并回填旧行，之后纯文本比较即正确，
  索引恢复成能用范围条件、扫描能在边界停下。

另有一个不改存储的取巧办法（未验证、未采用）：在谓词里加一条**可索引的文本上界**
（`next_attempt_at < 截断到秒的 now+1s`）作为超集，再把 cast 条件留作精确过滤。
它依赖"同一秒内文本序反转但都小于下一秒"这个性质，够用但脆，读代码的人很难看出为什么成立。

相关语句：`chat_inbox.go`（探测 + 取批）、`reaction_inbox.go`（同）、
`manager.go` 的 `reapStaleInboxLeases`、`chat_inbox.go` 的 `reviveExhaustedInboxEvents`
（后两处的注释已写明"表会保持很小，所以丢掉 updated_at 上的索引支持不要紧"——
**上面这组数字就是那个假设失效的位置**）。

### 2. 相册链接解析要读两次历史（不做）

`manager.go` 的 `resolve`：先 `tmsg.GetSingleMessage`（1 条）判断是不是相册，
是再用 `tmsg.GetGroupedMessages` 读一个 20 条的窗口。而那个窗口
（`OffsetID(id+11)`、batch 20）**本来就包含目标消息**，所以第二次读已经覆盖第一次的答案。

省下的只有"每个相册链接 1 次请求"，代价却是重做"消息已删除"的判定：窗口里没有目标消息
**不等于**它被删了（更新的消息可能占满窗口）。这个项目在"空页 vs 请求失败"上已经付过学费
（见 `tmsg.GetSingleMessage` 的注释），1 次请求不值得再冒一次。`?single` 链接已经正确地只花 1 次。

### 3. 会话历史的多趟扫描（维持不做，但算式要写对）

默认配置（`image`+`video`、`IncludeReplies=true`）确实开 **3 趟**：
`photo_video`、`round_voice`（`messages.search`，每页 100 条**匹配**）+ `reply_candidates`
（**全量** `messages.getHistory`，每页 100 条**消息**）。

看起来"合并成一趟全量"能省掉两趟 search 的页数，但**顺序是故意的**：媒体趟在前，
所以第一页回来就能开始下载；全量趟放最后，超大文本历史不会挡住首批下载。
合并会把媒体发现的速度从"每页 100 个媒体"降到"每页 100 条消息"。
省的是请求数，付的是首字节时间，而首字节时间才是用户看得见的东西。
