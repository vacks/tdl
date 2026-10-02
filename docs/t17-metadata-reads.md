# T17 深度分析：下载路径的元数据读取

分析基准：`v1.11.4`（`503a27f`）。
涉及：`internal/download`、`internal/telegram`、vendored 上游 `app/dl`（`patches/`）。

---

## 0. 摘要

T17 不是一个"有点慢"的小瑕疵，而是同一个设计缺口的三张面孔：

> **下载任务里"取消息"这一步，是上游 CLI 迭代器为"一条命令下载几个链接"写的，
> 不是为"一个会话几万个文件"写的。**

三个缺陷，按严重程度排序：

| # | 缺陷 | 位置 | 后果 |
|---|---|---|---|
| **A** | 每条消息一次 `messages.getHistory`，**串行、在关键路径上** | `app/dl/iter.go:177` | 文件吞吐被 RPC 往返锁死；大批量任务元数据阶段 = N × RTT |
| **B** | 这些请求走的是 **dcpool 自己的 invoker 链**，不在账户限流闸门里 | `app/dl/dl.go:111` + `accounts.go:339-351` | (1) 不限流；(2) FLOOD_WAIT 被 floodwait 中间件吞掉 → 账户级冷却**永不写入**、日志里**什么都没有** |
| **C** | 相册被展开两次 | 我方 5 处 `GetGroupedMessages` + `iter.go:285` | 每个相册多一次 20 条历史的读取；并派生出 `?single` 的隐性浪费（见 §4） |

三者互相耦合：**单独加限流（B）会让吞吐原地踏步**（4/s 闸门 ≈ 现在的 3.3/s），
单独批量取消息（A）则把 pool 上的请求数降到"每 100 条一次"，让限流变得廉价。
所以**推荐 A + B + C 一起做**。

---

## 1. 缺陷 A：每条消息一次 `getHistory`，而且不预读

### 1.1 代码证据链

```
internal/download/manager.go:1874   upstreamDL.Run(...)          ← 链接任务
internal/download/chat.go:1153       upstreamDL.Run(...)          ← 会话任务
        ↓
.upstream/tdl/app/dl/dl.go:111       dcpool.NewPool(c, poolSize, tclient.NewDefaultMiddlewares(...)...)
.upstream/tdl/app/dl/dl.go:134       newIter(pool, manager, dialogs, opts, delay)
        ↓
.upstream/tdl/app/dl/iter.go:177     tutil.GetSingleMessage(ctx, i.pool.Default(ctx), peer, msg)
        ↓
core@v0.20.4/util/tutil/tutil.go:177 query.Messages(c).GetHistory(peer).OffsetID(msg+1).BatchSize(1).Iter()
```

`GetSingleMessage` 就是 **一次 `messages.getHistory`、只取 1 条**。所以：

> **一个媒体文件 = 一次 MTProto 往返。**

### 1.2 关键：迭代器**不预读**，RPC 是严格串行的

```go
// app/dl/iter.go:136
if len(i.elem) > 0 { // there are messages(grouped) in channel that not processed
    return true
}
for {
    ok, skip := i.process(ctx)   // process 里恰好 push 一个元素（iter.go:268）
    ...
}
```

消费端：

```go
// core@v0.20.4/downloader/downloader.go
for d.opts.Iter.Next(wgctx) {
    elem := d.opts.Iter.Value()   // <-i.elem，立刻取走
    wg.Go(func() { ... })
}
```

`Next()` 返回 true 后 `Value()` 立刻把那个元素取走 → channel 又空了 → 下一次 `Next()` 必须再跑一次
`process`（= 一次 RPC）。**`elem` 那 10 格缓冲在非相册场景下永远用不上：生产与消费严格交替。**

唯一有预读的是相册：`processGrouped` 一次 push 最多 10 个（`iter.go:301`）。

### 1.3 量化

默认配置（`internal/settings/store.go:72`）：`Threads=4, TaskLimit=2, ConcurrentJobs=1, PoolSize=8, DelayMS=0`。

令 RTT ≈ 300 ms（国内经代理通常 200–500 ms），单文件传输耗时 T，并发槽 L = TaskLimit = 2：

- 生产者速率上限 = `1/RTT` ≈ **3.3 文件/s**
- worker 速率上限 = `L/T`

| 场景 | 今天 | 批量取消息后（每 100 条一次请求） |
|---|---|---|
| 小文件 T≈200 ms | min(3.3, 10) = **3.3 文件/s**（元数据受限） | min(333, 10) = **10 文件/s**（槽位受限）→ **≈3×** |
| 大文件 T≈30 s | min(3.3, 0.067) = 0.067 文件/s（传输受限） | 同样是 0.067 → 无差别 |

一个 5000 个媒体文件的会话任务，**光元数据链就是 5000 × 300 ms ≈ 25 分钟**，
而且这 25 分钟是"每次只能交出一个文件"的串行时间，池子里 8 条连接和 4 个线程大部分时间在挨饿。

⚠️ 顺带说明：`i.delay`（`DelayMS`，默认 0）是在 `Next()` 里**每条消息** sleep 的，
批量取消息**不会**改变这个语义（用户设了延迟仍然按条生效）。

---

## 2. 缺陷 B：pool 的请求不在限流闸门里，而且 FLOOD_WAIT 是**不可见**的

### 2.1 闸门只装在会话连接上

```go
// internal/telegram/accounts.go:1097 / :1611
upstreamClient.New(ctx, opts, login, m.rpcGateMiddleware(accountID), m.usernameCacheMiddleware(accountID))
```

`core/tclient.New` 把 `NewDefaultMiddlewares`（recovery → retry → floodwait）放在前面，
调用方的中间件放最后（最内层），所以 `rpcGateMiddleware` 能看见**原始的** FLOOD_WAIT。

而下载池是**另一条链**：

```go
// app/dl/dl.go:111
pool := dcpool.NewPool(c, poolSize, tclient.NewDefaultMiddlewares(ctx, reconnectTimeout)...)
// core/dcpool/dcpool.go: chainMiddlewares(invoker, p.middlewares...) —— 只有 recovery/retry/floodwait
```

代码里已经把这个洞写得非常清楚（`accounts.go:339-351`、`telegram_rate_limit.go:82-85`、
`accounts.go:427-428`）：**"the download pool ... still does not pass through here"**。

### 2.2 危害一：不限流

账户级预算是 4 req/s、burst 8（`telegram_rate_limit.go:17-20`）。
一个 5000 文件的会话任务会**不受任何约束地**打出 5000 次 history 读取，
而 `telegramAccountBlocked` 只挡新任务的准入，挡不住已经在跑的这份。

### 2.3 危害二（更致命）：FLOOD_WAIT 被吞掉，整件事**不可观测**

`floodwait.NewSimpleWaiter()` 会**自己 sleep + retry**，错误永远不上浮到 `gate.Report`。
于是：账户冷却表 `telegram_rate_limits` 不写、`slow_rpc` 不报、日志里一行都没有。
唯一的观测是"这个请求莫名其妙花了 90 秒"。

**这不是假想——`503a27f` 的真实故障就是这么发生的**（一次反应点下去再也没反应，
没有任何日志、没有 inbox 行，重启后连触发的痕迹都没有）。
那次提交修的是会话连接上的 `resolveUsername`，并在提交信息里明确写下"下载池的请求仍然不被测量"。

---

## 3. 缺陷 C：相册被展开两次

我方在**建条目**时就展开过相册（5 处，全部是同一个 `tutil.GetGroupedMessages`）：

| 路径 | 位置 |
|---|---|
| 链接任务 resolve | `internal/download/manager.go:2990` |
| reaction / 监听 / 回复 | `internal/download/manager.go:3072` |
| 会话历史扫描 | `internal/download/chat.go:2846` |
| 会话回复扫描 | `internal/download/chat.go:2964` |
| 讨论区评论 | `internal/download/discussion.go:173` |

但交给上游时**只传锚点 id + `Group: true`**（`manager.go:1831-1845` 的 `seenGroups` 去重、
`chat.go:1072-1082` 的 `groups` 去重），于是 `iter.process` 发现 `GroupedID` 命中 →
`processGrouped` → **`tutil.GetGroupedMessages` 再展开一次**（`iter.go:285`，
`getHistory(offset_id=msg.ID+11, limit=20)`）。

每个相册白白多读 20 条历史。展开本身还是"读历史区间"，比按 id 取更贵。

### 3.1 派生的真实缺陷：`?single` 白下载整个相册

```go
// internal/download/manager.go:2989
if !linkAsksForSingleMessage(sourceURL) {
    messages, err = tutil.GetGroupedMessages(...)   // 跳过展开
}
// 但 groupedID 仍然记在 item 上 → runTransfer 传 Group: true
```

于是 `?single` 的链接：我方只建了 1 个条目，上游却按 `GroupedID` 把**整个相册都下下来**，
每个成员都触发 `FileCompletedCallback`，而回调里的

```go
item, ok := byMessage[update.MessageID]
if !ok { return }        // manager.go:1862 / chat.go:1133
```

把非锚点成员**全部丢弃**——文件下完 → 临时目录 → 最后被 `RemoveAll(tmpRoot)`（`manager.go:1921`）删掉。

**净效果：`?single` 花掉整个相册的带宽，只为了留 1 个文件。**（发布结果是对的，所以一直没被发现。）

---

## 4. 方案

### 4.0 总览

| 步骤 | 内容 | 是否需要新上游补丁 |
|---|---|---|
| **A** | 按 id 批量取消息（窗口 + TTL + 降级） | 是（`iter.go` 一个取值点 + `dl.go` 一个 Option） |
| **B** | 传全部相册成员 + `Group: false`，删掉上游的二次展开 | **否**（纯调用点改动） |
| **C** | 把闸门装进 dcpool 的 invoker 链 | 是（与 A 同一个补丁：`dl.go` 一个 Option） |
| **D** | 两个 `dl.Run` 调用点合并成一个 helper | 否 |

A 与 C 共用一个新补丁文件 `patches/tdl-metadata-0.20.4.patch`，因为二者改的是同一个文件的同一处。

---

### 4.1 A：批量取消息

**核心接口（打进上游，保持上游"顺序 + 进度 + resume"的语义不变）**

```go
// app/dl —— 新增，默认实现即今天的 tutil.GetSingleMessage
type MessageSource interface {
    Message(ctx context.Context, pool dcpool.Pool, peer tg.InputPeerClass, id int) (*tg.Message, error)
}

// iter.go:177 改为
message, err := i.messageSource().Message(ctx, i.pool, peer, msg)
```

`Options` 增加 `Messages MessageSource`；为 nil 时用默认实现（等价于今天的行为，上游 CLI 不受影响）。

**我们的实现（`internal/download/message_source.go`）**

```go
const (
    messageBatchSize = 100              // pyrogram 文档称上限 200，留一倍余量
    messageBatchTTL  = 3 * time.Minute  // 见 §4.1.2
)

type batchedMessages struct {
    ids    []int              // 本次 dl.Run 的 id 顺序（调用点已经知道）
    index  map[int]int
    cached map[int]cachedMessage   // 命中即 0 次请求
    degraded bool                  // 批量失败一次后彻底退回逐条
}
```

行为：

1. `degraded` → 直接走逐条 `tutil.GetSingleMessage(ctx, pool.Default(ctx), peer, id)`（**与今天逐字节等价**）。
2. 命中且 `now - fetched < TTL` → 直接返回（含"已删除"标记）。
3. 未命中 → 对窗口 `ids[pos : pos+messageBatchSize]` **发一次请求**：
   - `*tg.InputPeerChannel`（`directPeer.kind == "channel"`，带 access hash）→
     `channels.getMessages` + `&tg.InputChannel{ChannelID, AccessHash}`
   - `self` / `user` / `chat` → `messages.getMessages`（裸 `InputMessageID`；
     **这是 pyrogram 多年来的既有做法**：非频道 peer 的消息 id 出自账户全局序号，所以不需要带 peer）
4. **错误映射必须保持语义**：响应里缺失 / `*tg.MessageEmpty` → 返回 `tutil.ErrMessageDeleted`
   （上游据此静默跳过并计数，与今天 `m.GetID() != msg` 的判定完全同构）。
5. 请求整体报错（`CHANNEL_PRIVATE` / `MSG_ID_INVALID` / 网络）→ `degraded = true`，
   打一行 `Warn`，**本条**用逐条 `GetSingleMessage` 兜底。此后该批再也不批量。
6. id 不在 `ids` 里（peer 不匹配等意外）→ 逐条兜底。**任何异常路径都不会比今天更差。**

请求走 **`pool.Default(ctx)`**（当前 DC，与上游今天的选择一致），不是 session client：
理由是 §4.3 把闸门装进了这条链，所以批量请求同样被限流、同样能上报 FLOOD_WAIT，
同时不占用承载 update 流的那条常驻连接去传几百 KB 的响应。

**4.1.1 为什么接口要带 `pool` 参数**

让我们的实现与上游默认实现**同形**：默认实现要 pool，我们的实现也要 pool（限流在链上），
而且 `dcpool.Pool` 是接口（4 个方法），单测里用假 pool + 假 invoker 就能**数请求条数**——
这正是仓库里已有的手法（见 FACT「不碰网络就能驱动上游下载器」）。

**4.1.2 TTL 为什么必须有，取 3 分钟**

窗口一次取 100 条，但消费者是一条一条拿的。如果第 1 个文件传了 30 分钟，
这个窗口的第 100 条在**被交出去的那一刻**已经老了 30 分钟——`file_reference` 可能过期。

规则：**在"交出去"的那一刻判年龄，超时就丢弃并按当前 id 重开一个窗口。**
代价最坏也只是"每 TTL 一次批量请求"（3 分钟 1 次），**永远不会退化成每文件一次**。

过期失败也是响的：`3d0e326` 加的 `incompleteFileError` 会把它报成失败而不是"已完成"。

**4.1.3 收益**

| 批次 | 今天 | 之后 |
|---|---|---|
| 5000 个媒体 | 5000 次请求，串行 ≈ 25 分钟 | 50 次请求（被闸门限流时 ≈ 12.5 s） |
| 100 个媒体 | 100 次 | 1 次 |

---

### 4.2 B：不再重复展开相册

调用点改动（**零上游补丁**）：

```go
// manager.go:1828-1845 / chat.go:1072-1082
// 删掉 seenGroups / groups 那一段去重，按 id 去重即可，把所有成员都放进列表
// Options 里 Group: true → Group: false
```

为什么是安全的：

- 我方**每条**建条路径都已经展开过相册（§3 表），所有成员都已经是独立条目/行；
- `byMessage` 本来就装了**每一个**成员 → 今天被丢弃的那些回调，如今全都命中，
  发布集合**逐条不变**（只是不再下载那些注定被丢弃的文件）；
- `?single` 从此名副其实：只取锚点那一条，**发布结果不变、带宽不再浪费**；
- `Total()` 会从"按锚点计数"变成"按文件计数"，更准；它只用于进度条，
  而我们是 `Quiet: true` + `DisableProgressPS: true`，无副作用。

顺带把上游那个"读 20 条历史来找同组"的展开彻底从下载路径上摘掉——
它本身就是"我明明知道 id 却还要去读历史"的典型。

---

### 4.3 C：把闸门装进 dcpool

```diff
--- a/app/dl/dl.go
+++ b/app/dl/dl.go
@@ type Options struct
+	// Middlewares are appended to the download pool's invoker chain, after the
+	// defaults, so this sits innermost and sees a FLOOD_WAIT before the
+	// flood-wait middleware consumes it.
+	Middlewares []telegram.Middleware
@@ func Run
 	pool := dcpool.NewPool(c,
 		poolSize,
-		tclient.NewDefaultMiddlewares(ctx, reconnectTimeout)...)
+		append(tclient.NewDefaultMiddlewares(ctx, reconnectTimeout), opts.Middlewares...)...)
```

调用点传 `m.accounts.RPCGateMiddleware(accountID)`（`accounts.go:444` 的 `rpcGateMiddleware` 加一个导出包装）。

**顺序是关键，不能搞反**：放在 `NewDefaultMiddlewares` **之后**（= 链的最内层），
`gate.Report` 才能在 floodwait 消费掉错误之前看到它。
这与会话连接上的装法完全一致（`core/tclient.New` 也是把调用方中间件放最后）。

**不会误伤传输**：`pacedRequest`（`accounts.go:382`）是**白名单**，
`upload.getFile` 不在其中，所以字节传输一个令牌都不花。
每个 part 多一次 `pacedRequest` 类型判断（一次 type switch），可忽略。

**不会死锁**：pool 的建连（`c.Pool(size)`）在 `dcpool.invoker` 里发生在**挂中间件之前**；
非当前 DC 的 `c.DC(...)` 里的 `c.transfer(...)` 也早于中间件安装。
所以链上只会有真正的业务请求，没有"等待令牌才能建立连接"的自锁。
（对照会话连接：那里之所以必须用白名单，正是因为 `initConnection` 走同一条链。）

---

### 4.4 D：两个调用点合并（结构性防御）

今天 `manager.go:1845` 和 `chat.go:1122` 各自**手写一遍** `upstreamDL.Options{...}`。
T17 恰好在两个调用点都要改，而仓库里已经吃过"message 路径修了、chat 路径没修"的亏。

方案：把 `Options` 的构造收进**一个** helper（回调作为参数传入），
于是 `Messages` / `Middlewares` / `Group:false` / `Runtime` 只有一处需要正确。
单测直接断言这个 helper 产出的 `Options`——**接线本身可测**，而不是只测被调用的纯函数。

---

### 4.5 补丁与需要同步改的文件

```
patches/tdl-metadata-0.20.4.patch       新增（dl.go 两个 Option + iter.go 一个取值点 + 默认实现）
scripts/prepare-upstream-progress.sh    加 patch 文件、校验和（app/dl/iter.go）、应用顺序
Dockerfile                              加一行 COPY
```

`prepare-upstream-progress.sh` 现有的 sha256 守卫要照旧覆盖 `iter.go`：
**上游一变，脚本就 fail-closed**，不会静默构建出一个没打补丁的镜像。

---

## 5. 被否决的方案

| 方案 | 为什么不选 |
|---|---|
| **只做 C（加限流）** | 4/s 的闸门 ≈ 现在的 3.3/s，**吞吐一点不涨**；而且这 4/s 是账户共享预算，会和 chat 扫描、列表刷新抢令牌。C 的价值要等 A 把请求数降下来才兑现。 |
| **只做 A（不装闸门）** | 若批量请求改走 session client，确实已被限流；但 pool 上仍有 peers 解析等元数据请求不受控、FLOOD_WAIT 仍不可见。可以接受，但不彻底。 |
| **给 core 打补丁**（改 `tutil.GetSingleMessage`） | core 没有 replace，成本高；而且 T17 的问题出在**调用模式**（每条一次），不在那个函数本身。 |
| **用大 batch 的 `getHistory` 代替 `getMessages`** | 我们要的 id 是稀疏的（只有媒体），按 id 取 100 条远比扫 100 条历史便宜。 |
| **跨 `dl.Run` 缓存消息** | 任务可能排队几小时，`file_reference` 会过期。每次任务重新读是对的，缓存只做**本批次内**的窗口。 |
| **自己接管下载循环**（不用 `dl.Run`） | 见下。 |

### 关于"自己接管下载循环"

它其实很有吸引力：`dcpool.NewPool`、`downloader.New/Options/Iter/Elem/File/Progress`、
`tmedia.GetMedia` **全是公开接口**，因此可以在我们自己的仓库里：
零上游补丁地拿到 C，并且用假 pool + 假 invoker + 假 iter 做**真正的端到端**测试。

代价是要自己重写：临时文件创建、进度包装（`writeAt`）、`donePost`（改名 + mtime）、
回调契约、以及上游的 resume/saveProgress，约 250–300 行，且全部落在最关键路径上。

**结论：本次不做，但作为备选保留。** 如果以后还要在迭代器里塞更多东西
（真正的预读、并发取消息、按 DC 分片），这条路会越来越划算——
那时把它当成一次独立重构，而不是塞进这次修复。

---

## 6. 验证计划

### 6.1 单元测试（我们仓库，假 pool + 计数 invoker）

1. 250 个 id 全部存在 → **恰好 3 次请求**（100/100/50）。
   变异：去掉批量 → 250 次 → 红。
2. 响应中缺失某 id → 返回 `tutil.ErrMessageDeleted`，且上游把它计入 `skippedDeleted`。
3. 批量请求报错 → 1 次失败的批量 + 逐条兜底，**此后不再发批量请求**（`degraded`）。
4. 超过 TTL 的缓存条目 → 重新取（并且是按当前 id 重开窗口，不是逐条）。
5. peer 路由：channel → `channels.getMessages`；self/user/chat → `messages.getMessages`。
6. 接线：D 的 helper 产出的 `Options` 里 `Group == false`、id 列表含全部相册成员、
   `Messages != nil`、`Middlewares` 含闸门。
   **每一项都做变异验证**（把它改回旧形态，确认对应测试变红）。

### 6.2 端到端（本地开发实例，真实账号）

- 一个**相册**链接（普通 + `?single`）：断言 `?single` 只传输 1 个文件、不再全相册下载。
- 一个**会话任务**（挑媒体多的）：前后对比。
- 断言：发布出的文件集合与 `mtime`（`donePost` 的 `Chtimes`）逐条不变；
  `.tdl-tmp` 无残留；`mtime` 与 Telegram 的 `date` 一致。

### 6.3 线上账目（把不可见变成可见）

在 D 的 helper 里，任务结束时打一行：

```
metadata_reads job_id=... messages=5000 batch_calls=50 single_calls=0 deleted=3 degraded=false
```

这一行既是本次改动的证据，也顺手补上 `noteSlowRPC` 注释里承认的那个洞
（"the download pool ... does not pass through here at all"）。
没有它，T17 这类问题下次还是只能靠"感觉变慢了"被发现。

---

## 7. 风险与缓解

| 风险 | 缓解 |
|---|---|
| `messages.getMessages`（裸 id）在非频道 peer 上的行为 | pyrogram 多年既有做法；**上线前先在真实账号上各验一次**（self / user / chat / channel 各一条）；任何异常都退到逐条兜底，与今天逐字节等价。 |
| 窗口内的 `file_reference` 过期 | TTL + 交付时判年龄；最坏退化为"每 TTL 一次请求"；失败是响的（`incompleteFileError`）。 |
| 给 pool 加限流后，flood 窗口会表现成"卡住" | 这正是目的：卡住 > 静默失败。`telegramAccountBlocked` 已经挡住新任务准入；传输看门狗（`upstreamIdleTimeout=10min`）会把任务重排。 |
| 上游补丁维护成本 +1 | 脚本已 fail-closed（校验和不匹配就拒绝构建），不会静默跳过；补丁面积极小（两个 Option + 一行）。 |
| 改 `Group: true → false` 影响已有任务 | 只影响"下载了哪些字节"，不影响"发布哪些文件"；DB 里已存在的条目/行不受影响。 |

---

## 8. 落地顺序

1. **B + D**（零补丁、风险最低，先落地拿掉重复展开和 `?single` 浪费）。
2. **A + C**（同一个补丁一起上；A 的实现和单测先在仓库内做完，最后一行接线）。
3. 线上账目 + 端到端复测 → 打 patch 版本（`v1.11.5`）。

---

## 附：本文引用的关键位置

```
.upstream/tdl/app/dl/dl.go:111          dcpool.NewPool(..., tclient.NewDefaultMiddlewares(...)...)
.upstream/tdl/app/dl/iter.go:136        Next() 的 len(i.elem)>0 短路（唯一的"预读"）
.upstream/tdl/app/dl/iter.go:177        每条消息一次 tutil.GetSingleMessage
.upstream/tdl/app/dl/iter.go:285        processGrouped → GetGroupedMessages（第二次展开）
core@v0.20.4/util/tutil/tutil.go:177   GetSingleMessage = getHistory(offset=msg+1, limit=1)
core@v0.20.4/downloader/downloader.go  for Next() { Value(); wg.Go(...) } —— 严格交替
core@v0.20.4/dcpool/dcpool.go          chainMiddlewares(invoker, p.middlewares...)
internal/download/manager.go:1845      链接任务的 Options（Group: true）
internal/download/chat.go:1122         会话任务的 Options（Group: true）
internal/download/manager.go:2989      ?single 跳过展开，但 GroupedID 仍然记录
internal/download/manager.go:1862      回调按 byMessage 过滤 → 多余的相册成员被丢弃
internal/download/telegram_rate_limit.go:17-20    burst 8 / 4 req·s⁻¹
internal/telegram/accounts.go:339-351  "the download pool ... does not pass through here"
internal/telegram/accounts.go:382      pacedRequest 白名单（不含 upload.getFile）
internal/telegram/accounts.go:444      rpcGateMiddleware（要加导出包装）
503a27f                                FLOOD_WAIT 不可观测导致的真实故障
```
