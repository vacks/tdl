# 对上游 TDL 的依赖评估与解耦方案

分析基准：`v1.11.4`（`503a27f`）。所有数字都是实测（`go list -deps` / `go mod why` / LOC 统计）。

---

## 0. 结论

**依赖面比"感觉"小得多，但结构上极不划算。**

- 真正用到的上游 API 只有 **9 个包、约 20 个符号**；
- 却有 **28 个上游包、约 4,820 行**（不含测试）被编进二进制；
- **构建闭包里有 69 个第三方模块，其中 42 个只为上游而存在**（实测）——viper、cobra、survey、
  bbolt、go-pretty、gopsutil、go-terminal-size、filenamify……**一个 distroless 服务端镜像里
  塞着一整套终端 CLI 框架**；
- 而"打补丁"这套装置（`go.work` replace + 3 个 patch + 生成脚本 + Dockerfile 3 处 COPY +
  sha256 守卫）存在的**唯一理由，是 `app/dl` 的两个文件**：`dl.go` 和 `progress.go`。

**可以解耦，而且应该做。** 但建议分三步。**先看收益分布，它和直觉不一样**：

| 阶段 | 内容 | 模块变化（实测） | 规模 |
|---|---|---|---|
| **0** | 拆掉 `internal/adapter/upstream` 的两个**空引用** | 69 → 64（**只掉 5 个**） | 1 小时 |
| **1** | **接管下载路径**（自己写引擎，不再用 `app/dl`） | 64 → 29（**掉 35 个**） | 2–4 天 |
| **2** | 吸收 `iyear/tdl/core` 的 3 个原语 | 29 → 27（只掉 2 个） | 1 天 |

⚠️ **收益几乎全部集中在阶段 1，而不是阶段 0。** 这一点和直觉相反，值得说清楚：

- 阶段 0 那个"死边界"虽然确实该删，但它**不是 CLI 包袱的源头**——viper / survey / cobra
  同时也被 `app/dl` 自己 import（`dl.go` 读 viper 配置、`survey` 问"是否续传"），
  所以删掉 adapter 只掉 validator / locales / universal-translator / urn / bbolt 这 5 个；
- 阶段 2 掉的两个模块里还有一个是 `iyear/connectproxy`（`core/util/netutil` 的依赖），
  **它的价值不在模块数，而在于"仓库里再没有 iyear 字样"和"core 不可改"这件事消失**。

**阶段 1 是唯一的拐点**：做完它，`github.com/iyear/tdl` 这个模块就没用了，
`patches/`、`scripts/prepare-upstream-progress.sh`、`go.work` 的 replace、Dockerfile 里的
三行 COPY 全部可以删掉。

---

## 1. 依赖清点

### 1.1 两个模块

```
github.com/iyear/tdl       v0.20.4   ← go.work replace 到 ./.upstream/tdl（可打补丁）
github.com/iyear/tdl/core  v0.20.4   ← 模块缓存，不可打补丁
```

AGPL-3.0（两个模块都是）。README 已经声明了 AGPL-3.0 并注明了上游，所以
**解耦在许可证上不引入新的义务**，只需要保留版权声明和出处（见 §5.4）。

### 1.2 编进二进制的 28 个上游包

```
app/dl ★            app/login †         core/dcpool         core/downloader †
core/logctx         core/middlewares/recovery               core/middlewares/retry
core/middlewares/takeout                core/storage ★      core/storage/keygen
core/tclient        core/tmedia ★       core/util/fsutil    core/util/netutil
core/util/tutil ★   pkg/consts          pkg/filterMap       pkg/key ★
pkg/kv              pkg/prog            pkg/ps              pkg/tclient ★
pkg/tdesktop        pkg/tmessage ★      pkg/tpath           pkg/tplfunc
pkg/utils           pkg/validator

★ = 我们自己的代码直接 import（共 9 个）
† = 只有那个"死边界"包在 import（见 §1.6）
```

**其余 19 个包全是跟着进来的**，我们一行都没调用。

### 1.3 我们真正调用的符号（完整清单）

| 包 | 用到的符号 |
|---|---|
| `app/dl` | `Run` `Options` `RuntimeOptions` `ProgressUpdate` `FileCompletedUpdate` |
| `core/util/tutil` | `GetSingleMessage` `GetGroupedMessages` `GetInputPeer` `ParseMessageLink` |
| `core/tmedia` | `GetMedia`（及其返回的 `Media{Name,Size,DC,Date,InputFileLoc}`） |
| `core/storage` | `Storage` 接口、`NewPeers`、`ErrNotFound` |
| `pkg/tmessage` | `Dialog{Peer, Messages}` —— **一个 3 行的结构体** |
| `pkg/tclient` | `New` `Options` `AppDesktop` |
| `pkg/key` | `App()` |
| `app/login` | `QR`（**只在死边界的空引用里**） |
| `core/downloader` | `Iter`（**同上，只在空引用里**） |

就这么点。**其余全部是"引擎"内部实现，被我们当作黑盒依赖着。**

### 1.4 只为上游存在的第三方模块（42 个，实测）

判定方法：用 `go list -deps -json ./cmd/tdl` 建出**完整的包导入图**，
再从我们自己所有包的根做可达性 BFS，逐个阶段把边剪掉，看哪些模块真的消失。

⚠️ **不要用 `go mod why -m` 做这个判断**：它只返回**最短**路径。比如 `fatih/color` 同时被
`app/login` 和 `app/dl` 依赖，`go mod why` 会报成"因为 app/login"，于是得出"删掉 adapter
就掉 27 个模块"的错误结论——实测只有 5 个。

实际分布：

| 剪掉什么 | 消失的模块数 |
|---|---|
| `internal/adapter/upstream` → `app/login` + `core/downloader` | **5**（validator、locales、universal-translator、urn、bbolt） |
| 整个 `github.com/iyear/tdl` 模块（= 阶段 1） | 再 **35** |
| `github.com/iyear/tdl/core`（= 阶段 2） | 再 **2**（`iyear/tdl/core`、`iyear/connectproxy`） |

节选（阶段 1 会带走的那一批）：

```
github.com/spf13/viper  cobra  pflag  cast  afero          ← CLI 配置/命令行
github.com/AlecAivazis/survey/v2                           ← CLI 交互式提问
go.etcd.io/bbolt                                           ← CLI 本地 KV（pkg/kv）
github.com/jedib0t/go-pretty/v6  shirou/gopsutil/v3        ← 终端进度条
github.com/kopoli/go-terminal-size  fatih/color  mattn/*   ← 终端 UI
github.com/gabriel-vasile/mimetype  flytam/filenamify      ← RewriteExt / 文件名模板
github.com/bcicen/jstream  beevik/ntp  go-playground/*     ← tmessage 解析 / NTP / 校验
github.com/gorilla/mux  sourcegraph/conc  x/text  x/sync   ← 链路依赖
go.uber.org/zap  go.uber.org/atomic  go-faster/errors      ← 上游的日志/错误包装
```

⚠️ 已逐一验证：**zap / x/text / x/sync / gorilla/mux / go-faster-errors 我们自己的代码一行都没用**
（`grep '"<pkg>' internal cmd` 全空）。它们 100% 是上游带来的。

典型链条：

```
internal/download → iyear/tdl/app/dl → pkg/prog → gopsutil + go-pretty + go-terminal-size
internal/adapter/upstream → iyear/tdl/app/login → spf13/viper + survey/v2 + pkg/kv → bbolt
internal/download → iyear/tdl/app/dl → pkg/tplfunc → flytam/filenamify
internal/download → iyear/tdl/app/dl → pkg/utils → spf13/cobra
```

### 1.5 打补丁的代价

```
patches/tdl-progress-0.20.4.patch        → app/dl/dl.go, app/dl/progress.go
patches/tdl-runtime-options-0.20.4.patch → app/dl/dl.go
patches/tdl-cancel-0.20.4.patch          → app/dl/dl.go
```

**三个补丁，两个文件。** 为了改这两个文件，构建链上有 5 处配套：

1. `go.work` 的 `replace github.com/iyear/tdl => ./.upstream/tdl`
2. `scripts/prepare-upstream-progress.sh`（下载模块 → 拷贝 → 打补丁 → sha256 守卫）
3. `Dockerfile` 里 3 行 `COPY patches/...`
4. `.upstream/` 不出现在 git 里，**每次构建都要重新生成**
5. 本地开发必须记得 `sh scripts/prepare-upstream-progress.sh`，否则 `go.work` 指向一个不存在的目录

**换句话说：整套 fork 机制，服务的是 600 行代码。**

### 1.6 `internal/adapter/upstream` 是个死边界

```go
// Package upstream is the only layer that may import iyear/tdl packages.
// 实际内容：
const Version = "v0.20.4"
var (
    _ = upstreamLogin.QR   // 编译期引用，仅此而已
    _ downloader.Iter      // 同上
)
```

- 注释说"只有这一层可以 import 上游"，**但实际上 `internal/download`、`internal/telegram` 各自直接 import 了 9 个上游包**；
- 这个包唯一的实际用途是提供 `upstream.Version` 字符串，被 `httpapi/server.go`（3 处）
  和 `bot/service.go`（2 处）用来显示"上游 TDL v0.20.4"；
- 它为了两个**空引用** import 了 `app/login` 和 `core/downloader`，
  于是 `pkg/kv` → `bbolt`、校验器三件套被拖进二进制；
  （注意别高估：viper/survey/cobra 同时也是 `app/dl` 自己在用的，删掉 adapter 掉不了它们，见 §1.4）
- 而我们自己的 QR 登录**早就用 gotd 原生实现了**：
  `client.QR().Auth(ctx, qrlogin.OnLoginToken(...))` + `client.Auth().Password(...)`
  （`internal/telegram/accounts.go:1617`）。`app/login.Q`**从未被调用过一次**。

**结论：这个包是纯粹的负债，拆掉它零风险。**

---

## 2. 为什么现在"不上不下"

不是补丁打少了，是**我们想要的层和上游给的层不是同一个**：

```
上游 app/dl  =  [建池] + [读 viper 配置] + [解析 URL/文件] + [迭代消息] + [下载] + [终端进度条] + [resume 落盘]
                    ↑            ↑                ↑              ↑           ↑            ↑              ↑
我们要的        改它         不用(有自己的设置)   不用(有自己的源)   改它       用它        不用          不用
```

`app/dl.Run` 是一个**命令行应用的入口**，把"引擎"和"CLI"焊死在一起。
我们只想要中间那三层（池 / 迭代 / 传输），却被要求接受整个命令行应用的全部依赖和全部行为。

于是只能打补丁，而补丁只能改表层：

- T17（每条消息一次 `getHistory`、不过限流闸门）→ 要再打一个补丁，改 `iter.go`；
- v1.11.2 的根因（上游 `Download()` **吞掉传输错误**，`core@v0.20.4/downloader/downloader.go:43-63`）
  → core 没有 replace，**至今只能在我们自己这层绕**（`incompleteFileError` 事后校验）；
- 想给下载加预读、加并发取消息、按 DC 分片 → 全部要打补丁。

**这就是"难受"的根源**：改不动，又绕不开。

---

## 3. 方案

### 阶段 0：拆掉死边界（~1 小时，零风险，立即可做）

1. `internal/adapter/upstream`：删掉 `app/login` 和 `core/downloader` 两个空引用；
   保留 `Version`（或改成 `buildinfo` 里的常量）。
2. 顺手在文档里记明：**真正的上游边界是 `internal/download` 和 `internal/telegram`**，
   不要再维护一个名不副实的"边界包"。

**收益（实测）**：构建闭包里的第三方模块 69 → **64**，消失的是：

```
github.com/go-playground/validator/v10
github.com/go-playground/locales
github.com/go-playground/universal-translator
github.com/leodido/go-urn
go.etcd.io/bbolt
```

**（只有 5 个——不要为了模块数做这一步，做它是因为那是死代码。）**

**验证**：`go build` + 全量测试 + `docker compose -f compose.yaml -f compose.dev.yaml up -d --build`。

---

### 阶段 1：接管下载路径（**建议做，收益最大**）

自己写下载引擎，不再调用 `app/dl.Run`。新的包结构建议：

```
internal/tgclient/            ← 从 core/dcpool + core/tclient + core/middlewares 适配（~600 行）
    pool.go        连接池（Client/Takeout→不需要/Default/Close）
    client.go      客户端构造（proxy / backoff / 中间件链）
    middlewares.go recovery + retry（floodwait 直接用 gotd/contrib）
    kv.go          Storage 接口 + 会话/peers 存储适配（暂时继续用 core/storage）

internal/download/transfer/   ← 从 app/dl 重写（~500 行，其中迭代器是新写的）
    iter.go        迭代器（**这里做 T17：按 id 批量取消息 + 预读窗口**）
    elem.go        下载元素
    progress.go    进度 + 文件完成回调（保留我们依赖的契约）
    run.go         入口：建池 → 迭代 → 传输 → 回调
```

**为什么值得**：

1. **T17 从"要打补丁"变成"改自己的代码"**，而且能做到补丁做不到的事
   （真正的预读：现在的迭代器 `Next()/Value()` 严格交替，一次 RPC 换一个文件）；
2. **v1.11.2 的根因可以直接修**：吞错误的 `Download()` 就在我们要替换的那个文件里，
   不用再靠 `incompleteFileError` 事后校验；
3. **补丁装置整体删除**：`patches/`（3 个文件）、`scripts/prepare-upstream-progress.sh`、
   `.upstream/`、`go.work` 的 replace、`Dockerfile` 的 3 行 COPY、sha256 守卫——全没了；
4. **CLI 包袱全掉**：cobra、go-pretty、gopsutil、go-terminal-size、fatih/color、
   mimetype、filenamify、jstream、ntp、zap…（阶段 0 之后剩下的全部）；
5. 顺带可以砍掉**从未被使用**的路径：`Takeout`（两个调用点都没设 `Takeout: true`）、
   `RewriteExt`、`SkipSame`、`Include/Exclude`、`Desc`、终端进度条、URL/文件解析器、
   `resume` 落盘（我们自己的 DB 才是权威，`Continue/Restart` 只在补丁语义里有意义）。

**要接管的行为契约**（不能丢的，逐条对齐）：

| 契约 | 现值 | 为什么重要 |
|---|---|---|
| 临时文件 `<MessageID>_<filenamify(Name)>.tmp` | `app/dl` | `publishItem` 拿到的最终路径 |
| `donePost`：去 `.tmp` 改名 + **`Chtimes` 设为消息日期** | `app/dl/progress.go` | 发布后的文件 mtime 必须与 Telegram 的 `date` 一致 |
| `ProgressUpdate{DialogID,MessageID,Downloaded,Total,Completed}` | 同上 | 驱动 `progressStore` 与 `markItemStarted` |
| `FileCompletedUpdate{DialogID,MessageID,Path}` | 同上 | `publishItem` / `publishChatItem` 的输入 |
| `PartSize=1MB` + `tutil.BestThreads` | `core/downloader` | 传输参数 |
| `messages.getHistory` 的**删除消息**语义（`ErrMessageDeleted` → 静默跳过） | `core/tutil` | 跳过而不失败 |

**估计**：新增/适配 ≈ **1,000–1,100 行**（其中约 400 行是从 AGPL 上游适配而来，需保留出处）。

**做完这一阶段的 Go 依赖**：仍然有 `github.com/iyear/tdl/core`（用它的 `tmedia`/`tutil`/`storage`），
但 `github.com/iyear/tdl` 可以整个删掉。

---

### 阶段 2：吸收 core 原语（可选，1 天）

只剩 3 样东西要用：

| 包 | 需要的部分 | 行数 |
|---|---|---|
| `core/tmedia` | `GetMedia`/`ExtractMedia`/`GetPhotoInfo`/`GetDocumentInfo`（+ 名字、尺寸、DC、日期、`InputFileLoc`） | ~250（`convert.go` 那 150 行是发送方向，不需要） |
| `core/util/tutil` | `GetSingleMessage` `GetGroupedMessages` `ParseMessageLink` `GetInputPeer` `GetInputPeerID` | ~150（其余 130 行不需要） |
| `core/storage` | `Storage` 接口 + `NewPeers` + `NewSession` | ~170（`state.go` 不需要） |

≈ **570 行**。做完这一步：

- `go.mod` 里 `github.com/iyear/tdl` 和 `github.com/iyear/tdl/core` **同时消失**；
- `go.work` 可以删掉（或只剩 `use .`）；
- 全仓库 `grep iyear` 返回空。

---

## 4. 收益量化

| 指标 | 现在 | 阶段 0 后 | 阶段 1 后 | 阶段 2 后 |
|---|---|---|---|---|
| **构建闭包里的第三方模块**（实测） | **69** | **64** | **29**（见下） | **27** |
| go.mod 里的 require 行 | 86 | ~64（tidy 后） | ~30 | ~27 |
| 上游包数（编进二进制） | 28 | 26 | 1（`core`） | **0** |
| 上游 LOC（编进二进制） | ~4,820 | ~4,300 | ~1,100 | **0** |
| 补丁文件 | 3 | 3 | **0** | 0 |
| 构建链上的补丁设施 | 5 处 | 5 处 | **0** | 0 |
| 改下载逻辑要动的东西 | 上游补丁 | 上游补丁 | **自己的代码** | 自己的代码 |
| 终端 CLI 框架进二进制 | 是 | 否 | 否 | 否 |

⚠️ 注意三点，免得被上表误导：

- **收益全在阶段 1**：阶段 0 只掉 5 个，阶段 2 只掉 2 个。中间那一档才是本体。
- **阶段 1 的 29 是"还回来的数"**：新引擎会**重新**直接依赖 `gotd/contrib`（floodwait）和
  `iyear/connectproxy`（代理解析），所以实际落地大约落在 **31 个**。
- **阶段 2 的价值不在模块数**（29 → 27），而在：① 仓库里再没有 `iyear` 字样；
  ② `core` 不可打补丁这件事不再存在——想改 `tmedia`/`tutil` 直接改自己的代码。**它是收尾，不是重点。**

**最值钱的一行是倒数第二行**：从"打补丁"变成"改自己的代码"。

---

## 5. 代价与风险

### 5.1 抄过来的代码必须**读懂**，不能只是搬

上游有几处"不读就不会发现"的行为，搬的时候必须逐条复刻或明确改变：

- `GetGroupedMessages` 用 `OffsetID(msg.ID + 11)` + `BatchSize(20)` **读历史区间**来找同组，
  而不是按 id 取——这正是 T17 里"相册被展开两次"的第二次；
- `GetSingleMessage` 判"消息已删除"的方式是 `m.GetID() != msg`（读到的不是它本人 = 它没了），
  以及空页时 `errors.Wrap(nil, "get single message")` **仍然非 nil**（v1.11.3 踩过）；
- `donePost` 里 `RewriteExt` 用 `mimetype.DetectFile` 改写扩展名——我们没用，别顺手抄进来；
- `download()` 里 `BestThreads(size, threads)` 会按文件大小降线程数。

### 5.2 失去上游未来的修复

上游对 `app/dl`/`core/downloader` 的改动我们**不再自动获得**。
缓解：

- 我们用的是**很窄、很稳定**的子集（MTProto 下载原语），不是上游活跃演进的部分；
- **协议层的修复仍然自动获得**：`gotd/td` 依旧是普通依赖，可以照常升级——这是关键，
  我们吸收的是"gotd 之上的一层薄应用",不是协议栈；
- 现在的补丁模式本来就已经拿不到干净的上游更新（这正是问题的来源）。

### 5.3 下载路径是最高风险的地方

缓解（按仓库既有做法）：

- 逐项做**变异验证**：把每处修复改回旧形态，确认对应测试变红；
- 端到端对拍：**先让新旧两条路径同时存在**，用同一个任务跑两边，比对
  （a）发布出的文件集合与 mtime、（b）`download_items` 行的状态迁移、
  （c）请求条数。对拍通过再删旧路径。
- 阶段 1 可以拆成两次提交：先"引擎就位但未切换"，再"切换 + 删补丁"。

### 5.4 AGPL

- README 已经声明 AGPL-3.0 并注明"依赖上游 iyear/tdl"，**许可证类别不变**；
- 但**直接复制代码**比"链接依赖"要求更明确：需要
  1. 在仓库根放一份 `LICENSE`（AGPL-3.0 全文），
  2. 在每个改编文件头写明出处与版权：
     `// Adapted from github.com/iyear/tdl (AGPL-3.0), Copyright (c) iyear.`
  3. README 的许可证段落在"依赖"之外补一句"部分实现改编自上游"。
- 目前仓库**没有 LICENSE 文件**（只有 README 里的徽章），这一步无论如何都该补。

### 5.5 不要顺手做的事

- **不要脱离 gotd**：协议栈、`telegram.Client`、`peers.Manager`、`tg` 类型全部继续用 gotd；
- **不要重写 `upload.getFile` 的并发分片**：`gotd/td/telegram/downloader` 的 `Parallel` 已经很好，
  上游也只是薄薄包了一层；
- **不要碰 `core/storage` 之前先想清楚**：会话文件格式（`storage.NewSession`）一旦改动，
  现有账号可能掉线。**阶段 2 动它时必须逐字节兼容**，或者干脆把 `session.go` 原样搬过来。

---

## 6. 建议的落地顺序

```
阶段 0（今天，1 小时）
    └─ 删死边界 → 构建 + 全量测试 + 起容器验证 → 一个 commit

阶段 1（2–4 天，分两次提交）
    ├─ 1a：internal/tgclient + internal/download/transfer 落地，T17 的批量取消息一并做掉，
    │      但先不切换（旧路径仍在），端到端对拍
    └─ 1b：切换到新引擎 → 删除 app/dl 依赖、patches/、.upstream/、go.work replace、
           Dockerfile 的 COPY、prepare 脚本 → 全量回归

阶段 2（1 天，可选）
    └─ 吸收 tmedia/tutil/storage → 删掉最后两个 iyear 模块 → go.work 清理
```

**如果只做一件事**：做阶段 1。阶段 0 是顺手，阶段 2 是收尾。

---

## 附录：完整依赖清单

### A. 我们的代码直接 import 的上游包（9）

```
internal/adapter/upstream     app/login, core/downloader          ← 都是空引用，阶段 0 删
internal/download             app/dl, core/storage, core/tmedia,
                              core/util/tutil, pkg/tmessage
internal/telegram             core/storage, pkg/key, pkg/tclient
```

### B. 只为上游存在的模块（42，实测）

其中 **5 个**（validator、locales、universal-translator、urn、bbolt）在阶段 0 消失，
**2 个**（`iyear/tdl/core`、`iyear/connectproxy`）在阶段 2 消失，其余 35 个在阶段 1 消失。

```
github.com/AlecAivazis/survey/v2        github.com/bcicen/jstream
github.com/beevik/ntp                   github.com/clipperhouse/uax29/v2
github.com/fatih/color                  github.com/flytam/filenamify
github.com/fsnotify/fsnotify            github.com/gabriel-vasile/mimetype
github.com/go-faster/errors             github.com/go-playground/locales
github.com/go-playground/universal-translator
github.com/go-playground/validator/v10  github.com/go-viper/mapstructure/v2
github.com/gorilla/mux                  github.com/iancoleman/strcase
github.com/jedib0t/go-pretty/v6         github.com/kballard/go-shellquote
github.com/kopoli/go-terminal-size      github.com/leodido/go-urn
github.com/mattn/go-colorable           github.com/mattn/go-isatty
github.com/mattn/go-runewidth           github.com/mgutz/ansi
github.com/mitchellh/mapstructure       github.com/pelletier/go-toml/v2
github.com/sagikazarmark/locafero       github.com/shirou/gopsutil/v3
github.com/sourcegraph/conc             github.com/spf13/afero
github.com/spf13/cast                   github.com/spf13/cobra
github.com/spf13/pflag                  github.com/spf13/viper
github.com/subosito/gotenv              github.com/tklauser/go-sysconf
github.com/tklauser/numcpus             go.etcd.io/bbolt
go.uber.org/atomic                      go.uber.org/zap
go.yaml.in/yaml/v3                      golang.org/x/sync
golang.org/x/term                       golang.org/x/text
```

### C. 上游包 LOC（不含测试，实测）

```
app/dl                     1064    ← 阶段 1 替换
app/login                   508    ← 阶段 0 删除（从未调用）
pkg/kv                      675    ← 阶段 0 随 app/login 消失
core/storage                324    ← 阶段 2 部分吸收
core/tmedia                 322    ← 阶段 2 吸收（约 250 行）
core/util/tutil             287    ← 阶段 2 吸收（约 150 行）
pkg/tmessage                210    ← 阶段 1 用自己的 Dialog 取代
core/downloader             181    ← 阶段 1 替换
pkg/tplfunc                 169    ← 阶段 1 消失（不需要文件名模板）
core/dcpool                 147    ← 阶段 1 适配
core/tclient                119    ← 阶段 1 适配
pkg/prog                    103    ← 阶段 1 消失（终端进度条）
pkg/tpath                    91    ← 阶段 1 消失
pkg/utils                    81    ← 阶段 1 消失
pkg/tclient                  72    ← 阶段 1 适配
core/middlewares/takeout     70    ← 阶段 1 消失（Takeout 从未启用）
core/middlewares/recovery    64    ← 阶段 1 适配
pkg/ps                       62    ← 阶段 1 消失
pkg/consts                   60    ← 阶段 1 消失
core/middlewares/retry       58    ← 阶段 1 适配
core/util/netutil            32    ← 阶段 1 适配
core/storage/keygen          25    ← 阶段 2 吸收
core/logctx                  24    ← 换成我们自己的 applog
core/util/fsutil             24    ← 阶段 1 适配
pkg/validator                15    ← 阶段 1 消失
pkg/key                      13    ← 阶段 1 吸收（3 行）
pkg/tdesktop                 11    ← 阶段 1 消失（会话格式，见 §5.5）
pkg/filterMap                 9    ← 阶段 1 消失
                          ------
                          ≈ 4820
```
