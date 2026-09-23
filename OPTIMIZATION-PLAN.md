# llm-gateway 优化计划（v1）

> 基线：本轮修复后 **32 个测试套件 / 815 项断言全绿**（`node test/<name>.mjs` 逐个跑）。
> 本计划的所有验收都以"基线不回归"为前提：任何工作流结束时，`test/` 下**既有套件必须仍然全绿**。

## 0. 背景与总目标

这个网关现在功能是齐的、也能扛住真实使用了，但还有三类"能用但不够好"的问题：

1. **选路只解决了"别老用一家"，还没解决"该多用谁"**。现在的 `round-robin` 是**等权**轮换：p1 的和 p100 的拿一样多。而 `tiered` 只有开/关两档——要么只用优先池 4 家，要么 23 家平均分。中间那档（高优先多拿、低优先也参与）没有。
2. **还有两条已知的"会挂死/会泄漏"路径没堵**（上一轮明确标注为遗留项）：客户端既不断开也不读取时会卡在 `waitDrain`；非流式的首字节等待复用了流式的静默超时配置。
3. **省钱和排查两件事都有明显缺口**：Anthropic 的 `cache_control`（prompt caching）在协议转换时被**完全丢弃**（`grep cache_control lib/` 零命中），等于白丢缓存命中；同时任务日志没有耗时分解，出了慢请求只能看到"换了哪几家"，看不到"时间花在哪一段"。

目标：在不引入新依赖、不改变现有默认行为语义的前提下，把**选路质量**、**连接可靠性**、**可观测性**、**协议保真**四条补上，并把测试基建做到能并行跑不打架。

## 1. 优先级总览

| 工作流 | 主题 | 文件归属（**独占**） | 优先级 | 波次 |
|---|---|---|---|---|
| **P1** | 优先级加权轮询 + 半开试探 | `lib/channels.mjs` | 高 | 1 |
| **P2** | 首字节超时 + `waitDrain` 可中断 + 出站代理路径 + 排队时长统计 | `lib/proxy.mjs`、`lib/concurrency.mjs` | 高 | 1 |
| **P3** | 耗时分解 + 模型级指标 + 面板 | `server.mjs`、`public/index.html`、`lib/tasklog.mjs` | 中 | 1 |
| **P4** | prompt caching + `top_k` + 思考预算策略 | `lib/adapters/anthropic.mjs`、`lib/adapters/openai.mjs` | 中高 | 1 |
| **P5** | 测试运行器 + 动态端口 + `package.json` | 新增文件 + 全部 `test/*.mjs` | 中 | **2** |

**波次说明**：P1–P4 文件互不重叠，可并行。P5 要改**所有**测试文件（把硬编码端口换成动态分配），必须等 P1–P4 各自的新测试落地后再做，否则会互相覆盖。

## 2. 给所有执行者的铁律

1. **只改"文件归属"里列给你的文件**。越界改动会让并行的其它工作流产生假失败，也会让验收失效。
2. **不要动 `config.json`**。它是线上配置，只有用户能决定策略。要加新配置项时，**写进 `config.example.json` 并给默认值**，靠默认值保证不配也能跑。
3. **不要新增运行期依赖**。这个项目是零依赖的。
4. **不要为了让测试变绿而放宽断言**。如果发现既有测试与你的改动冲突，先判断"是谁写错了"，并在汇报里**单独用一段**说明理由（上一轮就有一次是旧断言写死了 bug 行为）。
5. **端口**：新增测试**不要用** `8793–8813`、`8877–8878`、`9101–9184` 这些已被占用的段。优先从 `9200+` 里挑，且**逐个文件唯一**。
6. **汇报必须包含**：改了哪些文件（逐条）／每条改动的验证命令与**真实输出**／**没能验证的部分**。做不到的就说做不到，不要编。
7. 收尾时清理自己产生的临时文件与后台进程。

---

## P1 — 优先级加权轮询 + 半开试探（`lib/channels.mjs`）

### 现状（代码事实）

- `lib/channels.mjs:676-690`：`round-robin` 分支目前是"按 priority 排好 + 用 `this.turn++ % open.length` 轮换起点"的**等权**轮换，按请求轮换起点。所以 p1 与 p100 拿到的份额相同。
- `:690-700`：`least-loaded` 用 `inFlightOf()` 排序；当 `maxConcurrentPerChannel` 为 0（不限流）时，`inFlightOf` 恒为 0，排序退化成"什么都排不出来"，等价于保持原顺序。
- `:713`、`:761-778`：`least-loaded` 下不做模型级粘性（有意为之，别改）。
- 冷却机制：`markFailure` → `cool(backoff)`；排序里冷却渠道被**垫到末尾**（上一轮修复）。但冷却**到期后**它立刻恢复满份额，没有"先放一个请求试探"的中间态。
- `this.turn` 的自增发生在 **`candidatesFor()` 每次调用**上，而一个请求在 `retryLoop` 多轮里会多次调用 `candidatesFor()`，所以游标会多跳。

### 做什么

**1.1 优先级加权轮询（核心）**
新增 `strategy: "weighted"`（**不要改动 `round-robin` 的现有语义**，避免破坏既有 31 个套件的断言）。规则：

- 权重按优先级分档，默认映射（写进 `config.example.json` 的 `_note_`）：
  - `priority <= 10` → 权重 8
  - `11–30` → 权重 4
  - `31–60` → 权重 2
  - `> 60` → 权重 1
- 允许 `routing.priorityWeights` 覆盖，形如 `{ "10": 8, "30": 4, "60": 2, "100": 1 }`（键是优先级**上界**，取第一个 `priority <= 键` 的档）。
- 实现方式：用"平滑加权轮询"（smooth weighted round-robin）而不是"每 N 次才轮到一次"——给每个候选维护 `currentWeight`，每次挑选时 `currentWeight += weight`、选中最大的、被选中的 `-= totalWeight`。这样输出是**交错**的（p1,p1,p2,p1,p3…），不会出现"前 8 个请求全是同一家"的突发。
- 状态存在 manager 上（`this.weightState = new Map()`），键用渠道名，渠道被删除/重载时清理（`load()` 里已经会重建，确认一下别留垃圾）。

**1.2 半开试探（half-open）**
- 给 `Channel` 增加 `probeUntil` 语义：冷却到期后进入半开窗口（默认 `routing.halfOpenMs: 15000`），窗口内该渠道**最多放行 `routing.halfOpenMaxInFlight: 1` 个在途请求**，其余请求继续跳过它。
- 判定需要知道"当前有几个在途"：`candidatesFor()` 里能用 `this.inFlightOf(name)`（已存在，`:797`），不要新造计数器。
- 半开期间**成功**（`markSuccess`）→ 立刻清掉半开态、恢复满权重；**失败**→ 重新冷却（复用既有指数退避）。
- 关键约束：半开窗口内不能把渠道整个从候选里剔除（既有测试断言"冷却渠道仍留在候选里"）。

**1.3 `least-loaded` 退化修复**
`maxConcurrentPerChannel === 0`（或未配置导致的限流关闭）时，`least-loaded` 的 `inFlightOf` 恒为 0，排序无意义 → 此时**回退成 `priority` 排序**（`priority || latency`），并 `log.warn` 一次说明"未开渠道级限流，least-loaded 退化为 priority"（同一个 manager 只警告一次，别每请求刷屏）。

**1.4 游标改为每请求一次**
`this.turn` 的自增从 `candidatesFor()` 移到"每个请求第一次选路时"。（提示：`proxy.mjs` 持有请求级状态，但**你不能改 `proxy.mjs`**；可行做法是在 `candidatesFor` 的 `opts` 里接受一个可选的 `requestId`，同一 `requestId` 只前进一次；不传 `requestId` 时保持旧行为。这样既修了问题又不越界。）

### 验收（必须逐条跑并贴输出）

新增 `test/test-routing-weighted.mjs`（纯单元，不占端口），断言：

1. **权重分布**：构造 4 家渠道（priority 1/20/50/100，权重 8/4/2/1），跑 1500 次 `candidatesFor`，统计首选渠道占比 → 期望约 `53%/27%/13%/7%`，**每档容差 ±4 个百分点**。
2. **交错而非突发**：前 12 次首选序列里，**同一家连续出现不超过 2 次**（这条是"平滑加权"与"分桶轮询"的分水岭，很重要）。
3. **半开只放 1 个**：让某渠道冷却到期进入半开，模拟 5 个并发在途 → 该渠道被选中次数 `<= 1`；调 `markSuccess` 后恢复满份额。
4. **半开不剔除**：半开期间该渠道仍在 `candidatesFor(...)` 返回的列表里。
5. **least-loaded 退化**：`maxConcurrentPerChannel: 0` + `strategy: 'least-loaded'` → 首选顺序与 `priority` 排序一致。
6. **游标每请求一次**：同一 `requestId` 连续调 3 次 `candidatesFor` → 首选渠道相同；换 `requestId` → 前进一档。
7. **默认不漂移**：不配 `strategy` 时行为与现在完全一致（`priority` 排序）。

回归门槛：`test-tiered`、`test-pool`、`test-routing-notes`、`test-routing-policy`、`test-sticky-provider`、`test-budget`、`test-routing-hardening`、`test-resilience` **全部仍 0 失败**。

### 不做

- 不改 `round-robin` / `priority` 的既有语义。
- 不实现"按模型独立权重"。
- 不动 `lib/proxy.mjs`。

---

## P2 — 首字节超时 + `waitDrain` 可中断 + 出站代理路径（`lib/proxy.mjs`、`lib/concurrency.mjs`）

### 现状（代码事实）

- 上一轮给响应体读取加了统一空闲看门狗（`idleTimeoutMs` = `routing.streamIdleTimeoutMs`，默认 120000），拿到响应头后 `armIdle()`，**流式与非流式共用**。
- 因此**非流式的"等首字节"也被 120s 约束**——运维把流式静默超时调小/调大，非流式会跟着变。当时明确标注"没有独立配置项"。
- `waitDrain(res)`（`lib/proxy.mjs` 约 `:96-101`）：对已销毁/已终结的 `res` 会立即返回，但**如果客户端既不断开也不读取**，`res.write()` 返回 false 后它等 `'drain'`/`'close'` 会**永久挂住**——上一轮标注为"改前就存在、不在 9 条内、未修"。
- `lib/outbound-proxy.mjs`（478 行，渠道级 `channel.proxy` 走本机代理）路径下，看门狗与 abort signal 的语义**从未实测**。

### 做什么

**2.1 独立的首字节超时**
- 新增 `routing.firstByteTimeoutMs`，语义：**从请求发出到收到响应头**的最大等待。默认值取 `timeoutMs`（180000）以保持向后兼容。
- 拿到响应头后 `clearIdle()` 再 `armIdle()`，两段计时不许叠加（否则最坏情况 = 两个超时相加）。
- 非流式与流式**都要**受首字节超时保护（上游只回响应头不给数据的情况）。

**2.2 `waitDrain` 可中断**
- 签名扩成 `waitDrain(res, { signal, timeoutMs })`：接入请求级 abort signal，并加自身超时（建议复用 `streamIdleTimeoutMs`）。
- 超时或 abort 时**不能**静默当成成功——按"渠道失败 + 客户端已断开"处理，并确保 `finally` 里许可一定归还。
- 保持既有行为：`res` 已销毁/已终结时立即 resolve。

**2.3 出站代理路径的看门狗覆盖**
- 给 `channel.proxy`（走 `lib/outbound-proxy.mjs`）补一条端到端测试：mock 一个"只回响应头不给数据"的上游，经本机代理转发，断言仍在首字节超时内 503，而不是挂死。

**2.4 排队时长统计（本项归 P2，因为 `lib/concurrency.mjs` 是本工作流独占文件；P3 只负责把它显示出来）**
- `Semaphore` 记录 `waitMsTotal` / `waitMsMax` / `waitedCount`（在 `acquire()` 真正拿到许可时结算等待时长）。
- `GatewayLimiter.stats()` 暴露 `queueWaitMsTotal` / `queueWaitMsMax` / `waitedCount`。
- 注意既有字段一个都不能改名或删除（P3 与面板要消费）。

**2.5 三段耗时的**采集**（P3 反馈的跨文件缺口，数据源在这里，所以补进 P2）**

P3 已确认：`lib/proxy.mjs` 里 `ttfb|bodyMs|queueMs|firstByte` **零命中**，而 `attempts.push({...})`（约 `:409-413`）只写 `channel/tier/error/status/kind/fingerprint/tries/ms`；更关键的是**成功路径**（约 `:376-386`）`attempts` 通常是**空数组**、`finish()` 也没有任何耗时字段——也就是说最常见的情况（第一家就成功）根本不会产生任何 attempt 记录。所以这三段耗时必须由 P2 在 `proxy.mjs` 里采集并带出：

- `queueMs`：进入限流排队 → 拿到许可（`limiter.acquire` 前后）。
- `ttfbMs`：发出上游请求 → 收到响应头（就是 `armIdle()` 那个点，约 `:674`）。
- `bodyMs`：收到响应头 → 响应体读完 / 流结束。

**字段名冻结**，必须是 `queueMs` / `ttfbMs` / `bodyMs`（P3 的 tasklog 契约、聚合与面板都按这三个名字消费）。

**成功路径也必须带出**：不能只在 `attempts.push` 那条失败/换家分支里记。成功时请在请求级记录里暴露一份（例如 `timings: { queueMs, ttfbMs, bodyMs }`），否则最常见的成功请求在面板上看不到任何耗时分解——这一项的意义就废了一半。

**不许造假**：某一段确实测不到（例如非流式聚合路径拿不到中间点）时，该键**直接不写**，不要写 0——P3 的契约是"字段不存在就不显示"，写 0 会被当成"真的等了 0ms"，反而误导排查。

### 验收

新增 `test/test-proxy-timeouts.mjs`（端口从 `9200+` 挑，唯一），断言：

1. **首字节超时独立生效**：`firstByteTimeoutMs: 500`、`streamIdleTimeoutMs: 60000` → 上游只回响应头时，**500ms 量级**（断言 `< 2000ms`）返回 503，错误文案能区分"首字节超时"与"静默超时"。
2. **两段不叠加**：上游"首字节慢但身体正常"（例如 300ms 后开始持续吐数据）→ 请求成功，不被首字节超时误杀。
3. **`waitDrain` 不再永久挂住**：裸 socket 客户端发请求后**既不读也不断开**，上游持续吐大响应体 → 断言请求在超时内结束、`/api/metrics` 的该渠道在途回到 0、网关随后仍能正常服务（再发一个请求拿 200）。
4. **许可不泄漏**：上述场景结束后 `global.active === 0`。
5. **出站代理路径**：经 `channel.proxy` + 本机代理的"只给响应头"上游 → 同样在超时内 503。
6. **三段耗时被真实采集**（§2.5）：mock 上游注入"首字节延迟 300ms + 正文分 3 块每块 100ms" → 断言成功请求的记录里 `ttfbMs >= 250`、`bodyMs >= 200`；再用 `maxConcurrentPerChannel: 1` 并发 2 个请求 → 第二个请求 `queueMs > 0`。**成功路径必须有这三个字段**（§2.5 的重点），不是只有换家失败时才有。

回归门槛：`test-proxy-hardening`（47）、`test-concurrency`（40）、`test-resilience`（28）、`test-retry`、`test-context`、`smoke` 全绿。

### 不做

- 不改 `lib/proxy.mjs` 的选路/重试预算语义。
- 不为 HTTP/2 单独适配（当前是 HTTP/1.1；若发现天然支持则顺带断言，不支持则**在汇报里写明未覆盖**）。

---

## P3 — 耗时分解 + 模型级指标 + 面板（`server.mjs`、`public/index.html`、`lib/tasklog.mjs`）

### 现状（代码事实）

- `lib/tasklog.mjs`：`write()` 在 `:234`，记录里有 `tries` / `attempts[]` / `fingerprint` / `kind`，`errors()` 在 `:308` 把 attempts 摊平，`snapshot()` 在 `:342`。
- **没有耗时分解**：看不到"排队等了多久 / 等上游首字节多久 / 流式吐了多久"。慢请求只能靠肉眼猜。
- `lib/concurrency.mjs:319-340` 的 `stats()` 返回 `active/pending/queued/maxQueue/overloads/queueTimeoutMs` —— 有排队**数量**，没有排队**时长**。
- `server.mjs` 的管理端点：`/api/tasks`(:278)、`/api/errors`(:293)、`/api/status`(:302)、`/api/metrics`(:311)。
- `public/index.html`（552 行）有健康/熔断/任务日志视图，但没有"这次请求时间花在哪"和"每模型健康度"。

### 做什么

**3.1 每个 attempt 记三段耗时（**数据源在 P2**，P3 只做承载/归一/聚合/脱敏）**
`queueMs` / `ttfbMs` / `bodyMs` 三个字段由 **P2** 在 `lib/proxy.mjs` 采集（见 P2 §2.5；含**成功路径**），P3 **不改 `lib/proxy.mjs`**。P3 在 `lib/tasklog.mjs` 实现完整契约：
- 字段存在 → 透传进落盘副本、内存环、以及 `/api/errors` + `/api/tasks` 的聚合。
- 字段不存在 → **键直接不写入，不要造假 0**（否则面板会把"没测到"显示成"等了 0ms"，误导排查）。
- 落盘前同样走 `redactSecrets`，别让新字段绕过脱敏。
（`totalMs` 已经存在于请求级，保留。）

**3.2 `/api/metrics` 暴露排队时长（**消费 P2 产出的字段**，P3 不要改 `lib/concurrency.mjs`）**
`GatewayLimiter.stats()` 由 **P2** 增加 `queueWaitMsTotal` / `queueWaitMsMax` / `waitedCount`；P3 只把它们透传到 `/api/metrics` 并在面板显示。若 P2 尚未落地、字段不存在，则**照常透传**（不要造假的 0），并在验收里把这一条标为"依赖 P2"。

**3.3 `/api/metrics` 增加模型级聚合**
按模型聚合：请求数、成功率、失败指纹 Top 5、p50/p95 的 `ttfbMs`。

**3.4 面板**
新增两个区块：**「每模型健康度」**（模型 × 成功率 × p95 首字节）与**「最近一次请求的耗时分解」**（排队 / 首字节 / 正文三段堆叠条）。样式沿用既有卡片风格，别引入外部资源。

### 验收

新增 `test/test-metrics-observability.mjs`（端口 `9200+` 唯一），断言：

1. **三段耗时存在且量级正确**：mock 上游注入"首字节延迟 300ms + 正文分 3 块每块 100ms + 渠道级排队（`maxConcurrentPerChannel: 1`，并发 2 个请求）"→ 断言 `ttfbMs >= 250`、`bodyMs >= 200`、第二个请求 `queueMs > 0`。
2. **`/api/metrics` 排队时长**：`queueWaitMsMax > 0` 且 `>= queueWaitMsTotal / waitedCount`。
3. **模型级聚合**：造 1 个成功 + 1 个失败 → 该模型成功率 = 0.5，失败指纹出现 1 次。
4. **面板可用且安全**：`/` 返回 200、HTML 含新字段名；**沿用 `test-admin-security.mjs` 的口径**断言面板不泄露明文 key。
5. **脱敏不绕过**：新字段里塞一个 `sk-` 开头的假错误文本 → 落盘副本里是 `***REDACTED***`。

回归门槛：`test-tasklog-rotate`（28）、`test-admin-security`（34）、`test-concurrency`（40）、`test-workbuddy`（69）、`smoke` 全绿。

### 不做

- 不引入图表库 / 外部 CDN（零依赖 + 离线可用是硬要求）。
- 不做历史趋势持久化（只做内存环 + 现有 JSONL）。

---

## P4 — prompt caching + `top_k` + 思考预算策略（`lib/adapters/anthropic.mjs`、`lib/adapters/openai.mjs`）

### 现状（代码事实）

- **`cache_control` 在整个 `lib/` 里零命中**（`grep -r cache_control lib/` 无结果）。Anthropic 官方用它标记可缓存的 prompt 前缀，命中后输入 token 显著便宜。现在这个标记在协议转换时被**直接丢弃**，等于白丢省钱机会。
- `top_k`：`anthropic.mjs:313-315` 只有一句 `delete payload.top_k` 守卫，注释写明"当前 buildRequest 不转发 top_k（客户端即使传了也不会进 payload）"——即**从来不转发**。
- 上一轮 W2 标注的取舍：开 thinking 时，客户端显式写 `max_tokens: 100` 会被**抬到渠道上限**（8192），因为要满足官方 `budget_tokens < max_tokens`。这是当时按任务书做的选择，但业务上未必想要。

### 做什么

**4.1 `cache_control` 往返保真**
- **Anthropic → OpenAI 方向**（`/v1/messages` 进来）：客户端消息里的 `cache_control` 标记必须保留在对应内容块上；若目标是 OpenAI 协议渠道，保留在内部表示里（上游不认识就忽略，但**不能在网关这一层丢**），走到 Anthropic 协议渠道时按原位置重新发出。
- **OpenAI → Anthropic 方向**（把 OpenAI 响应转成 Anthropic 形状）：不涉及 `cache_control`（它是请求侧字段），确认不要误删请求侧标记即可。
- 关键点：**"丢弃"和"透传但上游忽略"是两种语义**，本任务要求后者。加一条统一入口（类似 W2 的 `stripThinkingIncompatible`）集中处理，避免以后又漏。

**4.2 转发 `top_k`**
- `buildRequest` 转发 `body.top_k`（透传到 payload）。
- 开 thinking 时按官方互斥规则清理（`stripThinkingIncompatible` 里已经有 `delete payload.top_k`，保留它作为"开 thinking 时清理"的守卫，但**不许再默认删掉**）。

**4.3 思考预算策略可配**
- 新增 `routing.thinkingMaxTokensPolicy`：
  - `"raise"`（默认，保持现状）：把 `max_tokens` 抬到上限以满足 `budget < max_tokens`。
  - `"drop-thinking"`：客户端给的 `max_tokens` 是硬意图，**宁可不思考也不放大**——日志里 warn 一次说明"客户端 max_tokens 过小，已放弃思考"。
- 默认必须是 `"raise"`，保证不配时行为与现在逐位一致。

**4.4 OpenAI 线格式上必须剥掉 `cache_control`（实现反馈补充，**必须做**）**

P4 实测确认的调用链：`server.mjs:551` 用 `fromAnthropicBody(body)` 把 Anthropic 客户端的请求体转成**内部 OpenAI 形状**，再交给 `handleChatCompletions` 路由（`clientProtocol: 'anthropic'`）。所以当目标渠道是 OpenAI 协议时，`openai.mjs` 的 `buildRequest` 会把这个内部 body 直接铺进上游 payload。

**问题**：带 `cache_control` 的内容块会**原样发给 OpenAI 中转**。这是本次改动新引入的风险面——改之前 `fromAnthropicBody` 会把文本块折叠成字符串（标准 OpenAI 形态），不会有任何非标准字段外泄；改之后"有标记就保留块结构"，标记就随块一起出海了。而用户的主力路径恰好是 23 个 OpenAI 协议渠道 + Claude Code（会发 `cache_control`），所以这条必须堵。

要求（改动都在 `lib/adapters/openai.mjs`）：
- `buildRequest` 组装好 payload 后，逐条 message 处理 `content`：
  - `content` 是数组 → **删掉每个块上的 `cache_control`**（OpenAI 不支持它，发过去零收益、纯风险）。
  - 数组里**全是 text 块** → 折叠成字符串（回到标准 OpenAI 形态）；含图片等非 text 块 → 保持数组（只剥字段、不改结构）。
  - `content` 是字符串 → 不动。
- 这样分工才正确：`cache_control` 只留在**内部表示**里给 Anthropic 协议渠道用；"在网关这一层不丢"和"不把不支持的字段发给上游"是两件事，都要满足。
- 不许动 `stream_options` / `stop_reason` / `reasoning_effort` 逻辑。

### 验收

新增 `test/test-cache-control.mjs` + 扩充 `test-anthropic-thinking-compat.mjs`，断言：

1. **cache_control 不丢**：请求里带 `cache_control: {type:'ephemeral'}` 的内容块，经转换后（a）内容与文本逐字不变（b）标记仍在**同一个块**上（c）标记数量不变。
2. **多标记**：两个块各带一个标记 → 转换后仍是两个。
3. **`top_k` 能到达 payload**：客户端传 `top_k: 40` → payload 里有 `top_k: 40`。
4. **开 thinking 时 top_k 被清**：`reasoning_effort: 'high'` + `top_k: 40` → payload 无 `top_k`，且 `temperature`/`top_p` 同时被清（既有断言不许退化）。
5. **策略 `raise`（默认）**：`max_tokens: 100` + 渠道上限 8192 + high → `{max_tokens: 8192, budget_tokens: 7168}`（与当前逐位一致，**这是回归锁**）。
6. **策略 `drop-thinking`**：同上输入 → 无 `thinking` 字段、`max_tokens` 保持 100（不放大）。
7. **不泄漏到 OpenAI 线格式**（§4.4）：拿带 `cache_control` 的内部 body 交给 `openaiAdapter.buildRequest` → 断言 `JSON.stringify(payload)` 里 **`cache_control` 出现 0 次**；且纯文本块被折叠成字符串、含图片的块仍是数组且文本/图片内容逐字不变。
8. **不误伤内部表示**（§4.4 的反向锁）：同一个内部 body 走 `anthropic.mjs` 路径时标记**仍在**（数量与块位置不变）——即"剥的是 OpenAI 线格式，不是内部表示"。

回归门槛：`test-anthropic-thinking-compat`（45）、`test-anthropic-reasoning`（23）、`test-openai-compat`（50）、`test-effort-levels`（21）、`test-effort`（16）、`test-rewrite`（12）、`test-toolcall-guard`（16）、`smoke`（28）全绿。

### 不做

- 不实现 `cache_control` 的**自动注入**（只做保真透传，不替用户猜哪里该加缓存）。
- 不改 OpenAI 侧的 `stream_options` / `stop_reason` 逻辑（上一轮刚验过，别碰）。

---

## P5 — 测试运行器 + 动态端口 + `package.json`（**波次 2**）

### 现状（代码事实）

- **没有 `package.json`**、**没有测试运行器**、没有 CI。
- `test/` 下一批 `test-*.mjs`（**以你实际看到的文件数为准**，P1–P4 可能已各新增套件），每个自己 `spawn` 网关/mock 并**硬编码固定端口**（`8793–8813`、`8877–8878`、`9101–9184`、`9200+` 等）。本轮并行执行时**至少 3 次假失败**是抢端口造成的，排查成本很高。

### 做什么

1. 新增 `package.json`：`"type": "module"`（必须，否则 `.mjs` 语义变了）、`scripts`: `test` / `test:one` / `start`；`"private": true`；**不写 dependencies**。
2. 新增 `test/lib/ports.mjs`：`freePort()`（用 `net.createServer().listen(0)` 拿一个空闲端口后关闭，带重试）。允许各测试按需组合（例如"连续拿 3 个"）。
3. 把 `test/` 下**全部** `test-*.mjs` 里的硬编码端口替换为 `freePort()`（数量以实际为准，包含 P1–P4 新增的套件）。注意：网关与 mock 上游、以及测试里**断言过的 URL**都要一起改。
4. 新增 `test/run-all.mjs`：按文件名**串行**跑全部套件（串行是为了让输出可读、并避免新引入的动态端口在极端情况撞车），实时打印每个套件的 `结果:` 行，结尾汇总 `N 通过 / M 失败` 并以失败数作为退出码。
5. `README.md` 的测试章节补一句"`node test/run-all.mjs` 一把跑完"。

### 验收

1. `node test/run-all.mjs` → 全部套件 `0 失败`，退出码 0。
2. **连跑两遍**都全绿（证明端口不再泄漏/冲突）。
3. `npm test` 与上面等价（若环境没有 npm 则说明并跳过，不算失败）。
4. `node test/run-all.mjs` 的汇总数字与手动逐个跑一致。
5. 至少抽查 3 个被改过的测试，确认它们**单独**跑也全绿（防止只在 runner 里成立）。

### 不做

- 不引入 jest/vitest/mocha 等框架（保持零依赖 + 直接 `node` 可跑）。
- 不改任何测试的**断言语义**（只动端口与装配方式）。

---

## P6 — 出站代理的 abort 兑现 + CONNECT/TLS 覆盖（`lib/outbound-proxy.mjs`）（**波次 2**）

### 现状

P2 在 §2.3 测出一个**真实挂死**，并已在 `lib/proxy.mjs` 侧绕过：`lib/outbound-proxy.mjs` 的 `roundTrip` 在**响应头解析完成那一刻**就 `signal.removeEventListener('abort', onAbort)`，`openTunnel` 的 abort 监听也在返回时移除。于是走 `channel.proxy` 时看门狗准时 abort，但 body 读取**永不返回** → 请求挂死 + 并发许可泄漏（实测 `status=0 15003ms`）。P2 的 `watchdogBody()` 让请求不再挂死、许可已归还，但 **abort 传不到 socket**，被中断的代理请求其上游 socket 要等对端关闭才回收。

P2 未改此文件（不在其归属内，且已明确声明），并留下一个未覆盖点：`https` 目标的 **CONNECT + TLS 隧道分支没有验证**（本机无可信 CA 的 TLS 上游）。

### 做什么

1. `roundTrip` / `openTunnel`：**让 abort 在响应头之后依然可达** —— 摘监听改为在 body 结束/出错/销毁时摘；abort 时让读流以 abort 错误收场并回收底层 socket。不得改变既有返回契约（P2 的 33 条断言锁着）。
2. 新增 `test/test-outbound-proxy-abort.mjs`：
   - HTTP/1.1 明文经代理：abort 后代理侧 socket 被回收（用 socket 计数或 `close` 事件断言，别只看请求结束）。
   - **CONNECT + TLS 隧道**：用自签证书起 https mock 上游，经 `NODE_EXTRA_CA_CERTS`（或等效方式）让网关照常校验，验证隧道分支下首字节/空闲看门狗同样生效、abort 后 socket 回收。
3. 端口用 **9300-9310**（唯一；勿与 8793–8813 / 8877–8878 / 9101–9184 / 9251–9252 / 9270–9281 冲突）。

### 验收

1. 新增套件全绿且**非空**：临时 revert 你的修复 → 必须有红灯；还原后逐字节一致再全绿。
2. P2 的 `test-proxy-timeouts.mjs`（33 条）**一条不许退化**。
3. `test-proxy-hardening`（47）、`smoke`（28）、`test-concurrency`（40）全绿。
4. 汇报里写清 CONNECT+TLS 分支到底验到了什么、以及仍然没验到什么。

### 不做

- **不改 `lib/proxy.mjs`**（P2 的 `watchdogBody` 已完成请求级修复，别重复劳动、更别两处互相掩盖）。
- 不做 HTTP/2 适配。

---

## 3. 完成定义（Definition of Done）

对每个工作流：

1. 文件归属内的改动完成，且**没有越界改动**。
2. 新增验收测试全部通过，并贴出**真实输出**。
3. 回归门槛里点名的既有套件**逐个跑过**且 0 失败。
4. 汇报里明确写出：改了哪些文件、每条改动的验证命令与输出、**没能验证的部分**。
5. 收尾清理临时文件与后台进程。

整体完成：P1–P4 全部落地且测试全绿后再做 P5；**P6 与 P5 文件不重叠，可并行**。P5 完成时，`node test/run-all.mjs` 一把全绿且可重复。

## 4. 变更记录

- v1：初版。基于本轮修复后的代码基线（32 套件 / 815 断言全绿）编写。
- v2：排队等待统计（`queueWaitMs*`）从 P3 移到 P2 §2.4 —— 避免 P2/P3 抢 `lib/concurrency.mjs`。
- v3：新增 **P2 §2.5**（三段耗时采集，含**成功路径**）+ 验收第 6 条。P3 反馈的跨文件缺口：`ttfbMs/bodyMs/queueMs` 在 `lib/proxy.mjs` 里**没有数据源**，且成功路径原本 `attempts` 为空、`finish()` 无耗时字段。
- v4：新增 **P4 §4.4** + 验收第 7、8 条 —— 在 **OpenAI 线格式**上剥掉 `cache_control`。起因：`server.mjs:551` 用 `fromAnthropicBody` 把 Anthropic 请求体转成内部 OpenAI 形状，标记会随 body 发给 23 个 OpenAI 协议渠道；发过去零收益、纯风险。
- v5：P1 交接 —— 我补写 `config.example.json` 的 `priorityWeights` / `halfOpen`；`requestId` 接线派给 P2。
- v6：P2 交接 —— 我补写 `config.example.json` 的 `firstByteTimeoutMs` / `writeDrainTimeoutMs`。
- v7：新增 **P6**（出站代理 abort 兑现 + CONNECT/TLS 覆盖），原因是 P2 在 §2.3 发现 `outbound-proxy.mjs` 在响应头后摘掉 abort 监听导致挂死/许可泄漏，而该文件无归属。
- **口径确认（P2 §2.2）**：`waitDrain` 自超时判为 `kind:'client_abort'`（499、retryable=false），**不计入渠道失败** —— 上游没做错，是客户端不读。§2.2 原文"渠道失败 + 客户端已断开"指的是"本次请求记为失败、且客户端已断开"，**不是**给渠道同时打两个标记。
- v8（收口）：P1–P6 全部落地。`node test/run-all.mjs` = **37 套件 / 1031 断言 / 0 失败**（P5 连跑两遍，我本人再独立复跑一次，exit 0）。P5 交付 `package.json`（零依赖）+ `test/lib/ports.mjs` + `test/run-all.mjs`，并把全部套件迁到动态端口；P6 修掉 `lib/outbound-proxy.mjs` 的 abort 根因（P2 测出的挂死/许可泄漏）并补上 CONNECT+TLS 覆盖。
- 收口期我本人做的三件事：① 修好 P4 断气留下的半成品测试文件（语法错误 + 3 条与 fixture 自相矛盾的断言）→ `test-cache-control.mjs` 51/0；② **独立端到端验证 §4.4**：Anthropic 客户端带 `cache_control` 打到 OpenAI 协议渠道，断言 mock 上游收到的**真实 HTTP body** 里零标记、文本逐字保真；③ 修掉 `test-rewrite.mjs` 跑完不删 `rewrite-tmp.test.json` 的卫生问题（原先每跑一次测试就在仓库里留一个生成物）。
- 与本轮无关、但值得记下的环境事实：用户机器上 `lghub_updater`（罗技 G HUB）**占着 9100/9180**，而 9180 正落在旧的硬编码测试区间（9101-9184）内 —— 也就是说旧套件理论上会和用户的系统软件抢端口。这是 P5 动态端口迁移价值的具体证据。
