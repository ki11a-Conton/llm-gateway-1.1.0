**本地 LLM 中转站代码审查 — 2026-09-20**

这份代码目前最需要完善的是流式结果判定、协议转换和请求生命周期。在本次审查中，确认了 **8 类代码问题，并通过 9 个本地模拟场景复现**。其中，流式失败被记录为成功、并行工具调用被破坏，会直接影响 agent 的任务结果，建议优先处理。

审查对象是你上传的 `7092d0cf-8154-4133-b8aa-0a39ed9dd3f2.zip` 中的 `llm-gateway/`。原压缩包 SHA-256 为 `b33ec0174b66416da0bd1b61bfa1a21dccfe1dfcc396fd59bd9f30b82772b150`。下文所有代码行号对应这一份快照。

本次检查覆盖了 `server.mjs`、转发与重试、渠道选择、并发控制、OpenAI/Anthropic 流式转换、用量采集，以及相关测试。现有 **39 个测试套件、1100 条断言逐项通过**；新增复现脚本中的 9 个场景均观察到了缺陷。测试运行于 Linux、Node.js v24.19.0，使用回环地址模拟上游和假密钥；没有验证真实供应商账号或 Windows 运行环境。压缩包中的原始文件保持不变。

关于测试证据：第一轮总运行器日志完整记录了前 30 个套件，在第 31 个套件期间停止记录；随后单独执行第 31—39 个套件，均返回退出码 0。这里的“39 个套件通过”是逐项汇总结果，并不表示拿到了单次不间断 `npm test` 的最终汇总。具体结果在附件 `baseline-summary.json` 和两份基线日志中。

| 编号 | 优先级 | 已确认的问题 | 直接影响 |
| --- | --- | --- | --- |
| F1 | P1 | 流式错误或缺失终止信号仍被当成成功 | agent 接收半截回答，成功率和渠道健康状态失真 |
| F2 | P1 | OpenAI → Anthropic 转换无法正确处理交错的并行工具调用 | 工具参数残缺，产生多余的 tool_use 块 |
| F3 | P1 | `maxTotalWaitMs` 没有实现请求总时限 | 排队、生成和读取耗时可超出配置预算 |
| F4 | P1 | embeddings 未接入超时与客户端取消 | 已取消请求继续占用共享并发名额 |
| F5 | P2 | 正则采集漏掉带嵌套对象的 usage | 成功调用的 Token 和请求数漏记 |
| F6 | P1 | 等待 agent 配额时预占全局和渠道名额 | 一个繁忙 agent 阻塞其他 agent |
| F7 | P2 | 并发配置热重载丢失在途计数 | 调低上限后仍放行新请求，指标低报 |
| F8 | P2 | 半开并发上限只影响排序 | 故障渠道恢复时被多个请求同时试探 |

P1 表示影响核心正确性或可用性，建议先修；P2 表示统计或特定条件下的行为错误。优先级按你目前本地自用、多 agent 调用的场景评估。

**F1 · P1：上游失败可能被包装成正常结束。**

位置：`lib/adapters/anthropic.mjs:277`、`lib/adapters/anthropic.mjs:291`、`lib/proxy.mjs:1138`。

Anthropic 转 OpenAI 的流式适配器捕获 `MID_STREAM` 错误后，仅调用日志回调，随后生成 `finish_reason: "stop"` 和 `[DONE]`。其他读取异常也会被这个 catch 吞掉并补正常结束帧。遇到未按协议结束的 EOF，后面的兜底分支同样补结束帧。于是路由层看到的是正常返回，会执行 `channel.markSuccess()`、更新成功渠道记忆并记录 `ok: true`。

同协议透传还有另一条路径：`proxy.mjs` 的流读取循环正常退出后，没有统一检查是否收到有效终止信号，直接返回成功。HTTP 响应体读完，只能说明传输结束，不能证明模型已经完成输出。

**实际复现：**

- 模拟 Anthropic 上游先发部分正文，再发 `overloaded_error`：客户端收到 HTTP 200 和 `[DONE]`，上游错误文本消失，任务日志仍为 `ok: true`。
- 模拟 OpenAI 上游只发正文增量就结束 HTTP body，没有 `finish_reason`、usage 尾帧或 `[DONE]`：任务日志仍为成功。

这会让 agent 将中断内容视为完整结果，同时让故障渠道继续获得“成功过”的路由优先待遇。复现标识为 `F1_swallowed_anthropic_error` 和 `F1_missing_terminal`。

**修复建议：**

1. 在原始上游事件层跟踪有效终止事件与错误事件，将协议完成状态交给转发层；不要通过“生成一个 stop”消除上游异常。
2. 响应尚未向客户端提交时，按错误性质决定是否切换渠道；已经提交时，按下游协议报告流内错误或终止连接，并记录失败，禁止拼接另一家供应商的回答。
3. 统一覆盖同协议透传、双向协议转换和强制流式聚合路径，保留现有客户端取消分类。

**验收：**上游流内报错、无终止信号的 EOF、socket 中断、idle 超时分别测试；这些请求不能更新成功渠道记忆。正常终止的短回答、工具调用和带 usage 的流仍正确结束。应允许已经输出有效 `finish_reason`、但缺少最后 `[DONE]` 的兼容上游，具体规则写进测试。

可参考 [Sub2API 的原始流终止状态检查](https://github.com/Wei-Shaw/sub2api/blob/1a9d49e16f7a22c432b428fce4af8d731f1fa364/backend/internal/service/openai_raw_stream_truncation.go)：它单独记录终止信号，并为未开始下发与已开始下发的截断分别处理。这里值得借鉴的是“协议完成状态与 HTTP 结束状态分开判断”。

**F2 · P1：两个并行工具调用会被转换成四个残缺调用。**

位置：`lib/adapters/openai.mjs:281`，尤其是 `current.toolIndex !== tcIndex` 分支。

当前转换器只保存一个 `current` 内容块；当工具 index 改变时，就关闭当前块并创建新块。然而 OpenAI 允许两个工具的参数增量交错到达，例如第一帧同时包含 index 0、1，第二帧继续补 index 0、1。第二帧的增量通常不再带完整 id 和函数名。

**实际复现：**输入两个合法工具调用，参数分别为 `{"q":"a"}`、`{"q":"b"}`；输出变成四个 `tool_use` 块。前两个只保留 `{"q":`，后两个函数名为空、id 被重新生成，参数分别只有 `"a"}` 和 `"b"}`。复现标识为 `F2_parallel_tools`。

该问题发生在 OpenAI 上游 → Anthropic 客户端的流式转换方向，会导致工具参数解析失败或额外工具调用。

**修复建议：**

1. 为每个工具保存独立状态，如 `Map<toolIndex, { id, name, blockIndex, pendingArguments, started, stopped }>`，必要时再按 id 建索引。
2. 同一 index 的所有参数增量必须进入同一内容块，不能因其他工具插入而重新创建工具。对 id/name 分片或迟到的情况先缓存所需元数据。
3. 根据下游协议组织合法的块生命周期；若选择缓冲后顺序输出，需要明确延迟与内存上限。

**验收：**两个和三个工具交错增量、一个工具跨多帧、id/name 后到、工具与正文相邻等用例。最终工具数量、id、名称和 JSON 参数与上游一致，每块只开始和结束一次。

可参考 [New API 的 OpenAI → Claude 流式转换](https://github.com/QuantumNous/new-api/blob/972aed1972820389ea0b603ca58f03f846fbf790/relaykit/relayconvert/internal/oai_chat/to_claude_messages_resp.go)：实现使用 `ToolCallByIndex`、`ToolCallByID` 和各工具的待输出参数状态，能够保持交错工具增量的归属。

**F3 · P1：总超时预算只检查重试等待时间。**

位置：`lib/proxy.mjs:395`、`lib/proxy.mjs:655`、`lib/proxy.mjs:660`，以及 `attemptChannelInner()` 的计时器清理逻辑。

`maxTotalWaitMs` 最终比较的是 `waitedMs`，而这个变量主要累加重试之间的等待，没有累计并发排队和上游生成耗时。检查还放在 `if (!retryLoop) break` 之后：不开整池循环重试时，这个检查无法生效。

同时，`channel.timeoutMs` 对应的 `headerTimer` 在取得响应头后就被清除；后续首字节和 idle 看门狗都无法限制持续有数据到来的长流。配置注释中的“总预算”和实际行为不一致。

**实际复现：**设置 `maxTotalWaitMs=70`、`timeoutMs=250`，上游每 30ms 发一个 chunk，因此不会触发 idle 超时；请求在约 **367ms** 后返回成功，越过两项配置时长。复现标识为 `F3_total_deadline`。

**修复建议：**

1. 建立请求级绝对 deadline，例如 `startedAt + maxTotalWaitMs`，从统一入口贯穿到请求收尾。
2. 将同一个预算/取消信号传给并发排队、单次上游请求、body 读取、背压等待和重试等待；各阶段用剩余时间限制自身等待。
3. 区分总预算到期、单次首字节超时、idle 超时与客户端取消；总预算到期不应继续切渠道。保留配置为 0 时允许不限总时长的明确语义。

**验收：**排队超预算、单次持续流超预算、渠道内重试超预算、跨渠道切换超预算均能收尾并释放名额；响应已经提交时使用合法的流式失败处理。定时测试允许合理的事件循环误差。

**F4 · P1：embeddings 没有接入超时和取消。**

位置：`server.mjs:627`、`server.mjs:640`、`server.mjs:652`、`server.mjs:658`。

这个入口申请了全局/渠道/agent 并发许可，但 `fetch()` 没有传取消信号，`r.text()` 也没有接入聊天路径的 body 看门狗，且没有监听客户端关闭来终止上游。`finally` 中虽然写了 release，但只有前面的 await 完成或抛错，才会走到释放逻辑。

**实际复现：**模拟上游只发 HTTP 200 响应头，正文一直不结束。配置超时 250ms，观察超过 350ms 后，全局 active 仍为 1；随后取消客户端请求，再等待 150ms，active 仍为 1，上游连接没有关闭。复现标识为 `F4_embedding_no_timeout_or_cancel`。

这里没有声称 Node 的底层传输永远不会自行超时；已确认的是网关配置的时限和客户端取消都没有覆盖此入口。持续积累这类请求，会占满与聊天共用的名额。

**修复建议：**抽出聊天与 embeddings 可共用的请求生命周期组件，让 embeddings 的排队、fetch、body 读取和客户端断开都接入同一套取消机制，设置有界响应读取，并在取消后停止后续渠道尝试。

**验收：**不回响应头、只回响应头、正文中途停住、客户端在排队时取消、客户端在读取时取消，分别验证按期结束；最终在途/等待计数回到初始值，上游连接得到取消，后续聊天能继续执行。直连和配置出站代理两种路径均覆盖。

**F5 · P2：带嵌套字段的 usage 整体漏记。**

位置：`lib/proxy.mjs:37`、`lib/proxy.mjs:59`。

当前使用 `/"usage"\s*:\s*\{[^{}]*\}/g` 在原始字节中寻找 usage。`[^{}]*` 明确排除了内部对象，所以只要 usage 含 `prompt_tokens_details`、`completion_tokens_details` 等嵌套对象，这个匹配就失败；外层本来存在的 prompt/completion 总数也随之丢失。

**实际复现：**一个普通 usage 请求记录为输入 100、输出 20；另两个同样返回输入 100、输出 20 的成功请求，其中一个非流式、一个流式，仅多了嵌套 details 字段，却完全没有进入统计。三个请求应累计 360 token，面板接口实际只返回 120。复现标识为 `F5_nested_usage`。

**修复建议：**非流式直接从已解析 JSON 中读取 usage；流式在完整 SSE 事件解析后，从协议规定的位置读取，避免在任意正文里用正则搜索字段。统计继续取自原始上游事件，防止转换器补的 0 污染来源。缓存和 reasoning 明细若后续展示，应作为独立字段保留，避免重复加进已有总数。

**验收：**普通 usage、嵌套 details、分 chunk 的事件、较大的 usage 对象、Anthropic 多事件用量、没有 usage 的响应，以及正文中谈论 usage 的内容。输入/输出总数准确、请求数无遗漏或重复，新增解析不改变客户端收到的内容。

当前实现定位为“成功交付请求的 Token 用量”，不是完整上游成本账本。即便修复本条，也需要单独定义失败尝试、部分输出、缓存读写等口径，才能进一步用于成本对账。

**F6 · P1：一个 agent 的排队请求占住其他 agent 的执行容量。**

位置：`lib/concurrency.mjs:249`，许可申请顺序为 global → channel → agent。

当 agent 自己的额度已满时，新请求已持有 global/channel 名额，再等待 agent 名额。其他 agent 随后到达，就可能被预占的全局容量挡住。

**实际复现：**全局上限 2、渠道上限 2、单 agent 上限 1。A 的第一条请求运行；A 的第二条请求排队，但持有第二个 global 名额。B 向另一个空闲渠道申请执行时无法进入。此时实际只有一条上游请求，统计中的 global.active 已为 2。复现标识为 `F6_agent_starvation`。

**修复建议：**为本地单进程网关建立统一准入调度：只有 global/channel/agent 都可用时，才同步占用三个名额；不可用时只进入等待队列。取消或超时应能移除等待项。先拿 agent 再拿 global 可缓解本例，但仍需考虑不同渠道互相占位的问题，单纯交换申请顺序不足以完成完整隔离。

**验收：**A 持续提交积压请求时，B 向空闲渠道的请求仍能利用剩余容量；等待请求不计为执行中的全局/渠道负载。验证取消、超时和重复释放不残留名额，队列设置明确上限。

可参考 [Sub2API 的并发服务接口](https://github.com/Wei-Shaw/sub2api/blob/1a9d49e16f7a22c432b428fce4af8d731f1fa364/backend/internal/service/concurrency_service.go) 将用户/账号执行槽位和等待数量分开管理，并对等待数量提供 maxWait 边界。这个参考说明职责拆分方式；本地版本可以用内存结构实现。

**F7 · P2：热重载重建信号量，旧请求从计数中消失。**

位置：`lib/concurrency.mjs:201`、`lib/concurrency.mjs:208`。

修改全局并发上限时，会新建 `Semaphore`；修改渠道上限时，会清空已有 channels Map。正在执行的请求仍持有旧信号量的 release，而新信号量从 active=0 开始，无法看到旧请求。

**实际复现：**已有 2 条在途请求，把全局和渠道上限从 2 调到 1，第三条请求仍立刻获得许可；实际已获准的请求数量为 3，新 metrics 只报告 active=1。复现标识为 `F7_reload_limit`。

**修复建议：**保留同一信号量实例及其 active 状态，调整上限后重新调度队列。降低上限时允许旧请求完成，但在 active 未降到新上限以下之前不再放行新请求；提高上限时及时唤醒等待者。仅改成调用当前 `setLimit()` 还不够，当前 `_handoff()` 也需要按新上限决定能否移交名额。

**验收：**2→1、1→3、有限→不限→有限，以及全局与渠道分别变更。新请求准入符合最新上限，旧请求释放后计数正确，排队请求不会丢失或重复执行。

**F8 · P2：半开限额没有约束真正的上游请求。**

位置：`lib/channels.mjs:913`、`lib/channels.mjs:921`、`lib/channels.mjs:935`，以及 `proxy.mjs` 取渠道后的准入路径。

`#halfOpenSaturated()` 能识别已占满的半开渠道，但排序函数会把这些渠道放到候选末尾。转发层遍历到它们时，没有申请半开专用许可；只有这一家渠道、或前面的渠道都失败时，仍会继续调用。并发请求也可能在前一个请求登记在途前拿到相同的候选快照。

**实际复现：**让唯一渠道先失败进入冷却，冷却到期后设置 `halfOpenMaxInFlight=1`，同时发 3 个请求；模拟上游实际观察到峰值并发 **3**。复现标识为 `F8_half_open_limit`。

**修复建议：**在真正发出上游请求前，检查并同步占用半开试探名额；达上限时选择其他可用渠道，或受预算约束地等待。试探成功/失败要使状态转换与名额释放一致。候选排序保留为偏好策略，准入阶段负责强制上限。

**验收：**仅一个半开渠道时，多个并发请求也不能突破试探上限；有健康备用渠道时多余请求转移；显式 routes、tiered、priority、weighted 等路由方式均不能绕过同一个准入检查。

**参考项目中值得采用的具体做法。**

参考源码于本轮初始审查时读取，以下链接固定在当时的提交，避免默认分支变动导致复核位置漂移。并未执行这两个参考项目的完整测试，也不据此声称它们在所有场景都正确。

| 参考实现 | 本次实际核对的机制 | 对当前代码的用途 |
| --- | --- | --- |
| [New API：工具流转换](https://github.com/QuantumNous/new-api/blob/972aed1972820389ea0b603ca58f03f846fbf790/relaykit/relayconvert/internal/oai_chat/to_claude_messages_resp.go) | 按工具 index/id 保存多个独立状态，缓存尚未可输出的参数 | 修 F2 的交错工具调用 |
| [Sub2API：流终止检查](https://github.com/Wei-Shaw/sub2api/blob/1a9d49e16f7a22c432b428fce4af8d731f1fa364/backend/internal/service/openai_raw_stream_truncation.go) | 记录终止信号，显式处理上游截断 | 修 F1 的假成功与错误收尾 |
| [Sub2API：并发服务](https://github.com/Wei-Shaw/sub2api/blob/1a9d49e16f7a22c432b428fce4af8d731f1fa364/backend/internal/service/concurrency_service.go) | 执行槽位、等待数量和释放职责分开表达 | 改善 F6—F8 的准入与生命周期设计 |

建议继续保留当前 Node.js 架构，先把这些机制补完整。修复顺序可以分为三批：第一批 F1/F2，保证回答与工具调用正确；第二批 F3/F4/F6，保证请求有界且取消后能释放容量；第三批 F5/F7/F8，补齐统计、热重载和故障恢复。每批添加对应反例测试，再运行现有回归套件。

**复现材料使用方法。**

压缩包内包含本报告、`reproduce-review.mjs`、`reproduction-results.jsonl`、`baseline-summary.json` 和两份基线测试日志。复现脚本只启动本地 mock，不修改待审查项目；临时配置和假密钥在脚本中生成并清理。

将脚本放在任意位置，在已安装 Node.js 的终端执行：

```text
node reproduce-review.mjs "你的 llm-gateway 文件夹绝对路径"
```

Windows 示例：

```powershell
node .\reproduce-review.mjs "D:\你的目录\llm-gateway"
```

本轮在 Node.js v24.19.0 下运行。脚本输出 `reproduced: true` 表示成功观察到缺陷；最后 `demonstrated: 9, checks: 9` 表示九个缺陷场景都被复现，**不代表代码通过正确性测试**。修复时，应把相应场景转换为断言正确行为的正式回归测试。由于脚本主要服务于本次快照，修复后的错误返回方式变化可能使它提前退出，应以报告中的验收条件为准。

---

## 修复记录（2026-09-20 晚，按本报告的三批顺序逐条落地）

判定依据：每条都先写**能复现缺陷的回归测试**（在修复前先确认它变红），再改代码，最后跑全量回归。
未采信任何"已完成"的口头结论。

| 编号 | 状态 | 改动位置 | 守门测试 |
| --- | --- | --- | --- |
| F1 | ✅ 已修 | `lib/adapters/anthropic.mjs`（MID_STREAM/读流异常/无终止信号 → 打 `STREAM_TRUNCATED` 抛出，不再伪造 `stop`+`[DONE]`）、`lib/adapters/openai.mjs`（新增 `sawDone` + 无终止信号抛错，兼容"有 finish_reason 无 [DONE]"）、`lib/proxy.mjs`（`createTerminalTracer` 逐块追踪终止信号；透传路径结尾校验；`emitStreamError` 按下游协议下发流内错误帧；`stream_truncated` 分类；兼容上游补终止帧） | `test/test-stream-integrity.mjs`（15 条） |
| F2 | ✅ 已修 | `lib/adapters/openai.mjs` `streamToAnthropic`：按 `Map<toolIndex,{id,name,args}>` 缓冲，收尾按 index 顺序一次性输出工具块，交错增量不再被拆成多余残缺块 | `test/test-parallel-tools.mjs`（14 条） |
| F3 | ✅ 已修 | `lib/proxy.mjs`：请求级绝对 `deadlineAt` 贯穿排队/单次上游/读体/背压/重试等待；新增 `budgetAbort` 中止器与 `total_deadline` 分类；`maxTotalWaitMs=0` 明确表示不限；`lib/concurrency.mjs` `acquire(..., budgetMs)` 让排队等待也受限 | `test/test-total-deadline.mjs`（10 条） |
| F4 | ✅ 已修 | `server.mjs` `handleEmbeddings`：接入客户端断开（`res.on('close')`）+ 请求级超时 `embeddingsTimeoutMs`（回退 providerTimeoutMs/timeoutMs），signal 约束 fetch 与正文读取；取消后不再尝试其它渠道；`finally` 保证释放 | `test/test-embeddings-lifecycle.mjs`（9 条） |
| F5 | ✅ 已修 | `lib/proxy.mjs`：`/"usage"\s*:\s*\{[^{}]*\}/g` → 括号配对扫描 `findUsageObjects`（支持嵌套 details、跳过被转义的 `\"usage\"`），仍只采上游原始字节 | `test/test-usage-nested.mjs`（7 条） |
| F6 | ✅ 已修 | `lib/concurrency.mjs` `acquire`：改为**先拿 agent 名额**再抢 global/channel，积压请求不再预占共享容量 | `test/test-agent-admission.mjs`（10 条） |
| F7 | ✅ 已修 | `lib/concurrency.mjs`：`configure()` 保留 global/渠道信号量实例（改上限不再重建/清空）；`setLimit` 调高时 `_pump()` 唤醒等待者；`_handoff` 按新上限决定能否移交；新增 `_pump()` | `test/test-reload-concurrency.mjs`（11 条） |
| F8 | ✅ 已修 | `lib/channels.mjs`：新增 `halfOpenMaxInFlight` / `tryAcquireHalfOpen()` / `releaseHalfOpen()`；`lib/proxy.mjs` `attemptChannel` 在发上游请求前强制准入（`half_open_saturated` 不计渠道故障） | `test/test-half-open-limit.mjs`（9 条） |

**回归结果**：本报告快照基线为 39 套件 / 1100 条断言；修复后为 **49 套件 / 1317 条断言 / 0 失败**
（新增 8 个套件、85 条断言，全部针对本报告的复现场景）。

**测试期发现的额外真实副作用（已一并修掉）**：被半开窗口挡下的请求原先会被计成渠道故障，
把正在恢复的渠道重新打回冷却——`half_open_saturated` 现在与 `client_abort` / `overloaded` 一样豁免熔断计数。

**仍未覆盖 / 需注意**：复现脚本 `reproduce-review.mjs` 未在工作区内（随原 zip），
本轮的复现是按其报告描述的机制自行重写的；
F3 的"跨渠道切换超预算"与 F6 的"取消排队项"只覆盖了主要路径，未穷举所有路由方式组合。
