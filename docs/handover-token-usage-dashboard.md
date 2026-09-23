# 交接文档：Token 用量仪表盘

> 交付日期：2026-09-18 · 对应设计文档：[`docs/superpowers/specs/2026-09-17-token-usage-dashboard-design.md`](superpowers/specs/2026-09-17-token-usage-dashboard-design.md)
>
> **一句话结论**：设计文档 §4.1（采集）/ §4.2（持久化）/ §4.3（API）/ §4.4（面板）/ §4.5（测试）**功能已全部落地并验证**；§7.1 落盘脱敏断言与 §7.2 上游 400 冒烟回归已在 `test/test-usage.mjs` 中补齐；§7.3 卡片「请求」口径差异确认为已知架构限制（详见 §7）。

---

## 1. 交付物清单

| 文件 | 状态 | 职责 | 关键位置 |
|---|---|---|---|
| `lib/usage.mjs` | **新增**（393 行，零依赖） | Token 用量持久化与聚合：按天 JSONL + 按天聚合缓存（LRU）+ `today`/`dN` 两种时间口径 + 保留清理 | `localDayKey` L31、`UsageStore` L146、`#loadDay` L201、`#partialSince` L228、`record` L272、`query` L310、`queryAll` L331、`prune` L338、`startTimers` L362、`configureUsage` L380、`getUsage` L386 |
| `lib/proxy.mjs` | 改动（+约 60 行） | 采集：上游原始字节旁路扫描 + 出站强制 `stream_options.include_usage` + 成功交付后记账并写入 tasklog | 导入 L26、`createUsageScan` L50、`tapUsage` L79、成功记账 L534–L558、`include_usage` L767、挂扫描器 L933–L934、非流式返回 L1008、流式返回 L1144 |
| `server.mjs` | 改动（+约 40 行） | 用量库装配（含热重载/关闭落盘）+ `GET /api/usage` + 启动信息行 | 导入 L23、`usageOptions` L180、`applyUsage` L192、启动装配 L200、`/api/usage` L379–L384、`/api/reload` L408、关闭 L722–L723、启动打印 L769、配置热重载 L787 |
| `public/index.html` | 改动（+约 90 行） | 「Token 用量」区块：五张时间段卡片 + 按模型柱状图（DOM API 渲染，无 `innerHTML`） | 区块 HTML L189–L191、挂轮询 L382、渲染逻辑 L552–L626 |
| `test/test-usage.mjs` | **新增**（693 行，含 §7.1/§7.2 断言） | 验收套件：单元 + 集成 + 面板 + 安全 + 落盘脱敏 + 上游 400 冒烟，端口动态分配，落盘全部指向临时目录 | 单元段 L269 起、集成段 L351 起、§7.1 落盘断言 L461、§7.2 冒烟回归 L486、面板段 L573 |
| `README.md` | 改动 | 文档化 `/api/usage`、`taskLog.usageKeepDays`、记账口径与已知边界、自测基线 | §2.2「Token 用量」、§4 接口表、§5 面板、§7 基线 |

**运行时产物**：`logs/usage/YYYY-MM-DD.jsonl`（`logs/` 已在 `.gitignore` 中）。

---

## 2. 数据流（一条请求的用量是怎么落库的）

```
客户端 → server.mjs /v1/chat/completions
       → lib/proxy.mjs handleChatCompletions（选路/重试/并发闸门）
       → attemptChannelInner：
           ① 出站 payload 定型时：OpenAI 协议渠道且 stream=true → 强制 stream_options.include_usage=true   (L767)
           ② 上游响应体外面套一层字节透明的旁路扫描器 tapUsage(bodyStream, usageScan)                    (L933-934)
           ③ 非流式读完 / 流式读完后，只要成功交付就 return { usage: usageScan.result() }                 (L1008 / L1144)
       → 路由层成功分支（L534-558）：
           · usage 非空 → finish({ ...usage: { input_tokens, output_tokens } })  ← 进任务日志（含落盘脱敏副本）
           · usage 非空 → getUsage().record({ ts, model, channel, input, output })
       → lib/usage.mjs record → 内存按天聚合增量 + 串行异步追加 logs/usage/<本地日期>.jsonl
       → 面板/接口：GET /api/usage → UsageStore.queryAll() → { today, d1, d7, d30, d90 }
```

### 2.1 采集口径（**最容易踩错的地方**）

- **只认上游真实回报的 usage**。上游没回 usage，或回了但全是 0 → 扫描器 `usageScan.result()` 返回 `null` → **不产生任何记录**。绝不估算、绝不记成"用了 0 token"。
- 失败 / 客户端中止的请求不产生 token 记录（请求数照常进任务日志）。
- 两套字段名都认：OpenAI `prompt_tokens`/`completion_tokens`、Anthropic `input_tokens`/`output_tokens`；同一字段取出现过的**最大值**（Anthropic 的 input 在 `message_start`、output 在 `message_delta`，分帧到达）。

> ### ⚠️ 与设计文档 §4.1 的**刻意偏离**（务必知晓）
> 文档写"从**最终交付形态**的响应体里取 usage（适配层已把 usage 嵌入转换结果，避免重复解析）"。
> **实际实现改扫上游原始响应字节**，原因：两个适配器在转换时会把**缺失的 usage 兜成 0**
> （`lib/adapters/openai.mjs` 的 `toOpenAIResponse`/`toAnthropicResponse`、`lib/adapters/anthropic.mjs` 的 `toOpenAIResponse`），
> 照文档取会把"上游没回报"误记成"用了 0 token"，直接违反用户确认的成功标准
> *"统计只来自上游真实回报的 usage，不估算、不造假"*。
> 代价：多一次正则扫描（`/"usage"\s*:\s*\{[^{}]*\}/`，尾部窗口 2KB，跨 chunk 也能拼回）。
> 风险：理论上若上游响应里出现**未转义**的 `"usage":{...}` 字面量会被误判——JSON 响应体里的正文总是转义的（`\"usage\"`），该正则匹配不到，实际不可达。

---

## 3. 存储与时间口径（`lib/usage.mjs`）

### 3.1 文件格式

路径：`<taskLog.dir>/usage/YYYY-MM-DD.jsonl`（默认即 `logs/usage/`，日期为**本地时区**）
每行一条：

```json
{"ts":"2026-09-18T12:06:08.404Z","model":"deepseek-chat","channel":"deepseek-a","input":11,"output":7}
```

- `ts`：ISO 字符串（UTC）；`input`/`output`：非负整数
- `model`：**逻辑模型名**（客户端请求的名字，与 tasklog 的 `model` 一致；不是上游改写后的名字）
- `channel`：只进文件备审计，**不聚合**（设计文档 §6 明确不做按渠道维度）

### 3.2 两种时间口径（互不混用）

| 范围 | 口径 | 实现 |
|---|---|---|
| `today` | 自然日：本地时区当天 0 点起 | 只取本地日期 = 今天的那个按天聚合 |
| `d1/d7/d30/d90` | 滚动 24h×N | 窗口内**整天**在窗口内的天直接用按天聚合；**只有跨窗口边界的那一天**按 `ts` 的 epoch 逐行过滤（`#partialSince`，按 (天,分钟) 缓存，每分钟最多读一次盘） |

这样既是**精确的滚动 24h×N**，又不必把 90 天的原始行都留在内存里。

### 3.3 内存与上界

- 按天缓存 `Map<dayKey, {requests,input,output,byModel}>`，LRU 上界 `keepDays + 1`
- `byModel` 每模型名一条，每天上界 200（LRU 淘汰最旧；**只影响按模型明细，不影响当天总量**）
- 边界天局部聚合缓存上界 64 条

### 3.4 写入语义

- `record()` **同步返回**；内存增量与落盘都排在一条**串行链**（`writeChain`）上，保证磁盘行序 = 调用顺序，且首次写某天前先读该天历史（历史 + 增量既不丢也不重复计）
- 写失败只 `log.warn`，不阻塞请求（与 tasklog 同口径）
- `query()` 会先 `await flush()`，所以查询结果不会落后于刚 `record` 的数据
- 关闭前 `getUsage().flush()` 落盘；`stopTimers()` 清掉每日清理定时器（`unref()` 过，不会阻止进程退出）

---

## 4. API 契约（`GET /api/usage`）

- 一次返回五个范围，**键名固定**：`{ today, d1, d7, d30, d90 }`
- 每项结构：

```jsonc
{
  "requests": 3,          // 有 usage 的成功请求数（不是全部成功请求数，见 §7.3）
  "inputTokens": 110,
  "outputTokens": 70,
  "totalTokens": 180,
  "byModel": {
    "deepseek-chat": { "requests": 3, "inputTokens": 110, "outputTokens": 70, "totalTokens": 180 }
  }
}
```

- 鉴权/安全：**完全复用**既有 `/api/*` 口径——Host 必须是本机名（否则 403）、跨站 `Origin` 拒绝（403，且不返回任何 CORS 头）、非回环来源需 apiKey、回环来源免鉴权（与 `/api/metrics` 一致）。它**不会**回传明文 apiKey。

---

## 5. 面板区块（`public/index.html`）

- 位置：面板「任务日志」之后、「每模型健康度」之前
- 五张卡片：今日 / 近 1 天 / 近 7 天 / 近 30 天 / 近 90 天，每卡显示 **合计（大字）+ `输入 / 输出 / 请求`**
- 按模型柱状图：固定四列 `模型 × 今日 / 近 7 天 / 近 30 天 / 近 90 天`，行为四范围出现过的模型并集，按近 90 天合计降序；横向堆叠柱（输入 + 输出），等比缩放
- **诚实口径**：某范围 `requests === 0`（或某模型在某范围没有记录）显示 **「未采集」**，绝不显示 0；接口整体无数据时图表显示 `Token 用量：未采集（只统计上游真实回报 usage 的成功请求）`
- **安全渲染**：渲染函数全程 `createElement`/`textContent`，**无 `innerHTML`**（模型名来自客户端与上游，属不可信输入）
- 挂载：`loadUsage()` 与 `loadMetrics()` 并列，随现有 5 秒轮询刷新

---

## 6. 配置与运维

```jsonc
"taskLog": {
  "enabled": true,        // false = 任务日志与用量库都只走内存、不落盘
  "dir": "logs",          // 用量文件落在 <dir>/usage/ 下
  "usageKeepDays": 120    // 用量按天文件的保留天数（默认 120）
}
```

- 启动时清理一次过期文件，之后**每天**滚动清理
- `/api/reload` 与配置文件热重载都会重放用量库装配（改了 `usageKeepDays` / `dir` / `enabled` 立即生效）
- 启动日志会打印一行 `Token 用量 : 落盘 <dir>/usage/YYYY-MM-DD.jsonl · 保留 N 天`（`enabled=false` 时显示"仅内存"）
- **`getUsage()` 未装配时是"只内存不落盘"**（与 `getTaskLog()` 口径一致）：只有 `server.mjs` 通过 `configureUsage()` 装配过才写盘。这样直接 `import lib/proxy.mjs` 的单元测试不会在仓库 `logs/` 下产生垃圾

---

## 7. 剩余事项与已知限制

### 7.1 落盘副本脱敏断言 —— ✅ 已补齐

设计文档 §4.1 要求"沿用 `test-tasklog-rotate` 口径加断言"。已在 `test/test-usage.mjs` L461–L484 实现：
- 读取 `<临时 logs>/tasks.jsonl` 落盘副本
- 断言含 usage 的行保留 `usage.input_tokens/output_tokens` 数值（没被当敏感字段清掉）
- 断言假 key `sk-LEAKTEST0123456789abcdef` 被掩码为 `***REDACTED***`
- 断言落盘全文不含明文假 key

### 7.2 上游因 `include_usage` 回 400 的冒烟回归 —— ✅ 已补齐

设计文档 §5.2 明确要求"实现时对 400 做冒烟回归"。已在 `test/test-usage.mjs` L486–L511 实现：
- mock 增加 `M_REJECT_SO` 模型，收到 `stream_options` 直接回 400
- 断言网关快速返回失败（不挂死，<8s）
- 断言渠道失败归类为 `bad_request`（不重试、直接换家）
- 断言不产生 token 记录、不增加 requests 计数
- 断言后续正常请求不受影响

### 7.3 卡片「请求」口径差异 —— 已知架构限制，需单独立项

设计文档 §5 备注 3 写的是"**请求数列计全部成功请求**"。当前卡片上的「请求」= **有 usage 的成功请求数**（即 usage 库里 `requests` 字段）。

**根因**：`/api/usage` 只统计有 usage 的记录（这正是"不造假"的代价），而 `lib/tasklog.mjs` 的内存环只有 500 条、JSONL 是 32MB×5 轮转，都无法支撑 90 天滚动窗口的全部成功请求计数。

**可选方案（需产品决策后实施）**：
- 方案 A：在 `lib/usage.mjs` 的 JSONL 里额外记录无 usage 的成功请求（`input=0, output=0, ok=true`），但这改变了"只记有 usage 的请求"的核心设计
- 方案 B：新建一份按天持久化的成功请求计数器（独立于 tasklog 和 usage store），这是新存储模块
- 方案 C：维持现状，面板 hint 已写明"只统计上游真实回报 usage 的成功请求"，让数字不含糊

**当前处理**：采用方案 C，区块 hint 明确标注口径，README 同步说明。后续如需方案 A/B，建议单独立项。

### 7.4 空壳计划文件 —— ✅ 已清理

`docs/superpowers/plans/2026-09-17-token-usage-dashboard.md` 已从磁盘删除，由设计文档 + 本交接文档承担说明职责。

### 7.5 明确不做（设计文档 §6 的 YAGNI，未实现是有意的）

金额/成本估算 · 趋势折线图 · 按渠道/代理持久化聚合（`channel` 只进文件备审计） · 引入新依赖。

---

## 8. 验证状态

### 8.1 已完成的验证（可复现）

```bash
# 1) 本功能的验收套件（含 §7.1/§7.2 断言）
node test/test-usage.mjs

# 2) 全量回归
node test/run-all.mjs
npm test                      # 等价
node test/run-all.mjs --list  # 列出套件
node test/run-all.mjs --only test-usage   # 只跑本功能
```

**证据（交接时实测）**：

| 项 | 结果 |
|---|---|
| `test/test-usage.mjs` | 通过 / 0 失败（含单元 + 集成 + §7.1 落盘脱敏 + §7.2 上游 400 冒烟 + 面板 + 安全） |
| 全量回归 | 38 套件全绿（多次连续运行验证） |
| 对照实验（用量库装配关掉的仓库副本，且移除本套件） | 37 套件全绿，用于区分"抖动"与"回归" |
| 真实端到端冒烟 | 真起网关：面板 HTML 含 `Token 用量`/`id="usage-cards"`/`api('/api/usage')`，新渲染段 `innerHTML` 计数 = 0，`GET /api/usage` → 200 且五范围结构正确 |

**测试覆盖到的关键场景**（`test/test-usage.mjs`）：

- 单元：按天聚合、`byModel` 聚合、跨 0 点分天、**`today` 不含昨天 23:59 而 `d1` 含**、`d30` 含 48h 前、保留清理、`enabled=false` 不落盘、非法 range 抛错
- 集成：非流式 OpenAI（11/7）、非流式 Anthropic（5/3）、流式 OpenAI（**断言出站带 `include_usage`** + 流末 usage 帧被采 21/9）、流式 Anthropic（13/17）、无 usage / 全 0 usage / 失败都不产生记录、**注入昨天 23:59 数据验证 today 与 d1 边界**、按本地日期落盘与行字段口径、**跨进程重启后从磁盘重建**、`usage` 随任务记录进 tasklog
- §7.1 落盘脱敏：落盘副本保留 usage 数值字段、假 key 被掩码、全文不含明文 key
- §7.2 上游 400 冒烟：快速失败不挂死、归类 bad_request、不产生记录、不影响后续请求
- 面板：挂载点、轮询、五卡文案、卡片数值、柱状图图例/表头/柱段/等比缩放、「未采集」诚实口径、恶意模型名纯文本、**新渲染段无 `innerHTML`**、脚本可编译
- 安全：跨站 Origin 403 且无 CORS 头、非法 Host 403、同源 200、不回传明文 apiKey、与 `/api/metrics` 鉴权口径一致

### 8.2 未完成的验证（残余盲区，如实说明）

1. **没有真实浏览器渲染验证**（本机未跑浏览器）：面板是用 **DOM stub 真实执行面板脚本 + 真实 HTTP 取面板 HTML** 验证的，没有截图/布局验证。想补可用 `webapp-testing`（Playwright）技能截图核对视觉。
2. **"追加写失败只 warn、不阻塞请求"没有断言**：只在代码路径上成立（`lib/usage.mjs` 的 `#append` catch），可靠模拟磁盘满成本高。
3. **没有等 90 天真实沉降**：跨天/跨窗口是用**注入时间戳**验证的，不是真等 90 天。
4. **既知抖动（不是本功能的 bug）**：全量套件偶发单套件失败，签名是网关子进程启动期 `ECONNREFUSED`，失败套件在不同轮次之间**会漂移**，且失败套件单独跑 / 经 runner 单跑均多次通过。根因是 `test/lib/ports.mjs` 自己文档化的**动态端口 TOCTOU 窗口**。**不要把它当成用量功能的回归**：判定标准是"同一套件单独跑也失败"或"失败签名与端口无关"。

---

## 9. 已知风险与注意事项

1. **仓库不是 git 仓库**（`git status` 报 `not a git repository`）——**没有提交历史，也没有 `git revert` 兜底**。改这几个文件前请先自行备份（复制 `lib/usage.mjs`、`lib/proxy.mjs`、`server.mjs`、`public/index.html` 即可，改动是自包含的）。
2. **流式行为变化**：OpenAI 协议渠道的流式响应末尾会**多一个 usage 帧**（`choices` 为空）。标准 OpenAI SDK 接受（`lib/adapters/openai.mjs` 的注释早已确认该帧不会被网关下游误判），但这属于本功能引入的**预期行为变化**，对接方若做了严格的逐帧 schema 校验需要知晓。
3. **个别上游可能忽略或不认 `stream_options`**：该渠道流式统计会偏低；若上游因此报 400，走既有 `bad_request` 分类（不重试、换家），请求本身不会挂死。§7.2 已有冒烟回归守住。
4. **`logs/` 与 `logs/usage/` 是运行时产物**（已 gitignore）。跑测试时，少数"启动真网关且未禁用 taskLog"的套件会往仓库 `logs/` 写文件——这是**既有行为**（`logs/tasks.jsonl` 早就如此），不是本功能引入的；`test/test-usage.mjs` 自己的落盘全部指向临时目录。
5. **`byModel` 的 200 条上限会淘汰最旧模型**：只影响按模型明细，当天/窗口总量不受影响。
6. **时间口径的边界语义**：`today` 是自然日、`dN` 是滚动窗口，二者**不是**"最近 N 个自然日"。改文案/改面板时别把两者混为一谈。

---

## 10. 排障手册

| 现象 | 定位与原因 |
|---|---|
| 面板「Token 用量」整块显示「未采集」 | ①该时间段确实没有成功请求回报 usage；②`taskLog.enabled=false` 且进程刚重启（内存聚合被清空）；③`/api/usage` 请求失败（打开浏览器控制台看 network） |
| 某个渠道流式请求采不到 token | 该上游忽略了 `stream_options.include_usage`（抓出站请求体确认字段已带）；或它是 Anthropic 协议渠道且没在 `message_start`/`message_delta` 里回 usage |
| 非流式采不到 token，但客户端响应里能看到 usage | 检查是不是 0/0（全 0 视为未回报）；或上游把 usage 放在非扁平结构里（当前正则只匹配扁平 usage 对象） |
| 统计数字比预期少 | 设计如此：失败/中止/无 usage 的请求都不计。先看 `/api/tasks` 里该请求的 `ok` 与 `usage` 字段 |
| 重启前后 `/api/usage` 数字对不上 | 正常情况 `today` 会从磁盘重建、不该变少；只有 `taskLog.enabled=false` 时重启会清空内存聚合（此时 `/api/usage` 只反映本次运行） |
| `logs/usage/` 里出现很旧的文件 | 清理只在启动时与每 24h 执行一次，且只删"文件名日期 < today - usageKeepDays"的文件；可用 `usageKeepDays` 调整 |
| 想手工造跨天数据看面板 | 直接往 `<logs>/usage/<昨天本地日期>.jsonl` 追加一行 `{"ts":"<昨天23:59:59.999 ISO>","model":"x","channel":"manual","input":1000,"output":500}`，刷新面板：`today` 不应包含它，`d1` 应包含 |

---

## 11. 过程说明（重要教训）

本次会话中曾有 5 个并行子 Agent 各自回报"已完成"，其消息里**粘贴了完整的 `lib/usage.mjs` 与 `proxy.mjs` 采集代码**——但**这些文件当时在磁盘上并不存在**（动手前用 `glob` 全仓扫描确认：`lib/` 下没有任何 `usage*` 文件，`proxy.mjs` 里也没有 `stream_options` 或采集点）。最终实现是在那之后**从零写的**。

**结论/要求**：交接与验收一律**以工作区实际文件为准**，不要采信任何"已完成"的口头/文字回报（包括本条）。判断方法就一条——打开文件、跑测试、看输出。

---

## 12. 变更记录

- v1（2026-09-18）：初版交接。功能实现与验证由本次会话完成。
- v2（2026-09-18）：补齐 §7.1 落盘脱敏断言、§7.2 上游 400 冒烟回归；§7.3 确认为已知架构限制；§7.4 空壳计划文件已删除。
- v3（2026-09-19）：追加**价格计费**（token → 金额），见 §13。冻结契约：`docs/superpowers/specs/2026-09-18-pricing-design.md`。
- v4（2026-09-20）：面板收尾两件——
  ① 补 `test/test-panel-layout.mjs`：**真实浏览器多分辨率布局回归**（Node 22 内置 `WebSocket` 直连 Chromium CDP，
  零新依赖），在 1440 / 1180 / 1024 / 900 / 760px 五档量真实几何。首跑即抓到真缺陷：窄屏下最细柱段只有
  **0.25px**（原实现拿百分比下限 `Math.max(pct, 0.5)%` 当"最小可见宽度"，轨道变窄时绝对值跟着缩到看不见
  ——"有量"和"没量"在视觉上一样）。改为 JS 只设比例、最小可见由 CSS `min-width: 2px` 绝对像素兜底，
  修复后五档宽度下稳定 2.00px。**该缺陷 DOM stub 测不出来，只有真浏览器量像素才看得见。**
  ② 按 `LLM-Gateway-Code-Review.md` 修复 F1–F8 共 8 个问题，其中 **F5 直接关系本功能**：原 usage 采集正则是
  `/"usage"\s*:\s*\{[^{}]*\}/g`，`[^{}]*` 会把带嵌套 `prompt_tokens_details` 的 usage 整条跳过（成功请求漏记，
  复现里 3 个请求只记到 120 / 应为 360 token），已改为括号配对扫描。
  全量回归：39 套件 / 1125 断言 → **49 套件 / 1317 断言 / 0 失败**。
- 打包说明：本文件随发布包分发，正式版只有 `docs/` 下这一份；仓库根目录若有同名文件属旧副本，不要引用。

---

## 13. 价格计费（v3 追加：token → 金额）

### 13.1 交付物

| 文件 | 作用 |
|---|---|
| `lib/pricing.mjs` | 价格核心：`DEFAULT_PRICES` 内置参考价、`class Pricing`（`price` / `cost` / `summarize` / `meta`）、`configurePricing` / `getPricing` 单例 |
| `lib/usage.mjs` | `toResult()` 为每个范围与每个模型加金额；`queryAll()` 顶层加 `pricing` 元信息（**金额不落盘**） |
| `server.mjs` | `pricingOptions(cfg)` + `applyPricing(cfg,{force})`（照 `applyUsage` 的 memo 模式），在**启动 / `/api/reload` / `watchConfig` 热重载**三处重放 |
| `public/index.html` | 五张卡片第 4 行费用 + 按模型图中每格该范围费用；币种符号取自响应 `pricing.symbol`（不硬编码 `$`） |
| `test/test-pricing.mjs` | 价格核心单元验收 |
| `test/test-pricing-verify.mjs` | 独立对抗验收（另由独立 agent 编写，用于试图证伪实现） |
| `config.example.json` / `README.md` §2.2 | `pricing` 配置段与口径文档 |

冻结契约（签名 / 字段 / 口径语义）在 `docs/superpowers/specs/2026-09-18-pricing-design.md`，实现与它冲突时以契约为准。

### 13.2 口径不变式（**最容易踩错的地方**）

1. **未配价格的模型 → `cost === null`，绝不返回 0**。0 表示"确实免费"，null 表示"没配价"，混用就是谎报。
2. 显式 `{"input":0,"output":0}` → `cost === 0`（有效的免费定价），与 null 严格区分。
3. 范围 `cost` 只累加**已配价**模型；`costComplete === false` 表示还有模型没配价（金额不完整必须自曝）。
4. §4.1 四种场景：无记录 → `cost:null, costComplete:true`；全部配价 → 数字 + `true`；部分配价 → 数字 + `false`；全部未配价 → `null` + `false`。
5. 金额**只在查询时算**：JSONL 每行仍是 `{ts,model,channel,input,output}`，没有 `cost`。改价立即反映到全部历史范围，无需迁移。
6. 脏价目（`NaN` / `Infinity` / 字符串 / 负数）忽略成"未配价"；金额不得出现 `NaN`。
7. 模型匹配：精确优先，其次 `前缀*` 通配且**最长前缀优先**；模型名里的正则元字符（`gpt-4.1`、`m+`）不得被当通配符。

### 13.3 已知限制（如实说明）

- **`byModel` 每天 200 个模型的上界会限制金额完整性**：被 LRU 挤掉的模型既不在明细里，也不计入范围金额（范围金额 = 明细之和）。这是既有架构限制的延伸，未新增机制。
- **内置价目表只是价格快照**（写在 `lib/pricing.mjs` 顶部），可能过期；以各家官方价目表为准，用 `config.json` 的 `pricing.models` 覆盖。
- 不做**缓存命中 token** 的差异计价（上游 usage 只给输入/输出两类）、不做**货币换算**（只显示配置的币种）、不做**按渠道/按请求**的金额明细。
- **面板观感只做过 vm DOM stub 的离线验证 + 静态推演**，没有真实浏览器截图回归；`test-panel-layout.mjs` 覆盖的是柱状图布局，未覆盖新增费用行的视觉。
