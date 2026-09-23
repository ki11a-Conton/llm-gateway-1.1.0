# Token 用量仪表盘 — 设计文档（2026-09-17）

## 1. 目标

在现有状态面板（`public/index.html`）新增「Token 用量」区块，统计并展示以下五个时间段的 token 消耗：

| 范围 | 口径（已与用户确认） |
|---|---|
| 今日（today） | 自然日：本地时区当天 0 点起 |
| 近 1 天（d1） | 滚动 24h |
| 近 7 天（d7） | 滚动 7×24h |
| 近 30 天（d30） | 滚动 30×24h |
| 近 90 天（d90） | 滚动 90×24h |

统计维度：总量（输入/输出/合计）+ **按模型细分**（已与用户确认）。

成功标准：面板能稳定看到五个时间段的 token 用量与按模型明细；数据跨进程重启不丢（90 天）；统计只来自上游真实回报的 usage，不估算、不造假。

## 2. 现状与数据缺口

- `lib/adapters/openai.mjs` / `lib/adapters/anthropic.mjs` 已把上游 usage 解析出来做协议转换（`prompt_tokens/completion_tokens` ↔ `input_tokens/output_tokens`），但转换完即丢弃。
- `lib/tasklog.mjs` 的任务记录与模型级统计（`#noteModel`）只记请求数/成功率/失败指纹/三段耗时，**没有任何 token 字段**。
- 任务日志 JSONL 会轮转（`maxFileBytes` 32MB × `keepFiles` 5），90 天前的数据会被删除 → 不能靠扫任务日志做长期聚合。

## 3. 已确认的决策

1. 时间口径：今日 = 自然日；1/7/30/90 天 = 滚动窗口。
2. 统计维度：总量 + 按模型。
3. 流式统计：出站强制补 `stream_options: {include_usage: true}`（仅 OpenAI 协议渠道且 stream=true），保证流式 usage 可采；Anthropic 协议流式从 `message_delta` 的 usage 提取。
4. 存储：每日 JSONL（`logs/usage/YYYY-MM-DD.jsonl`）+ 按天缓存聚合（方案 A）。
5. UI：现有面板新增「Token 用量」区块。

## 4. 设计

### 4.1 数据采集（`lib/proxy.mjs` + 适配层）

- **非流式**：响应体读完、最终 JSON 在手时提取 usage（OpenAI 口径 `prompt_tokens/completion_tokens`；Anthropic 口径 `input_tokens/output_tokens`）。适配层已有等价解析，采集点放在 proxy 层拿到最终 body 的位置，避免重复解析。
- **流式**：
  - OpenAI 协议渠道：出站强制 `stream_options: {include_usage: true}`，流末 usage 帧（choices 为空）被现有管道吸收（`openai.mjs` 注释已确认该帧不会被误判）；采集点放在流结束、usage 已汇聚的位置。
  - Anthropic 协议渠道：从流式 `message_delta` / 结束帧的 usage 提取。
- **记账口径**：只记录**成功交付给客户端**的那次 attempt 的 usage；失败 / 客户端中止 / 上游未回报 usage 的请求不产生 token 记录（请求数仍照常进任务日志）。
- **字段口径**：采集认两套字段名——OpenAI 口径 `prompt_tokens/completion_tokens`、Anthropic 口径 `input_tokens/output_tokens`；从**最终交付形态**的响应体里取（适配层已把 usage 嵌入转换结果，避免重复解析）。两套都转成统一的 `input` / `output` 整数。
- usage 随任务记录进入 tasklog（请求级 `usage: { input_tokens, output_tokens }`），落盘副本照常走 `redactSecrets`（数值字段不受脱敏影响，沿用 `test-tasklog-rotate` 口径加断言）。

### 4.2 持久化（新文件 `lib/usage.mjs`，零依赖）

- **文件布局**：`logs/usage/YYYY-MM-DD.jsonl`，按**本地时区**分天（day 键 = `ts` 转换到本地日期）；每请求一行：
  `{ ts, model, channel, input, output }`（`ts` 为 ISO 字符串；`input`/`output` 为整数，缺则 0）。
- **写路径**：请求完成时 `record(entry)` → 内存按天缓存增量更新 + 异步追加文件（失败只 warn 不阻塞请求，与 tasklog 同口径）。
- **按天缓存（LRU）**：`Map<day, { requests, input, output, byModel: Map<model, {requests, input, output}> }>`；**查询缺天时懒加载对应文件**重建该天聚合（today 与 dN 都要），已加载的天不重复读盘。天键数量有上界（`usageKeepDays` + 1），超界淘汰最旧。
- **保留策略**：`taskLog.usageKeepDays`（默认 120）；启动时与每日滚动时清理过期文件（按文件名日期判断）。
- **聚合查询** `query(range)`：today / dN 两种口径；
  - today：只查本地日期 = 今天的按天聚合；
  - dN：取最近 N 天（含今天）的按天聚合相加；
  - 时间比较统一规则：**日历日**（today / 文件分桶）用本地日期；**滚动窗口**（dN）用 `Date.now() - N*24*3600*1000` 与 `ts` 的 epoch 比较（`ts` 存 ISO 即 UTC，换算无歧义）；两种口径互不混用。
  - 返回 `{ requests, inputTokens, outputTokens, totalTokens, byModel: {...} }`，`byModel` 键为模型名。
- **上限保护**：byModel 表按模型名聚合，模型名来自客户端 → 沿用 tasklog 的 LRU 上限思路（仅聚合键，不做无限增长）。

### 4.3 API（`server.mjs`）

- `GET /api/usage` 一次返回五个范围：
  `{ today, d1, d7, d30, d90 }`，每项结构同 4.2 的 query 返回。
- 复用现有 /api/* 的鉴权（apiKey）、Host 校验、跨站 Origin 拒绝。
- 与 `/api/metrics` 并列挂到面板轮询。

### 4.4 面板（`public/index.html`）

- 新增「Token 用量」区块：
  - 五个时间段卡片（今日 / 1 天 / 7 天 / 30 天 / 90 天），每卡显示 输入 / 输出 / 合计 + 请求数；
  - 一张「按模型」表：固定四列 —— 模型 × 今日 / 近 7 天 / 近 30 天 / 近 90 天的合计 token；
  - 数据缺失时显示「未采集」，不显示 0（沿用 `/api/metrics` 的诚实口径）。
- 样式沿用现有卡片风格，渲染全程无 `innerHTML`（沿用 `test-metrics-observability` 的安全渲染断言口径）。

### 4.5 测试（新增 `test/test-usage.mjs`，端口动态分配）

- 单元（直接 `import` `lib/usage.mjs`，指向临时目录）：
  - 追加 + 按天聚合正确；跨天分桶正确；
  - 本地时区边界（跨 0 点的两条记录落不同天文件）；
  - 保留清理（`usageKeepDays` 过期文件被删）；
  - 查询 today / dN 口径正确；`byModel` 聚合正确。
- 集成（mock 上游回 usage）：
  - 非流式 OpenAI：`prompt_tokens/completion_tokens` 被采到；
  - 非流式 Anthropic：`input_tokens/output_tokens` 被采到；
  - 流式 OpenAI：断言**出站请求体带 `stream_options.include_usage=true`**，且流末 usage 帧被采到；
  - 流式 Anthropic：`message_delta` usage 被采到；
  - 上游不回 usage / 请求失败：不产生 token 记录；
  - `GET /api/usage`：注入时间戳造跨天数据，断言 today 不含昨天、d1 含昨天、边界正确；
  - 面板：HTML 含「Token 用量」新区块字段名、无 `innerHTML`；
  - 安全：`/api/usage` 不泄露 apiKey、跨站 Origin 403。
- 回归门槛（既有套件必须全绿）：
  `test-openai-compat`（50）、`test-cache-control`（51）、`test-proxy-timeouts`（33）、`test-metrics-observability`（58）、`test-tasklog-rotate`（28）、`smoke`（28）——以及全量 37 套件。

## 5. 已知边界（写进 README，不装作没看见）

1. 流式 OpenAI 客户端收到的字节流会**多一个 usage 帧**（choices 为空）：现有管道已兼容（openai.mjs 注释确认），标准 OpenAI SDK 接受；这属于本功能引入的预期行为变化。
2. 极少数兼容上游可能忽略 `include_usage`（或对未知字段报错）→ 该渠道统计偏低或失败；实现时对 400 做冒烟回归（`test-classify-hardening` 的 bad_request 分类兜底）。
3. 失败 / 中止请求的 token 统计不到（上游不会回报）——面板 token 列只计有 usage 的成功请求，请求数列计全部成功请求。
4. 这是**自用统计，不是计费依据**：缓存命中、折扣、计费倍率等上游口径不同。
5. 追加写失败只 warn（磁盘满等场景），统计会缺当日增量，不阻塞服务。

## 6. 不做（YAGNI）

- 不做金额/成本估算（用户未要求，口径不可靠）。
- 不做趋势折线图（先交付五个时间段数字 + 按模型表；图表属后续增强）。
- 不做按渠道 / 按代理的持久化维度（用户只选了按模型；`channel` 字段先进文件备审计，不聚合）。
- 不引入任何新依赖。

## 7. 变更记录

- v1：初版，基于与用户确认的五项决策（时间口径 / 统计维度 / include_usage / 存储方案 A / 面板区块）。
