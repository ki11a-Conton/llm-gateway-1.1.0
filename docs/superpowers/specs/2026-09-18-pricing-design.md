# Token 价格计费（pricing）设计 + 冻结接口

> 版本：v1 · 2026-09-18 · 上游依赖：[`2026-09-17-token-usage-dashboard-design.md`](2026-09-17-token-usage-dashboard-design.md)
>
> 本文档是**并行开发的冻结契约**：`lib/pricing.mjs` 的签名、`/api/usage` 新增字段、面板口径在实现期间不得单方面改动。
> 需要改接口 → 先改本文档并通知 Lead，不要各自改。

---

## 1. 目标

在既有 Token 用量统计（只记 token 数）之上，增加**按 token 数量计算金额**的能力：

1. 内置常见模型的参考价（每 100 万 token 单价），开箱即可看到费用
2. 支持在 `config.json` 里覆盖/新增任意模型的价格（含 `前缀*` 通配）
3. `/api/usage` 每个时间范围与每个模型都返回金额
4. 面板显示金额
5. **未配价格的模型绝不当 0 计**（沿用"不造假"铁律）

## 2. 非目标（本版明确不做）

- 不做真实扣费/余额联动（只做"用量 × 单价"的估算展示）
- 不做缓存命中 token 的差异计价（上游 usage 里只有输入/输出两类）
- 不做货币换算（只显示配置的币种，不做汇率）
- 不把金额写进 JSONL（价格会变，历史数据不该被冻死在后端文件里）
- 不做按渠道/按请求的金额明细（只在"范围"与"按模型"两个粒度）

---

## 3. 冻结接口：`lib/pricing.mjs`（新增，零依赖）

```js
/** 每 100 万 token 的单价；内置表仅为参考快照，以官方价为准，可用 config 覆盖 */
export const DEFAULT_PRICES = {
  'deepseek-chat':     { input: 0.28,  output: 0.42 },
  'deepseek-reasoner': { input: 0.28,  output: 0.42 },
  'gpt-4o':            { input: 2.50,  output: 10.00 },
  'gpt-4o-mini':       { input: 0.15,  output: 0.60 },
  'gpt-4.1':           { input: 2.00,  output: 8.00 },
  'gpt-4.1-mini':      { input: 0.40,  output: 1.60 },
  'claude-3-5-sonnet': { input: 3.00,  output: 15.00 },
  'claude-3-5-haiku':  { input: 0.80,  output: 4.00 },
  'claude-sonnet-4':   { input: 3.00,  output: 15.00 },
  'gemini-2.0-flash':  { input: 0.10,  output: 0.40 },
  'glm-4-plus':        { input: 0.72,  output: 0.72 },
  'qwen-max':          { input: 1.60,  output: 6.40 },
};
export const DEFAULT_CURRENCY = 'USD';
export const DEFAULT_SYMBOL = '$';
/** 未配价格时回调 API 的模型名清单上限（响应必须有界） */
export const UNPRICED_LIST_MAX = 20;

export class Pricing {
  /** @param {{currency?:string, symbol?:string, models?:object}} opts */
  constructor({ currency, symbol, models } = {})

  get currency()   // string，默认 'USD'
  get symbol()     // string，默认 '$'

  /**
   * 查模型单价。
   * 匹配顺序：① 精确匹配模型名 ② `前缀*` 通配（多个命中时取**最长前缀**）
   * @returns {{input:number, output:number} | null}  没配上 -> null（调用方按"未配置价格"处理）
   */
  price(model)

  /**
   * 算一个模型一个范围的金额。
   * 公式：amount = (inputTokens * input + outputTokens * output) / 1e6，四舍五入到 6 位小数。
   * @returns {number | null}  未配价格 -> null（绝不返回 0）；token 全为 0 且已配价 -> 0
   */
  cost(model, inputTokens, outputTokens)

  /**
   * 汇总一个范围的 byModel（`{模型名: {inputTokens, outputTokens, ...}}`）。
   * @returns {{cost:number|null, complete:boolean, priced:number, unpriced:string[], unpricedTokens:number}}
   *   cost          已配价格模型的金额之和；一个都没配上 -> null
   *   complete      unpriced 为空 -> true
   *   priced        配到价格的模型数
   *   unpriced      未配价格的模型名（最多 UNPRICED_LIST_MAX 个，按 token 降序）
   *   unpricedTokens 未配价格模型的 token 总数（用于面板提示"还有多少没计价"）
   */
  summarize(byModel)

  /** @returns {{currency:string, symbol:string, models:number}} models = 生效的价目条数 */
  meta()
}

/** 装配单例（server 启动 / 热重载调用）。models 与 DEFAULT_PRICES 合并，配置覆盖同名 */
export function configurePricing(opts = {})

/** 取单例；**未装配时返回内置默认价目表**（只读语义，immer 安全） */
export function getPricing()
```

### 3.1 语义要求（会被独立验收）

1. `cost()` 未配价格 → `null`，**不是 0**
2. 金额四舍五入 6 位小数（`0.0000005` 级别不引入浮点脏值）
3. 通配 `a*` 命中 `abc`；同时有 `a*` 与 `ab*` 时取 `ab*`（最长前缀优先）
4. `configurePricing` 可多次调用（热重载），每次都重建；`getPricing()` 在装配后返回同一实例
5. `summarize({})` → `{cost:null, complete:true, priced:0, unpriced:[], unpricedTokens:0}`
6. **显式配置的 0 价是"已定价为 0"**：`{input:0, output:0}` → `cost()` 返回 `0`（**不是 `null`**）。
   `null` 只表示"根本没配这个模型的价格"，不能用来表示"免费"。
7. **脏价目视为不存在**：非有限数（`NaN`/`Infinity`/字符串）或负数的价目条直接忽略（当成没这条），
   不允许脏配置把金额算歪；忽略后该模型按"未配价格"处理。

---

## 4. 冻结契约：`/api/usage` 响应扩展（**只增字段，不改既有字段**）

既有字段（`requests`/`inputTokens`/`outputTokens`/`totalTokens`/`byModel`）语义与命名一律不动。

```jsonc
{
  "today": {
    "requests": 3,
    "inputTokens": 110,
    "outputTokens": 70,
    "totalTokens": 180,

    "cost": 0.000061,          // number | null
    "costComplete": true,      // false = 有模型未配价格，cost 只是"已配价格部分"的和
    "unpricedTokens": 0,       // 未配价格模型的 token 数
    "unpriced": [],            // 未配价格的模型名（≤ 20 个）

    "byModel": {
      "deepseek-chat": {
        "requests": 3, "inputTokens": 110, "outputTokens": 70, "totalTokens": 180,
        "cost": 0.000061         // number | null（该模型未配价格 -> null）
      }
    }
  },
  "d1": { }, "d7": { }, "d30": { }, "d90": { },
  "pricing": { "currency": "USD", "symbol": "$", "models": 12 }   // 顶层新增
}
```

### 4.1 口径不变式（必须逐条断言）

| # | 场景 | `cost` | `costComplete` |
|---|---|---|---|
| 1 | 该范围无任何记录（`requests === 0`） | `null`（未采集） | `true` |
| 2 | 有记录，全部模型配了价 | 数字 | `true` |
| 3 | 有记录，部分模型未配价 | 数字（只含已配价部分） | `false` |
| 4 | 有记录，全部模型未配价 | `null` | `false` |

- 金额**只在查询时计算**，JSONL 每行仍是 `{ts, model, channel, input, output}`，不落 `cost`
- 改价格立即反映到所有历史范围（无需迁移数据）
- `byModel` 的 200 条/天 LRU 上界对金额同样适用：金额的完整性与按模型明细一致（README 需说明）

---

## 5. 配置与装配（`server.mjs`）

```jsonc
"pricing": {
  "currency": "USD",        // 仅作显示币种，不做换算
  "symbol": "$",
  "models": {
    "my-model":  { "input": 1.2,  "output": 3.4 },
    "gpt-*":     { "input": 2.0,  "output": 8.0 }
  }
}
```

- `config.json` 没有 `pricing` 段 → 用内置默认价目表（功能开箱可用）
- 装配函数照抄既有 `usageOptions` / `applyUsage` 的 memo 模式：
  `pricingOptions(cfg)` + `applyPricing(cfg, {force})`（`JSON.stringify` 做 key 比较）
- 必须在三处重放：**启动时**、`/api/reload`、`manager.watchConfig` 热重载（与 `applyUsage` 同一批位置）
- 启动日志增加一行说明当前币种与价目条数（`log.raw` 或 `log.info`，风格与现有一致）

---

## 6. 面板（`public/index.html`）

- 五张时间卡片：在现有 `输入 / 输出 / 请求` 行下方增加一行**费用**
  - 有金额：`费用 $0.000061`
  - 有记录但未配价：`费用 未配置价格`
  - 无记录：`费用 未采集`
  - `costComplete === false`（部分计价）：在费用后追 `（部分）` 并在 `title` 里说明未计价模型数
- 按模型图/表：每个模型行显示该模型费用；未配价显示 `未配置价格`
- 币种符号取自响应的 `pricing.symbol`，**不硬编码 `$`**
- 保持既有限制：**全程 `createElement`/`textContent`，不得出现 `innerHTML`**（`test-metrics-observability` 与 `test-usage` 会断言）
- 金额格式化要短（例如 ≤ 0.01 时保留 6 位、否则 2~4 位），避免撑破卡片

---

## 7. 任务边界与写范围（并行不重叠）

| 任务 | 负责人 | 写范围 | 依赖 |
|---|---|---|---|
| T1 价格核心 | pricing-core | `lib/pricing.mjs`、`test/test-pricing.mjs` | 无（按 §3 冻结接口） |
| T2 用量聚合与 API | usage-api | `lib/usage.mjs`、`server.mjs`、`test/test-usage.mjs` | 按 §3/§4/§5 冻结契约 |
| T3 面板 | panel-ui | `public/index.html` | 按 §4/§6 冻结契约 |
| T4 独立验收 | verifier | `test/test-pricing-verify.mjs` | 按 §3.1/§4.1 独立写对抗断言 |
| T5 文档与打包 | Lead | `README.md`、`config.example.json`、`docs/handover-token-usage-dashboard.md`、`dist/*` | 全部完成后 |

**硬规则**：任何人不许改别人范围内的文件；需要别人配合 → 发消息或找 Lead。

---

## 8. 验收标准（整体 DoD）

1. `node test/test-pricing.mjs`、`node test/test-pricing-verify.mjs`、`node test/test-usage.mjs` 全绿
2. `node test/run-all.mjs` 全量全绿（基线从 39 套件上升，README 基线数字由 Lead 更新）
3. 未配价格 → `cost === null`，任何位置都不出现"把没价格的模型算成 0"
4. 改 `config.json` 的 `pricing` 后热重载生效（金额变化），且历史范围金额同步变化
5. 面板无 `innerHTML`；`$`/币种来自响应而非硬编码
6. 生成的发布包仍不含任何真实 key（Lead 重打包时复验）