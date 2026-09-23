// 价格计费：把 token 用量折算成金额。
//
// 设计要点（契约见 docs/superpowers/specs/2026-09-18-pricing-design.md §3）：
//   - 零依赖；单价一律是"每 100 万 token 的金额"，与各家官方价目表单位一致
//   - **未配价格的模型 -> cost() 返回 null，绝不返回 0**：0 表示"确实免费"，null 表示"没配价"，
//     两者混在一起就等于把"未知"谎报成"免费"（沿用用量统计"不造假"的同一条铁律）
//   - 显式配置的 0 价是有效的"免费"定价 -> 返回 0
//   - 脏价目（NaN / Infinity / 字符串 / 负数）直接忽略、当成没配这条：不容许脏配置把金额算歪
//   - 金额只在**查询时**计算，不落盘：改价格立即反映到历史数据，不需要迁移任何文件
//   - 单例由 server.mjs 的 applyPricing() 装配；未装配时 getPricing() 返回内置默认价目表

/** 每 100 万 token 的单价。内置表只是参考快照（可能过期，以官方价为准），可用配置覆盖同名 */
export const DEFAULT_PRICES = {
  'deepseek-chat': { input: 0.28, output: 0.42 },
  'deepseek-reasoner': { input: 0.28, output: 0.42 },
  'gpt-4o': { input: 2.5, output: 10 },
  'gpt-4o-mini': { input: 0.15, output: 0.6 },
  'gpt-4.1': { input: 2, output: 8 },
  'gpt-4.1-mini': { input: 0.4, output: 1.6 },
  'claude-3-5-sonnet': { input: 3, output: 15 },
  'claude-3-5-haiku': { input: 0.8, output: 4 },
  'claude-sonnet-4': { input: 3, output: 15 },
  'gemini-2.0-flash': { input: 0.1, output: 0.4 },
  'glm-4-plus': { input: 0.72, output: 0.72 },
  'qwen-max': { input: 1.6, output: 6.4 },
};
export const DEFAULT_CURRENCY = 'USD';
export const DEFAULT_SYMBOL = '$';
/** 未配价格的模型名回报上限（API 响应必须有界：模型名来自客户端） */
export const UNPRICED_LIST_MAX = 20;

const PER_UNIT = 1e6;   // 单价单位：每 100 万 token
const ROUND = 1e6;      // 金额精度：6 位小数（微单位）

/** 价目条是否合法：只有"原始类型为 number 的非负有限数"才算，字符串/布尔等一律视为脏配置忽略 */
function validEntry(v) {
  if (!v || typeof v !== 'object') return null;
  if (typeof v.input !== 'number' || typeof v.output !== 'number') return null;
  if (!Number.isFinite(v.input) || !Number.isFinite(v.output)) return null;
  if (v.input < 0 || v.output < 0) return null;
  return { input: v.input, output: v.output };
}

/** 归一化成只含合法条目的 Map */
function normalize(models) {
  const map = new Map();
  if (!models || typeof models !== 'object') return map;
  for (const [name, v] of Object.entries(models)) {
    const e = validEntry(v);
    if (e) map.set(name, e);
  }
  return map;
}

/** 四舍五入到 6 位小数，避免浮点脏值（如 0.30000000000000004） */
function round6(n) {
  return Math.round(n * ROUND) / ROUND;
}

/** token 数归一：非法/负数一律按 0 计（上游数字已经过 usage 层校验，这里只是兜底） */
function tokens(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

export class Pricing {
  /** @param {{currency?:string, symbol?:string, models?:object}} opts */
  constructor({ currency, symbol, models } = {}) {
    this._currency = (typeof currency === 'string' && currency) ? currency : DEFAULT_CURRENCY;
    this._symbol = (typeof symbol === 'string' && symbol) ? symbol : DEFAULT_SYMBOL;
    // 配置与内置表合并：同名以配置为准（配置是用户意图，内置表只是开箱默认）
    const merged = { ...DEFAULT_PRICES, ...(models && typeof models === 'object' ? models : {}) };
    this._prices = normalize(merged);
    // 通配键按"前缀长度降序"排好，查的时候第一个命中就是最长前缀（最长优先，无需再比较）
    this._wild = [...this._prices.entries()]
      .filter(([k]) => k.endsWith('*') && k.length > 1)
      .map(([k, entry]) => ({ prefix: k.slice(0, -1), entry }))
      .sort((a, b) => b.prefix.length - a.prefix.length);
  }

  get currency() { return this._currency; }
  get symbol() { return this._symbol; }

  /**
   * 查模型单价：① 精确匹配 ② `前缀*` 通配（多个命中取最长前缀）。
   * @returns {{input:number, output:number} | null} 没配上 -> null
   */
  price(model) {
    if (typeof model !== 'string' || model === '') return null;
    const exact = this._prices.get(model);
    if (exact) return exact;
    for (const w of this._wild) {
      if (model.startsWith(w.prefix)) return w.entry;
    }
    return null;
  }

  /**
   * 算一个模型一个范围的金额：(输入×单价in + 输出×单价out) / 1e6，四舍五入 6 位小数。
   * @returns {number | null} 未配价格 -> null（绝不返回 0）
   */
  cost(model, inputTokens, outputTokens) {
    const p = this.price(model);
    if (!p) return null;
    return round6((tokens(inputTokens) * p.input + tokens(outputTokens) * p.output) / PER_UNIT);
  }

  /**
   * 汇总一个范围的 byModel。
   * cost 只累加**已配价格**模型；一个都没配上 -> null（而不是 0）。
   * @returns {{cost:number|null, complete:boolean, priced:number, unpriced:string[], unpricedTokens:number}}
   */
  summarize(byModel) {
    let sum = 0;
    let priced = 0;
    let any = false;
    let unpricedTokens = 0;
    const unpriced = [];
    for (const [name, v] of Object.entries(byModel || {})) {
      const inT = tokens(v?.inputTokens);
      const outT = tokens(v?.outputTokens);
      const c = this.cost(name, inT, outT);
      if (c == null) {
        unpriced.push({ name, tokens: inT + outT });
        unpricedTokens += inT + outT;
      } else {
        sum += c;
        priced += 1;
        any = true;
      }
    }
    // 未配价模型按 token 降序（先让用户看到"最花钱但没配价"的那几个），同量按名字稳定排序
    unpriced.sort((a, b) => b.tokens - a.tokens || a.name.localeCompare(b.name));
    return {
      cost: any ? round6(sum) : null,
      complete: unpriced.length === 0,
      priced,
      unpriced: unpriced.slice(0, UNPRICED_LIST_MAX).map((x) => x.name),
      unpricedTokens,
    };
  }

  /** @returns {{currency:string, symbol:string, models:number}} models = 生效的价目条数 */
  meta() {
    return { currency: this._currency, symbol: this._symbol, models: this._prices.size };
  }
}

// ---- 单例：网关全局一份价目表 ----
let singleton = null;

/** 装配单例（server 启动 / 热重载调用）。models 与内置表合并，配置覆盖同名 */
export function configurePricing(opts = {}) {
  singleton = new Pricing(opts);
  return singleton;
}

/** 取单例；未装配时返回内置默认价目表 */
export function getPricing() {
  if (!singleton) singleton = new Pricing({});
  return singleton;
}