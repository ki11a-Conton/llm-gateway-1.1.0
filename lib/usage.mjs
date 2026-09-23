// Token 用量持久化与聚合（零依赖）——见 docs/superpowers/specs/2026-09-17-token-usage-dashboard-design.md §4.2
//
// 设计要点：
//   - 文件布局：logs/usage/YYYY-MM-DD.jsonl，按**本地时区**分天；每请求一行
//     { ts, model, channel, input, output }（ts 为 ISO 字符串，input/output 为整数）。
//   - 只记"成功交付给客户端且上游真的回报了 usage"的请求：本模块不做任何估算/兜底成 0，
//     调用方（lib/proxy.mjs）拿不到真实 usage 时**根本不调用** record（设计文档 §4.1）。
//   - 内存按天缓存聚合（Map + LRU 上界 keepDays+1）；查询缺天时懒加载对应文件，已加载的天不重复读盘。
//   - 两种时间口径互不混用（§4.2）：
//       today = 自然日（本地日期当天 0 点起，只看当天的按天聚合）；
//       dN    = 滚动窗口，用 Date.now() - N*24h 与 ts 的 epoch 比较；
//               窗口内"整天在窗口内"的天直接用按天聚合，只有**跨窗口边界的那一天**按 ts 逐行过滤，
//               这样既是精确的滚动 24h×N，又不必把 90 天的原始行都留在内存里。
//   - 写路径：内存增量 + 串行异步追加；写失败只 warn 不阻塞请求（与 tasklog 同口径）。
import { appendFile, mkdir, readdir, readFile, unlink } from 'node:fs/promises';
import path from 'node:path';
import { log } from './logger.mjs';
import { getPricing } from './pricing.mjs';

/** API 固定返回的五个范围（§4.3） */
export const USAGE_RANGES = ['today', 'd1', 'd7', 'd30', 'd90'];

/** 保留天数默认值（taskLog.usageKeepDays 可覆盖） */
const DEFAULT_KEEP_DAYS = 120;
/** 按天缓存的键数上界：keepDays + 1（今天之外的窗口） */
const MAX_MODELS_PER_DAY = 200;   // byModel 有上界：模型名来自客户端，不能无界增长（§4.2）
const PARTIAL_CACHE_MAX = 64;     // 滚动窗口边界天的局部聚合缓存（按分钟粒度），很小且有限
const DAY_FILE_RE = /^\d{4}-\d{2}-\d{2}\.jsonl$/;
const DAY_MS = 24 * 3600 * 1000;

/** 本地时区的日期键 YYYY-MM-DD（分天口径的唯一实现，测试与 UI 都引用它） */
export function localDayKey(ms = Date.now()) {
  const d = new Date(ms);
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${d.getFullYear()}-${m}-${day}`;
}

/** 日期键 -> 本地时区当天 0 点的 epoch 毫秒 */
function dayStartMs(dayKey) {
  const [y, m, d] = String(dayKey).split('-').map(Number);
  return new Date(y, m - 1, d).getTime();
}

/** 日期键区间（含两端），按本地日历逐日推进 */
function dayKeysBetween(fromKey, toKey) {
  const out = [];
  const [fy, fm, fd] = String(fromKey).split('-').map(Number);
  let cur = new Date(fy, fm - 1, fd);
  const end = dayStartMs(toKey);
  // 上界保护：即使 key 异常也不会死循环（keepDays 量级最多几百天）
  for (let i = 0; i < 1000 && cur.getTime() <= end; i += 1) {
    out.push(localDayKey(cur.getTime()));
    cur = new Date(cur.getFullYear(), cur.getMonth(), cur.getDate() + 1);
  }
  return out;
}

/** 解析范围：'today' | 'd<N>'；非法值直接抛错，绝不静默兜底成 today */
function parseRange(range) {
  if (range == null || range === 'today') return { kind: 'today' };
  const m = /^d(\d+)$/.exec(String(range));
  if (!m) throw new RangeError(`未知的用量范围 "${range}"（支持 today / d<N>）`);
  const days = Number(m[1]);
  if (!Number.isFinite(days) || days < 1) throw new RangeError(`非法的用量范围 "${range}"`);
  return { kind: 'rolling', days };
}

/** token 数归一：非有限值 / 负数一律算 0（不造假，也不让脏数据污染聚合） */
function toToken(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}

/** ts 归一：ISO 字符串 / 毫秒时间戳都认；无法解析时返回 null */
function toMs(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isNaN(t) ? null : t;
}

function emptyDay() {
  return { requests: 0, input: 0, output: 0, byModel: new Map() };
}

/** 把一条记录累加进某天的聚合（byModel 走 LRU 上界） */
function addEntry(day, model, input, output) {
  day.requests += 1;
  day.input += input;
  day.output += output;
  const name = model == null || model === '' ? 'unknown' : String(model);
  const cur = day.byModel.get(name);
  if (cur) {
    cur.requests += 1; cur.input += input; cur.output += output;
    day.byModel.delete(name); // 重新插入 = 刷新 LRU 位置
    day.byModel.set(name, cur);
  } else {
    day.byModel.set(name, { requests: 1, input, output });
  }
  while (day.byModel.size > MAX_MODELS_PER_DAY) {
    day.byModel.delete(day.byModel.keys().next().value);
  }
}

/** 把 src 的聚合并入 dst（byModel 沿用 LRU 上界） */
function mergeDay(dst, src) {
  dst.requests += src.requests;
  dst.input += src.input;
  dst.output += src.output;
  for (const [name, v] of src.byModel) {
    const cur = dst.byModel.get(name);
    if (cur) {
      cur.requests += v.requests; cur.input += v.input; cur.output += v.output;
      dst.byModel.delete(name);
      dst.byModel.set(name, cur);
    } else {
      dst.byModel.set(name, { requests: v.requests, input: v.input, output: v.output });
    }
    while (dst.byModel.size > MAX_MODELS_PER_DAY) {
      dst.byModel.delete(dst.byModel.keys().next().value);
    }
  }
}

/** 聚合 -> 对外结果（byModel 键为模型名）。
 *  金额在**查询时**按当前价目表计算（pricing 设计 §4）：JSONL 里不落 cost，改价格立即反映到历史范围。
 *  未配价格的模型 cost 为 null（不是 0）；范围 cost 只累加已配价模型，完整性由 costComplete 标注。 */
function toResult(day) {
  const pricing = getPricing();
  const byModel = {};
  for (const [name, v] of day.byModel) {
    byModel[name] = {
      requests: v.requests,
      inputTokens: v.input,
      outputTokens: v.output,
      totalTokens: v.input + v.output,
      cost: pricing.cost(name, v.input, v.output),
    };
  }
  const sum = pricing.summarize(byModel);
  return {
    requests: day.requests,
    inputTokens: day.input,
    outputTokens: day.output,
    totalTokens: day.input + day.output,
    cost: sum.cost,
    costComplete: sum.complete,
    unpricedTokens: sum.unpricedTokens,
    unpriced: sum.unpriced,
    byModel,
  };
}

export class UsageStore {
  /**
   * @param {{dir?:string, keepDays?:number, enabled?:boolean}} opts
   *   dir      落盘目录（默认 <repo>/logs/usage）
   *   keepDays 保留天数（默认 120；server 用 taskLog.usageKeepDays 覆盖）
   *   enabled  false 时只做内存聚合、绝不落盘（taskLog.enabled=false 的运行口径）
   */
  constructor({ dir, keepDays, enabled = true } = {}) {
    this.dir = dir || path.join(process.cwd(), 'logs', 'usage');
    this.keepDays = Number.isFinite(Number(keepDays)) && Number(keepDays) > 0
      ? Math.floor(Number(keepDays))
      : DEFAULT_KEEP_DAYS;
    this.enabled = enabled !== false;
    this.cache = new Map();        // dayKey -> 按天聚合（Map 顺序即 LRU 顺序）
    this.loadedDays = new Set();   // 已从文件加载过的天（避免重复读盘）
    this.partial = new Map();      // `${dayKey}|${minute}` -> 滚动窗口边界天的局部聚合
    this.writeChain = Promise.resolve();
    this.ready = null;
    this.timer = null;
  }

  get maxCachedDays() {
    return this.keepDays + 1;
  }

  /** 惰性创建目录（只做一次）；enabled=false 时完全不碰磁盘 */
  async #ensureDir() {
    if (!this.enabled) return;
    if (!this.ready) {
      this.ready = mkdir(this.dir, { recursive: true }).catch((err) => {
        log.warn(`用量目录创建失败(${this.dir}): ${err.message}`);
      });
    }
    return this.ready;
  }

  #touch(dayKey, day) {
    this.cache.delete(dayKey);
    this.cache.set(dayKey, day);
    while (this.cache.size > this.maxCachedDays) {
      const oldest = this.cache.keys().next().value;
      this.cache.delete(oldest);
      this.loadedDays.delete(oldest);
    }
  }

  /** 取（或建立）某天的内存聚合对象，并刷新其 LRU 位置 */
  #dayFor(dayKey) {
    let day = this.cache.get(dayKey);
    if (!day) day = emptyDay();
    this.#touch(dayKey, day);
    return day;
  }

  /** 从文件加载某天的聚合（只加载一次；已有内存增量时并入而不是覆盖） */
  async #loadDay(dayKey) {
    const existing = this.cache.get(dayKey) || null;
    if (this.loadedDays.has(dayKey)) {
      if (existing) this.#touch(dayKey, existing);
      return existing || emptyDay();
    }
    const day = existing || emptyDay();
    if (this.enabled) {
      try {
        const text = await readFile(path.join(this.dir, `${dayKey}.jsonl`), 'utf8');
        for (const line of text.split('\n')) {
          if (!line) continue;
          let e = null;
          try { e = JSON.parse(line); } catch { continue; } // 半截行/脏行跳过，不影响其余数据
          addEntry(day, e?.model, toToken(e?.input), toToken(e?.output));
        }
      } catch { /* 文件不存在 / 不可读：该天按空处理 */ }
    }
    this.loadedDays.add(dayKey);
    this.#touch(dayKey, day);
    return day;
  }

  /**
   * 滚动窗口边界天的局部聚合：只统计 ts >= cutoffMs 的行（精确到 ts 的 epoch 比较，§4.2）。
   * 结果按 (天, 分钟) 缓存——每分钟最多读一次盘。
   */
  async #partialSince(dayKey, cutoffMs) {
    const key = `${dayKey}|${Math.floor(cutoffMs / 60000)}`;
    const hit = this.partial.get(key);
    if (hit) {
      this.partial.delete(key);
      this.partial.set(key, hit);
      return hit;
    }
    const agg = emptyDay();
    if (this.enabled) {
      try {
        const text = await readFile(path.join(this.dir, `${dayKey}.jsonl`), 'utf8');
        for (const line of text.split('\n')) {
          if (!line) continue;
          let e = null;
          try { e = JSON.parse(line); } catch { continue; }
          const ms = toMs(e?.ts);
          if (ms == null || ms < cutoffMs) continue;
          addEntry(agg, e?.model, toToken(e?.input), toToken(e?.output));
        }
      } catch { /* 该天没有文件 */ }
    }
    this.partial.set(key, agg);
    while (this.partial.size > PARTIAL_CACHE_MAX) {
      this.partial.delete(this.partial.keys().next().value);
    }
    return agg;
  }

  async #append(dayKey, line) {
    try {
      await this.#ensureDir();
      await appendFile(path.join(this.dir, `${dayKey}.jsonl`), `${JSON.stringify(line)}\n`, 'utf8');
    } catch (err) {
      log.warn(`用量写入失败(${dayKey}): ${err.message}`);
    }
  }

  /**
   * 记一条用量。同步返回；内存增量与落盘都排在串行链上，保证：
   *   - 磁盘行序 == 调用顺序（不会交叉写坏 JSONL）
   *   - 首次写某天前先把该天已有历史读进内存，历史 + 增量既不丢也不重复计
   * @param {{ts?:string|number, model?:string, channel?:string, input?:number, output?:number}} entry
   */
  record(entry = {}) {
    const tsMs = toMs(entry.ts) ?? Date.now();
    const dayKey = localDayKey(tsMs);
    const input = toToken(entry.input);
    const output = toToken(entry.output);
    const model = entry.model == null || entry.model === '' ? 'unknown' : String(entry.model);
    const line = { ts: new Date(tsMs).toISOString(), model, channel: entry.channel ?? null, input, output };

    const apply = () => {
      const day = this.#dayFor(dayKey);
      addEntry(day, model, input, output);
      // 边界天局部聚合可能因新记录而失效（缓存是分钟粒度的近似）
      this.partial.clear();
    };

    if (!this.enabled) {
      apply();
      return;
    }
    this.writeChain = this.writeChain
      .then(() => this.#loadDay(dayKey))   // 先把该天历史读进来（只一次）
      .then(() => {
        apply();
        return this.#append(dayKey, line);
      })
      .catch((err) => log.warn(`用量记录失败(${dayKey}): ${err.message}`));
  }

  /** 等所有排队中的写入落盘（测试与关闭前使用） */
  async flush() {
    await this.writeChain.catch(() => {});
  }

  /**
   * 聚合查询。
   * @param {'today'|'d1'|'d7'|'d30'|'d90'|string} range
   * @returns {Promise<{requests:number,inputTokens:number,outputTokens:number,totalTokens:number,byModel:object}>}
   */
  async query(range = 'today') {
    const spec = parseRange(range);
    // 先等排队中的写入：内存增量与磁盘内容对齐，查询结果不落后于 record
    await this.flush();

    if (spec.kind === 'today') {
      // 自然日：只看本地日期 = 今天的那一天聚合
      return toResult(await this.#loadDay(localDayKey()));
    }

    // 滚动窗口：窗口内整天用按天聚合，跨边界的那一天按 ts 逐行过滤
    const cutoff = Date.now() - spec.days * DAY_MS;
    const total = emptyDay();
    for (const key of dayKeysBetween(localDayKey(cutoff), localDayKey())) {
      const whole = dayStartMs(key) >= cutoff;
      mergeDay(total, whole ? await this.#loadDay(key) : await this.#partialSince(key, cutoff));
    }
    return toResult(total);
  }

  /** §4.3：一次返回五个范围（外加生效价目表的元信息，面板据此显示币种符号） */
  async queryAll() {
    const out = {};
    for (const range of USAGE_RANGES) out[range] = await this.query(range);
    out.pricing = getPricing().meta();
    return out;
  }

  /** 清理超出保留期的天文件（按文件名日期判断），并同步清掉内存里对应的天 */
  async prune() {
    if (!this.enabled) return { removed: 0 };
    await this.flush();
    const cutoffKey = localDayKey(Date.now() - this.keepDays * DAY_MS);
    const todayKey = localDayKey();
    let removed = 0;
    try {
      const files = await readdir(this.dir);
      for (const f of files) {
        if (!DAY_FILE_RE.test(f)) continue;
        const key = f.slice(0, 10);
        if (key > todayKey || key >= cutoffKey) continue; // 未来日期不动；窗口内保留
        try {
          await unlink(path.join(this.dir, f));
          removed += 1;
          this.cache.delete(key);
          this.loadedDays.delete(key);
        } catch { /* 已被别的进程删掉 */ }
      }
    } catch { /* 目录不存在 */ }
    return { removed };
  }

  /** 启动时清理一次 + 每天滚动清理（timer 不阻止进程退出） */
  startTimers() {
    this.stopTimers();
    if (!this.enabled) return;
    this.prune().catch(() => {});
    this.timer = setInterval(() => { this.prune().catch(() => {}); }, DAY_MS);
    this.timer.unref?.();
  }

  stopTimers() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}

// ---- 单例：网关全局一个用量库 ----
let singleton = null;

/** 按当前配置（重新）装配用量库；返回新实例 */
export function configureUsage(opts = {}) {
  if (singleton) singleton.stopTimers();
  singleton = new UsageStore(opts);
  return singleton;
}

export function getUsage() {
  // 未配置时的兜底实例：只做内存聚合、**不落盘**。
  // 口径与 getTaskLog() 完全一致——落盘目录/保留天数属于"启动期一次性装配"的配置，
  // 只有 server.mjs 通过 configureUsage() 装配过才写盘；否则任何直接 import
  // lib/proxy.mjs 的调用方（单元测试等）都会在仓库 logs/ 下产生运行时垃圾。
  if (!singleton) singleton = new UsageStore({ enabled: false });
  return singleton;
}