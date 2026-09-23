// 任务日志：把每一次请求（任务）的路由过程、每次尝试、每个错误都落盘成 JSONL，
// 方便事后排查"为什么这次调用失败 / 为什么走了这家渠道"。
//
// 设计要点：
//   - 零依赖，异步追加写（appendFile），不阻塞事件循环；写失败只告警不抛出，
//     绝不能因为日志问题让网关请求失败（转发型网关以可用性优先）。
//   - 内存环形缓冲保留最近 N 条，供 /api/tasks 直接查询，不必读文件。
//   - 一次请求 = 一条记录：包含尝试链（每次 failover 的渠道/错误/耗时）与最终结果。
//   - 记录里带脱敏的 agent 标签与错误指纹，便于按错误类型聚合。

import { appendFile, mkdir, stat, rename, readdir, unlink } from 'node:fs/promises';
import path from 'node:path';
import { log } from './logger.mjs';

const RING_MAX = 500;      // 内存里保留最近多少条任务记录
const MAX_STRING = 600;    // 单字段最长字符数，避免日志被超长错误体撑爆
const FLUSH_DEBOUNCE_MS = 200;
// ---- 落盘轮转 ----
const DEFAULT_MAX_FILE_BYTES = 32 * 1024 * 1024; // 单档 32MB 上限
const DEFAULT_KEEP_FILES = 5;                    // 保留最近 5 档
// ---- 耗时分解（P3）----
// 每个 attempt 可以带三段耗时：排队（等并发许可）/ 首字节（发请求到响应头）/ 正文（响应头到读完）。
// 数据源在上游转发层（lib/proxy.mjs），任务日志只负责"承载 + 归一 + 聚合 + 脱敏"：
// 字段不存在时不补 0，直接不写这个键，消费方（面板 / /api/metrics）按"未采集"渲染。
const TIMING_KEYS = ['queueMs', 'ttfbMs', 'bodyMs'];
// 每个模型最多保留多少条耗时样本（算 p50/p95 用）。有界，避免长跑进程内存无上限增长。
const MODEL_SAMPLE_MAX = 500;
// 最多跟踪多少个模型名。模型名来自客户端请求（未命中模型池的请求也照样记一条），
// 必须有上界：否则任意模型名就能把 byModel/modelSamples 和 /api/metrics 的响应体积撑爆。
const MODEL_MAX = 200;
// 每模型聚合时，失败指纹 / 救回指纹各取 Top N
const MODEL_FP_TOP = 5;

// ---------- 密钥脱敏（B3） ----------
// 部分中转在 4xx/5xx 的响应体里会回显请求头或 key 片段，任务日志又会把这段文本原样落盘。
// 内存环形缓冲保留原文（排障要看到完整信息），**落盘前必须脱敏**。
const SECRET_PATTERNS = [
  // OpenAI / 各类中转的 key 前缀
  /\bsk-[A-Za-z0-9_\-]{6,}/g,
  // Authorization: Bearer xxx
  /(Bearer\s+)[A-Za-z0-9._\-]{6,}/gi,
  // 各种 key / token 键名后跟的长值（"apiKey":"..." / x-api-key: ... / access_token=...）
  // 负向先行断言排除 Bearer/Basic 这类"方案词"，否则 `Authorization: Bearer <token>`
  // 会被这里二次掩码成 `Authorization: ***REDACTED*** ***REDACTED***`，掩码形状难以辨认。
  /((?:api[-_]?key|x-api-key|authorization|access[-_]?token|refresh[-_]?token|auth[-_]?token|secret)["'\s:=]+)(?!(?:bearer|basic)\b)[A-Za-z0-9._\-]{6,}/gi,
];

/**
 * 把文本里疑似密钥的片段替换成掩码。
 * 落盘路径专用；失败不影响日志写入（异常由调用方吞掉）。
 */
export function redactSecrets(value) {
  if (value == null) return value;
  if (typeof value !== 'string') {
    // 非字符串（数字/对象）尝试序列化后脱敏，再还原成字符串交给调用方
    let s;
    try { s = JSON.stringify(value); } catch { return value; }
    return redactSecrets(s);
  }
  let out = value;
  for (const re of SECRET_PATTERNS) {
    out = out.replace(re, (m, group1) => (group1 ? `${group1}***REDACTED***` : '***REDACTED***'));
  }
  return out;
}

/** 错误分类指纹：把上游错误归到稳定的小类别，便于统计"哪类错误最多" */
export function errorFingerprint(kind, message) {
  const t = String(message || '').toLowerCase();
  if (/insufficient|balance|quota|欠费|余额|credit|exceeded your current quota|not enough/.test(t)) return 'insufficient_balance';
  if (kind === 'auth' || /invalid.*(api.?key|token)|unauthor|鉴权|未授权/.test(t)) return 'auth';
  if (/model.*(not|no).*(found|exist)|does not exist|unknown model|模型不存在|no provider supported/.test(t)) return 'model_not_found';
  if (kind === 'rate_limit' || /rate.?limit|tpm|rpm|too many requests|限流/.test(t)) return 'rate_limit';
  if (kind === 'timeout' || /timeout|timed out|超时/.test(t)) return 'timeout';
  if (kind === 'network' || /econn|enotfound|socket|fetch failed|network/.test(t)) return 'network';
  if (kind === 'tool_call_text') return 'tool_call_text_leak';
  if (kind === 'bad_response' || kind === 'stream_break') return kind;
  const m = /^http_(\d{3})$/.exec(String(kind || ''));
  if (m) return `http_${m[1]}`;
  return kind || 'unknown';
}

function cut(v, n = MAX_STRING) {
  if (v == null) return v;
  const s = typeof v === 'string' ? v : JSON.stringify(v);
  if (s == null) return null;
  return s.length > n ? s.slice(0, n) + '…' : s;
}

/** 把"毫秒时间戳"或"可被 Date.parse 解析的字符串"统一成毫秒；无效返回 null */
function toMs(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isNaN(t) ? null : t;
}

// ---------- 耗时分解（P3 §3.1） ----------

/** 单段耗时归一：只有"真正的有限数字且 >= 0"才算采到，其余一律视为未采集（null） */
function timingMs(v) {
  // 必须按类型严格判断：null / '' / false / [] 经 Number() 都会变成 0，
  // 那正好是"没采集"被伪装成"0ms"的坑（P3 §3.1 明确不许造假 0）。
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) return null;
  return Math.round(v);
}

/**
 * 归一化一条记录（或一次 attempt）上的三段耗时。
 *  - 缺失 / 非法 -> **删掉该键**（不写假的 0，消费方据此区分"没采到"与"真的是 0"）
 *  - `deriveTotal` 为真且至少采到一段时，补 `totalMs` = 各段之和，供面板算堆叠条比例
 */
function normalizeTimings(obj, { deriveTotal = false } = {}) {
  const out = { ...obj };
  let saw = false;
  for (const k of TIMING_KEYS) {
    const v = timingMs(out[k]);
    if (v == null) delete out[k];
    else { out[k] = v; saw = true; }
  }
  if (deriveTotal) {
    if (saw) out.totalMs = TIMING_KEYS.reduce((sum, k) => sum + (out[k] ?? 0), 0);
    else delete out.totalMs;
  }
  return out;
}

/**
 * 取"这条记录的代表性三段耗时"：优先最后一次 attempt（转发层实测），
 * 否则退回请求级字段。都没有则返回 null。
 */
function pickTimings(entry) {
  const attempts = Array.isArray(entry.attempts) ? entry.attempts : [];
  const last = attempts.length ? attempts[attempts.length - 1] : null;
  for (const src of [last, entry]) {
    if (!src) continue;
    if (TIMING_KEYS.some((k) => typeof src[k] === 'number')) return src;
  }
  return null;
}

/** 往有界样本环里追加一个耗时样本 */
function pushSample(arr, v) {
  if (typeof v !== 'number' || !Number.isFinite(v)) return;
  arr.push(v);
  if (arr.length > MODEL_SAMPLE_MAX) arr.splice(0, arr.length - MODEL_SAMPLE_MAX);
}

/**
 * 线性插值分位数（p 取 0-100）。样本为空返回 null —— 不造假 0。
 */
export function percentile(samples, p) {
  if (!Array.isArray(samples) || !samples.length) return null;
  const s = [...samples].sort((a, b) => a - b);
  if (s.length === 1) return s[0];
  const idx = (Math.min(100, Math.max(0, p)) / 100) * (s.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return s[lo];
  return Math.round(s[lo] + (s[hi] - s[lo]) * (idx - lo));
}

/** 指纹计数对象 -> Top N [{fingerprint, count}]（按次数降序，同次数按键名稳定排序） */
function topFingerprints(counts, n = MODEL_FP_TOP) {
  return Object.entries(counts || {})
    .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
    .slice(0, n)
    .map(([fingerprint, count]) => ({ fingerprint, count }));
}

/** 递归脱敏对象里的**所有字符串**：新增字段（如耗时的文本备注）不能绕过 B3 */
function redactStrings(value) {
  if (typeof value === 'string') return redactSecrets(value);
  if (Array.isArray(value)) return value.map(redactStrings);
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = redactStrings(v);
    return out;
  }
  return value;
}

/**
 * 生成"落盘专用"的脱敏副本（B3）。
 *
 * 内存环形缓冲保留原文（排障要看到完整信息），落盘的副本则把疑似密钥掩码掉：
 * 上游在 4xx/5xx 响应体里回显 Authorization / api key 片段是常见现象，
 * 任务日志又会把这段错误文本原样落盘——不脱敏就等于把密钥写进磁盘。
 *
 * 只处理会承载上游错误文本的字段（error / attempts[] / events[] 里的字符串），
 * 其余字段原样复制，避免误伤。
 * attempts[] / events[] 走**递归**脱敏：P3 新增的耗时字段可能带文本备注（note 等），
 * 不能因为"只认 error"就让新字段绕过脱敏。
 */
function redactedCopy(entry) {
  const out = { ...entry };
  if (out.error) out.error = redactSecrets(out.error);
  if (Array.isArray(out.attempts)) {
    out.attempts = out.attempts.map((a) => (a && typeof a === 'object' ? redactStrings(a) : a));
  }
  if (Array.isArray(out.events)) {
    out.events = out.events.map((e) => (e && typeof e === 'object' ? redactStrings(e) : e));
  }
  return out;
}

export class TaskLog {
  /**
   * @param {{enabled?:boolean, dir?:string, file?:string, maxBodyChars?:number,
   *          maxFileBytes?:number, keepFiles?:number}} opts
   *   enabled=false 时只保留内存环形缓冲（不落盘）
   *   maxFileBytes 单档上限（<=0 关闭轮转）；keepFiles 保留档数（含当前档）
   */
  constructor({ enabled = true, dir, file = 'tasks.jsonl', ringMax = RING_MAX, maxFileBytes, keepFiles } = {}) {
    this.enabled = enabled !== false;
    this.dir = dir || path.resolve(process.cwd(), 'logs');
    this.file = path.join(this.dir, file);
    this.ringMax = ringMax;
    // 轮转配置：maxFileBytes<=0 表示不轮转（保留旧行为）
    const maxBytes = Number(maxFileBytes);
    this.maxFileBytes = Number.isFinite(maxBytes) && maxBytes > 0 ? maxBytes : DEFAULT_MAX_FILE_BYTES;
    const keep = Number(keepFiles);
    this.keepFiles = Number.isFinite(keep) && keep >= 1 ? Math.floor(keep) : DEFAULT_KEEP_FILES;
    this.ring = [];
    this.seq = 0;
    this.stats = {
      tasks: 0,
      failed: 0,           // 整体失败（ok:false）的请求数
      failedAttempts: 0,   // 所有"尝试级"错误数：换家后即使整体成功也计入，排查就靠它
      attempts: 0,
      byFingerprint: {},   // 按错误指纹聚合（含尝试级）
      byChannel: {},
      byModel: {},         // P3：按模型聚合（requests/failed/failures/recovered），明细见 modelMetrics()
      writeErrors: 0,
    };
    // P3：每模型的三段耗时样本环（有界），modelMetrics() 用它算 p50/p95
    this.modelSamples = new Map(); // model -> { queueMs:[], ttfbMs:[], bodyMs:[] }
    this.modelSeen = new Map();    // model -> 最近一次写入的 seq（LRU：保证 byModel 有上界）
    this.pending = [];
    this.timer = null;
    this.ready = null;
    this.lastWriteError = null;
  }

  /** 惰性创建目录，只做一次 */
  async #ensureDir() {
    if (!this.ready) {
      this.ready = mkdir(this.dir, { recursive: true }).catch((err) => {
        this.stats.writeErrors += 1;
        this.lastWriteError = err.message;
        log.warn(`任务日志目录创建失败(${this.dir}): ${err.message}`);
      });
    }
    return this.ready;
  }

  #schedule() {
    if (!this.enabled || this.timer || !this.pending.length) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.#flush().catch(() => {});
    }, FLUSH_DEBOUNCE_MS);
    this.timer.unref?.();
  }

  /** 当前日志文件大小（不存在 = 0）。任何异常都退回 0，绝不能因为 stat 失败丢日志 */
  async #sizeOf(file) {
    try {
      const st = await stat(file);
      return st.size;
    } catch {
      return 0;
    }
  }

  /**
   * 落盘轮转（B2）：把当前档重命名成 tasks.1.jsonl，旧的依次后移（1->2, 2->3 …），
   * 并删除超出 keepFiles 的最老档。命名规则：<stem>.<i><ext>。
   * 任何一步失败都只影响该档，不阻断后续 append（日志写入优先级最高）。
   */
  async #rotate() {
    const base = this.file;
    const ext = path.extname(base) || '.jsonl';
    const stem = base.slice(0, base.length - ext.length);
    const archive = (i) => `${stem}.${i}${ext}`;
    const keep = this.keepFiles;

    if (keep <= 1) {
      // 只保留当前档：直接清空，不产生归档
      try { await unlink(base); } catch { /* 不存在 */ }
      return;
    }
    // 删掉最老的一档（archive(keep-1) 之后的就是要淘汰的）
    try { await unlink(archive(keep - 1)); } catch { /* 不存在 */ }
    // 依次后移
    for (let i = keep - 2; i >= 1; i -= 1) {
      try { await rename(archive(i), archive(i + 1)); } catch { /* 该档不存在，跳过 */ }
    }
    // 当前档 -> 第 1 档
    try { await rename(base, archive(1)); } catch { /* 当前档不存在，跳过 */ }
  }

  async #flush() {
    if (!this.pending.length) return;
    const lines = this.pending.splice(0, this.pending.length).map((r) => JSON.stringify(r)).join('\n') + '\n';
    try {
      await this.#ensureDir();
      // 轮转：写入前先判体积。size>0 才轮转——否则单次写入超过上限时会无限轮转空文件
      if (this.maxFileBytes > 0) {
        const size = await this.#sizeOf(this.file);
        if (size > 0 && size + Buffer.byteLength(lines) > this.maxFileBytes) {
          await this.#rotate();
        }
      }
      await appendFile(this.file, lines, 'utf8');
    } catch (err) {
      this.stats.writeErrors += 1;
      this.lastWriteError = err.message;
      log.warn(`任务日志写入失败(${this.file}): ${err.message}`);
    }
  }

  /** 立即把缓冲刷盘（关闭前调用） */
  async flushNow() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.#flush();
  }

  /**
   * 记录一条任务。
   * @param {object} rec 任务记录（允许任意字段，字符串会被截断）
   */
  write(rec) {
    this.seq += 1;
    const entry = normalizeTimings({
      seq: this.seq,
      ts: new Date().toISOString(),
      ...rec,
    });
    if (entry.error) entry.error = cut(entry.error);
    if (Array.isArray(entry.attempts)) {
      // 三段耗时随 attempt 一起承载：能拿到就留，拿不到就不写这个键（P3 §3.1）
      entry.attempts = entry.attempts.map((a) => normalizeTimings({
        ...a,
        error: a.error ? cut(a.error, 300) : a.error,
      }, { deriveTotal: true }));
    }

    this.stats.tasks += 1;
    if (entry.ok === false) this.stats.failed += 1;
    if (Number.isFinite(entry.tries)) this.stats.attempts += entry.tries;
    // 尝试级错误也要进统计：一次请求可能先撞了 3 家坏渠道、最后一家成功。
    // 整体是 200，但那 3 次错误正是运维要看的东西——只统计最终失败会漏掉绝大多数线索。
    if (Array.isArray(entry.attempts)) {
      for (const a of entry.attempts) {
        if (!a?.error) continue;
        const fp = a.fingerprint || errorFingerprint(a.kind, a.error);
        this.stats.failedAttempts += 1;
        this.stats.byFingerprint[fp] = (this.stats.byFingerprint[fp] || 0) + 1;
        if (a.channel) this.stats.byChannel[a.channel] = (this.stats.byChannel[a.channel] || 0) + 1;
      }
    }
    if (entry.fingerprint) {
      this.stats.byFingerprint[entry.fingerprint] = (this.stats.byFingerprint[entry.fingerprint] || 0) + 1;
    }
    if (entry.channel) {
      this.stats.byChannel[entry.channel] = (this.stats.byChannel[entry.channel] || 0) + 1;
    }
    this.#noteModel(entry);

    this.ring.push(entry);
    if (this.ring.length > this.ringMax) this.ring.splice(0, this.ring.length - this.ringMax);

    if (this.enabled) {
      // 内存 ring 存原文；落盘存脱敏副本（B3，密钥绝不写进磁盘）
      this.pending.push(redactedCopy(entry));
      this.#schedule();
    }
    return entry;
  }

  /**
   * 任务只写一条：模型级聚合在这里记账（P3 §3.3）。
   *
   * 口径说明（避免同一件事被重复计一次）：
   *  - `failures`：**最终失败**的请求按最终指纹计数——每个失败请求恰好计 1 次；
   *    尝试级错误不再重复计入（否则一次失败会变成 2 次，成功率/指纹数都对不上）。
   *  - `recovered`：整体成功、但过程中撞过错的尝试级指纹（"换家救回来"的那些），
   *    这些错误会被 overall ok 掩盖，单独记一笔用于看上游抖动。
   *  - 耗时样本只取**成功请求**：失败请求的 ttfb/body 语义不同（可能连响应体都没读完），
   *    混进首字节健康度会让 p95 失真。
   *  - 三段耗时字段缺失时不写键、不折算成 0（数据源在上游转发层，见 TIMING_KEYS 注释）。
   */
  #noteModel(entry) {
    const model = entry.model;
    if (model == null || model === '') return;
    let m = this.stats.byModel[model];
    if (!m) {
      m = { requests: 0, failed: 0, recovered: 0, failures: {}, recoveredFp: {} };
      this.stats.byModel[model] = m;
    }
    m.requests += 1;

    // LRU 上界：模型名来自客户端，跟踪表绝不能无界增长（否则内存与 /api/metrics 响应体积都能被撑爆）
    this.modelSeen.delete(model);
    this.modelSeen.set(model, this.seq);
    while (this.modelSeen.size > MODEL_MAX) {
      const oldest = this.modelSeen.keys().next().value;
      this.modelSeen.delete(oldest);
      delete this.stats.byModel[oldest];
      this.modelSamples.delete(oldest);
    }

    const failed = entry.ok === false;
    if (failed) {
      m.failed += 1;
      const fp = entry.fingerprint || errorFingerprint(entry.kind, entry.error);
      m.failures[fp] = (m.failures[fp] || 0) + 1;
    } else if (Array.isArray(entry.attempts)) {
      for (const a of entry.attempts) {
        if (!a?.error) continue;
        const fp = a.fingerprint || errorFingerprint(a.kind, a.error);
        m.recoveredFp[fp] = (m.recoveredFp[fp] || 0) + 1;
        m.recovered += 1;
      }
    }

    if (!failed) {
      const t = pickTimings(entry);
      if (t) {
        let s = this.modelSamples.get(model);
        if (!s) {
          s = { queueMs: [], ttfbMs: [], bodyMs: [] };
          this.modelSamples.set(model, s);
        }
        for (const k of TIMING_KEYS) pushSample(s[k], t[k]);
      }
    }
  }

  /**
   * 模型级指标（P3 §3.3）：请求数 / 成功率 / 失败指纹 Top5 / ttfb 的 p50、p95。
   * 没有样本时 p50/p95 为 null —— 不造假 0，面板按"未采集"渲染。
   * @param {{top?:number, limit?:number}} opts
   */
  modelMetrics({ top = MODEL_FP_TOP, limit = 50 } = {}) {
    const out = [];
    for (const [model, m] of Object.entries(this.stats.byModel)) {
      const s = this.modelSamples.get(model) || {};
      const succeeded = Math.max(0, m.requests - m.failed);
      const stat = (arr) => ({
        p50: percentile(arr, 50),
        p95: percentile(arr, 95),
        samples: Array.isArray(arr) ? arr.length : 0,
      });
      out.push({
        model,
        requests: m.requests,
        succeeded,
        failed: m.failed,
        successRate: m.requests ? Math.round((succeeded / m.requests) * 1000) / 1000 : null,
        failures: topFingerprints(m.failures, top),
        recovered: topFingerprints(m.recoveredFp, top),
        ttfb: stat(s.ttfbMs),
        queue: stat(s.queueMs),
        body: stat(s.bodyMs),
      });
    }
    // 请求多的排前面（面板一眼看到主力模型）
    out.sort((a, b) => (b.requests - a.requests) || a.model.localeCompare(b.model));
    return out.slice(0, Math.max(1, limit));
  }

  /**
   * 最近的任务记录（倒序）
   * @param {{onlyFailed?:boolean, fingerprint?:string, since?:number|string, until?:number|string}} opts
   *   since/until 支持毫秒时间戳或可被 Date.parse 解析的字符串（ISO 等），闭区间过滤。
   */
  recent(limit = 50, { onlyFailed = false, fingerprint = null, since = null, until = null } = {}) {
    const lo = toMs(since);
    const hi = toMs(until);
    let list = [...this.ring].reverse();
    if (lo != null || hi != null) {
      list = list.filter((r) => {
        const t = Date.parse(r.ts);
        if (Number.isNaN(t)) return false;
        if (lo != null && t < lo) return false;
        if (hi != null && t > hi) return false;
        return true;
      });
    }
    if (onlyFailed) list = list.filter((r) => r.ok === false);
    if (fingerprint) list = list.filter((r) => r.fingerprint === fingerprint);
    return list.slice(0, Math.max(0, limit));
  }

  /**
   * 只保留"错误集合"视图：每条记录可能带多个错误来源（尝试链 / 探活 / 鉴权），
   * 这里把它们摊平成独立条目，方便按错误聚合排查。
   */
  errors(limit = 100) {
    const out = [];
    for (const rec of [...this.ring].reverse()) {
      if (Array.isArray(rec.attempts)) {
        for (const a of rec.attempts) {
          if (!a?.error) continue;
          out.push({
            ts: rec.ts, seq: rec.seq, phase: 'attempt', channel: a.channel,
            kind: a.kind || null, fingerprint: a.fingerprint || errorFingerprint(a.kind, a.error),
            error: a.error, requestId: rec.requestId, agent: rec.agent, model: rec.model,
          });
        }
      }
      for (const ev of rec.events || []) {
        if (ev?.level === 'error' || ev?.level === 'warn') {
          out.push({
            ts: rec.ts, seq: rec.seq, phase: ev.phase || 'event', channel: ev.channel || null,
            kind: ev.kind || null, fingerprint: ev.fingerprint || errorFingerprint(ev.kind, ev.message),
            error: ev.message, requestId: rec.requestId, agent: rec.agent, model: rec.model,
          });
        }
      }
      if (rec.ok === false && rec.error) {
        out.push({
          ts: rec.ts, seq: rec.seq, phase: 'final', channel: rec.channel || null,
          kind: rec.kind || null, fingerprint: rec.fingerprint || errorFingerprint(rec.kind, rec.error),
          error: rec.error, requestId: rec.requestId, agent: rec.agent, model: rec.model,
        });
      }
      if (out.length >= limit) break;
    }
    return out.slice(0, limit);
  }

  snapshot({ limit = 50 } = {}) {
    return {
      enabled: this.enabled,
      file: this.enabled ? this.file : null,
      ringMax: this.ringMax,
      maxFileBytes: this.maxFileBytes,
      keepFiles: this.keepFiles,
      kept: this.ring.length,
      stats: this.stats,
      lastWriteError: this.lastWriteError,
      recent: this.recent(limit),
    };
  }
}

// ---- 单例：网关全局一个任务日志 ----
let singleton = null;

export function configureTaskLog(opts = {}) {
  singleton = new TaskLog(opts);
  return singleton;
}

export function getTaskLog() {
  if (!singleton) singleton = new TaskLog({ enabled: false });
  return singleton;
}
