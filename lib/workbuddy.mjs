// lib/workbuddy.mjs —— 国际版 WorkBuddy（腾讯 CodeBuddy 海外站）账号接入纯函数集。
//
// 上游基址固定 https://www.workbuddy.ai（聊天/授权/刷新/余额/用户信息全家桶同域名，
// 对齐 docs/workbuddy-intl-task.md §1.2/§4 与参考实现 codebuddy_code_channel_intl.py）。
// 本模块只提供**纯函数**，不持有状态、不读写配置：设备码授权（start/poll）、登录后
// 主动查用户信息（login/account 为主、plugin/accounts 兜底）、refresh_token 续期、
// 余额聚合、出站 body 风控脱敏（11128）+ tool_choice 归一（11101）、SSE 流聚合为
// 标准 OpenAI chat.completion JSON、渠道落号条目构造。
//
// 约定：
// - 统一信封 {code, msg, data}，code===0 为成功；业务错误抛 Error("WorkBuddy 业务错误 <code>: <msg>")；
// - 全部出站请求走 _req（proxy 非空 → 动态 import('./outbound-proxy.mjs') 的 gwFetch，
//   否则全局 fetch；超时 AbortSignal.timeout，外部 signal 用 AbortSignal.any 合并）；
// - 零依赖：只用 node: 内置能力与全局 fetch，不引入任何第三方包。

// 上游基址固定 https://www.workbuddy.ai；测试可用 WORKBUDDY_BASE_URL 指到 mock 上游
// （聊天/授权/刷新/余额/用户信息全家桶同域名，对齐 docs/workbuddy-intl-task.md §1.2/§4）。
export const WORKBUDDY_BASE = (process.env.WORKBUDDY_BASE_URL || 'https://www.workbuddy.ai').replace(/\/+$/, '');
export const WORKBUDDY_UA = 'CLI/2.63.2 CodeBuddy/2.63.2';

/** X-Domain 头默认值（= 基址域名，账号 domain 字段可覆盖） */
const WORKBUDDY_DOMAIN = 'www.workbuddy.ai';

// 余额查询时间窗口：PackageEndTimeRangeEnd = now + 365*101 天（对齐参考实现，覆盖全部在售套餐）
const BALANCE_WINDOW_DAYS = 365 * 101;

// ---------------------------------------------------------------------------
// 内部小工具
// ---------------------------------------------------------------------------

/**
 * 解析统一信封 JSON 文本 → data payload。
 * 兼容单层 {code,msg,data}、双层 data.data 嵌套、以及无 data 键的"顶层即 payload"；
 * 传入已解析对象亦可。解析失败/非对象返回 null（调用方自行判断）。
 * @param {string|object} input 响应文本或已解析的 JSON 对象
 * @returns {any|null} data payload，或 null
 */
export function _unwrapEnv(input) {
  let env = input;
  if (typeof input === 'string') {
    try { env = JSON.parse(input); } catch { return null; }
  }
  if (!env || typeof env !== 'object' || Array.isArray(env)) return null;
  let payload = Object.prototype.hasOwnProperty.call(env, 'data') ? env.data : env;
  if (payload && typeof payload === 'object' && !Array.isArray(payload)
    && Object.prototype.hasOwnProperty.call(payload, 'data')) {
    payload = payload.data;
  }
  return payload;
}

/**
 * 单字段取值：按顺序返回第一个非空值（转字符串）；空值指 null/undefined/''/空数组/空对象。
 * @param {object|null|undefined} d 数据源
 * @param {...string} keys 候选键，按优先级排列
 * @returns {string} 命中的值转字符串；未命中返回 ''
 */
export function _pick(d, ...keys) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return '';
  for (const k of keys) {
    const v = d[k];
    if (v === null || v === undefined) continue;
    if (typeof v === 'string' && v === '') continue;
    if (Array.isArray(v) && v.length === 0) continue;
    if (typeof v === 'object' && Object.keys(v).length === 0) continue;
    return String(v);
  }
  return '';
}

/** 把缺失的键从 src 合并进 target（先到先得，不覆盖已有键；空值跳过）。返回 target。 */
function _mergeMissing(target, src) {
  for (const [k, v] of Object.entries(src)) {
    if (v === null || v === undefined) continue;
    if (v === '') continue;
    if (Array.isArray(v) && v.length === 0) continue;
    if (typeof v === 'object' && Object.keys(v).length === 0) continue;
    if (!(k in target)) target[k] = v;
  }
  return target;
}

/** 解析 JSON 文本，失败返回 null（不抛）。 */
function _parseJson(text) {
  try { return JSON.parse(text); } catch { return null; }
}

/**
 * 业务错误抛出（统一信封 code!==0）。
 * @param {object|null} env 信封对象；null 时 code=-1、msg=''
 */
function _throwBiz(env) {
  const code = env && typeof env === 'object' ? env.code : -1;
  const msg = env && typeof env === 'object' ? String(env.msg ?? '') : '';
  throw new Error(`WorkBuddy 业务错误 ${code}: ${msg}`);
}

/**
 * 授权类请求共用的基础头（Origin/Referer 固定为基址，任务书逐字要求带尾斜杠）。
 * @param {object} extra 额外头（如 Content-Type / Authorization）
 * @returns {object}
 */
function _authHeaders(extra = {}) {
  return {
    'Accept': 'application/json, text/plain, */*',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': `${WORKBUDDY_BASE}/`,
    'Referer': `${WORKBUDDY_BASE}/`,
    'User-Agent': WORKBUDDY_UA,
    ...extra,
  };
}

/** 时间戳格式化为 "YYYY-MM-DD HH:MM:SS"（余额查询窗口用）。 */
function _fmtDateTime(d) {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} `
    + `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/**
 * 内部统一发请求 helper。
 * - proxy 非空 → 动态 import('./outbound-proxy.mjs') 用其 gwFetch；否则全局 fetch；
 * - body 一律为字符串（JSON.stringify 或 '{}'）；
 * - 超时 AbortSignal.timeout(timeoutMs)，与外部 signal 用 AbortSignal.any 合并；
 * - 返回响应体文本；HTTP>=400 → 抛 Error(`WorkBuddy HTTP <status> <前200字符>`)，
 *   错误对象带 .status 属性供调用方区分（如 410 授权过期）；网络/超时错误包装为
 *   Error('WorkBuddy 请求失败: …')。
 * @param {string} url 完整 URL
 * @param {object} [opts]
 * @param {string} [opts.method='GET']
 * @param {object} [opts.headers={}]
 * @param {string} [opts.body]
 * @param {string} [opts.proxy] 出站代理（如 http://127.0.0.1:7897），空则直连
 * @param {number} [opts.timeoutMs=15000]
 * @param {AbortSignal} [opts.signal] 外部取消信号
 * @returns {Promise<string>} 响应体文本
 */
export async function _req(url, { method = 'GET', headers = {}, body, proxy, timeoutMs = 15000, signal } = {}) {
  const timeout = AbortSignal.timeout(timeoutMs);
  const merged = signal ? AbortSignal.any([timeout, signal]) : timeout;
  let resp;
  try {
    if (proxy) {
      // 动态加载：outbound-proxy.mjs 未提供时不影响本模块加载/直连路径
      const { gwFetch } = await import('./outbound-proxy.mjs');
      resp = await gwFetch(url, { proxy, method, headers, body, signal: merged, redirect: 'follow' });
    } else {
      resp = await fetch(url, { method, headers, body, signal: merged, redirect: 'follow' });
    }
    const text = await resp.text();
    if (resp.status >= 400) {
      const err = new Error(`WorkBuddy HTTP ${resp.status} ${String(text).slice(0, 200)}`);
      err.status = resp.status;
      throw err;
    }
    return text;
  } catch (err) {
    if (err && err.status) throw err; // 已是 HTTP 层错误（带状态码），原样透传
    throw new Error(`WorkBuddy 请求失败: ${err && err.message ? err.message : String(err)}`);
  }
}

// ---------------------------------------------------------------------------
// 设备码授权
// ---------------------------------------------------------------------------

/**
 * 启动设备码授权：POST /v2/plugin/auth/state?platform=workbuddy。
 * authUrl 由上游签发（上游登录页），直接展示给用户打开——本模块绝不自行拼 authUrl。
 * @param {object} [opts]
 * @param {string} [opts.proxy] 出站代理
 * @returns {Promise<{state: string, authUrl: string}>}
 * @throws {Error} 网络失败 / HTTP>=400 / 业务错误 / 缺 state 或 authUrl
 */
export async function startDeviceFlow({ proxy } = {}) {
  const url = `${WORKBUDDY_BASE}/v2/plugin/auth/state?platform=workbuddy`;
  const headers = _authHeaders({ 'Content-Type': 'application/json' });
  const text = await _req(url, { method: 'POST', headers, body: '{}', proxy });
  const env = _parseJson(text);
  if (!env || typeof env !== 'object' || env.code !== 0) _throwBiz(env);
  const data = _unwrapEnv(env);
  const state = _pick(data, 'state');
  const authUrl = _pick(data, 'authUrl', 'auth_url');
  if (!state || !authUrl) throw new Error('WorkBuddy auth/state 未返回 state/authUrl');
  return { state, authUrl };
}

/**
 * 单步轮询授权结果：GET /v2/plugin/auth/token?state=<state>。
 * - HTTP 410 → {status:'expired'}（授权已过期，需重新发起）
 * - HTTP 5xx / 网络错误 → 抛错（调用方自行重试）
 * - code!==0 或 data 无 accessToken → {status:'pending'}（登录未完成，继续轮询）
 * - code===0 且 data.accessToken → {status:'authorized', accessToken, refreshToken?, domain?}
 * @param {string} state 上游签发的授权状态串
 * @param {object} [opts]
 * @param {string} [opts.proxy] 出站代理
 * @returns {Promise<{status:string, accessToken?:string, refreshToken?:string, domain?:string}>}
 * @throws {Error} 非 410 的 HTTP 错误 / 网络错误 / 超时
 */
export async function pollDeviceFlow(state, { proxy } = {}) {
  if (!state) throw new Error('pollDeviceFlow 缺少 state');
  const url = `${WORKBUDDY_BASE}/v2/plugin/auth/token?state=${encodeURIComponent(state)}`;
  const headers = _authHeaders();
  let text;
  try {
    text = await _req(url, { method: 'GET', headers, proxy });
  } catch (err) {
    if (err && err.status === 410) return { status: 'expired' };
    throw err; // 5xx / 网络错误等 → 抛错，由调用方重试
  }
  const env = _parseJson(text);
  if (!env || typeof env !== 'object') return { status: 'pending' };
  const data = _unwrapEnv(env);
  const access = _pick(data, 'accessToken');
  if ((env.code ?? 0) !== 0 || !access) return { status: 'pending' };
  const out = { status: 'authorized', accessToken: access };
  const rt = _pick(data, 'refreshToken');
  if (rt) out.refreshToken = rt;
  const domain = _pick(data, 'domain');
  if (domain) out.domain = domain;
  return out;
}

// ---------------------------------------------------------------------------
// 用户信息（登录完成后 Bearer 主动查）
// ---------------------------------------------------------------------------

/** 用户信息信封 → 扁平字段 dict：account/user 子对象 + 顶层并集（先到先得，不互覆盖）。 */
function _unwrapAcctInfo(env) {
  const payload = _unwrapEnv(env);
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
  const merged = {};
  for (const src of [payload.account, payload.user, payload]) {
    if (src && typeof src === 'object' && !Array.isArray(src)) _mergeMissing(merged, src);
  }
  return merged;
}

/** plugin/accounts 的 payload → 账号条目 dict 列表（list / {accounts|list|items|rows:[…]} 双形态）。 */
function _accountsEntries(payload) {
  if (Array.isArray(payload)) return payload.filter((x) => x && typeof x === 'object');
  if (payload && typeof payload === 'object') {
    for (const key of ['accounts', 'list', 'items', 'rows']) {
      const v = payload[key];
      if (Array.isArray(v)) return v.filter((x) => x && typeof x === 'object');
    }
  }
  return [];
}

/**
 * 登录完成后主动查用户信息（Bearer），拿 username / uid / enterpriseId。
 * 主：GET /v2/plugin/login/account?state=<state>（payload 允许 {account}/ {user}/顶层字段，
 *     扁平合并先到先得）；主路失败或缺 username+uid → 兜底 GET /v2/plugin/accounts。
 * 兜底采纳纪律：仅「有 uid 且条目 uid 匹配」或「恰好唯一条目」才采纳——绝不盲取第一条
 * （会串号把凭据并进已存在的其它账号行）。
 * 任一路失败都不抛（token 已有效，用户信息只是补充字段），返回尽力而为的结果。
 * @param {string} accessToken Bearer access token
 * @param {object} [opts]
 * @param {string} [opts.state] 授权 state（login/account 需要）
 * @param {string} [opts.proxy] 出站代理
 * @returns {Promise<{username?:string, uid?:string, enterpriseId?:string}>}
 */
export async function fetchUserInfo(accessToken, { state, proxy } = {}) {
  if (!accessToken) throw new Error('fetchUserInfo 缺少 accessToken');
  const bearer = { 'Authorization': `Bearer ${accessToken}` };
  let info = {};
  // —— 主：login/account?state= ——
  try {
    const qs = state ? `?state=${encodeURIComponent(state)}` : '';
    const text = await _req(`${WORKBUDDY_BASE}/v2/plugin/login/account${qs}`, {
      method: 'GET', headers: { ..._authHeaders(), ...bearer }, proxy,
    });
    info = _unwrapAcctInfo(text);
  } catch {
    /* 主路失败 → 走兜底 */
  }
  const username = _pick(info, 'username', 'login', 'nickname');
  const uid = _pick(info, 'uid', 'id', 'accountId');
  // —— 兜底：plugin/accounts（缺 username 或 uid 才走）——
  if (!username || !uid) {
    try {
      const text = await _req(`${WORKBUDDY_BASE}/v2/plugin/accounts`, {
        method: 'GET', headers: { ..._authHeaders(), ...bearer }, proxy,
      });
      const entries = _accountsEntries(_unwrapEnv(text));
      const uidRef = uid || _pick(info, 'uid', 'id');
      let chosen = null;
      for (const entry of entries) {
        if (uidRef && _pick(entry, 'uid', 'id') === uidRef) { chosen = entry; break; }
      }
      if (!chosen && entries.length === 1) chosen = entries[0]; // 恰唯一条目才采纳
      if (chosen && typeof chosen === 'object') _mergeMissing(info, chosen);
    } catch {
      /* 兜底失败不阻塞（token 已有效） */
    }
  }
  const out = {};
  const u = _pick(info, 'username', 'login', 'nickname', 'email', 'uid');
  if (u) out.username = u;
  const i = _pick(info, 'uid', 'id', 'accountId');
  if (i) out.uid = i;
  const e = _pick(info, 'enterpriseId', 'enterprise_id', 'eid');
  if (e) out.enterpriseId = e;
  return out;
}

// ---------------------------------------------------------------------------
// 令牌续期 / 余额
// ---------------------------------------------------------------------------

/**
 * 用 refresh_token 续期 access_token：POST /v2/plugin/auth/token/refresh。
 * 头带 X-Refresh-Token / X-Auth-Refresh-Source: workbuddy；有 enterprise_id 加 X-Enterprise-Id。
 * refreshToken 上游不回传时沿用旧值。
 * @param {object} [args]
 * @param {string} [args.accessToken] 旧 access token（本接口不参与鉴权，仅透传签名）
 * @param {string} [args.refreshToken] refresh token（必填）
 * @param {string} [args.user_id]
 * @param {string} [args.enterprise_id]
 * @param {string} [args.domain]
 * @param {string} [args.proxy]
 * @returns {Promise<{accessToken: string, refreshToken: string}>}
 * @throws {Error} 缺 refreshToken / 网络 / HTTP>=400 / 业务错误 / 响应缺 accessToken
 */
export async function refreshAccountToken({ accessToken, refreshToken, user_id, enterprise_id, domain, proxy } = {}) {
  if (!refreshToken) throw new Error('WorkBuddy 刷新缺少 refreshToken');
  const headers = _authHeaders({
    'Content-Type': 'application/json',
    'X-Refresh-Token': String(refreshToken),
    'X-Auth-Refresh-Source': 'workbuddy',
  });
  if (enterprise_id != null && enterprise_id !== '') headers['X-Enterprise-Id'] = String(enterprise_id);
  const text = await _req(`${WORKBUDDY_BASE}/v2/plugin/auth/token/refresh`, {
    method: 'POST', headers, body: '{}', proxy,
  });
  const env = _parseJson(text);
  if (!env || typeof env !== 'object' || env.code !== 0) _throwBiz(env);
  const data = _unwrapEnv(env);
  const newAccess = _pick(data, 'accessToken');
  if (!newAccess) throw new Error('WorkBuddy 刷新响应缺少 accessToken，请重新授权');
  const newRefresh = _pick(data, 'refreshToken') || String(refreshToken);
  return { accessToken: newAccess, refreshToken: newRefresh };
}

/**
 * 查询账号额度余量：POST /v2/billing/meter/get-user-resource。
 * headers：Bearer + X-User-Id（有 user_id）/ X-Enterprise-Id+X-Tenant-Id（有 enterprise_id）/
 * X-Domain（默认 www.workbuddy.ai）。
 * 解析 data.Response.Data.Accounts[]，每项取 CycleCapacityRemain（缺则 CapacityRemain），
 * 负值钳 0，累加返回整数。
 * @param {object} [args]
 * @param {string} [args.accessToken] Bearer access token（必填）
 * @param {string} [args.user_id]
 * @param {string} [args.enterprise_id]
 * @param {string} [args.domain]
 * @param {string} [args.proxy]
 * @returns {Promise<number>} 剩余额度聚合整数
 * @throws {Error} 缺 accessToken / 网络 / HTTP>=400 / 业务错误
 */
export async function queryBalance({ accessToken, user_id, enterprise_id, domain, proxy } = {}) {
  if (!accessToken) throw new Error('queryBalance 缺少 accessToken');
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'Accept': 'application/json',
    'Content-Type': 'application/json',
  };
  if (user_id != null && user_id !== '') headers['X-User-Id'] = String(user_id);
  if (enterprise_id != null && enterprise_id !== '') {
    headers['X-Enterprise-Id'] = String(enterprise_id);
    headers['X-Tenant-Id'] = String(enterprise_id);
  }
  headers['X-Domain'] = domain || WORKBUDDY_DOMAIN;
  const now = new Date();
  const end = new Date(now.getTime() + BALANCE_WINDOW_DAYS * 86400 * 1000);
  const body = JSON.stringify({
    PageNumber: 1,
    PageSize: 100,
    ProductCode: 'p_tcaca',
    Status: [0, 3],
    PackageEndTimeRangeBegin: _fmtDateTime(now),
    PackageEndTimeRangeEnd: _fmtDateTime(end),
  });
  const text = await _req(`${WORKBUDDY_BASE}/v2/billing/meter/get-user-resource`, {
    method: 'POST', headers, body, proxy,
  });
  const env = _parseJson(text);
  if (!env || typeof env !== 'object' || env.code !== 0) _throwBiz(env);
  const data = _unwrapEnv(env);
  const accounts = data && data.Response && data.Response.Data ? data.Response.Data.Accounts : null;
  let total = 0;
  if (Array.isArray(accounts)) {
    for (const acct of accounts) {
      if (!acct || typeof acct !== 'object') continue;
      let r = acct.CycleCapacityRemain;
      if (r === null || r === undefined) r = acct.CapacityRemain;
      if (typeof r === 'number' && Number.isFinite(r) && r > 0) total += Math.trunc(r); // 负值钳 0
    }
  }
  return total;
}

// ---------------------------------------------------------------------------
// 出站 body 风控脱敏（11128）+ tool_choice 归一（11101）
// ---------------------------------------------------------------------------
// 上游对请求体做「逐字精确匹配」黑名单审核：Claude Code/Codex 客户端注入的固定模板句、
// x-anthropic-billing-header 与 cc_* 键值、role=developer 命中即 400 code=11128；
// tool_choice 对象形式 400 code=11101。规则对齐参考实现 codebuddy_code_channel_intl.py
// 的 _SANITIZE_* 常量与 _sanitize_outbound_body。

/** 脱敏特征预检列表（Contains 快速路径）：命中任一即进入改写/剥离。
 *  裸 Claude / Codex 也列为特征：上游对"逐字精确匹配"黑名单，正文里出现即应替换
 *  （对齐参考实现 _SANITIZE_REWRITE_* 的全量替换语义，避免指纹预检把它们漏掉）。 */
const _SANITIZE_FEATURES = [
  'x-anthropic-billing-header',
  'cc_entrypoint=',
  'cc_version=',
  'You are Claude Code',
  'Main branch (',
  'You are a coding agent running in the Codex CLI',
  'Claude',
  'Codex',
];

/** 整段剥离：x-anthropic-billing-header 键值段（键名无关值） */
const _HDR_RE = /x-anthropic-billing-header:[^;\n]*;?\s*/gi;
/** 整段剥离：cc_* 尾随裸键值（循环清到无残留） */
const _KV_RE = /\bcc_[a-z0-9_]+=[^;\n]*;?\s*/gi;
/** 顺序改写（幂等硬要求：新串不得含旧串——workbuddy 不含 Claude/Codex，重试二次过钩子不滚雪球） */
const _SANITIZE_REWRITES = [
  ["You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK", ''],
  ['Claude Code', 'workbuddy'],
  ['Claude', 'workbuddy'],
  ['Codex', 'workbuddy'],
  ["Anthropic's official CLI for Claude", "Anthropic's official CLI tool for Claude"],
  ['Main branch (you will usually use this for PRs)', 'Default branch (you will usually use this for PRs)'],
];

/** 指纹预检：命中才进行改写/剥离（热路径零成本，原样返回）。 */
function _hasFingerprint(text) {
  for (const feature of _SANITIZE_FEATURES) {
    if (text.includes(feature)) return true;
  }
  return /x-anthropic-billing-header:/i.test(text);
}

/** 单段文本脱敏：预检不中原样返回；命中才改写/剥离并去首尾空白。 */
function _sanitizeText(text) {
  if (!_hasFingerprint(text)) return text;
  for (const [oldStr, newStr] of _SANITIZE_REWRITES) {
    text = text.split(oldStr).join(newStr);
  }
  if (/x-anthropic-billing-header:/i.test(text)) text = text.replace(_HDR_RE, '');
  if (text.includes('cc_')) {
    let prev = '';
    while (prev !== text) {
      prev = text;
      text = text.replace(_KV_RE, '');
    }
  }
  return text.trim();
}

/**
 * 单条消息 content 的脱敏副本：(新 content, 是否变化)。
 * 兼容字符串与多模态 parts 数组（只动 text part）；不改原对象（幂等可重入的前提）。
 * @param {any} content messages[i].content
 * @returns {[any, boolean]}
 */
function _sanitizeContentCopy(content) {
  if (typeof content === 'string') {
    const sanitized = _sanitizeText(content);
    return [sanitized, sanitized !== content];
  }
  if (Array.isArray(content)) {
    let parts = null;
    for (let idx = 0; idx < content.length; idx += 1) {
      const part = content[idx];
      if (part && typeof part === 'object' && typeof part.text === 'string') {
        const sanitized = _sanitizeText(part.text);
        if (sanitized !== part.text) {
          if (parts === null) parts = content.slice();
          parts[idx] = { ...part, text: sanitized };
        }
      }
    }
    if (parts !== null) return [parts, true];
  }
  return [content, false];
}

/**
 * tool_choice 归一为上游能解析的 string 形态（对象形式 400 code=11101，对齐
 * 参考实现 _normalize_tool_choice_out）：
 * - "none" / {type:"none"} → 删 tool_choice + 删 tools/functions
 * - {type:"auto"|"required"|"any"} → "auto" / "required"
 * - {type:"function"|"tool", function:{name}} → 函数名（缺名回退 "auto"）
 * - 其它无法识别 → 删字段
 * @param {object} body
 * @returns {object} 可能返回新对象或原对象
 */
function _normalizeToolChoice(body) {
  const tc = body.tool_choice;
  if (tc === undefined || tc === null) return body;
  if (typeof tc === 'string') {
    if (tc.trim().toLowerCase() !== 'none') return body;
    const out = { ...body };
    delete out.tool_choice; delete out.tools; delete out.functions;
    return out;
  }
  const out = { ...body };
  if (typeof tc === 'object') {
    const typ = String(tc.type || '').trim().toLowerCase();
    if (typ === 'none') {
      delete out.tool_choice; delete out.tools; delete out.functions;
    } else if (typ === 'auto' || typ === 'required' || typ === 'any') {
      out.tool_choice = typ === 'any' ? 'required' : typ;
    } else if (typ === 'function' || typ === 'tool') {
      const fn = tc.function && typeof tc.function === 'object' ? tc.function : {};
      const name = String(fn.name || tc.name || '').trim();
      out.tool_choice = name || 'auto';
    } else {
      delete out.tool_choice;
    }
  } else {
    delete out.tool_choice;
  }
  return out;
}

/**
 * 出站 body 总入口：role developer→system + 指纹脱敏 + tool_choice 归一。
 *
 * ⚠️ 不修改原对象：出站 messages 可能与客户端请求体/请求日志共享 dict，原地改会污染；
 * 重试路径二次过本钩子的幂等也靠「不改原值 + 新串不含旧串」保证。
 *
 * @param {object} body OpenAI 出站请求体
 * @returns {object} 脱敏后的 body（无指纹/无 developer/tool_choice 合法时原对象原样返回）
 */
export function sanitizeWorkbuddyBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return body;
  const items = Array.isArray(body.messages) ? body.messages : [];

  let newMessages = null;
  for (let idx = 0; idx < items.length; idx += 1) {
    const msg = items[idx];
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) continue;
    const role = String(msg.role || '').trim().toLowerCase();
    const [content, changed] = _sanitizeContentCopy(msg.content);
    if (role === 'developer') {
      if (newMessages === null) newMessages = items.slice();
      const replaced = { ...msg, role: 'system' };
      if (changed) replaced.content = content;
      newMessages[idx] = replaced;
    } else if (changed) {
      if (newMessages === null) newMessages = items.slice();
      newMessages[idx] = { ...msg, content };
    }
  }

  let newSystem = null;
  if (typeof body.system === 'string' && _hasFingerprint(body.system)) {
    newSystem = _sanitizeText(body.system);
  }

  let out = body;
  if (newMessages !== null || newSystem !== null) {
    out = { ...body };
    if (newMessages !== null) out.messages = newMessages;
    if (newSystem !== null) out.system = newSystem;
  }
  return _normalizeToolChoice(out);
}

// ---------------------------------------------------------------------------
// SSE 流聚合 → 标准 OpenAI chat.completion JSON（非流式客户端消费 forceStream 渠道）
// ---------------------------------------------------------------------------

/**
 * 把上游 SSE 事件序列聚合为标准 OpenAI chat.completion JSON。
 * 入参为 {data: string} 对象的数组或异步可迭代器（for await 统一处理，数组也可）。
 * 跳过 data==='[DONE]' 与空帧；逐帧取 choices[0].delta：
 * - content 逐帧拼接
 * - tool_calls 按 index 合并（id/name 取首帧、arguments 拼接）
 * - finish_reason 取末帧非空值
 * - usage 取末帧
 * - id / model 保留自帧（缺省时生成兜底 id）
 * @param {Array<{data:string}> | AsyncIterable<{data:string}>} events
 * @returns {Promise<object>} 标准 chat.completion 形态 JSON
 */
export async function aggregateStreamToCompletion(events) {
  let id = null;
  let model = null;
  let created = null;
  let content = '';
  const toolCalls = new Map(); // index -> {id, name, arguments}
  let finishReason = null;
  let usage = null;

  for await (const ev of events) {
    const data = ev && ev.data;
    if (!data || data === '[DONE]') continue;
    const chunk = _parseJson(data);
    if (!chunk || typeof chunk !== 'object' || Array.isArray(chunk)) continue;
    if (!id && chunk.id) id = chunk.id;
    if (!model && chunk.model) model = chunk.model;
    if (created === null && chunk.created !== undefined && chunk.created !== null) created = chunk.created;
    if (chunk.usage && typeof chunk.usage === 'object') usage = chunk.usage;

    const choice = Array.isArray(chunk.choices) && chunk.choices[0] ? chunk.choices[0] : null;
    if (!choice || typeof choice !== 'object') continue;
    const delta = choice.delta && typeof choice.delta === 'object' ? choice.delta : {};
    if (typeof delta.content === 'string') content += delta.content;
    if (Array.isArray(delta.tool_calls)) {
      for (const tc of delta.tool_calls) {
        if (!tc || typeof tc !== 'object') continue;
        const idx = tc.index ?? 0;
        let slot = toolCalls.get(idx);
        if (!slot) {
          slot = { id: null, name: null, arguments: '' };
          toolCalls.set(idx, slot);
        }
        if (!slot.id && tc.id) slot.id = String(tc.id);
        const fn = tc.function && typeof tc.function === 'object' ? tc.function : {};
        if (!slot.name && fn.name) slot.name = String(fn.name);
        if (typeof fn.arguments === 'string') slot.arguments += fn.arguments;
      }
    }
    if (choice.finish_reason) finishReason = choice.finish_reason;
  }

  const message = { role: 'assistant', content };
  if (toolCalls.size > 0) {
    message.tool_calls = [...toolCalls.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([, slot]) => ({
        id: slot.id,
        type: 'function',
        function: { name: slot.name, arguments: slot.arguments },
      }));
  }

  const out = {
    id: id || `chatcmpl-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`,
    object: 'chat.completion',
    created: created !== null ? created : Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message,
      finish_reason: finishReason || 'stop',
    }],
  };
  if (usage && typeof usage === 'object') out.usage = usage;
  return out;
}

// ---------------------------------------------------------------------------
// 渠道落号条目构造（一账号 = 一渠道，落 config.json 用）
// ---------------------------------------------------------------------------

/**
 * 构造 WorkBuddy 国际版渠道条目（设备码授权成功/手动添加共用）。
 * name = 'wb-intl-' + (username 或 'acct' 小写化、非法字符换 '-'、截断 40、保证非空)。
 * headers 只包含非空字段；refreshToken 有值才带上。
 * @param {object} [args]
 * @param {string} [args.username]
 * @param {string} [args.accessToken] access token（apiKey）
 * @param {string} [args.refreshToken]
 * @param {string} [args.uid] 用户 ID（X-User-Id）
 * @param {string} [args.enterpriseId] 企业 ID（X-Enterprise-Id，可选）
 * @param {string} [args.domain] X-Domain（可选）
 * @param {string} [args.model='deepseek-v4.1-flash'] 默认绑定模型
 * @param {number} [args.priority=60] 渠道优先级
 * @returns {object} 渠道条目
 */
export function buildProvisionEntry({
  username,
  accessToken,
  refreshToken,
  uid,
  enterpriseId,
  domain,
  model = 'deepseek-v4.1-flash',
  priority = 60,
} = {}) {
  let suffix = String(username || 'acct')
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .slice(0, 40);
  if (!suffix) suffix = 'acct';

  const headers = {};
  if (uid != null && uid !== '') headers['X-User-Id'] = String(uid);
  if (enterpriseId != null && enterpriseId !== '') headers['X-Enterprise-Id'] = String(enterpriseId);
  if (domain != null && domain !== '') headers['X-Domain'] = String(domain);

  const entry = {
    name: `wb-intl-${suffix}`,
    preset: 'workbuddy-intl',
    apiKey: String(accessToken || ''),
    model,
    priority,
    headers,
    description: `WorkBuddy 国际版账号 ${username || uid || ''}`,
  };
  if (refreshToken != null && refreshToken !== '') entry.refreshToken = String(refreshToken);
  return entry;
}