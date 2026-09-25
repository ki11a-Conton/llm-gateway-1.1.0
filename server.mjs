#!/usr/bin/env node
// 本地多模型聚合网关：一个 base_url + 一个 api key，自动路由到可用的上游供应商
// 零依赖，仅用 Node 内置模块。Node >= 20

import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { log } from './lib/logger.mjs';
import { resolveConfigPath, ChannelManager, ROOT } from './lib/channels.mjs';
import { PRESETS } from './lib/presets.mjs';
import {
  handleChatCompletions,
  readJsonBody,
  sendJson,
  corsHeaders,
  openaiError,
  getStackedKeyStats,
} from './lib/proxy.mjs';
import { fromAnthropicBody } from './lib/adapters/anthropic.mjs';
import { identifyAgent } from './lib/agent.mjs';
import { configureTaskLog, getTaskLog } from './lib/tasklog.mjs';
import { configureUsage, getUsage, USAGE_RANGES } from './lib/usage.mjs';
import { configurePricing, getPricing } from './lib/pricing.mjs';
import { isOverloadError } from './lib/concurrency.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));

// ---------- 命令行参数 ----------
function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const [k, inlineV] = a.slice(2).split('=');
    const v = inlineV ?? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : 'true');
    out[k] = v;
  }
  return out;
}

const args = parseArgs(process.argv);
if (args.help || args.h) {
  console.log(`用法: node server.mjs [选项]
  --port <n>           监听端口，默认 8787（覆盖 config.json）
  --host <addr>        监听地址，默认 127.0.0.1
  --config <path>      指定配置文件，默认 ./config.json
  --api-key <key>      对外 API Key（覆盖 config.json）
  --log-level <lv>     debug|info|warn|error|silent
  --no-discover        启动时不拉取上游模型列表
  --list-presets       列出所有可用的 provider 预设后退出
  --refresh-models     拉取所有渠道的模型列表并写回 config.json，然后退出
  --no-save            配合 --refresh-models：只打印，不写回配置`);
  process.exit(0);
}

if (args['log-level']) log.setLevel(args['log-level']);

// ---------- 初始化 ----------
const configPath = resolveConfigPath(args.config);
const manager = new ChannelManager(configPath);

try {
  manager.load();
} catch (err) {
  log.error(`配置加载失败: ${err.message}`);
  log.raw(`  -> ${configPath}`);
  process.exit(1);
}

// 监听地址 / 端口只能在启动时定（socket 已绑定）；其余 server.* 配置一律"用时再读"，
// 否则 /api/reload 与配置文件热重载都改不动它们（轮换 apiKey 后旧 key 仍有效）。
const bootServerCfg = manager.config.server || {};
const HOST = args.host || process.env.GW_HOST || bootServerCfg.host || '127.0.0.1';
const PORT = Number(args.port || process.env.GW_PORT || bootServerCfg.port || 8787);
// 允许访问管理面的"本机"Host / Origin 白名单（防 DNS rebinding 与跨站 CSRF，见 isLocalHost）
const LOCAL_HOSTS = new Set(['127.0.0.1', 'localhost', '::1']);
// 显式绑到具体地址（--host 192.168.x.x）时也认这个 Host，避免正常部署被误伤；
// 通配地址（0.0.0.0 / ::）不加入白名单。
if (HOST && !['0.0.0.0', '::', '*'].includes(String(HOST))) LOCAL_HOSTS.add(String(HOST).toLowerCase());

/** 当前生效的对外 API Key：命令行 / 环境变量优先，其次读**当前**配置（每次调用重读，热重载即生效） */
function currentApiKey() {
  return args['api-key'] || process.env.GW_API_KEY || manager.config?.server?.apiKey || 'PROXY_MANAGED';
}

/** 面板开关：每次请求重读当前配置，改 server.panel 后 /api/reload 即生效 */
function panelEnabled() {
  return (manager.config?.server || {}).panel !== false;
}

/** 入站模型名归一化：客户端写的模型名 -> 网关内部的逻辑模型名（每次调用重读，改 modelMap 后生效） */
function mapModel(m) {
  const map = manager.config?.modelMap || {};
  return map[m] ?? m;
}

/** apiKey 掩码：面板只需要"配了哪个 key"的提示，完整密钥不再出门（Bug 1） */
function maskApiKey(key) {
  const k = String(key || '');
  if (!k) return '';
  if (k.length <= 8) return '•'.repeat(k.length);
  return `${k.slice(0, 3)}${'•'.repeat(Math.min(8, k.length - 5))}${k.slice(-2)}`;
}

/** Host 头必须是本机名（含端口）：恶意域名解析到 127.0.0.1 时浏览器仍会带攻击者域名，这里挡掉 */
function isLocalHost(hostHeader) {
  if (!hostHeader) return false;
  let h = String(hostHeader).trim().toLowerCase();
  if (h.startsWith('[')) {
    const end = h.indexOf(']'); // [::1]:8787
    if (end < 0) return false;
    h = h.slice(1, end);
  } else {
    h = h.split(':')[0]; // 127.0.0.1:8787 / localhost:8787
  }
  return LOCAL_HOSTS.has(h);
}

/**
 * Origin 只允许"本机回环"来源：浏览器对跨站请求**总会**带 Origin（含简单表单 POST），
 * 这条能干净地挡掉 CSRF。没有 Origin 头的是非浏览器请求（curl / 模型客户端），放行。
 */
function isLocalOrigin(origin) {
  if (origin == null || origin === '') return true;
  try {
    const u = new URL(String(origin));
    return LOCAL_HOSTS.has(u.hostname.toLowerCase().replace(/^\[|\]$/g, ''));
  } catch {
    return false; // Origin: null / 畸形值一律视为跨站
  }
}

// ---------- 任务日志 ----------
// 每次调用 = 一条任务记录（路由过程 + 每次尝试 + 错误），落盘 JSONL 供事后修复。
// taskLog.enabled=false 时只保留内存里的最近 N 条（面板 / /api/tasks 仍可看）。
let tasklog = null;
let taskLogKey = null;

/** 把 config.taskLog 归一化成 TaskLog 的构造参数（启动与热重载共用一份口径） */
function taskLogOptions(cfg) {
  const c = cfg?.taskLog || {};
  return {
    enabled: c.enabled !== false,
    dir: c.dir ? path.resolve(ROOT, c.dir) : path.join(ROOT, 'logs'),
    file: c.file || 'tasks.jsonl',
    ringMax: Number(c.ringMax) || 500,
    // 轮转：单档体积上限 + 保留档数（默认 32MB / 5 档）
    maxFileBytes: Number(c.maxFileBytes) || undefined,
    keepFiles: Number(c.keepFiles) || undefined,
  };
}

/**
 * 按当前配置（重新）装配任务日志。taskLog.* 也属于"启动期一次性捕获"的配置，
 * /api/reload 与配置文件热重载必须让它生效，否则改了 taskLog 段等于没改（Bug 4）。
 * 配置段没变时不动，避免白白清空内存里已有的最近记录。
 * @returns {boolean} 是否真的重建了实例
 */
function applyTaskLog(cfg, { force = false } = {}) {
  const key = JSON.stringify(cfg?.taskLog ?? null);
  if (!force && key === taskLogKey) return false;
  // 换配置前先把旧实例内存里没落盘的记录刷出去，避免热重载丢日志（日志失败不影响主流程）
  if (tasklog) {
    try { Promise.resolve(tasklog.flushNow?.()).catch(() => {}); } catch { /* ignore */ }
  }
  taskLogKey = key;
  tasklog = configureTaskLog(taskLogOptions(cfg));
  return true;
}
applyTaskLog(manager.config, { force: true });

// ---------- Token 用量库 ----------
// 落盘 logs/usage/YYYY-MM-DD.jsonl（本地时区分天），保留 taskLog.usageKeepDays 天（默认 120）。
// taskLog.enabled=false（"任务日志只留内存"的运行口径）时用量库同样只做内存聚合、不落盘——
// 与任务日志是同族运行时产物，避免"关了日志还在写盘"的意外。
// 与 applyTaskLog 一样属于"启动期一次性捕获"，所以 /api/reload 与配置文件热重载都要重放（Bug 4 口径）。
let usageKey = null;

/** 把 config 归一化成 UsageStore 的构造参数（启动与热重载共用一份口径） */
function usageOptions(cfg) {
  const c = cfg?.taskLog || {};
  const keep = Number(c.usageKeepDays);
  const tl = taskLogOptions(cfg);
  return {
    dir: path.join(tl.dir, 'usage'),
    keepDays: Number.isFinite(keep) && keep > 0 ? Math.floor(keep) : undefined,
    enabled: c.enabled !== false,
  };
}

/** @returns {boolean} 是否真的重建了实例 */
function applyUsage(cfg, { force = false } = {}) {
  const opts = usageOptions(cfg);
  const key = JSON.stringify(opts);
  if (!force && key === usageKey) return false;
  usageKey = key;
  configureUsage(opts).startTimers();
  return true;
}
applyUsage(manager.config, { force: true });

// ---------- 价格计费 ----------
// 单价来自 config 的 pricing 段（单位：每 100 万 token 的金额）；没配就用 lib/pricing.mjs 的内置参考价。
// 与 applyUsage 同属"启动期一次性捕获"，因此 /api/reload 与配置文件热重载都要重放（Bug 4 口径）。
let pricingKey = null;

/** 把 config 归一化成 Pricing 的构造参数（启动与热重载共用一份口径） */
function pricingOptions(cfg) {
  const p = cfg?.pricing || {};
  return { currency: p.currency, symbol: p.symbol, models: p.models };
}

/** @returns {boolean} 是否真的重建了实例 */
function applyPricing(cfg, { force = false } = {}) {
  const opts = pricingOptions(cfg);
  const key = JSON.stringify(opts);
  if (!force && key === pricingKey) return false;
  pricingKey = key;
  configurePricing(opts);
  return true;
}
applyPricing(manager.config, { force: true });

// ---------- 纯工具模式（不启动服务）----------
if (args['list-presets']) {
  const builtin = new Set(Object.keys(PRESETS));
  log.raw('\n可用 provider 预设（在 config.json 里用 "preset": "<名字>" 引用）:\n');
  for (const [k, v] of Object.entries(manager.presets)) {
    const mark = builtin.has(k) ? ' ' : '*';
    log.raw(
      ` ${mark} ${k.padEnd(14)} ${String(v.protocol).padEnd(10)} ${String(v.baseUrl).padEnd(56)} ${v.description ? '#' + v.description : ''}`,
    );
  }
  log.raw(`\n  * = 来自 providers.json 的自定义 provider（共 ${Object.keys(manager.presets).length} 个）`);
  log.raw('  任意字段都能在渠道配置里写同名键覆盖，例如 {"preset":"deepseek","baseUrl":"https://my-mirror/v1"}\n');
  process.exit(0);
}

if (args['refresh-models']) {
  const results = await manager.discoverAll({ force: true });
  log.raw('');
  for (const r of results) {
    log.raw(`  ${r.ok ? '✓' : '✗'} ${r.name.padEnd(14)} ${r.ok ? `${r.count} 个模型` : r.error}`);
  }
  log.raw('');
  if (args['no-save'] === undefined) {
    const { changed, file } = manager.saveDiscoveredModels();
    log.ok(changed ? `已写回 ${changed} 个渠道的模型白名单 -> ${file}` : '模型列表无变化，未写入');
    if (changed) log.raw('  提示：写回后这些渠道变成固定白名单，想恢复自动发现把 channels[].models 改成 []');
  }
  log.raw('');
  process.exit(0);
}

// ---------- 鉴权 ----------
function authorized(req) {
  const key = currentApiKey(); // 每次重读：轮换 server.apiKey 后 /api/reload 立即生效
  if (!key) return true;
  const auth = req.headers.authorization || '';
  const bearer = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : null;
  const token = bearer || req.headers['x-api-key'] || req.headers['api-key'] || '';
  return token === key;
}

function unauthorized(res) {
  // 不要在这里回显真实 key：/v1/* 保留宽松 CORS，回显等于把 apiKey 送给任意网页。
  return sendJson(
    res,
    401,
    openaiError('API key 无效。请在 Authorization 头中携带 Bearer <你的 API Key>', 'invalid_request_error', 'invalid_api_key', 401),
  );
}

// ---------- HTTP 服务 ----------
const PANEL_FILE = path.join(HERE, 'public', 'index.html');

const server = http.createServer(async (req, res) => {
  const requestId = 'req_' + Math.random().toString(36).slice(2, 8);
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname;

  // CORS 只发给 /v1（模型客户端用，需要跨源）。/api 是管理面：同源面板不需要任何 CORS 头，
  // 一旦回 `access-control-allow-origin: *`，任意网页都能读到响应体（含 apiKey）并改写配置（Bug 1）。
  if (pathname.startsWith('/v1/')) corsHeaders(res);

  // ---- 管理面安全前置检查 ----
  // 1) Host 必须是本机名：防 DNS rebinding（攻击者域名解析到 127.0.0.1）
  // 2) /api/* 的 Origin 必须是本机同源：防 CSRF（浏览器跨站请求总能被看到 Origin）
  const isAdminPath = pathname.startsWith('/api/');
  if (isAdminPath || pathname === '/' || pathname === '/panel') {
    if (!isLocalHost(req.headers.host)) {
      log.warn(`拒绝非本机 Host 请求 ${req.method} ${pathname} host=${req.headers.host}`, requestId);
      return sendJson(res, 403, { ok: false, error: '非法的 Host 头：管理面只接受本机回环地址' });
    }
    if (isAdminPath && !isLocalOrigin(req.headers.origin)) {
      log.warn(`拒绝跨站管理请求 ${req.method} ${pathname} origin=${req.headers.origin}`, requestId);
      return sendJson(res, 403, { ok: false, error: '跨站请求已被拒绝' });
    }
  }

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  try {
    // ---- 面板与内部状态 ----
    if (pathname === '/' || pathname === '/panel') {
      if (!panelEnabled()) return sendJson(res, 404, { error: 'panel disabled' });
      const html = existsSync(PANEL_FILE) ? readFileSync(PANEL_FILE, 'utf8') : '<h1>panel missing</h1>';
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(html);
    }
    if (pathname === '/health') {
      const snap = manager.snapshot();
      const healthy = snap.channels.filter((c) => c.enabled && c.healthy).length;
      const conc = manager.limiter.stats();
      return sendJson(res, 200, {
        status: healthy ? 'ok' : 'degraded',
        channels: snap.channels.length,
        healthy,
        models: snap.models.length,
        uptime: Math.round(process.uptime()),
        inFlight: conc.global ? conc.global.active : null,
        queued: conc.global ? conc.global.pending : null,
      });
    }
    const isLoopback = ['::1', '127.0.0.1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress || '');
    const trusted = isLoopback || authorized(req);

    if (pathname === '/api/tasks') {
      if (!trusted) return unauthorized(res);
      const limit = Math.min(500, Number(url.searchParams.get('limit')) || 50);
      const onlyFailed = url.searchParams.get('failed') === '1';
      const fingerprint = url.searchParams.get('fingerprint') || null;
      // 时间窗：since/until 支持毫秒时间戳或 ISO 字符串（闭区间）
      const since = url.searchParams.get('since');
      const until = url.searchParams.get('until');
      return sendJson(res, 200, {
        ...tasklog.snapshot({ limit }),
        since: since || null,
        until: until || null,
        recent: tasklog.recent(limit, { onlyFailed, fingerprint, since, until }),
      });
    }
    if (pathname === '/api/errors') {
      if (!trusted) return unauthorized(res);
      const limit = Math.min(1000, Number(url.searchParams.get('limit')) || 100);
      return sendJson(res, 200, {
        file: tasklog.enabled ? tasklog.file : null,
        byFingerprint: tasklog.stats.byFingerprint,
        errors: tasklog.errors(limit),
      });
    }
    if (pathname === '/api/status') {
      if (!trusted) return unauthorized(res);
      // 不再回传完整 apiKey（Bug 1）：只给"面板显示用"的掩码，完整密钥留在 config.json 与启动日志里。
      const key = currentApiKey();
      return sendJson(res, 200, {
        server: { host: HOST, port: PORT, panel: panelEnabled(), apiKeyMasked: maskApiKey(key) },
        ...manager.snapshot(),
      });
    }
    if (pathname === '/api/metrics') {
      if (!trusted) return unauthorized(res);
      const conc = manager.limiter.stats();
      const mem = process.memoryUsage();
      // 只回有活动的渠道/代理，避免渠道多时全量列表淹没关键信息
      const busyChannels = Object.fromEntries(
        Object.entries(conc.channels).filter(([, s]) => s.active > 0 || s.pending > 0),
      );
      const busyAgents = Object.fromEntries(
        Object.entries(conc.agents).filter(([, s]) => s.active > 0 || s.pending > 0),
      );
      // 排队时长（P3 §3.2）：字段由 lib/concurrency.mjs 的 GatewayLimiter.stats() 产出，
      // 这里**只透传**——字段不存在就不写这个键（绝不兜底成 0，否则面板会把"没采集"显示成"不用排队"）。
      const queueWait = {};
      for (const k of ['queueWaitMsTotal', 'queueWaitMsMax', 'waitedCount']) {
        if (conc[k] !== undefined && conc[k] !== null) queueWait[k] = conc[k];
      }
      return sendJson(res, 200, {
        uptime: Math.round(process.uptime()),
        memory: { rss: mem.rss, heapUsed: mem.heapUsed, external: mem.external },
        concurrency: { ...conc, channels: busyChannels, agents: busyAgents },
        queueWait: Object.keys(queueWait).length ? queueWait : null,
        // 叠 Key 专属指标（TASK 11）：与渠道级成功率隔离，供 /api/metrics 观察省积分效果
        stackedKeys: getStackedKeyStats(),
        // 模型级聚合（P3 §3.3）：请求数 / 成功率 / 失败指纹 Top5 / 首字节 p50、p95
        models: tasklog.modelMetrics(),
        agents: manager.agentsView(),
        channels: manager.channels.map((c) => ({
          name: c.name,
          inFlight: conc.channels[c.name]?.active ?? 0,
          healthy: c.healthy,
          coolingDown: c.coolingDown,
          failures: c.failures,
          total: c.total,
          failed: c.failed,
          latency: Math.round(c.latency),
          keyCount: c.usableKeys.length,
          stackedKeyStrategy: c.usableKeys.length > 1 ? c.stackedKeyStrategy : null,
        })),
      });
    }
    if (pathname === '/api/usage') {
      if (!trusted) return unauthorized(res);
      // Token 用量（设计文档 2026-09-17 §4.3）：一次返回 today/d1/d7/d30/d90。
      // 口径见 lib/usage.mjs：只统计上游真实回报 usage 的成功请求，不估算、不造假。
      return sendJson(res, 200, await getUsage().queryAll());
    }
    if (pathname === '/api/probe' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      await manager.probe();
      await manager.discoverAll({ quiet: true });
      return sendJson(res, 200, { ok: true });
    }
    if (pathname === '/api/discover' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      const only = url.searchParams.get('channel');
      const results = await manager.discoverAll({ force: true, quiet: true, channel: only });
      let saved = null;
      if (url.searchParams.get('save') === '1') {
        saved = manager.saveDiscoveredModels({ only });
      }
      return sendJson(res, 200, { results, saved, models: manager.availableModels().length });
    }
    if (pathname === '/api/reload' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      try {
        manager.load();
        // 任务日志与定时器间隔也来自配置：一并按新配置重放，否则 reload 之后改了不生效（Bug 4）。
        // 其余"启动期捕获"的项（apiKey / panel / modelMap）已改成用时重读，无需在这里搬运。
        applyTaskLog(manager.config);
        applyUsage(manager.config);
        applyPricing(manager.config);
        manager.startTimers();
        await manager.discoverAll({ quiet: true });
        return sendJson(res, 200, { ok: true });
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
    }

    // ---- 渠道管理（面板"添加供应商"用）----
    if (pathname === '/api/presets' && req.method === 'GET') {
      if (!trusted) return unauthorized(res);
      const presets = Object.entries(manager.presets).map(([name, p]) => ({
        name,
        protocol: p.protocol,
        baseUrl: p.baseUrl,
        description: p.description || '',
      }));
      return sendJson(res, 200, { presets });
    }
    if (pathname === '/api/channels' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      let b;
      try {
        b = await readJsonBody(req);
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
      try {
        const channel = manager.addChannel(b);
        return sendJson(res, 200, { ok: true, channel });
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
    }
    const delMatch = pathname.match(/^\/api\/channels\/([^/]+)$/);
    if (delMatch && req.method === 'DELETE') {
      if (!trusted) return unauthorized(res);
      try {
        manager.removeChannel(decodeURIComponent(delMatch[1]));
        return sendJson(res, 200, { ok: true });
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
    }

    // ---- WorkBuddy 国际版：设备码授权（"获取 access token 的网址"）----
    // start：向上游 POST /v2/plugin/auth/state 签发 state + authUrl（上游登录页），
    // authUrl 直接展示给用户；poll 轮询 auth/token，授权完成后自动落号渠道进 config.json。
    if (pathname === '/api/workbuddy/auth/start' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      try {
        const wb = await import('./lib/workbuddy.mjs');
        const proxy = manager.config?.proxy || process.env.GW_PROXY || 'http://127.0.0.1:7897';
        const { state, authUrl } = await wb.startDeviceFlow({ proxy });
        return sendJson(res, 200, { ok: true, state, authUrl, expiresIn: 900, interval: 5 });
      } catch (err) {
        log.warn(`workbuddy auth/start 失败: ${err.message}`, requestId);
        return sendJson(res, 502, { ok: false, error: err.message });
      }
    }
    if (pathname === '/api/workbuddy/auth/poll' && req.method === 'GET') {
      if (!trusted) return unauthorized(res);
      const state = url.searchParams.get('state') || '';
      if (!state) return sendJson(res, 400, { ok: false, error: '缺少 state 参数' });
      try {
        const wb = await import('./lib/workbuddy.mjs');
        const proxy = manager.config?.proxy || process.env.GW_PROXY || 'http://127.0.0.1:7897';
        const r = await wb.pollDeviceFlow(state, { proxy });
        if (r.status === 'authorized') {
          const info = await wb.fetchUserInfo(r.accessToken, { state, proxy });
          const entry = wb.buildProvisionEntry({
            username: info.username,
            accessToken: r.accessToken,
            refreshToken: r.refreshToken,
            uid: info.uid,
            enterpriseId: info.enterpriseId,
            domain: r.domain,
          });
          const added = manager.addChannel(entry);
          return sendJson(res, 200, { status: 'authorized', channel: added });
        }
        return sendJson(res, 200, { status: r.status, error: r.error || null });
      } catch (err) {
        log.warn(`workbuddy auth/poll 失败: ${err.message}`, requestId);
        return sendJson(res, 502, { ok: false, error: err.message });
      }
    }

    // ---- WorkBuddy 批量导入：一行一个 access token，自动落号一账号一渠道进池子 ----
    // 面板主入口：粘一堆 token → 自动命名 wb-intl-1/2/3…（避开已存在名）→ 全部绑定
    // 同一模型（默认 deepseek-v4.1-flash）→ 同模型多账号 = 模型池，故障自动切换。
    if (pathname === '/api/workbuddy/batch' && req.method === 'POST') {
      if (!trusted) return unauthorized(res);
      let b;
      try {
        b = await readJsonBody(req);
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
      const tokens = Array.isArray(b?.tokens)
        ? b.tokens.map((t) => String(t ?? '').trim()).filter(Boolean)
        : [];
      if (!tokens.length) return sendJson(res, 400, { ok: false, error: '未提供任何 access token（tokens 数组为空）' });
      const model = String(b.model || 'deepseek-v4.1-flash').trim() || 'deepseek-v4.1-flash';
      const priority = Number.isFinite(Number(b.priority)) ? Number(b.priority) : 60;
      const existing = new Set(manager.channels.map((c) => c.name));
      const added = [];
      for (let i = 0; i < tokens.length; i += 1) {
        let name = `wb-intl-${i + 1}`;
        let k = 1;
        while (existing.has(name)) name = `wb-intl-${i + 1}-${++k}`;
        existing.add(name);
        try {
          const ch = manager.addChannel({ name, preset: 'workbuddy-intl', apiKey: tokens[i], model, priority });
          added.push(ch ? { name, ok: true } : { name, ok: false, error: '落号失败' });
        } catch (err) {
          added.push({ name, ok: false, error: err.message });
        }
      }
      return sendJson(res, 200, { ok: true, added });
    }

    // ---- OpenAI 兼容接口（需鉴权）----
    const isApi = pathname.startsWith('/v1/');
    if (isApi && !authorized(req)) {
      log.warn(`鉴权失败 ${req.method} ${pathname} from ${req.socket.remoteAddress}`, requestId);
      return unauthorized(res);
    }

    if (pathname === '/v1/models' && req.method === 'GET') {
      // ?refresh=1 强制重新拉取上游模型列表
      if (url.searchParams.get('refresh') === '1') {
        await manager.discoverAll({ force: true, quiet: true });
      }
      // 池子总入口模式：对外只暴露一个统一模型名（调用它 = 从整个池子里路由）
      const unified = manager.config?.unifiedModel;
      const models = unified ? [unified] : manager.availableModels();
      // 兜底模型也对外可见（unified 模式下列表保持只有一个统一名，兜底仍在请求侧生效）
      const fb = unified ? null : manager.config?.fallbackModel;
      if (fb && !models.includes(fb)) models.push(fb);
      return sendJson(res, 200, {
        object: 'list',
        data: models.sort().map((m) => ({
          id: m,
          object: 'model',
          created: Math.floor(Date.now() / 1000),
          owned_by: 'local-gateway',
        })),
      });
    }

    if (pathname === '/v1/chat/completions') {
      if (req.method !== 'POST') return sendJson(res, 405, openaiError('仅支持 POST', 'invalid_request_error', null, 405));
      let body;
      try {
        body = await readJsonBody(req);
      } catch (err) {
        return sendJson(res, err.status || 400, openaiError(err.message, 'invalid_request_error', null, err.status || 400));
      }
      log.info(`POST /v1/chat/completions model=${body?.model} stream=${body?.stream === true}`, requestId);
      if (body?.model) body.model = mapModel(body.model);
      // 识别请求来自哪个子代理 / 会话：用于限流配额、会话亲和路由与全链路打标
      const agent = identifyAgent(req, body);
      return await handleChatCompletions({
        manager, req, res, body, requestId, clientProtocol: 'openai',
        agentId: agent.id, agentSource: agent.source,
      });
    }

    // Anthropic 原生端点：Claude Code / Anthropic SDK 直接连这里，内部转成 OpenAI 中间格式
    if (pathname === '/v1/messages') {
      if (req.method !== 'POST') return sendJson(res, 405, openaiError('仅支持 POST', 'invalid_request_error', null, 405));
      let body;
      try {
        body = await readJsonBody(req);
      } catch (err) {
        return sendJson(res, err.status || 400, openaiError(err.message, 'invalid_request_error', null, err.status || 400));
      }
      if (body?.model) body.model = mapModel(body.model);
      log.info(`POST /v1/messages (anthropic) model=${body?.model} stream=${body?.stream === true}`, requestId);
      const openaiBody = fromAnthropicBody(body);
      const agentMsg = identifyAgent(req, body);
      return await handleChatCompletions({
        manager, req, res, body: openaiBody, requestId, clientProtocol: 'anthropic',
        agentId: agentMsg.id, agentSource: agentMsg.source,
      });
    }

    if (pathname === '/v1/embeddings') {
      if (req.method !== 'POST') return sendJson(res, 405, openaiError('仅支持 POST', 'invalid_request_error', null, 405));
      let body;
      try {
        body = await readJsonBody(req);
      } catch (err) {
        return sendJson(res, err.status || 400, openaiError(err.message, 'invalid_request_error', null, err.status || 400));
      }
      // 和聊天一样带代理标识，这样单代理并发配额对 embeddings 同样生效
      const agentEmb = identifyAgent(req, body);
      return await handleEmbeddings({ body, res, requestId, agentId: agentEmb.id });
    }

    return sendJson(res, 404, openaiError(`未实现的路由: ${req.method} ${pathname}`, 'invalid_request_error', null, 404));
  } catch (err) {
    log.error(`未处理异常: ${err.stack || err.message}`, requestId);
    if (!res.headersSent) {
      return sendJson(res, 500, openaiError('网关内部错误: ' + err.message, 'internal_error', null, 500));
    }
    res.end();
  }
});

// embeddings：仅支持 OpenAI 协议渠道的简单透传
//
// 与聊天共用同一个渠道池，但有两处必须与聊天区分开（Bug 3）：
//   1) 并发闸门：走和聊天同一套"全局 + 单渠道 + 单代理"限流，否则 embeddings 可以无限并发
//      直接把上游打爆，也会绕开 routing.maxConcurrent 的保护。
//   2) 熔断计数：embeddings 的失败只进统计与最近错误，**不推进聊天的熔断状态**——
//      否则一个只调 embeddings 的客户端能把渠道熔断掉，连带把聊天请求一起打挂。
async function handleEmbeddings({ body, res, requestId, agentId = null }) {
  const model = body?.model;
  const candidates = manager.candidatesFor(model).filter((c) => c.protocol === 'openai');
  if (!candidates.length) {
    return sendJson(res, 400, openaiError(`没有 OpenAI 协议渠道支持 ${model}`, 'invalid_request_error', null, 400));
  }
  // F4（代码审查 2026-09-20）：embeddings 以前既没有超时、也不接客户端取消——
  // fetch 不传 signal、r.text() 没有看门狗、不监听客户端关闭。后果：
  //   ① 上游只回响应头或正文中途停住 -> 请求永久挂着；
  //   ② 客户端取消后请求继续跑，`finally` 里的 release 永远走不到 -> 名额被永久占用，
  //      攒满 maxConcurrent 之后连聊天一起卡死（上游明明是好的）。
  // 现在：请求级 deadline（含客户端取消）统一约束 fetch + body 读取，并在循环入口检查取消。
  const clientAbort = new AbortController();
  const onClose = () => clientAbort.abort();
  res.on('close', onClose);
  // 单次 embeddings 尝试的总时限：优先 embeddingsTimeoutMs，其次 providerTimeoutMs，最后 timeoutMs
  const timeoutMs = Math.max(
    1,
    Number(manager.routing.embeddingsTimeoutMs)
      || Number(manager.routing.providerTimeoutMs)
      || Number(manager.routing.timeoutMs)
      || 60000,
  );
  const timeoutAbort = new AbortController();
  const timeoutTimer = setTimeout(() => timeoutAbort.abort(), timeoutMs);
  const signal = AbortSignal.any([clientAbort.signal, timeoutAbort.signal]);

  let lastErr = null;
  try {
    for (const ch of candidates.slice(0, manager.routing.maxAttempts)) {
      // 客户端已经走了：不再浪费上游配额，也不再尝试其它渠道
      if (clientAbort.signal.aborted) {
        lastErr = '客户端在 embeddings 处理期间断开';
        break;
      }
      const upstreamModel = ch.resolveModel(model);
      let release = null;
      try {
        // 三级限流与聊天一致；排队超时视为"这家暂时不可用"，直接换下一家（不计熔断）
        try {
          release = await manager.limiter.acquire(ch.name, agentId);
        } catch (err) {
          if (isOverloadError(err)) {
            log.warn(`embeddings 并发闸门排队超时，换下一家: ${err.message}`, requestId);
            lastErr = err.message;
            continue;
          }
          throw err;
        }
        const sendFetch = ch.proxy
          ? (await import('./lib/outbound-proxy.mjs')).gwFetch
          : fetch;
        const r = await sendFetch(`${ch.baseUrl}/embeddings`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${ch.apiKey}`, ...(ch.headers || {}) },
          body: JSON.stringify({ ...body, model: upstreamModel }),
          // F4：超时与客户端取消都要能打断上游连接（出站代理路径同样透传 signal）
          signal,
          ...(ch.proxy ? { proxy: ch.proxy } : {}),
        });
        // body 读取同样受 signal 约束：signal 一触发 r.text() 立即 reject，
        // 不会出现"上游只回响应头、正文永不到来"把名额永久挂住的情况。
        const text = await r.text();
        if (!r.ok) {
          markEmbeddingsFailure(ch, `http_${r.status}`, `HTTP ${r.status} ${text.slice(0, 200)}`);
          lastErr = `HTTP ${r.status} ${text.slice(0, 200)}`;
          continue;
        }
        ch.markSuccess(0);
        log.ok(`embeddings via ${ch.name}`, requestId);
        res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'x-gateway-channel': ch.name });
        return res.end(text);
      } catch (err) {
        const aborted = err.name === 'AbortError' || err.name === 'TimeoutError';
        if (aborted && clientAbort.signal.aborted) {
          lastErr = '客户端在 embeddings 处理期间断开';
          break; // 客户端走了，换家没有意义
        }
        if (aborted && timeoutAbort.signal.aborted) {
          lastErr = `embeddings 上游超时（>${timeoutMs}ms）`;
          markEmbeddingsFailure(ch, 'timeout', lastErr);
          continue; // 超时按"这家不可用"换下一家
        }
        markEmbeddingsFailure(ch, 'network', err.message);
        lastErr = err.message;
      } finally {
        release?.();
      }
    }
  } finally {
    clearTimeout(timeoutTimer);
    res.off('close', onClose);
  }
  if (clientAbort.signal.aborted || res.destroyed || res.writableEnded) {
    log.warn(`embeddings 客户端已断开，停止处理: ${lastErr}`, requestId);
    return undefined; // 客户端已经不听了，不要再往一个已断的连接写响应
  }
  return sendJson(res, 503, openaiError('embeddings 全部渠道失败: ' + lastErr, 'upstream_unavailable', null, 503));
}

/**
 * embeddings 的失败记账：只累计 total/failed 与最近错误，**不推进聊天的熔断状态**（Bug 3）。
 *
 * 走 Channel#noteFailure：语义直白（只登记、不熔断），且面板 lastError 里是真实错误类型，
 * 不会把上游 5xx 显示成"被限流"。
 */
function markEmbeddingsFailure(ch, kind, message) {
  ch.noteFailure(`embeddings/${kind}`, message);
}

// ---------- 高并发相关调优 ----------
// 默认值在高并发 / 长连接场景下会误伤：
//   keepAliveTimeout 默认 5s -> 复用连接频繁被服务端断开，客户端需反复重连
//   requestTimeout 默认 300s -> 会掐断长时间流式回答
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000; // 约定必须略大于 keepAliveTimeout
server.requestTimeout = 0;     // 不限单请求时长，交给上游 idle 超时与客户端自己控制
server.timeout = 0;            // 不用 socket 空闲超时一刀切
server.maxRequestsPerSocket = 0;

// 畸形请求及时关闭，避免 socket 长时间挂着占用句柄
server.on('clientError', (err, socket) => {
  log.warn(`客户端请求异常: ${err.code || err.message}`);
  if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
});

// ---------- 启动 ----------
server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    log.error(`端口 ${PORT} 已被占用，换一个：node server.mjs --port 8788`);
  } else {
    log.error(`服务错误: ${err.message}`);
  }
  process.exit(1);
});

const shutdown = () => {
  log.info('正在关闭…');
  manager.stopTimers();
  const dropped = manager.limiter.drain('网关正在关闭，排队请求已取消');
  if (dropped) log.warn(`已丢弃 ${dropped} 个排队中的请求`);
  // 任务日志可能还有内存里没落盘的记录，关闭前刷出去
  getTaskLog().flushNow().catch(() => {});
  // 用量库同理：把排队中的追加写落盘，并停掉每日清理定时器
  getUsage().stopTimers();
  getUsage().flush().catch(() => {});
  log.flush();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// 兜底：转发型网关以"保持存活"优先，任何单个请求的异常都不应拖垮整个服务
process.on('unhandledRejection', (err) => {
  log.error(`未处理的 Promise 拒绝: ${err?.stack || err}`);
});
process.on('uncaughtException', (err) => {
  log.error(`未捕获异常: ${err?.stack || err}`);
});

server.listen(PORT, HOST, async () => {
  const base = `http://${HOST}:${PORT}/v1`;
  log.raw('');
  log.raw('  ══════════════════════════════════════════════════');
  log.raw('     本地模型聚合网关已启动');
  log.raw('  ══════════════════════════════════════════════════');
  log.raw('');
  log.raw(`  base_url : ${base}`);
  log.raw(`  api_key  : ${currentApiKey()}`);
  log.raw(`  状态面板 : http://${HOST}:${PORT}/`);
  log.raw(`  配置文件 : ${configPath}`);
  log.raw('');
  const r = manager.routing;
  const concLine = r.maxConcurrent > 0
    ? `全局 ${r.maxConcurrent}${r.maxConcurrentPerChannel > 0 ? ` / 单渠道 ${r.maxConcurrentPerChannel}` : ''}${r.maxConcurrentPerAgent > 0 ? ` / 单代理 ${r.maxConcurrentPerAgent}` : ''}${r.queueTimeoutMs > 0 ? `，排队上限 ${Math.round(r.queueTimeoutMs / 1000)}s` : ''}`
    : '不限制';
  const strategyName = { 'least-loaded': '最少在途', 'round-robin': '轮询', priority: '优先级' }[r.strategy] || r.strategy;
  log.raw(`  选路策略 : ${strategyName}${r.sessionAffinity ? `，会话亲和 ${Math.round((r.affinityTtlMs ?? 0) / 1000)}s` : '，无会话亲和'}`);
  if (r.tiered !== false) {
    const pref = manager.channels.filter((c) => c.enabled && c.isPreferred).map((c) => c.name);
    const rest = manager.channels.filter((c) => c.enabled && !c.isPreferred).length;
    log.raw(`  两段选路 : 第1段 优先池(${pref.length}) [${pref.join(', ') || '无'}] 按优先级轮询`);
    log.raw(`             第2段 其余 ${rest} 家随机选，成功过的钉住复用；失败再试 ${Math.max(0, (r.fallbackAttempts ?? 3) - 1)} 次（间隔 ${Math.round((r.fallbackRetryIntervalMs ?? 3000) / 1000)}s）后换下一家`);
  } else {
    log.raw('  两段选路 : 已关闭（整池统一排序）');
  }
  log.raw(`  重试矩阵 : 优先池每渠道 ${r.attemptsPerChannel} 次 / 间隔 ${Math.round((r.retryPerAttemptMs ?? r.retryWaitMs ?? 0) / 1000)}s${r.retryLoop ? `，全池轮询间隔 ${Math.round(r.retryWaitMs / 1000)}s${r.retryMaxWaitMs > 0 ? `（上限 ${Math.round(r.retryMaxWaitMs / 1000)}s）` : '（无上限）'}` : '，不轮询'}${r.maxTotalWaitMs > 0 ? `，单请求总预算 ${Math.round(r.maxTotalWaitMs / 1000)}s` : ''}`);
  log.raw(`  失败处理 : 余额不足冷却 ${Math.round((r.balanceCooldownMs ?? 900000) / 1000)}s · 鉴权失败冷却 ${Math.round((r.authCooldownMs ?? 600000) / 1000)}s · 限流不熔断直接换家`);
  log.raw(`  思考强度 : ${r.forceMaxEffort !== false ? '能思考的模型统一最高强度(high)' : '按客户端传入'}`);
  log.raw(`  任务日志 : ${tasklog.enabled ? tasklog.file : '仅内存（未落盘）'} · 面板 /api/tasks · 错误 /api/errors`);
  log.raw(`  Token 用量 : ${usageOptions(manager.config).enabled ? `落盘 ${path.join(usageOptions(manager.config).dir, 'YYYY-MM-DD.jsonl')} · 保留 ${getUsage().keepDays} 天` : '仅内存（taskLog.enabled=false）'} · 面板 /api/usage（范围 ${USAGE_RANGES.join('/')}）`);
  log.raw(`  价格计费 : ${getPricing().meta().currency} ${getPricing().meta().symbol} · 生效价目 ${getPricing().meta().models} 条（每 100 万 token）· 未配价格的模型显示"未配置价格"，不当 0 计`);
  log.raw(`  并发控制 : ${concLine}`);
  log.raw(`  渠道(${manager.channels.length}): ${manager.channels.map((c) => (c.enabled ? c.name : c.name + '(停用)')).join(', ') || '无'}`);
  log.raw('');
  log.raw('  快速验证:');
  log.raw(`    curl ${base}/models -H "Authorization: Bearer ${currentApiKey()}"`);
  log.raw('');

  if (args['no-discover'] !== 'true' && args['no-discover'] !== true) {
    await manager.discoverAll();
    const total = manager.availableModels().length;
    log.ok(`聚合可用模型 ${total} 个，网关就绪`);
  }
  manager.startTimers();
  // 配置文件被外部改动时的热重载钩子：把"启动期一次性装配"的东西一并刷新（Bug 4）。
  // 定时器间隔（probeIntervalMs / discoverIntervalMs）只有重放 startTimers 才会生效。
  manager.watchConfig(() => {
    applyTaskLog(manager.config);
    applyUsage(manager.config);
    applyPricing(manager.config);
    manager.startTimers();
  });
});
