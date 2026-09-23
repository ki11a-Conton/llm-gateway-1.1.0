// Token 用量（usage）验收：持久化 lib/usage.mjs + 采集 lib/proxy.mjs + GET /api/usage
//
// 对齐 docs/superpowers/specs/2026-09-17-token-usage-dashboard-design.md：
//   §4.1 采集：只记"成功交付给客户端"的那次 attempt；上游没回报 usage 就不产生记录（不造假 0）；
//             流式 OpenAI 渠道出站强制 stream_options.include_usage=true。
//   §4.2 持久化：logs/usage/YYYY-MM-DD.jsonl（本地时区分天）；today = 自然日；dN = 滚动窗口（按 ts epoch 比较）。
//   §4.3 API：GET /api/usage 一次回 today/d1/d7/d30/d90，复用 /api/* 的鉴权 / Host / 跨站 Origin 拒绝。
//   §4.5 单元 + 集成 + 安全。
//
// 端口动态分配（test/lib/ports.mjs）；所有落盘都指向临时目录，不污染仓库 logs/。
import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import os from 'node:os';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync, existsSync, readdirSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';
import { UsageStore, localDayKey, USAGE_RANGES } from '../lib/usage.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const GW = `http://127.0.0.1:${GW_PORT}`;
const API_KEY = 'USAGEKEY';
const EVIL = 'https://evil.example';

// 模型名即 mock 的行为开关
const M_OPENAI = 'usage-openai';          // 非流式 OpenAI：prompt_tokens / completion_tokens
const M_ANTHROPIC = 'usage-anthropic';    // 非流式 Anthropic：input_tokens / output_tokens
const M_NOUSAGE = 'usage-nousage';        // 200 但完全没有 usage 字段
const M_ZEROUSAGE = 'usage-zerousage';    // 200 但 usage 全是 0（不能记成"用了 0 token"）
const M_FAIL = 'usage-fail';              // 500
const M_STREAM_OAI = 'usage-stream-openai';     // 流式 OpenAI：流末 usage 帧
const M_STREAM_ANTH = 'usage-stream-anthropic'; // 流式 Anthropic：message_delta.usage
// §7.2：上游因 stream_options 直接回 400（走既有 bad_request 分类）
const M_REJECT_SO = 'usage-reject-stream-options';
// §7.1：假 key 由 mock 在错误文本里回显，用于验证落盘副本的脱敏
const FAKE_KEY = 'sk-LEAKTEST0123456789abcdef';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

/** 本地时区当天 0 点（与 lib/usage.mjs 的分天口径一致） */
function localMidnight(ms = Date.now()) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

// ---------- mock 上游 ----------
const streamOptionsSeen = [];
const oaiBody = (model, usage) => JSON.stringify({
  id: 'o1', object: 'chat.completion', model,
  choices: [{ index: 0, message: { role: 'assistant', content: 'hello' }, finish_reason: 'stop' }],
  ...(usage ? { usage } : {}),
});
const anthBody = (model, usage) => JSON.stringify({
  id: 'm1', type: 'message', role: 'assistant', model,
  content: [{ type: 'text', text: 'hello' }], stop_reason: 'end_turn',
  ...(usage ? { usage } : {}),
});
// SSE 组帧：对象 -> `data: <json>`；字符串视为已组好的原始帧（event: / data: 行）
const sse = (frames) => frames.map((f) => (typeof f === 'string' ? f : `data: ${JSON.stringify(f)}\n\n`)).join('');

const mock = http.createServer((req, res) => {
  let rawBody = '';
  req.on('data', (c) => { rawBody += c; });
  req.on('end', () => {
    let body = {};
    try { body = JSON.parse(rawBody || '{}'); } catch { /* ignore */ }
    const model = body.model || '';
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: model || 'usage', object: 'model' }] }));
    }
    // 流式出站请求体必须带 stream_options.include_usage（仅 OpenAI 协议渠道）
    if (body.stream === true) streamOptionsSeen.push({ model, stream_options: body.stream_options ?? null });

    if (model === M_FAIL) {
      res.writeHead(500, { 'content-type': 'application/json' });
      // 错误文本里回显一个假 key：上游回显密钥是常见现象，用于验证落盘副本的脱敏
      return res.end(JSON.stringify({ error: { message: `usage upstream exploded (echo auth=${FAKE_KEY})` } }));
    }
    if (model === M_REJECT_SO) {
      // §7.2：不认 stream_options 的上游
      res.writeHead(400, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: { message: 'unknown field: stream_options' } }));
    }
    if (model === M_NOUSAGE) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(oaiBody(model, null));
    }
    if (model === M_ZEROUSAGE) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(oaiBody(model, { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }));
    }
    if (model === M_OPENAI) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(oaiBody(model, { prompt_tokens: 11, completion_tokens: 7, total_tokens: 18 }));
    }
    if (model === M_ANTHROPIC) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(anthBody(model, { input_tokens: 5, output_tokens: 3 }));
    }
    if (model === M_STREAM_OAI) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
      return res.end(sse([
        { id: 's1', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: { role: 'assistant', content: 'hi' }, finish_reason: null }] },
        { id: 's1', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] },
        { id: 's1', object: 'chat.completion.chunk', model, choices: [], usage: { prompt_tokens: 21, completion_tokens: 9, total_tokens: 30 } },
        'data: [DONE]\n\n',
      ]));
    }
    if (model === M_STREAM_ANTH) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
      return res.end([
        'event: message_start\n',
        `data: ${JSON.stringify({ type: 'message_start', message: { id: 'sm1', model, usage: { input_tokens: 13, output_tokens: 1 } } })}\n\n`,
        'event: content_block_start\n',
        `data: ${JSON.stringify({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } })}\n\n`,
        'event: content_block_delta\n',
        `data: ${JSON.stringify({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hello there' } })}\n\n`,
        'event: message_delta\n',
        `data: ${JSON.stringify({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 17 } })}\n\n`,
        'event: message_stop\n',
        `data: ${JSON.stringify({ type: 'message_stop' })}\n\n`,
      ].join(''));
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(oaiBody(model, null));
  });
});

async function getJson(p, headers = {}) {
  const r = await fetch(GW + p, { headers });
  let j = null;
  try { j = await r.json(); } catch { /* ignore */ }
  return { status: r.status, headers: r.headers, json: j };
}

async function postChat(model, { stream = false, headers = {} } = {}) {
  const r = await fetch(`${GW}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}`, ...headers },
    body: JSON.stringify({ model, stream, messages: [{ role: 'user', content: 'hi' }] }),
  });
  const text = await r.text();
  return { status: r.status, text };
}

/** 裸 http 请求：精确控制 Host / Origin（fetch 不允许改 Host） */
function raw({ method = 'GET', p, headers = {}, host = null } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port: GW_PORT, method, path: p, setHost: false,
      headers: { host: host ?? `127.0.0.1:${GW_PORT}`, ...headers },
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode, headers: res.headers, text: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    req.end();
  });
}

async function waitReady(tries = 80) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`${GW}/health`); if (r.ok) return true; } catch { /* retry */ }
    await wait(150);
  }
  return false;
}

function assertPortFree(port) {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.once('error', () => resolve(false));
    s.listen(port, '127.0.0.1', () => s.close(() => resolve(true)));
  });
}

/** /api/usage 里某个模型在某个范围的 token 汇总（缺失 -> null） */
const modelOf = (range, model) => range?.byModel?.[model] ?? null;

/** 面板脚本能否通过编译（把面板 JS 改崩时立刻失败，而不是等浏览器报错） */
function canCompile(src) {
  try { new Function(src); return true; } catch { return false; }
}

/**
 * 用最小 DOM stub 在 vm 里**真实执行**面板脚本，然后调用新增渲染函数。
 * 目的：验证 §4.4 的区块确实渲染出预期文本，且全程不使用 innerHTML（无注入面）。
 * 浏览器不可用，这是"能离线跑的真实验证"与"只 grep 字符串"之间的取舍。
 */
function runPanelRender(src) {
  const nodes = new Map();
  let innerHtmlWrites = 0;
  class El {
    constructor(tag) {
      this.tagName = String(tag || '').toUpperCase();
      this.children = [];
      this._text = '';
      this._html = '';
      this.style = {};
      this.className = '';
      this.title = '';
      this.value = '';
      this.classList = { add() {}, remove() {}, toggle() {} };
    }
    set textContent(v) { this._text = v == null ? '' : String(v); this.children = []; }
    get textContent() { return this._text; }
    set innerHTML(v) { innerHtmlWrites += 1; this._html = String(v); }
    get innerHTML() { return this._html; }
    appendChild(n) { this.children.push(n); return n; }
    addEventListener() {}
    removeEventListener() {}
    getAttribute() { return null; }
    setAttribute() {}
    focus() {}
  }
  const textOf = (n) => {
    if (!n) return '';
    if (n.children && n.children.length) return n.children.map(textOf).join(' ');
    return n.textContent || '';
  };
  const getById = (id) => {
    if (!nodes.has(id)) nodes.set(id, new El('div'));
    return nodes.get(id);
  };
  const ctx = {
    document: {
      getElementById: getById,
      createElement: (tag) => new El(tag),
      createTextNode: (t) => { const n = new El('#text'); n.textContent = t; return n; },
      addEventListener() {},
    },
    localStorage: { getItem: () => null, setItem() {} },
    location: { origin: `http://127.0.0.1:${GW_PORT}` },
    // 面板的所有网络调用一律失败：只验证渲染函数，不依赖网关
    fetch: async () => ({ ok: false, status: 404, statusText: 'Not Found', json: async () => null }),
    setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, confirm: () => false, console,
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: 'panel-extract.js' });
  return { ctx, getById, textOf, innerHtmlWrites: () => innerHtmlWrites };
}

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-usage-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
const USAGE_DIR = path.join(LOGS_DIR, 'usage');
const RUN_CFG = path.join(tmpDir, 'usage.test.run.json');
let gw = null;

try {
  // ============ 单元：lib/usage.mjs（直接 import，指向临时目录） ============
  console.log('— 单元：持久化与聚合（lib/usage.mjs）—');
  {
    const unitDir = path.join(tmpDir, 'unit-usage');
    const store = new UsageStore({ dir: unitDir, keepDays: 5, enabled: true });

    store.record({ ts: new Date().toISOString(), model: 'm-a', channel: 'c1', input: 100, output: 50 });
    store.record({ ts: new Date().toISOString(), model: 'm-b', channel: 'c1', input: 200, output: 60 });
    store.record({ ts: new Date().toISOString(), model: 'm-a', channel: 'c2', input: 5, output: 5 });
    await store.flush();

    const today = await store.query('today');
    ok('today 总量聚合正确（requests / input / output / total）',
      today.requests === 3 && today.inputTokens === 305 && today.outputTokens === 115 && today.totalTokens === 420,
      JSON.stringify(today));
    ok('today 按模型聚合正确',
      today.byModel['m-a']?.requests === 2 && today.byModel['m-a']?.inputTokens === 105
      && today.byModel['m-b']?.inputTokens === 200 && today.byModel['m-b']?.outputTokens === 60,
      JSON.stringify(today.byModel));

    // 跨 0 点（本地时区）：今天 00:01 / 昨天 23:59:59.999 / 前天
    const midnight = localMidnight();
    const tToday = midnight + 60_000;
    const tYesterdayEnd = midnight - 1;
    const tOld = midnight - 48 * 3600 * 1000;
    store.record({ ts: new Date(tToday).toISOString(), model: 'day-today', channel: 'c', input: 1, output: 1 });
    store.record({ ts: new Date(tYesterdayEnd).toISOString(), model: 'day-yesterday', channel: 'c', input: 10, output: 10 });
    store.record({ ts: new Date(tOld).toISOString(), model: 'day-old', channel: 'c', input: 1000, output: 1000 });
    await store.flush();

    const files = readdirSync(unitDir).filter((f) => f.endsWith('.jsonl')).sort();
    ok('按本地日期分天落盘（跨 0 点的两条记录落不同天文件）',
      localDayKey(tToday) !== localDayKey(tYesterdayEnd)
      && files.includes(`${localDayKey(tToday)}.jsonl`)
      && files.includes(`${localDayKey(tYesterdayEnd)}.jsonl`),
      files.join(','));

    const qToday = await store.query('today');
    ok('today 只算自然日当天（不含昨天 23:59 的记录，即便它在一小时之内）',
      qToday.byModel['day-yesterday'] === undefined && qToday.byModel['day-today'] !== undefined,
      JSON.stringify(Object.keys(qToday.byModel)));

    const q1 = await store.query('d1');
    ok('d1（滚动 24h）含昨天 23:59 的记录、不含前天的记录',
      modelOf(q1, 'day-yesterday') !== null && modelOf(q1, 'day-old') === null,
      JSON.stringify(Object.keys(q1.byModel)));
    ok('d1 跨天边界按 ts 精确计入（昨天 23:59 的 10/10 token 在窗口内）',
      q1.inputTokens >= 11 && q1.outputTokens >= 11, JSON.stringify({ i: q1.inputTokens, o: q1.outputTokens }));

    const q30 = await store.query('d30');
    ok('d30 是滚动窗口：含前天的记录（48h 前仍在 30 天窗口内）',
      modelOf(q30, 'day-old') !== null, JSON.stringify(Object.keys(q30.byModel)));
    ok('五个范围都能查（today/d1/d7/d30/d90）',
      USAGE_RANGES.length === 5 && (await Promise.all(USAGE_RANGES.map((r) => store.query(r)))).every((r) => typeof r.totalTokens === 'number'),
      USAGE_RANGES.join(','));

    // 保留策略
    const oldDay = localDayKey(Date.now() - 400 * 24 * 3600 * 1000);
    const keepDay = localDayKey(Date.now() - 2 * 24 * 3600 * 1000);
    writeFileSync(path.join(unitDir, `${oldDay}.jsonl`), `${JSON.stringify({ ts: new Date().toISOString(), model: 'x', channel: 'c', input: 1, output: 1 })}\n`);
    writeFileSync(path.join(unitDir, `${keepDay}.jsonl`), `${JSON.stringify({ ts: new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString(), model: 'x', channel: 'c', input: 2, output: 2 })}\n`);
    await store.prune();
    const after = readdirSync(unitDir);
    ok('保留策略：超出 usageKeepDays 的天文件被删、窗口内的保留',
      !after.includes(`${oldDay}.jsonl`) && after.includes(`${keepDay}.jsonl`), after.join(','));

    // enabled=false：内存聚合可用，但不落盘
    const memDir = path.join(tmpDir, 'mem-usage');
    const mem = new UsageStore({ dir: memDir, keepDays: 5, enabled: false });
    mem.record({ ts: new Date().toISOString(), model: 'mem', channel: 'c', input: 3, output: 4 });
    await mem.flush();
    const memToday = await mem.query('today');
    ok('enabled=false 时仍能内存聚合，但不创建任何文件',
      memToday.inputTokens === 3 && memToday.outputTokens === 4 && !existsSync(memDir),
      JSON.stringify({ memToday, exists: existsSync(memDir) }));

    // 非法 range 不做静默兜底
    let threw = false;
    try { await store.query('lastweek'); } catch { threw = true; }
    ok('非法 range 抛错（不静默当成 today）', threw);
  }

  // ============ 集成：采集 + /api/usage ============
  console.log('— 集成：采集（lib/proxy.mjs）+ GET /api/usage —');
  for (const [name, port] of [['mock', MOCK_PORT], ['gateway', GW_PORT]]) {
    if (!(await assertPortFree(port))) throw new Error(`端口 ${port} 已被占用（${name}），本套件要求独占动态申请到的端口`);
  }
  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));

  const cfg = {
    server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: true },
    taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200, usageKeepDays: 120 },
    modelMap: {},
    routing: {
      strategy: 'priority', tiered: false, attemptsPerChannel: 1, retryLoop: false,
      sessionAffinity: false, forceMaxEffort: false, maxConcurrent: 8, maxConcurrentPerChannel: 0,
      queueTimeoutMs: 5000, timeoutMs: 20000, streamIdleTimeoutMs: 20000,
      probeIntervalMs: 0, discoverIntervalMs: 0,
    },
    channels: [
      { name: 'u-openai', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_OPENAI, priority: 10 },
      { name: 'u-anthropic', protocol: 'anthropic', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_ANTHROPIC, priority: 10 },
      { name: 'u-nousage', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_NOUSAGE, priority: 10 },
      { name: 'u-zerousage', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_ZEROUSAGE, priority: 10 },
      { name: 'u-fail', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_FAIL, priority: 10 },
      { name: 'u-stream-openai', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_STREAM_OAI, priority: 10 },
      { name: 'u-stream-anthropic', protocol: 'anthropic', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_STREAM_ANTH, priority: 10 },
      { name: 'u-reject-so', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_REJECT_SO, priority: 10 },
    ],
    // 价格计费（pricing 设计 §5）：只给 M_OPENAI 配价 —— 用来验证
    // "已配价 -> 金额按单价算" 与 "未配价 -> code 为 null 而不是 0" 两条口径；
    // 币种故意用 CNY/¥，顺带验证符号不是硬编码的 $
    pricing: {
      currency: 'CNY',
      symbol: '¥',
      models: { [M_OPENAI]: { input: 1, output: 2 } },
    },
  };
  writeFileSync(RUN_CFG, `${JSON.stringify(cfg, null, 2)}\n`, 'utf8');

  // 注入"昨天 23:59:59.999"的历史数据（模拟跨天落盘），必须在任何 /api/usage 查询之前写
  mkdirSync(USAGE_DIR, { recursive: true });
  const midnight = localMidnight();
  const tYesterdayEnd = midnight - 1;
  writeFileSync(
    path.join(USAGE_DIR, `${localDayKey(tYesterdayEnd)}.jsonl`),
    `${JSON.stringify({ ts: new Date(tYesterdayEnd).toISOString(), model: 'injected-yesterday', channel: 'seed', input: 1000, output: 500 })}\n`,
    'utf8',
  );

  gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
  if (!(await waitReady())) throw new Error('网关未就绪');
  await wait(200);

  // §4.1 非流式 OpenAI / Anthropic
  {
    const a = await postChat(M_OPENAI);
    const b = await postChat(M_ANTHROPIC);
    ok('非流式 OpenAI / Anthropic 请求都成功', a.status === 200 && b.status === 200, `${a.status}/${b.status}`);
    await wait(150);
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('/api/usage 返回 200 且含五个范围', u.status === 200 && USAGE_RANGES.every((r) => u.json?.[r]), Object.keys(u.json || {}).join(','));
    ok('非流式 OpenAI 的 prompt_tokens/completion_tokens 被采到（11/7）',
      modelOf(u.json?.today, M_OPENAI)?.inputTokens === 11 && modelOf(u.json?.today, M_OPENAI)?.outputTokens === 7,
      JSON.stringify(modelOf(u.json?.today, M_OPENAI)));
    ok('非流式 Anthropic 的 input_tokens/output_tokens 被采到（5/3）',
      modelOf(u.json?.today, M_ANTHROPIC)?.inputTokens === 5 && modelOf(u.json?.today, M_ANTHROPIC)?.outputTokens === 3,
      JSON.stringify(modelOf(u.json?.today, M_ANTHROPIC)));
  }

  // 价格计费：金额 = (输入×单价in + 输出×单价out)/1e6；**未配价格的模型必须是 null 而不是 0**（不造假）
  // 此刻 today 只有两条记录：M_OPENAI（11/7，已配价 1/2）与 M_ANTHROPIC（5/3，未配价）
  {
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    const t = u.json?.today;
    const oai = modelOf(t, M_OPENAI);
    ok('已配价模型的金额 = (11×1 + 7×2)/1e6 = 0.000025（手算绝对值）',
      oai?.cost === 0.000025, JSON.stringify(oai));
    ok('未配价格的模型金额是 null（不是 0）',
      modelOf(t, M_ANTHROPIC)?.cost === null, JSON.stringify(modelOf(t, M_ANTHROPIC)));
    ok('范围 cost 只累加已配价模型（此刻只有 M_OPENAI = 0.000025）',
      t?.cost === 0.000025, JSON.stringify({ cost: t?.cost }));
    ok('存在未配价模型 -> costComplete=false（金额不完整必须自曝）',
      t?.costComplete === false, JSON.stringify(t?.costComplete));
    ok('unpriced 列出未配价模型名、unpricedTokens 统计其 token（5+3=8）',
      Array.isArray(t?.unpriced) && t.unpriced.includes(M_ANTHROPIC) && t.unpricedTokens === 8,
      JSON.stringify({ unpriced: t?.unpriced, unpricedTokens: t?.unpricedTokens }));
    ok('顶层 pricing 元信息来自配置（CNY / ¥ / 条数>0）',
      u.json?.pricing?.currency === 'CNY' && u.json?.pricing?.symbol === '¥' && u.json?.pricing?.models >= 1,
      JSON.stringify(u.json?.pricing));
    const dayFile = path.join(USAGE_DIR, `${localDayKey(Date.now())}.jsonl`);
    const dayRaw = existsSync(dayFile) ? readFileSync(dayFile, 'utf8') : '';
    ok('金额不落 JSONL（金额只在查询时算，改价无需迁移历史数据）',
      dayRaw.length > 0 && !dayRaw.includes('cost'), dayRaw.slice(0, 200));
  }

  // §4.1 流式 OpenAI：出站必须带 stream_options.include_usage，且流末 usage 帧被采到
  {
    streamOptionsSeen.length = 0;
    const r = await postChat(M_STREAM_OAI, { stream: true });
    ok('流式 OpenAI 请求成功（200 + SSE 帧）', r.status === 200 && r.text.includes('chat.completion.chunk'), `status=${r.status} len=${r.text.length}`);
    const seen = streamOptionsSeen.find((x) => x.model === M_STREAM_OAI);
    ok('出站请求体带 stream_options.include_usage=true',
      seen?.stream_options?.include_usage === true, JSON.stringify(seen));
    ok('客户端拿到的流末 usage 帧原样透传（21/9/30）',
      r.text.includes('"usage"') && r.text.includes('"completion_tokens":9'), r.text.slice(-200));
    await wait(150);
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('流式 OpenAI 的流末 usage 帧被采到（21/9）',
      modelOf(u.json?.today, M_STREAM_OAI)?.inputTokens === 21 && modelOf(u.json?.today, M_STREAM_OAI)?.outputTokens === 9,
      JSON.stringify(modelOf(u.json?.today, M_STREAM_OAI)));
  }

  // §4.1 流式 Anthropic：message_delta 的 usage 被采到
  {
    const r = await postChat(M_STREAM_ANTH, { stream: true });
    ok('流式 Anthropic 请求成功', r.status === 200 && r.text.includes('data:'), `status=${r.status}`);
    await wait(150);
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('流式 Anthropic 的 message_start/message_delta usage 被采到（13/17）',
      modelOf(u.json?.today, M_STREAM_ANTH)?.inputTokens === 13 && modelOf(u.json?.today, M_STREAM_ANTH)?.outputTokens === 17,
      JSON.stringify(modelOf(u.json?.today, M_STREAM_ANTH)));
  }

  // §4.1 不造假：上游没回报 usage / 回报全 0 / 请求失败 -> 不产生 token 记录
  {
    const before = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    const beforeRequests = before.json?.today?.requests ?? 0;
    const no = await postChat(M_NOUSAGE);
    const zero = await postChat(M_ZEROUSAGE);
    const bad = await postChat(M_FAIL);
    ok('无 usage / 全 0 usage / 失败三种请求分别返回 200 / 200 / >=400',
      no.status === 200 && zero.status === 200 && bad.status >= 400, `${no.status}/${zero.status}/${bad.status}`);
    await wait(150);
    const after = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('上游没回 usage 的成功请求不产生 token 记录', modelOf(after.json?.today, M_NOUSAGE) === null, JSON.stringify(modelOf(after.json?.today, M_NOUSAGE)));
    ok('上游回全 0 usage 不被记成"用了 0 token"（不造假）', modelOf(after.json?.today, M_ZEROUSAGE) === null, JSON.stringify(modelOf(after.json?.today, M_ZEROUSAGE)));
    ok('失败请求不产生 token 记录', modelOf(after.json?.today, M_FAIL) === null, JSON.stringify(modelOf(after.json?.today, M_FAIL)));
    ok('usage 的 requests 只计"有 usage 的成功请求"（三次都没加）',
      after.json?.today?.requests === beforeRequests, `${beforeRequests} -> ${after.json?.today?.requests}`);

    const tasks = await getJson('/api/tasks?limit=50', { authorization: `Bearer ${API_KEY}` });
    const rec = (tasks.json?.recent || []).find((x) => x.model === M_OPENAI);
    ok('usage 随任务记录进入 tasklog（请求级 usage: {input_tokens, output_tokens}）',
      rec?.usage?.input_tokens === 11 && rec?.usage?.output_tokens === 7, JSON.stringify(rec?.usage));
  }

  // §7.1（设计文档 §4.1 点名）落盘副本：usage 必须保留，上游回显的密钥必须掩码
  // 用整流器读 tasks.jsonl —— 这是"落盘副本"而不是"内存环"，两者脱敏口径不同
  {
    await wait(400); // tasklog 落盘 debounce 200ms
    const diskFile = path.join(LOGS_DIR, 'tasks.jsonl');
    const diskRaw = existsSync(diskFile) ? readFileSync(diskFile, 'utf8') : '';
    const diskLines = diskRaw.trim().split('\n').filter(Boolean)
      .map((l) => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean);
    ok('落盘副本存在且可逐行解析', diskLines.length > 0, `lines=${diskLines.length}`);

    const usageLine = diskLines.find((x) => x.model === M_OPENAI);
    ok('落盘副本保留 usage 字段（没被当敏感字段清掉，值是数值）',
      usageLine?.usage?.input_tokens === 11 && usageLine?.usage?.output_tokens === 7,
      JSON.stringify(usageLine?.usage));

    const failLine = diskLines.find((x) => x.model === M_FAIL);
    ok('落盘副本里上游回显的密钥被掩码（该行 error 含 REDACTED）',
      !!failLine && /REDACTED/.test(failLine.error || ''),
      JSON.stringify(failLine?.error));
    ok('落盘副本全文不含明文假 key',
      diskRaw.includes(M_FAIL) && !diskRaw.includes(FAKE_KEY),
      `containsFailRecord=${diskRaw.includes(M_FAIL)}`);
  }

  // §7.2（设计文档 §5.2 点名）上游因 stream_options 回 400：不挂死 / 归类 bad_request / 不产生记录 / 不影响后续
  {
    const before = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    const beforeReq = before.json?.today?.requests ?? 0;
    const t0 = Date.now();
    const rej = await postChat(M_REJECT_SO, { stream: true });
    const ms = Date.now() - t0;
    ok('上游因 stream_options 回 400 时网关快速返回失败（不挂死）',
      rej.status >= 400 && ms < 8000, `status=${rej.status} ${ms}ms`);

    await wait(150);
    const st = await getJson('/api/status', { authorization: `Bearer ${API_KEY}` });
    const ch = (st.json?.channels || []).find((c) => c.name === 'u-reject-so');
    ok('该渠道失败被归类为 bad_request（不重试、直接换家）',
      /\[bad_request\]/.test(ch?.lastError || ''), JSON.stringify(ch?.lastError));

    const after = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('被拒请求不产生 token 记录', modelOf(after.json?.today, M_REJECT_SO) === null,
      JSON.stringify(modelOf(after.json?.today, M_REJECT_SO)));
    ok('被拒请求不增加 requests 计数',
      after.json?.today?.requests === beforeReq, `${beforeReq} -> ${after.json?.today?.requests}`);

    // 用"200 但无 usage"的模型做后续探针：不扰动后面的固定数字断言
    const nxt = await postChat(M_NOUSAGE);
    ok('后续正常请求不受影响（仍能成功路由）', nxt.status === 200, `status=${nxt.status}`);
  }

  // §4.2/§4.3 today 与滚动窗口的边界（注入的"昨天 23:59"数据）
  {
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('today 不含昨天注入的记录（自然日口径）', modelOf(u.json?.today, 'injected-yesterday') === null,
      JSON.stringify(modelOf(u.json?.today, 'injected-yesterday')));
    ok('d1 含昨天注入的记录（滚动 24h 口径）',
      modelOf(u.json?.d1, 'injected-yesterday')?.inputTokens === 1000
      && modelOf(u.json?.d1, 'injected-yesterday')?.outputTokens === 500,
      JSON.stringify(modelOf(u.json?.d1, 'injected-yesterday')));
    ok('d7/d30/d90 单调不减（更大窗口不会更少）',
      u.json?.d7?.totalTokens >= u.json?.d1?.totalTokens
      && u.json?.d30?.totalTokens >= u.json?.d7?.totalTokens
      && u.json?.d90?.totalTokens >= u.json?.d30?.totalTokens,
      JSON.stringify(USAGE_RANGES.map((r) => u.json?.[r]?.totalTokens)));
    ok('每个范围都给出 requests / inputTokens / outputTokens / totalTokens / byModel',
      USAGE_RANGES.every((r) => {
        const x = u.json?.[r];
        return x && typeof x.requests === 'number' && typeof x.inputTokens === 'number'
          && typeof x.outputTokens === 'number' && x.totalTokens === x.inputTokens + x.outputTokens
          && x.byModel && typeof x.byModel === 'object';
      }), JSON.stringify(u.json?.today));
  }

  // §4.2 落盘：本地日期命名的 JSONL + 每请求一行的字段口径
  {
    const files = existsSync(USAGE_DIR) ? readdirSync(USAGE_DIR).filter((f) => f.endsWith('.jsonl')) : [];
    const todayFile = path.join(USAGE_DIR, `${localDayKey(Date.now())}.jsonl`);
    ok('按本地日期落盘 logs/usage/YYYY-MM-DD.jsonl', files.includes(`${localDayKey(Date.now())}.jsonl`), files.join(','));
    const lines = existsSync(todayFile)
      ? readFileSync(todayFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
      : [];
    const line = lines.find((x) => x.model === M_OPENAI);
    ok('每请求一行且字段为 { ts, model, channel, input, output }',
      !!line && typeof line.ts === 'string' && line.model === M_OPENAI && line.channel === 'u-openai'
      && line.input === 11 && line.output === 7, JSON.stringify(line));
  }

  // §1 成功标准：数据跨进程重启不丢（重启后从 logs/usage 重建聚合）
  {
    gw.kill();
    await wait(600);
    gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
    if (!(await waitReady())) throw new Error('网关重启后未就绪');
    await wait(200);
    const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
    ok('重启后 today 的 token 统计从磁盘重建（50 输入 / 36 输出 / 4 条记录）',
      u.json?.today?.inputTokens === 50 && u.json?.today?.outputTokens === 36 && u.json?.today?.requests === 4,
      JSON.stringify(u.json?.today && { i: u.json.today.inputTokens, o: u.json.today.outputTokens, r: u.json.today.requests }));
    ok('重启后 byModel 明细仍在（非流式 OpenAI 11/7）',
      modelOf(u.json?.today, M_OPENAI)?.inputTokens === 11 && modelOf(u.json?.today, M_OPENAI)?.outputTokens === 7,
      JSON.stringify(modelOf(u.json?.today, M_OPENAI)));
    ok('重启后昨天注入的历史仍在 d1 窗口里（跨重启 + 跨天都不丢）',
      modelOf(u.json?.d1, 'injected-yesterday')?.inputTokens === 1000,
      JSON.stringify(modelOf(u.json?.d1, 'injected-yesterday')));
    ok('重启后无 usage / 全 0 / 失败的请求依旧没有记录',
      modelOf(u.json?.today, M_NOUSAGE) === null && modelOf(u.json?.today, M_ZEROUSAGE) === null
      && modelOf(u.json?.today, M_FAIL) === null,
      JSON.stringify(Object.keys(u.json?.today?.byModel || {})));
  }

  // §4.4 面板：Token 用量区块（五个时间段卡片 + 按模型柱状图，全程无 innerHTML）
  {
    const panel = await raw({ p: '/', headers: { authorization: `Bearer ${API_KEY}` } });
    ok('面板 / 返回 200', panel.status === 200, `status=${panel.status}`);
    ok('面板 HTML 含「Token 用量」区块与两个挂载点',
      panel.text.includes('Token 用量') && panel.text.includes('id="usage-cards"') && panel.text.includes('id="usage-chart"'),
      `len=${panel.text.length}`);
    ok('面板把 /api/usage 挂到轮询（与 /api/metrics 并列）',
      panel.text.includes("api('/api/usage')"), 'no api(/api/usage) call');
    ok('面板不泄露明文 apiKey（沿用 test-admin-security 口径）',
      !panel.text.includes(API_KEY) && !panel.text.includes('sk-live'), panel.text.slice(0, 120));

    // 新渲染函数必须走 DOM API：不得用 innerHTML 拼上游数据（模型名来自客户端/上游）
    const fnStart = panel.text.indexOf('function renderUsage');
    const fnEnd = panel.text.indexOf('async function loadUsage');
    const newFns = fnStart >= 0 && fnEnd > fnStart ? panel.text.slice(fnStart, fnEnd) : '';
    ok('新增区块的渲染不含 innerHTML（无注入面）', newFns.length > 0 && !newFns.includes('innerHTML'), `sliceLen=${newFns.length}`);

    const scriptSrc = (panel.text.match(/<script>([\s\S]*?)<\/script>/) || [])[1] || '';
    ok('面板脚本可编译（没有把面板 JS 改出语法错误）', scriptSrc.length > 0 && canCompile(scriptSrc), `len=${scriptSrc.length}`);

    const p = runPanelRender(scriptSrc);
    const evil = '<img src=x onerror=alert(1)>';
    // 夹具与真实返回同构：总量 = byModel 各项之和；d7 无任何记录（requests=0）代表"未采集"
    p.ctx.renderUsage({
      today: { requests: 3, inputTokens: 110, outputTokens: 70, totalTokens: 180, byModel: { 'm-a': { requests: 3, inputTokens: 110, outputTokens: 70, totalTokens: 180 } } },
      d1: { requests: 3, inputTokens: 110, outputTokens: 70, totalTokens: 180, byModel: { 'm-a': { requests: 3, inputTokens: 110, outputTokens: 70, totalTokens: 180 } } },
      d7: { requests: 0, inputTokens: 0, outputTokens: 0, totalTokens: 0, byModel: {} },
      d30: {
        requests: 3, inputTokens: 190, outputTokens: 110, totalTokens: 300,
        byModel: {
          'm-a': { requests: 3, inputTokens: 110, outputTokens: 70, totalTokens: 180 },
          'm-b': { requests: 2, inputTokens: 80, outputTokens: 40, totalTokens: 120 },
        },
      },
      d90: { requests: 4, inputTokens: 200, outputTokens: 110, totalTokens: 310, byModel: { [evil]: { requests: 1, inputTokens: 6, outputTokens: 4, totalTokens: 10 } } },
    });

    const cardsText = p.textOf(p.getById('usage-cards'));
    ok('五个时间段卡片都在（今日 / 近 1 天 / 近 7 天 / 近 30 天 / 近 90 天）',
      ['今日', '近 1 天', '近 7 天', '近 30 天', '近 90 天'].every((t) => cardsText.includes(t)),
      cardsText.slice(0, 240));
    ok('卡片显示 输入 / 输出 / 合计 + 请求数', cardsText.includes('输入 110') && cardsText.includes('输出 70') && cardsText.includes('请求 3'), cardsText.slice(0, 240));
    ok('没有记录的时间段显示「未采集」而不是 0（诚实口径）',
      cardsText.includes('未采集'), cardsText.slice(0, 240));

    const chartText = p.textOf(p.getById('usage-chart'));
    ok('柱状图图例区分输入 / 输出',
      ['输入', '输出'].every((t) => chartText.includes(t)), chartText.slice(0, 160));
    ok('柱状图表头是固定四列：模型 × 今日 / 近 7 天 / 近 30 天 / 近 90 天',
      ['模型', '今日', '近 7 天', '近 30 天', '近 90 天'].every((t) => chartText.includes(t)), chartText.slice(0, 240));
    ok('柱右标注各范围的合计 token（m-a 今日 180、m-b 近 30 天 120）',
      chartText.includes('m-a') && chartText.includes('180') && chartText.includes('m-b') && chartText.includes('120'),
      chartText.slice(0, 300));

    // 真的画出了柱子：收集所有柱段（u-in / u-out）与空轨道
    const segs = [];
    let tracks = 0;
    const collect = (n) => {
      if (!n) return;
      if (n.className === 'utrack') tracks += 1;
      if (n.tagName === 'I' && (n.className === 'u-in' || n.className === 'u-out')) {
        segs.push({ cls: n.className, title: n.title, width: Number.parseFloat(n.style.width) });
      }
      for (const c of n.children || []) collect(c);
    };
    collect(p.getById('usage-chart'));
    ok('柱状图真的画出柱段（输入段 + 输出段都有，共 8 段 = evil 近90天 2 + m-a 今日/近30天 4 + m-b 近30天 2）',
      segs.length === 8 && segs.some((s) => s.cls === 'u-in') && segs.some((s) => s.cls === 'u-out'),
      JSON.stringify(segs.map((s) => s.title)));

    const todayIn = segs.find((s) => s.title === '今日 输入 110 tokens');
    ok('柱长按全表最大值等比缩放（今日输入 110/180 ≈ 61.1%）',
      !!todayIn && Math.abs(todayIn.width - (110 / 180) * 100) < 0.01, JSON.stringify(todayIn));
    const d30Out = segs.find((s) => s.title === '近 30 天 输出 40 tokens');
    ok('另一档同样等比（近 30 天输出 40/180 ≈ 22.2%）',
      !!d30Out && Math.abs(d30Out.width - (40 / 180) * 100) < 0.01, JSON.stringify(d30Out));

    ok('某范围缺该模型的记录时只留空轨道 +「未采集」，不画 0 长柱（3 模型 × 4 列 = 12 条轨道，近 7 天没有任何柱段）',
      tracks === 12 && !segs.some((s) => String(s.title || '').startsWith('近 7 天')), `tracks=${tracks}`);
    ok('恶意模型名只作为文本出现（新渲染全程未用 innerHTML）',
      chartText.includes(evil) && p.innerHtmlWrites() === 0, `innerHTML writes=${p.innerHtmlWrites()}`);

    p.ctx.renderUsage(null);
    ok('接口没数据时两个区块都显示「未采集」（不是一张空图冒充 0）',
      p.textOf(p.getById('usage-cards')).includes('未采集') && p.textOf(p.getById('usage-chart')).includes('未采集'),
      `${p.textOf(p.getById('usage-cards')).slice(0, 80)} | ${p.textOf(p.getById('usage-chart')).slice(0, 80)}`);
  }

  // §4.3 安全：复用 /api/* 的 Host / Origin / 鉴权
  {
    const cross = await raw({ p: '/api/usage', headers: { origin: EVIL } });
    ok('跨站 Origin 请求 /api/usage → 403 且无 CORS 头',
      cross.status === 403 && !cross.headers['access-control-allow-origin'],
      `status=${cross.status} acao=${cross.headers['access-control-allow-origin']}`);
    const badHost = await raw({ p: '/api/usage', host: `evil.example:${GW_PORT}` });
    ok('非法 Host 请求 /api/usage → 403', badHost.status === 403, `status=${badHost.status}`);
    const same = await raw({ p: '/api/usage', headers: { authorization: `Bearer ${API_KEY}` } });
    ok('同源 /api/usage → 200 且无 CORS 头',
      same.status === 200 && !same.headers['access-control-allow-origin'], `status=${same.status}`);
    ok('/api/usage 不回传明文 apiKey', !same.text.includes(API_KEY), same.text.slice(0, 160));

    // 回环来源沿用既有信任模型（与 /api/metrics 一致）：面板同源访问不需要 key。
    // 这里断言的是"新端点没有引入另一套鉴权口径"，而不是发明新规则。
    const wrongKey = await raw({ p: '/api/usage', headers: { authorization: 'Bearer WRONG' } });
    const metricsWrongKey = await raw({ p: '/api/metrics', headers: { authorization: 'Bearer WRONG' } });
    ok('/api/usage 与 /api/metrics 鉴权口径一致（回环来源同样处理）',
      wrongKey.status === metricsWrongKey.status, `usage=${wrongKey.status} metrics=${metricsWrongKey.status}`);
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  try { gw?.kill(); } catch { /* ignore */ }
  await wait(400);
  try { mock.close(); } catch { /* ignore */ }
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);