// P3 验收：耗时分解 + 模型级指标 + 面板（观测性）
//
// 覆盖 OPTIMIZATION-PLAN.md P3 §验收 的 5 条断言：
//   1. 三段耗时存在且量级正确（ttfbMs>=250 / bodyMs>=200 / 第二个请求 queueMs>0）
//   2. /api/metrics 排队时长（queueWaitMsMax>0 且 >= queueWaitMsTotal/waitedCount）
//   3. 模型级聚合（1 成功 + 1 失败 -> 成功率 0.5，失败指纹出现 1 次）
//   4. 面板可用且安全（/ 200 + 含新字段名 + 不泄露明文 key + /api 无 CORS 头 / 跨站 Origin 403）
//   5. 脱敏不绕过（新字段里的假密钥落盘后是 ***REDACTED***）
//
// 端口：P5 起改为运行时动态分配（原 9251 mock 上游 / 9252 网关）。
//
// 【重要 · 依赖标注】
//   §3.1 三段耗时（queueMs / ttfbMs / bodyMs）的**数据源在上游转发层 lib/proxy.mjs**，
//   该文件属 P2 独占。本套件按"契约"写断言：
//     - 字段存在  -> 必须量级正确（真实验证）
//     - 字段不存在 -> 打印 DEP 标记并说明原因，**不用假数据凑绿**（P2 落地后同一条测试自动变成真实验证）
//   同理 §3.2 的 queueWaitMsTotal/Max/waitedCount 由 P2 的 GatewayLimiter.stats() 产出。
//   测试里另有**不依赖 P2** 的旁证：并发期间 /api/metrics 的渠道 pending>=1，
//   证明"渠道级排队真的发生了"，这样 DEP 不会被误解成"mock 没生效"。
import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import os from 'node:os';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import { readFileSync, writeFileSync, rmSync, existsSync, mkdtempSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const RUN_CFG = path.join(HERE, 'metrics-observability.test.run.json');

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const GW = `http://127.0.0.1:${GW_PORT}`;
const API_KEY = 'OBSKEY';
const EVIL = 'https://evil.example';

// 模型名即 mock 的行为开关
const M_SLOW = 'obs-slow-model';   // 首字节慢 + 正文分块慢
const M_ERR = 'obs-err-model';     // 直接 500（用于失败指纹 / 脱敏场景）
const M_MIX = 'obs-mix-model';     // 第 1 次成功、第 2 次 500（用于模型级成功率 0.5）

let pass = 0;
let fail = 0;
let dep = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
/** 依赖尚未落地的字段：显式标记而不是假装通过 */
function depSkip(name, reason) {
  dep++;
  console.log(`  DEP  ${name} —— 未验证：${reason}`);
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/** 三段耗时取值：优先最后一次 attempt（转发层实测处），否则退回请求级字段 */
function timingsOf(rec) {
  const a = Array.isArray(rec?.attempts) ? rec.attempts : [];
  const src = a.length ? a[a.length - 1] : rec;
  return { queueMs: num(src?.queueMs), ttfbMs: num(src?.ttfbMs), bodyMs: num(src?.bodyMs) };
}

function assertPortFree(port) {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.once('error', () => resolve(false));
    s.listen(port, '127.0.0.1', () => s.close(() => resolve(true)));
  });
}

async function getJson(p, headers = {}) {
  const r = await fetch(GW + p, { headers });
  let j = null;
  try { j = await r.json(); } catch { /* 非 JSON */ }
  return { status: r.status, headers: r.headers, json: j };
}

async function postChat(model, headers = {}) {
  const r = await fetch(`${GW}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}`, ...headers },
    body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }] }),
  });
  let j = null;
  try { j = await r.json(); } catch { /* ignore */ }
  return { status: r.status, json: j };
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

/** 面板脚本能否通过编译（把面板 JS 改崩时立刻失败，而不是等浏览器报错） */
function canCompile(src) {
  try { new Function(src); return true; } catch { return false; }
}

/**
 * 用最小 DOM stub 在 vm 里**真实执行**面板脚本，然后调用新增的两个渲染函数。
 * 目的：验证 (§3.4) 两个区块确实渲染出预期文本，且全程不使用 innerHTML（无注入面）。
 * 浏览器不可用，这是"能离线跑的真实验证"与"只 grep 字符串"之间的取舍。
 */
function runPanelRender(src) {
  const nodes = new Map();
  let innerHtmlWrites = 0;
  const mkStyle = () => ({});
  class El {
    constructor(tag) {
      this.tagName = String(tag || '').toUpperCase();
      this.children = [];
      this._text = '';
      this._html = '';
      this.style = mkStyle();
      this.className = '';
      this.title = '';
      this.value = '';
      this.classList = { add() {}, remove() {}, toggle() {} };
    }
    get firstChild() { return this.children[0] || null; }
    set textContent(v) { this._text = v == null ? '' : String(v); this.children = []; }
    get textContent() { return this._text; }
    set innerHTML(v) { innerHtmlWrites += 1; this._html = String(v); }
    get innerHTML() { return this._html; }
    appendChild(n) { this.children.push(n); return n; }
    insertBefore(n, ref) {
      const i = this.children.indexOf(ref);
      if (i < 0) this.children.unshift(n); else this.children.splice(i, 0, n);
      return n;
    }
    addEventListener() {}
    removeEventListener() {}
    getAttribute() { return null; }
    setAttribute() {}
    focus() {}
  }
  const textOf = (n) => {
    if (!n) return '';
    if (n.children && n.children.length) return n.children.map(textOf).join('');
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
    setTimeout, clearTimeout, setInterval: () => 0, clearInterval: () => {},
    confirm: () => false,
    console,
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: 'panel-extract.js' });
  return { ctx, nodes, textOf, innerHtmlWrites: () => innerHtmlWrites, getById };
}

// ---------- mock 上游 ----------
// 三个模型三种行为，全部通过"先回响应头、再分块回正文"来制造可测量的 ttfb / body 分段。
const SLOW_HEAD = '{"id":"obs-slow","object":"chat.completion",';
const SLOW_MID = '"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hello world"}}],';
const SLOW_TAIL = '"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}';
const TTFB_DELAY_MS = 300;   // 响应头之前等这么久 -> ttfbMs 期望 >= 250
const BODY_SPAN_MS = 300;    // 正文分两块共跨这么久 -> bodyMs 期望 >= 200

let mixCalls = 0;
const mock = http.createServer((req, res) => {
  let rawBody = '';
  req.on('data', (c) => { rawBody += c; });
  req.on('end', () => {
    let model = '';
    try { model = JSON.parse(rawBody || '{}')?.model || ''; } catch { /* ignore */ }
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: model || 'obs', object: 'model' }] }));
    }
    if (model === M_ERR) {
      res.writeHead(500, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: { message: 'obs upstream exploded' } }));
    }
    if (model === M_MIX) {
      mixCalls += 1;
      if (mixCalls > 1) {
        res.writeHead(500, { 'content-type': 'application/json' });
        return res.end(JSON.stringify({ error: { message: 'obs mix exploded' } }));
      }
      const body = SLOW_HEAD + SLOW_MID + SLOW_TAIL;
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(body);
    }
    // 默认（M_SLOW 及任何其它模型）：首字节延迟 + 正文分 3 块
    setTimeout(() => {
      if (res.destroyed) return;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.write(SLOW_HEAD);
      setTimeout(() => {
        if (res.destroyed) return;
        res.write(SLOW_MID);
        setTimeout(() => {
          if (res.destroyed) return;
          res.end(SLOW_TAIL);
        }, BODY_SPAN_MS / 2);
      }, BODY_SPAN_MS / 2);
    }, TTFB_DELAY_MS);
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-obs-'));
let gw = null;

try {
  // 端口只能我们自己用：被别人占了就直接报错退出，避免"改错文件跑出来的假绿"
  for (const [name, port] of [['mock', MOCK_PORT], ['gateway', GW_PORT]]) {
    if (!(await assertPortFree(port))) throw new Error(`端口 ${port} 已被占用（${name}），本套件要求独占动态申请到的端口`);
  }

  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));

  const cfg = {
    server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: true },
    taskLog: { enabled: false }, // 只走内存环：验证面板/API 不需要落盘，也不污染 logs/
    modelMap: {},
    routing: {
      strategy: 'priority',
      tiered: false,
      attemptsPerChannel: 1,
      retryLoop: false,
      sessionAffinity: false,
      forceMaxEffort: false,
      maxConcurrent: 8,
      maxConcurrentPerChannel: 1,  // <- 关键：单渠道 1 个在途，让第 2 个请求真的排队
      maxConcurrentPerAgent: 0,
      queueTimeoutMs: 8000,
      timeoutMs: 30000,
      streamIdleTimeoutMs: 30000,
      probeIntervalMs: 0,
      discoverIntervalMs: 0,
    },
    channels: [
      { name: 'obs-slow', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_SLOW, priority: 10 },
      { name: 'obs-err', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_ERR, priority: 10 },
      { name: 'obs-mix', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: M_MIX, priority: 10 },
    ],
  };
  writeFileSync(RUN_CFG, JSON.stringify(cfg, null, 2) + '\n', 'utf8');

  gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
  if (!(await waitReady())) throw new Error('网关未就绪');
  await wait(200);

  const { TaskLog, percentile } = await import('../lib/tasklog.mjs');

  // ---------- 断言 1：三段耗时 ----------
  console.log('— 断言 1：三段耗时（ttfbMs / bodyMs / queueMs）—');
  {
    const st = await getJson('/api/status');
    ok('测试配置已生效（maxConcurrentPerChannel=1）',
      st.json?.routing?.maxConcurrentPerChannel === 1, JSON.stringify(st.json?.routing?.maxConcurrentPerChannel));

    // 两个同模型并发请求：A 占住唯一的渠道槽位，B 只能排队
    const pA = postChat(M_SLOW, { 'x-agent-id': 'obs-a' });
    const pB = postChat(M_SLOW, { 'x-agent-id': 'obs-b' });
    await wait(200); // A 已在途（上游首字节 300ms 后才回），B 应处于排队中
    const mid = await getJson('/api/metrics');
    const midCh = mid.json?.concurrency?.channels?.['obs-slow'];
    ok('旁证（不依赖 P2）：并发期间渠道 obs-slow 出现排队',
      !!midCh && (midCh.pending ?? 0) >= 1,
      `channel=${JSON.stringify(midCh)}`);

    const rA = await pA;
    const rB = await pB;
    ok('两个并发请求都成功（mock 上游可用）', rA.status === 200 && rB.status === 200,
      `${rA.status}/${rB.status} A=${JSON.stringify(rA.json).slice(0, 220)} B=${JSON.stringify(rB.json).slice(0, 160)}`);

    const tasks = await getJson('/api/tasks?limit=50');
    const recs = (tasks.json?.recent || []).filter((r) => r.model === M_SLOW);
    ok('/api/tasks 里有两条 obs-slow 记录', recs.length === 2, `count=${recs.length}`);
    console.log(`  · 两条记录: ${JSON.stringify(recs.map((r) => ({
      ok: r.ok, kind: r.kind ?? null, tries: r.tries, elapsedMs: r.elapsedMs,
      err: typeof r.error === 'string' ? r.error.slice(0, 140) : r.error ?? null,
    })))}`);

    const segs = recs.map(timingsOf);
    console.log(`  · 三段耗时实测: ${JSON.stringify(segs)}（同批 elapsedMs=${JSON.stringify(recs.map((r) => r.elapsedMs))}）`);
    // 旁证：mock 注入的延迟确实端到端发生了（与 P2 是否落地无关）
    const elapsed = recs.map((r) => num(r.elapsedMs) ?? 0).sort((a, b) => a - b);
    ok('旁证（不依赖 P2）：mock 注入的延迟端到端生效（两条 elapsedMs 均 >= 550ms）',
      elapsed.length === 2 && elapsed[0] >= 550, `elapsedMs=${JSON.stringify(elapsed)}`);

    const hasAny = segs.some((s) => s.ttfbMs != null || s.bodyMs != null || s.queueMs != null);
    if (!hasAny) {
      depSkip('ttfbMs >= 250 且 bodyMs >= 200（两个并发请求）',
        'lib/proxy.mjs 尚未采集三段耗时（P2 §2.5 未落地；该文件由 P2 独占，P3 不能改）');
      depSkip('第二个请求 queueMs > 0',
        '同上：queueMs 需在 limiter.acquire 前后采集（P2 独占文件）');
    } else {
      // 字段已存在 -> 必须量级正确（P2 落地后自动走到这里）
      const ttfbOk = segs.every((s) => s.ttfbMs != null && s.ttfbMs >= 250);
      const bodyOk = segs.every((s) => s.bodyMs != null && s.bodyMs >= 200);
      ok('ttfbMs >= 250（mock 首字节延迟 300ms）', ttfbOk, JSON.stringify(segs));
      ok('bodyMs >= 200（mock 正文跨 300ms）', bodyOk, JSON.stringify(segs));
      const queued = segs.map((s) => s.queueMs).filter((v) => v != null);
      ok('第二个请求 queueMs > 0（前一个请求占用了唯一的渠道槽位）',
        queued.some((v) => v > 0), JSON.stringify(segs));
    }
  }

  // ---------- 断言 2：/api/metrics 排队时长 ----------
  console.log('— 断言 2：/api/metrics 排队时长（依赖 P2 §2.4）—');
  {
    const m = await getJson('/api/metrics');
    const qw = m.json?.queueWait;
    const conc = m.json?.concurrency || {};
    const hasQw = conc.queueWaitMsTotal !== undefined || conc.queueWaitMsMax !== undefined
      || qw?.queueWaitMsTotal !== undefined || qw?.queueWaitMsMax !== undefined;
    if (!hasQw || qw == null) {
      depSkip('queueWaitMsMax > 0 且 >= queueWaitMsTotal / waitedCount',
        'lib/concurrency.mjs 的 GatewayLimiter.stats() 尚未产出 queueWaitMsTotal/Max/waitedCount（P2 §2.4 独占文件）');
      ok('透传不造假：字段缺失时 queueWait 为 null（没有兜底成 0）', m.json?.queueWait === null, JSON.stringify(m.json?.queueWait));
    } else {
      const total = num(qw.queueWaitMsTotal);
      const max = num(qw.queueWaitMsMax);
      const cnt = num(qw.waitedCount);
      console.log(`  · 实测 queueWait=${JSON.stringify(qw)}（本套件前面的并发场景制造了 1 次真实渠道级排队）`);
      ok('queueWaitMsMax > 0', max != null && max > 0, JSON.stringify(qw));
      ok('queueWaitMsMax >= queueWaitMsTotal / waitedCount',
        cnt != null && cnt > 0 && total != null && max >= total / cnt, JSON.stringify(qw));
      ok('waitedCount 与真实排队一致（只统计真正拿到许可的等待者，>=1）且时长在 queueTimeoutMs 之内',
        cnt != null && cnt >= 1 && max <= cfg.routing.queueTimeoutMs, JSON.stringify(qw));
      ok('concurrency 段原样透传这三个字段（面板/API 两个位置都能读到）',
        conc.queueWaitMsTotal === total && conc.queueWaitMsMax === max && conc.waitedCount === cnt,
        JSON.stringify({ total: conc.queueWaitMsTotal, max: conc.queueWaitMsMax, cnt: conc.waitedCount }));
    }
  }

  // ---------- 断言 3：模型级聚合 ----------
  console.log('— 断言 3：模型级聚合（1 成功 + 1 失败）—');
  {
    const first = await postChat(M_MIX);
    ok('obs-mix-model 第 1 次请求成功', first.status === 200, `status=${first.status}`);
    const second = await postChat(M_MIX);
    ok('obs-mix-model 第 2 次请求失败（mock 返回 500）', second.status >= 400, `status=${second.status}`);

    const m = await getJson('/api/metrics');
    const mm = (m.json?.models || []).find((x) => x.model === M_MIX);
    ok('/api/metrics 含 models 聚合段且能按模型查到', !!mm, JSON.stringify((m.json?.models || []).map((x) => x.model)));
    ok('该模型请求数 = 2', mm?.requests === 2, JSON.stringify(mm && { requests: mm.requests }));
    ok('该模型成功率 = 0.5', mm?.successRate === 0.5, JSON.stringify(mm && { successRate: mm.successRate }));
    ok('失败指纹出现 1 次（server_error）',
      Array.isArray(mm?.failures) && mm.failures.length === 1 && mm.failures[0].count === 1
        && mm.failures[0].fingerprint === 'server_error',
      JSON.stringify(mm?.failures));
    ok('ttfb 有样本时给出 p50/p95 数字，无样本时为 null（不造假 0）',
      mm?.ttfb && (mm.ttfb.samples > 0 ? num(mm.ttfb.p50) != null && num(mm.ttfb.p95) != null : mm.ttfb.p50 === null),
      JSON.stringify(mm?.ttfb));
  }

  // ---------- 断言 4：面板可用且安全 ----------
  console.log('— 断言 4：面板可用且安全 —');
  {
    const panel = await raw({ p: '/', headers: { authorization: `Bearer ${API_KEY}` } });
    ok('面板 / 返回 200', panel.status === 200, `status=${panel.status}`);
    ok('面板 HTML 含新字段名（模型健康度 / 耗时分解 / 三段耗时键 / 排队汇总）',
      panel.text.includes('model-health') && panel.text.includes('每模型健康度')
      && panel.text.includes('耗时分解') && panel.text.includes('ttfbMs')
      && panel.text.includes('bodyMs') && panel.text.includes('queueMs')
      && panel.text.includes('queue-wait'),
      `len=${panel.text.length}`);
    ok('面板不泄露明文 apiKey（沿用 test-admin-security 口径）',
      !panel.text.includes(API_KEY) && !panel.text.includes('sk-live'), panel.text.slice(0, 120));
    // 新渲染函数必须走 DOM API：不得用 innerHTML 拼上游数据（模型名/指纹都来自上游）
    const fnStart = panel.text.indexOf('function renderModelHealth');
    const fnEnd = panel.text.indexOf('async function loadMetrics');
    const newFns = fnStart >= 0 && fnEnd > fnStart ? panel.text.slice(fnStart, fnEnd) : '';
    ok('新增区块的渲染不含 innerHTML（无注入面）',
      newFns.length > 0 && !newFns.includes('innerHTML'), `sliceLen=${newFns.length}`);
    ok('/api/metrics 面板挂钩存在（面板会拉取模型级指标）', panel.text.includes("api('/api/metrics')"));

    // ---- 面板脚本：编译 + 在 DOM stub 里真实执行新增渲染函数 ----
    const scriptSrc = (panel.text.match(/<script>([\s\S]*?)<\/script>/) || [])[1] || '';
    ok('面板脚本可编译（没有把面板 JS 改出语法错误）',
      scriptSrc.length > 0 && canCompile(scriptSrc), `len=${scriptSrc.length}`);

    const p = runPanelRender(scriptSrc);
    p.ctx.renderModelHealth([
      { model: 'obs-mix-model', requests: 2, succeeded: 1, failed: 1, successRate: 0.5, failures: [{ fingerprint: 'server_error', count: 1 }], recovered: [], ttfb: { p50: 301, p95: 480, samples: 2 } },
      { model: 'no-sample-model', requests: 1, succeeded: 1, failed: 0, successRate: 1, failures: [], recovered: [], ttfb: { p50: null, p95: null, samples: 0 } },
      { model: '<img src=x onerror=alert(1)>', requests: 1, succeeded: 1, failed: 0, successRate: 1, failures: [], recovered: [{ fingerprint: 'auth', count: 1 }], ttfb: { p50: null, p95: null, samples: 0 } },
    ]);
    const mhText = p.textOf(p.getById('model-health'));
    const mhRows = p.getById('model-health').children[0]?.children?.[1]?.children || [];
    ok('渲染出「每模型健康度」表（模型 × 成功率 × p50/p95）',
      mhText.includes('obs-mix-model') && mhText.includes('50%')
      && mhText.includes('301ms') && mhText.includes('480ms'), mhText.slice(0, 200));
    ok('表头是"模型/请求/成功率/首字节 p50/首字节 p95/主要失败指纹"',
      mhText.includes('首字节 p50') && mhText.includes('首字节 p95'), mhText.slice(0, 120));
    ok('无首字节样本的模型两个耗时空格都是"未采集"（不是 0ms）',
      mhRows.find((tr) => tr.children[0].textContent === 'no-sample-model')?.children.slice(3, 5)
        .every((td) => td.textContent === '未采集'),
      JSON.stringify(mhRows.map((tr) => tr.children.map((td) => td.textContent))));
    ok('失败指纹列显示 server_error × 1；无失败但有换家救回时也标注',
      mhText.includes('server_error × 1') && mhText.includes('已换家救回 auth × 1'), mhText.slice(0, 300));
    ok('恶意模型名只作为文本出现（新渲染全程未用 innerHTML）',
      mhText.includes('<img src=x onerror=alert(1)>') && p.innerHtmlWrites() === 0,
      `innerHTML writes=${p.innerHtmlWrites()}`);

    p.ctx.renderTiming({
      model: 'obs-slow-model', ok: true, elapsedMs: 620, channel: 'obs-slow', ts: '2026-01-01T00:00:00.000Z',
      attempts: [{ queueMs: 20, ttfbMs: 300, bodyMs: 300 }],
    });
    const tHost = p.getById('timing');
    const bar = tHost.children.find((c) => c.className === 'bar');
    ok('「耗时分解」画了三段堆叠条（排队/首字节/正文）',
      bar?.children?.length === 3
      && bar.children.map((c) => c.className).join(',') === 'seg-queue,seg-ttfb,seg-body',
      JSON.stringify(bar?.children?.map((c) => c.className)));
    ok('堆叠条宽度按三段占比（20/300/300，总和 620）',
      bar?.children?.[0]?.style?.width === `${(20 / 620) * 100}%`
      && bar?.children?.[1]?.style?.width === `${(300 / 620) * 100}%`,
      JSON.stringify(bar?.children?.map((c) => c.style.width)));
    ok('图例给出三段毫秒数',
      p.textOf(tHost).includes('排队 20ms') && p.textOf(tHost).includes('首字节 300ms')
      && p.textOf(tHost).includes('正文 300ms'), p.textOf(tHost).slice(0, 200));

    p.ctx.renderTiming({ model: 'obs-slow-model', ok: true, elapsedMs: 12, attempts: [] });
    const tText2 = p.textOf(p.getById('timing'));
    ok('未采集分段时不画条、给出明确提示（不伪装成 0ms）',
      !p.getById('timing').children.some((c) => c.className === 'bar') && tText2.includes('没有分段耗时'),
      tText2.slice(0, 160));

    // §3.2：排队时长在面板上的展示（数据来自 /api/metrics 的 queueWait）
    p.ctx.renderQueueWait({ queueWaitMsTotal: 632, queueWaitMsMax: 632, waitedCount: 1 });
    const qwText = p.textOf(p.getById('queue-wait'));
    ok('面板显示限流器全局排队等待（最大/累计/次数）',
      qwText.includes('最大等待 632ms') && qwText.includes('累计等待 632ms') && qwText.includes('排队次数 1'),
      qwText.slice(0, 160));
    p.ctx.renderQueueWait(null);
    ok('排队字段缺失时显示"未采集"而不是 0',
      p.textOf(p.getById('queue-wait')).includes('未采集'), p.textOf(p.getById('queue-wait')).slice(0, 120));

    const st = await raw({ p: '/api/status', headers: { authorization: `Bearer ${API_KEY}` } });
    const stJson = JSON.parse(st.text);
    ok('/api/status 不回传完整 apiKey',
      stJson?.server?.apiKey === undefined && !st.text.includes(API_KEY), JSON.stringify(stJson?.server));

    // 安全红线：/api/* 无 CORS 头、跨站 Origin 403（含我新增的指标端点）
    const cross = await raw({ p: '/api/metrics', headers: { origin: EVIL } });
    ok('跨站 Origin 请求 /api/metrics → 403 且无 CORS 头',
      cross.status === 403 && !cross.headers['access-control-allow-origin'],
      `status=${cross.status} acao=${cross.headers['access-control-allow-origin']}`);
    const same = await raw({ p: '/api/metrics' });
    ok('同源 /api/metrics → 200 且无 CORS 头',
      same.status === 200 && !same.headers['access-control-allow-origin'],
      `status=${same.status} acao=${same.headers['access-control-allow-origin']}`);
    const badHost = await raw({ p: '/api/metrics', host: `evil.example:${GW_PORT}` });
    ok('非法 Host 请求 /api/metrics → 403', badHost.status === 403, `status=${badHost.status}`);
  }

  // ---------- 断言 5：脱敏不绕过 ----------
  console.log('— 断言 5：新字段同样走 redactSecrets（落盘不写明文 key）—');
  {
    const dir = path.join(tmpDir, 'tl');
    const file = path.join(dir, 'tasks.jsonl');
    const FAKE = 'sk-live-obsabcdef123456';
    const tl = new TaskLog({ enabled: true, dir, file: 'tasks.jsonl' });
    tl.write({
      requestId: 'obs-redact', model: 'redact-model', ok: false, kind: 'server_error',
      error: `HTTP 500 ${FAKE}`,
      attempts: [{
        channel: 'obs-err', error: `HTTP 500 bearer ${FAKE}`,
        queueMs: 5, ttfbMs: 12, bodyMs: 34,
        note: `timing remark mentions apiKey=${FAKE}`,
      }],
      events: [{ level: 'error', message: `event ${FAKE}`, queueMs: 1 }],
    });
    await tl.flushNow();
    const disk = existsSync(file) ? readFileSync(file, 'utf8') : '';
    ok('落盘文件已生成', existsSync(file), file);
    ok('落盘的 error 里没有明文 sk- key', !disk.includes(FAKE), disk.slice(0, 200));
    ok('落盘里出现 ***REDACTED*** 掩码', disk.includes('***REDACTED***'), disk.slice(0, 200));
    ok('attempts[].note（新字段的文本）也被脱敏', !disk.includes(`apiKey=${FAKE}`), disk.slice(0, 300));
    ok('events[].message 里的 key 也被脱敏', !disk.includes(`event ${FAKE}`), disk.slice(0, 300));

    const line = disk.trim().split('\n').filter(Boolean).pop() || '{}';
    let parsed = null;
    try { parsed = JSON.parse(line); } catch { /* ignore */ }
    ok('落盘仍是可解析 JSONL', !!parsed, line.slice(0, 120));
    ok('落盘保留了 attempts[] 的三段耗时数字（脱敏不误伤数值字段）',
      parsed?.attempts?.[0]?.queueMs === 5 && parsed?.attempts?.[0]?.ttfbMs === 12
      && parsed?.attempts?.[0]?.bodyMs === 34,
      JSON.stringify(parsed?.attempts?.[0]));

    // 内存环保留原文（排障需要），与落盘脱敏不冲突
    ok('内存环仍保留原文（排障口径不变）', tl.recent(1)[0]?.error === `HTTP 500 ${FAKE}`);

    // 分位数：无样本 -> null（不造假 0）
    ok('percentile 空样本返回 null（不造假 0）', percentile([], 95) === null);
    ok('percentile 单样本原样返回', percentile([7], 95) === 7);
  }

  // ---------- 补充：P3 侧耗时契约（合成记录，不依赖 P2） ----------
  // 断言 1（端到端量级）的数据源在 P2 独占的 lib/proxy.mjs；这里用合成记录证明
  // **P3 这一侧**的契约已经成立：承载 / 派生 totalMs / 非法值不写成 0 / 模型级 p50·p95。
  // 这样 P2 一落地，端到端断言与这里的契约断言会同时为真，无需返工。
  console.log('— 补充：P3 侧耗时契约（合成记录，不依赖 P2）—');
  {
    const tl = new TaskLog({ enabled: false, dir: path.join(tmpDir, 'tl-contract'), file: 'tasks.jsonl' });
    tl.write({ requestId: 'c1', model: 'contract-model', ok: true, attempts: [{ channel: 'c', queueMs: 250.4, ttfbMs: 300.6, bodyMs: 299.5 }] });
    tl.write({ requestId: 'c2', model: 'contract-model', ok: true, attempts: [{ channel: 'c', ttfbMs: 640 }] });
    tl.write({ requestId: 'c4', model: 'contract-model', ok: true, attempts: [{ channel: 'c', ttfbMs: 900 }] });
    tl.write({ requestId: 'c3', model: 'contract-model', ok: true, attempts: [{ channel: 'c', queueMs: -5, ttfbMs: 'bogus', bodyMs: null }] });
    const [c3, c4, c2, c1] = tl.recent(4);

    ok('attempts[] 承载三段耗时并派生 totalMs（250.4/300.6/299.5 -> 250/301/300/851）',
      c1.attempts[0].queueMs === 250 && c1.attempts[0].ttfbMs === 301
      && c1.attempts[0].bodyMs === 300 && c1.attempts[0].totalMs === 851,
      JSON.stringify(c1.attempts[0]));
    ok('非法耗时（负数/字符串/null）被丢弃，不写成 0',
      !('queueMs' in c3.attempts[0]) && !('ttfbMs' in c3.attempts[0])
      && !('bodyMs' in c3.attempts[0]) && !('totalMs' in c3.attempts[0]),
      JSON.stringify(c3.attempts[0]));
    ok('只采到一段时不补其它键（c2 只有 ttfbMs）',
      c2.attempts[0].ttfbMs === 640
      && !('bodyMs' in c2.attempts[0]) && !('queueMs' in c2.attempts[0]),
      JSON.stringify(c2.attempts[0]));

    const mm = tl.modelMetrics().find((m) => m.model === 'contract-model');
    ok('模型级 ttfb 用样本算 p50/p95（样本 301/640/900 -> p50=640, p95=874）',
      mm?.ttfb?.samples === 3 && mm.ttfb.p50 === 640 && mm.ttfb.p95 === 874,
      JSON.stringify(mm?.ttfb));
    ok('成功请求的 queue/body 样本各自独立（samples 分别 1）',
      mm?.queue?.samples === 1 && mm.queue.p50 === 250
      && mm?.body?.samples === 1 && mm.body.p50 === 300,
      JSON.stringify({ queue: mm?.queue, body: mm?.body }));
    ok('模型级成功率与失败计数（4 次全成功 -> 1）',
      mm?.requests === 4 && mm.failed === 0 && mm.successRate === 1,
      JSON.stringify(mm && { requests: mm.requests, failed: mm.failed, successRate: mm.successRate }));

    // 上界：模型名来自客户端请求，跟踪表必须有界（否则任意模型名能撑爆内存与 /api/metrics 响应体积）
    for (let i = 0; i < 260; i += 1) tl.write({ requestId: `junk-${i}`, model: `junk-model-${i}`, ok: true });
    const tracked = tl.modelMetrics({ limit: 1000 });
    ok('模型跟踪表有上界（<=200），不会被任意模型名撑爆',
      tracked.length <= 200 && tracked.length > 0, `tracked=${tracked.length}`);
    ok('最早写入的模型被 LRU 淘汰',
      !tracked.some((m) => m.model === 'contract-model'), tracked.slice(0, 3).map((m) => m.model).join(','));
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  try { gw?.kill(); } catch { /* ignore */ }
  await wait(400);
  try { mock.close(); } catch { /* ignore */ }
  try { if (existsSync(RUN_CFG)) rmSync(RUN_CFG); } catch { /* ignore */ }
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败${dep ? `, ${dep} 项 DEP（依赖 P2 尚未落地的字段）` : ''}`);
process.exit(fail ? 1 : 0);
