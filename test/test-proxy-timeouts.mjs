/**
 * P2 验收（OPTIMIZATION-PLAN.md §P2）：
 *   §2.1 独立的首字节超时（routing.firstByteTimeoutMs）
 *   §2.2 waitDrain 可中断（客户端既不读也不断开时不再永久挂住）
 *   §2.3 出站代理路径（channel.proxy -> lib/outbound-proxy.mjs）的看门狗覆盖
 *   §2.4 排队时长统计（Semaphore / GatewayLimiter 的 queueWaitMs*）
 *   §2.5 三段耗时采集 queueMs / ttfbMs / bodyMs（含**成功路径**）
 *   附加：candidatesFor 全部调用点透传 requestId（轮换游标"每请求只前进一格"）
 *
 * 端口：P5 起全部**运行时动态分配**（原 9270-9281）：
 *   PORT 网关 | P_HO 只回响应头 | P_SLOW 首字节慢 | P_FLOOD 狂灌 | P_PROXY 本机代理
 *   P_OK 健康上游 | P_CURSOR[0..5] 游标 preferred(a/b/c) + fallback(x/y/z)
 *
 * 跑法：node test/test-proxy-timeouts.mjs
 *
 * 说明：配置由本文件在系统临时目录里生成并在结束时删除，不往仓库里新增配置文件，
 * 也不占用任何既有测试的端口。端口由 test/lib/ports.mjs 动态申请，互不重叠。
 */
import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { writeFileSync, rmSync, existsSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { GatewayLimiter, Semaphore } from '../lib/concurrency.mjs';
import { ChannelManager } from '../lib/channels.mjs';
import { freePort, freePorts } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const KEY = 'TESTKEY';

const PORT = await freePort();
const BASE = `http://127.0.0.1:${PORT}`;
const P_HO = await freePort();
const P_SLOW = await freePort();
const P_FLOOD = await freePort();
const P_PROXY = await freePort();
const P_OK = await freePort();
const P_CURSOR = await freePorts(6); // a b c | x y z
const AUTH = { authorization: `Bearer ${KEY}` };

const TMP_DIR = path.join(os.tmpdir(), 'llm-gw-p2-timeouts');
const TMP_CFG = path.join(TMP_DIR, 'proxy-timeouts.tmp.json');

let pass = 0;
let fail = 0;
const ok = (name, cond, extra = '') => {
  if (cond) {
    pass += 1;
    console.log(`  PASS ${name}${extra ? `  [${extra}]` : ''}`);
  } else {
    fail += 1;
    console.log(`  FAIL ${name}${extra ? `  [${extra}]` : ''}`);
  }
};

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, ms = 3000, step = 50) {
  const deadline = Date.now() + ms;
  for (;;) {
    let v = false;
    try { v = await fn(); } catch { v = false; }
    if (v) return true;
    if (Date.now() > deadline) return false;
    await wait(step);
  }
}

// ---- HTTP 小工具（自带看门狗：修好之前这些请求确实是"挂住"的，不能让测试自己也挂死）----
async function call(pathname, { method = 'GET', body, headers = {}, timeoutMs = 15000 } = {}) {
  try {
    const r = await fetch(`${BASE}${pathname}`, {
      method,
      headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...AUTH, ...headers },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await r.text();
    let json = null;
    try { json = JSON.parse(text); } catch { /* SSE / 纯文本 */ }
    return { status: r.status, text, json, headers: r.headers };
  } catch (err) {
    return { status: 0, text: `TEST-CLIENT-TIMEOUT(${err?.name})`, json: null, headers: new Map() };
  }
}

const chat = (body, extra = {}) => call('/v1/chat/completions', { method: 'POST', body, ...extra });
const MSG = [{ role: 'user', content: '你好' }];
const recentTasks = async () => ((await call('/api/tasks?limit=100')).json?.recent) || [];
const metrics = async () => (await call('/api/metrics')).json;
const globalActive = async () => {
  const m = await metrics();
  return m?.concurrency?.global?.active ?? -1;
};
const channelInFlight = async (name) => {
  const m = await metrics();
  return (m?.channels || []).find((c) => c.name === name)?.inFlight ?? -1;
};

/** 从 SSE 文本里把 delta.content 拼回整段正文（流式响应里正文被切成多块，不能直接子串匹配） */
const sseContent = (text) => [...String(text).matchAll(/"content":"((?:[^"\\]|\\.)*)"/g)]
  .map((m) => m[1])
  .join('');

// ---- mock 上游 ----
const servers = [];
const sendJson = (res, code, obj) => {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body) });
  res.end(body);
};
const openaiBody = (model, text) => ({
  id: 'chatcmpl-t', object: 'chat.completion', created: 1, model,
  choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
  usage: { prompt_tokens: 3, completion_tokens: 5, total_tokens: 8 },
});
const chunk = (model, delta, finish = null) => `data: ${JSON.stringify({
  id: 'chatcmpl-s', object: 'chat.completion.chunk', created: 1, model,
  choices: [{ index: 0, delta, finish_reason: finish }],
})}\n\n`;
const readBody = (req) => new Promise((resolve) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => { try { resolve(JSON.parse(raw || '{}')); } catch { resolve({}); } });
  req.on('error', () => resolve({}));
});

function startServer(port, handler) {
  const server = http.createServer((req, res) => {
    res.on('error', () => { /* 客户端断开后写入 EPIPE，忽略 */ });
    req.on('error', () => { /* 同上 */ });
    Promise.resolve(handler(req, res, new URL(req.url, 'http://127.0.0.1'))).catch(() => {
      try { res.destroy(); } catch { /* ignore */ }
    });
  });
  server.on('clientError', (_err, socket) => { try { socket.destroy(); } catch { /* ignore */ } });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => { servers.push(server); resolve(server); });
  });
}

// 游标 mock：命中顺序计入 cursorHits（用来验证 requestId 接线）
const cursorHits = [];
const cursorMock = (label) => async (req, res) => {
  await readBody(req);
  cursorHits.push(label);
  return sendJson(res, 500, { error: { message: `cursor-${label} boom` } });
};

async function startMocks() {
  // P_HO 只回响应头、永不写 body（首字节超时的靶子；也供出站代理用例复用）
  await startServer(P_HO, async (req, res) => {
    await readBody(req);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.flushHeaders();
    return undefined;
  });

  // P_SLOW 首字节慢 300ms、正文分 3 块各 100ms（验收 §2.1-2 / §2.5）
  await startServer(P_SLOW, async (req, res, url) => {
    const body = await readBody(req);
    const model = body.model || 'slowfirst-model';
    await wait(300); // 首字节延迟
    if (body.stream === true) {
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' });
      for (const piece of ['慢', '首字', '节但正文正常']) {
        res.write(chunk(model, { role: 'assistant', content: piece }));
        await wait(100);
      }
      await wait(100); // 收尾前再等一拍，bodyMs 有足够余量（>= 200ms）
      res.write(chunk(model, {}, 'stop'));
      res.end('data: [DONE]\n\n');
      return undefined;
    }
    const text = JSON.stringify(openaiBody(model, '慢首字节但正文正常：这段正文分三块吐出来。'));
    const third = Math.ceil(text.length / 3);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(text) });
    res.write(text.slice(0, third));
    await wait(100);
    res.write(text.slice(third, third * 2));
    await wait(100);
    res.write(text.slice(third * 2));
    await wait(100);
    res.end();
    return undefined;
  });

  // P_FLOOD 持续狂灌（把网关顶进写回背压 -> waitDrain）
  await startServer(P_FLOOD, async (req, res) => {
    const body = await readBody(req);
    res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
    const frame = chunk(body.model || 'flood-model', { content: 'x'.repeat(4096) });
    let stopped = false;
    const timer = setInterval(() => {
      if (stopped || res.writableEnded || res.destroyed) { clearInterval(timer); stopped = true; return; }
      for (let i = 0; i < 16; i += 1) {
        if (res.write(frame) === false) break; // 上游侧背压：本 tick 先不发
      }
    }, 2);
    res.on('close', () => { stopped = true; clearInterval(timer); });
    req.on('error', () => { stopped = true; clearInterval(timer); });
    return undefined;
  });

  // P_OK 健康上游
  await startServer(P_OK, async (req, res) => {
    const body = await readBody(req);
    const model = body.model || 'ok-model';
    if (body.stream === true) {
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
      res.write(chunk(model, { role: 'assistant', content: '健康上游的正常回答' }));
      res.write(chunk(model, {}, 'stop'));
      res.end('data: [DONE]\n\n');
      return undefined;
    }
    return sendJson(res, 200, openaiBody(model, '健康上游的正常回答'));
  });

  // P_CURSOR[0..5] 游标渠道：全部 500（可重试），用于观察选路顺序
  const labels = ['a', 'b', 'c', 'x', 'y', 'z'];
  for (let i = 0; i < P_CURSOR.length; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await startServer(P_CURSOR[i], cursorMock(labels[i]));
  }
}

/**
 * 本机最小 HTTP 代理（零依赖）：客户端用绝对形式请求行打到它，它转发给目标。
 * 关键在 res.flushHeaders()：必须把上游的响应头**立刻**送回网关，
 * 否则"只回响应头不给数据"这个语义在代理这一跳就被吞掉了（网关只会等到 headerTimer）。
 */
function startProxyServer(port) {
  const server = http.createServer((req, res) => {
    let target;
    try { target = new URL(req.url); } catch { try { res.writeHead(400); res.end(); } catch { /* ignore */ } return; }
    const up = http.request({
      hostname: target.hostname,
      port: target.port || 80,
      path: `${target.pathname}${target.search}`,
      method: req.method,
      headers: { ...req.headers, host: target.host },
    }, (upRes) => {
      if (res.writableEnded || res.destroyed) { upRes.destroy(); return; }
      res.writeHead(upRes.statusCode || 502, upRes.headers);
      res.flushHeaders();
      upRes.pipe(res);
    });
    up.on('error', () => { try { if (!res.headersSent) res.writeHead(502); res.end(); } catch { /* ignore */ } });
    req.on('error', () => { try { up.destroy(); } catch { /* ignore */ } });
    req.pipe(up);
  });
  server.on('clientError', (_err, socket) => { try { socket.destroy(); } catch { /* ignore */ } });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => { servers.push(server); resolve(server); });
  });
}

// ---- 测试用配置（临时目录，结束即删）----
function writeConfig() {
  const cursorChannels = ['a', 'b', 'c'].map((label, i) => ({
    name: `cursor-${label}`,
    protocol: 'openai',
    baseUrl: `http://127.0.0.1:${P_CURSOR[i]}`,
    apiKey: 'k',
    models: ['cursor-model'],
    priority: i + 1,
    tier: 'preferred',
  })).concat(['x', 'y', 'z'].map((label, i) => ({
    name: `cursor-${label}`,
    protocol: 'openai',
    baseUrl: `http://127.0.0.1:${P_CURSOR[i + 3]}`,
    apiKey: 'k',
    models: ['cursor-model'],
    priority: i + 1,
    tier: 'fallback',
  })));

  const cfg = {
    server: { host: '127.0.0.1', port: PORT, apiKey: KEY },
    routing: {
      // 游标用例要 round-robin + 关 sticky（channels.mjs 里轮换只在这两个条件下生效）
      strategy: 'round-robin',
      sticky: false,
      sessionAffinity: false,
      tiered: true,
      maxAttempts: 8,
      attemptsPerChannel: 1,
      retryPerAttemptMs: 50,
      fallbackAttempts: 1,
      fallbackRetryIntervalMs: 50,
      fallbackShuffle: false,
      timeoutMs: 20000,
      // ---- P2 新增/相关 ----
      firstByteTimeoutMs: 500,     // 首字节超时（独立于 streamIdleTimeoutMs）
      streamIdleTimeoutMs: 60000,  // 空闲看门狗故意放大：证明 500ms 那一下是首字节超时干的
      writeDrainTimeoutMs: 800,    // waitDrain 自身超时（默认 = streamIdleTimeoutMs）
      failThreshold: 100,          // 别让冷却干扰选路顺序观察
      cooldownMs: 30000,
      maxCooldownMs: 300000,
      probeIntervalMs: 0,
      discoverIntervalMs: 0,
      retryLoop: true,
      retryWaitMs: 250,
      retryMaxWaitMs: 250,         // 最多两轮（游标用例需要跨轮）
      maxTotalWaitMs: 8000,
      maxConcurrent: 8,
      maxConcurrentPerChannel: 1,  // 渠道级排队 -> queueMs 可观测
      maxConcurrentPerAgent: 0,
      queueTimeoutMs: 3000,
    },
    taskLog: { enabled: true, dir: TMP_DIR, file: 'proxy-timeouts.jsonl', ringMax: 100 },
    channels: [
      { name: 'ho-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_HO}`, apiKey: 'k', models: ['ho-model'], priority: 1, tier: 'preferred' },
      { name: 'proxy-ho-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_HO}`, apiKey: 'k', proxy: `http://127.0.0.1:${P_PROXY}`, models: ['proxy-ho-model'], priority: 1, tier: 'preferred' },
      { name: 'slowfirst-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_SLOW}`, apiKey: 'k', models: ['slowfirst-model'], priority: 1, tier: 'preferred' },
      { name: 'queue-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_SLOW}`, apiKey: 'k', models: ['queue-model'], priority: 1, tier: 'preferred' },
      { name: 'flood-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_FLOOD}`, apiKey: 'k', models: ['flood-model'], priority: 1, tier: 'preferred' },
      { name: 'ok-1', protocol: 'openai', baseUrl: `http://127.0.0.1:${P_OK}`, apiKey: 'k', models: ['ok-model'], priority: 1, tier: 'preferred' },
      ...cursorChannels,
    ],
  };
  if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });
  writeFileSync(TMP_CFG, JSON.stringify(cfg, null, 2), 'utf8');
  return cfg;
}

// ---- 网关 ----
let gwLog = '';
function startGateway() {
  const gw = spawn(
    NODE,
    [path.join(ROOT, 'server.mjs'), '--config', TMP_CFG, '--no-discover', '--log-level', 'info'],
    { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] },
  );
  gw.stdout.on('data', (b) => { gwLog += b.toString(); });
  gw.stderr.on('data', (b) => { gwLog += b.toString(); });
  return gw;
}

async function waitReady(tries = 80) {
  for (let i = 0; i < tries; i += 1) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return true;
    } catch { /* 还没起来 */ }
    await wait(100);
  }
  return false;
}

async function main() {
  writeConfig();
  await startMocks();
  await startProxyServer(P_PROXY);
  const gw = startGateway();
  let sock = null;

  try {
    if (!(await waitReady())) {
      console.log('\n网关未能就绪，终止');
      console.log(gwLog.slice(-2000));
      return 1;
    }

    // ---------- §2.1 首字节超时：独立于 streamIdleTimeoutMs ----------
    console.log('\n[§2.1] 首字节超时独立生效（firstByteTimeoutMs=500 / streamIdleTimeoutMs=60000）');
    {
      const t0 = Date.now();
      const r = await chat({ model: 'ho-model', messages: MSG });
      const ms = Date.now() - t0;
      ok('非流式：上游只回响应头 -> 首字节超时内拿到 503（不是挂到 60s 的静默超时）',
        r.status === 503 && ms < 2000, `status=${r.status} ${ms}ms`);
      ok('非流式：错误文案是"首字节超时"，能区分静默/读取超时',
        /首字节超时/.test(r.text) && !/静默超时|读取超时/.test(r.text), JSON.stringify(r.text.slice(0, 180)));
    }
    {
      const t0 = Date.now();
      const r = await chat({ model: 'ho-model', messages: MSG, stream: true });
      const ms = Date.now() - t0;
      ok('流式：上游只回响应头 -> 同样受首字节超时保护并在超时内 503',
        r.status === 503 && ms < 2000 && /首字节超时/.test(r.text), `status=${r.status} ${ms}ms ${r.text.slice(0, 120)}`);
    }

    // ---------- §2.1-2 两段不叠加：首字节慢但身体正常不能被误杀 ----------
    console.log('\n[§2.1-2] 两段计时不叠加：首字节 300ms + 正文持续到 >500ms 仍然成功');
    {
      const t0 = Date.now();
      const r = await chat({ model: 'slowfirst-model', messages: MSG, stream: true });
      const ms = Date.now() - t0;
      const streamed = sseContent(r.text);
      ok('流式：首字节 300ms、正文分 3 块 -> 200 且正文完整',
        r.status === 200 && streamed.includes('慢首字节但正文正常'),
        `status=${r.status} ${ms}ms bytes=${r.text.length} content=${JSON.stringify(streamed)}`);
      ok('整段耗时已超过 firstByteTimeoutMs(500ms) 仍成功（首字节看门狗已解除，没有叠加计时）',
        ms > 500, `elapsed=${ms}ms`);
    }
    {
      const t0 = Date.now();
      const r = await chat({ model: 'slowfirst-model', messages: MSG });
      const ms = Date.now() - t0;
      ok('非流式：同样不被首字节超时误杀', r.status === 200 && /慢首字节/.test(r.text), `status=${r.status} ${ms}ms`);
    }

    // ---------- §2.2 waitDrain：客户端既不读也不断开 ----------
    console.log('\n[§2.2/§2.4] 客户端不读也不断开：请求必须自行结束、许可必须归还');
    {
      sock = net.connect(PORT, '127.0.0.1');
      await new Promise((resolve) => sock.once('connect', resolve));
      sock.on('error', () => { /* 收尾 destroy 时的 EPIPE 忽略 */ });
      const reqBody = JSON.stringify({ model: 'flood-model', messages: MSG, stream: true });
      sock.write(
        `POST /v1/chat/completions HTTP/1.1\r\n`
        + `Host: 127.0.0.1:${PORT}\r\n`
        + 'content-type: application/json\r\n'
        + `authorization: Bearer ${KEY}\r\n`
        + `content-length: ${Buffer.byteLength(reqBody)}\r\n\r\n${reqBody}`,
      );
      // 全程一个字节都不读：内核接收缓冲填满后，网关必然停在"写回受阻 -> await waitDrain"
      const flooding = await waitFor(async () => (await globalActive()) >= 1, 5000, 30);
      const released = await waitFor(async () => (await globalActive()) === 0, 8000, 60);
      ok('停滞的客户端不再把请求永久挂在 waitDrain（超时内自行结束）',
        flooding && released, `flooding=${flooding} released=${released} drainTimeoutMs=800 streamIdleTimeoutMs=60000`);
      ok('该渠道在途回到 0（/api/metrics channels[].inFlight）',
        (await channelInFlight('flood-1')) === 0, `inFlight=${await channelInFlight('flood-1')}`);
      ok('global.active === 0（并发许可没有泄漏）', (await globalActive()) === 0, `global.active=${await globalActive()}`);
      // 网关日志是异步写 stdout 的：等一下再断言，别抢跑
      const logged = await waitFor(async () => /停止读取响应/.test(gwLog), 3000, 50);
      ok('网关日志确认是 waitDrain 自超时收场（而不是空闲看门狗）',
        logged, 'grep 网关日志 "停止读取响应"');

      const rAfter = await chat({ model: 'ok-model', messages: MSG });
      ok('停滞请求结束后网关仍能正常服务（新请求 200）', rAfter.status === 200, `status=${rAfter.status}`);

      const rec = (await recentTasks()).find((t) => t.model === 'flood-model');
      ok('停滞的请求没有被记成渠道成功（clientAbort=true / ok!=true）',
        !!rec && rec.clientAbort === true && rec.ok !== true,
        rec ? `ok=${rec.ok} clientAbort=${rec.clientAbort} kind=${rec.kind}` : '没有找到任务记录');
    }

    // ---------- §2.3 出站代理路径 ----------
    console.log('\n[§2.3] 出站代理（channel.proxy -> lib/outbound-proxy.mjs）路径的看门狗');
    {
      const t0 = Date.now();
      const r = await chat({ model: 'proxy-ho-model', messages: MSG });
      const ms = Date.now() - t0;
      ok('经本机代理转发的"只给响应头"上游：同样在超时内 503（不是挂死）',
        r.status === 503 && ms < 2500, `status=${r.status} ${ms}ms`);
      ok('经代理路径的错误文案同样是"首字节超时"', /首字节超时/.test(r.text), JSON.stringify(r.text.slice(0, 180)));
    }

    // ---------- §2.5 三段耗时（成功路径也要有）----------
    console.log('\n[§2.5] 三段耗时采集：成功请求的 ttfbMs / bodyMs / queueMs');
    {
      const recs = (await recentTasks()).filter((t) => t.model === 'slowfirst-model');
      const rec = recs.sort((a, b) => a.seq - b.seq).pop();
      ok('成功请求的记录里有 ttfbMs / bodyMs / queueMs 三个键（不是只有换家失败才有）',
        !!rec && rec.ok === true && typeof rec.ttfbMs === 'number' && typeof rec.bodyMs === 'number'
          && typeof rec.queueMs === 'number',
        rec ? `ok=${rec.ok} queueMs=${rec.queueMs} ttfbMs=${rec.ttfbMs} bodyMs=${rec.bodyMs} seq=${rec.seq}` : '没有找到记录');
      ok('ttfbMs >= 250（mock 首字节延迟 300ms）', (rec?.ttfbMs ?? -1) >= 250, `ttfbMs=${rec?.ttfbMs}`);
      ok('bodyMs >= 200（正文分 3 块各 100ms）', (rec?.bodyMs ?? -1) >= 200, `bodyMs=${rec?.bodyMs}`);
      ok('请求级还有一份 timings 嵌套镜像，与平铺键一致',
        !!rec?.timings && rec.timings.ttfbMs === rec.ttfbMs && rec.timings.bodyMs === rec.bodyMs,
        JSON.stringify(rec?.timings ?? null));

      // 渠道级排队：并发 2 个 -> 第二个请求 queueMs > 0
      const [qa, qb] = await Promise.all([
        chat({ model: 'queue-model', messages: MSG }),
        chat({ model: 'queue-model', messages: MSG }),
      ]);
      ok('并发 2 个请求都成功（maxConcurrentPerChannel=1 时第二个靠排队拿到许可）',
        qa.status === 200 && qb.status === 200, `a=${qa.status} b=${qb.status}`);
      const qrecs = (await recentTasks()).filter((t) => t.model === 'queue-model').sort((a, b) => a.seq - b.seq);
      const qLast = qrecs[qrecs.length - 1];
      const qMax = Math.max(...qrecs.map((r) => r.queueMs ?? -1));
      ok('第二个并发请求 queueMs > 0（排队时长被真实采集）',
        qrecs.length === 2 && (qLast?.queueMs ?? 0) > 0 && qMax > 0,
        `records=${qrecs.length} queueMs=[${qrecs.map((r) => r.queueMs).join(',')}]`);
      ok('第一个请求的耗时键同样存在（立即拿到许可 = 真实 0ms，不是缺键）',
        typeof qrecs[0]?.queueMs === 'number', `queueMs=${qrecs[0]?.queueMs}`);
    }

    // ---------- §2.4 排队时长统计（单元）----------
    console.log('\n[§2.4] 排队时长统计（Semaphore / GatewayLimiter）');
    {
      const lim = new GatewayLimiter({ maxConcurrent: 1, maxConcurrentPerChannel: 1, queueTimeoutMs: 5000 });
      const r1 = await lim.acquire('ch');
      const p2 = lim.acquire('ch');
      await wait(120);
      r1();
      const r2 = await p2;
      const st = lim.stats();
      ok('GatewayLimiter.stats() 暴露 queueWaitMsTotal/queueWaitMsMax/waitedCount',
        typeof st.queueWaitMsTotal === 'number' && typeof st.queueWaitMsMax === 'number'
          && typeof st.waitedCount === 'number',
        `total=${st.queueWaitMsTotal} max=${st.queueWaitMsMax} count=${st.waitedCount}`);
      ok('等待被真实结算（waitedCount=1，total/max >= 100ms）',
        st.waitedCount === 1 && st.queueWaitMsTotal >= 100 && st.queueWaitMsMax >= 100,
        `total=${st.queueWaitMsTotal} max=${st.queueWaitMsMax} count=${st.waitedCount}`);
      ok('不变式：queueWaitMsMax >= queueWaitMsTotal / waitedCount',
        st.queueWaitMsMax >= st.queueWaitMsTotal / st.waitedCount,
        `${st.queueWaitMsMax} >= ${st.queueWaitMsTotal}/${st.waitedCount}`);
      ok('立即拿到许可的申请不计入 waitedCount（不被 0ms 稀释）',
        lim.stats().global.waitedCount === st.waitedCount, `global.waitedCount=${lim.stats().global.waitedCount}`);
      // 既有字段一个都不能改名/删除（P3 与面板在消费）
      const legacyLimiter = ['maxConcurrent', 'maxConcurrentPerChannel', 'maxConcurrentPerAgent', 'queueTimeoutMs',
        'overloads', 'agentRejections', 'global', 'channels', 'agents', 'agentCount', 'activeAgentCount'];
      ok('GatewayLimiter.stats() 既有字段全部保留',
        legacyLimiter.every((k) => k in st), legacyLimiter.filter((k) => !(k in st)).join(',') || 'all present');
      const legacySem = ['limit', 'active', 'pending', 'available', 'acquired', 'queued', 'timedOut', 'maxQueue'];
      ok('Semaphore.stats() 既有字段全部保留',
        legacySem.every((k) => k in st.global), legacySem.filter((k) => !(k in st.global)).join(',') || 'all present');
      r2();

      // 超时的等待者不结算（没拿到许可，时长没有"排队代价"的含义）
      const lim2 = new GatewayLimiter({ maxConcurrent: 1, maxConcurrentPerChannel: 0, queueTimeoutMs: 60 });
      const hold = await lim2.acquire('ch');
      let timedOut = false;
      try { await lim2.acquire('ch'); } catch (e) { timedOut = e?.code === 'CONCURRENCY_TIMEOUT'; }
      const st2 = lim2.stats();
      ok('排队超时的等待者不计入排队时长（也没丢掉 overloads 统计）',
        timedOut && st2.waitedCount === 0 && st2.queueWaitMsTotal === 0 && st2.queueWaitMsMax === 0 && st2.overloads === 1,
        `timedOut=${timedOut} waitedCount=${st2.waitedCount} total=${st2.queueWaitMsTotal} overloads=${st2.overloads}`);
      hold();
      const sem = new Semaphore(1, 'unit');
      const s1 = await sem.acquire(0);
      const sp = sem.acquire(0);
      await wait(80);
      s1();
      const s2 = await sp;
      ok('Semaphore 自身也记录 waitMsTotal/waitMsMax/waitedCount',
        sem.stats().waitedCount === 1 && sem.stats().waitMsMax >= 60,
        `count=${sem.stats().waitedCount} max=${sem.stats().waitMsMax} total=${sem.stats().waitMsTotal}`);
      s2();
    }

    // ---------- 轮换游标：requestId 接线 ----------
    console.log('\n[游标] 同一请求多次选路，首选渠道保持一致（requestId 已接线）');
    {
      const mgr = new ChannelManager(TMP_CFG).load();
      const firstOf = (rid) => [0, 1, 2].map(() => mgr.candidatesFor('cursor-model', { requestId: rid })[0]?.name);
      const same = firstOf('unit-req-1');
      const next = firstOf('unit-req-2');
      ok('同一 requestId 连续 3 次选路 -> 首选渠道完全一致（游标只前进一格）',
        same[0] && same[0] === same[1] && same[1] === same[2], same.join(','));
      ok('换一个 requestId -> 游标前进一档（首选渠道变化）',
        next[0] && next[0] !== same[0], `${same[0]} -> ${next[0]}`);

      cursorHits.length = 0;
      const t0 = Date.now();
      const r = await chat({ model: 'cursor-model', messages: MSG });
      const ms = Date.now() - t0;
      const hits = cursorHits.join(',');
      // 优先池 a,b,c 全部失败 -> 就地注入 fallback 池 x,y,z（同轮内第 2 次选路）
      // -> 轮间重试第 2 轮再选一次（同请求第 3 次选路）。
      // 三次选路都带同一个 requestId，所以顺序必须一直是 a,b,c,x,y,z。
      ok('端到端：一次请求内多轮 + 降级注入的选路顺序恒定（游标没有多跳）',
        hits.startsWith('a,b,c,x,y,z,a,b,c'), `hits=${hits} (${ms}ms, status=${r.status})`);
      ok('全部候选失败时仍返回 503（没有因为接线改动卡住）', r.status === 503, `status=${r.status}`);
    }

    // ---------- 网关日志证据 ----------
    const evidence = gwLog.split('\n').filter((l) =>
      /停止读取响应|首字节超时|排队超时|写回背压/.test(l));
    if (evidence.length) {
      console.log('\n网关日志证据：');
      for (const line of evidence.slice(0, 14)) console.log(`  | ${line.trim()}`);
    }

    return fail ? 1 : 0;
  } finally {
    try { sock?.destroy(); } catch { /* ignore */ }
    try { gw.kill(); } catch { /* ignore */ }
  }
}

let code = 1;
try {
  code = await main();
} catch (err) {
  console.log(`\n测试异常: ${err?.stack || err}`);
  code = 1;
} finally {
  for (const s of servers) { try { s.close(); } catch { /* ignore */ } }
  try { rmSync(TMP_DIR, { recursive: true, force: true }); } catch { /* ignore */ }
  console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
}
process.exit(code);
