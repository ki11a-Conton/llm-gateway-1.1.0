/**
 * W4 加固回归（任务书 .fixspec/W4-proxy.md 的九个 bug）。
 *
 * 覆盖点：
 *   bug1 排队期间客户端断开 -> 并发许可必须归还（泄漏会让网关在攒满 maxConcurrent 后假死）
 *   bug2 上游只回响应头不吐数据 -> 流式/非流式都必须在空闲超时内自行结束（绝不永久挂住）
 *   bug3 连续 client_abort / overloaded 不计入熔断（用户两次 Ctrl-C 不该熔断健康渠道）
 *   bug4 优先池用尽时就地续跑同一轮，不重启整轮重复捶已打满预算 / 已判死的渠道
 *   bug5 零帧 200 流、Anthropic 形态空响应、200+JSON 错误体 判为不可用并换家
 *   bug6 余额不足 / 鉴权失败的渠道在整个请求内判死，跨轮次不再命中
 *   bug7 背压等待（waitDrain）在客户端已断开的 res 上不再永久等待
 *   bug8 全局排队超时计入 overloads 统计
 *   bug9 流式中途断开的请求不记为渠道成功（不污染成功率与粘性路由）
 *
 * 跑法：node test/test-proxy-hardening.mjs
 */
import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { existsSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { findUnusableResponse, findUnusableStreamHead } from '../lib/proxy.mjs';
import { GatewayLimiter } from '../lib/concurrency.mjs';
import { freePort, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..');
const NODE = process.execPath;
const LOG_DIR = path.join(ROOT, 'logs-test');

// P5：端口全部改成运行时动态分配（原先写死 8813 + 9170-9184，并行跑时互相抢）。
// 下面这些 91xx 数字现在只是**逻辑标识**：真正监听在 ownPort[逻辑端口] 上，
// 配置里的地址也由 materializeConfig 按同一张表改写，两边始终一致。
const PORT = await freePort();
const BASE = `http://127.0.0.1:${PORT}`;
const OWN_PORTS = [9170, 9171, 9172, 9173, 9174, 9175, 9176, 9177, 9178, 9179, 9182, 9183, 9184];
const ownPort = {};
for (const p of OWN_PORTS) ownPort[p] = await freePort();
const CFG = materializeConfig(path.join(HERE, 'proxy-hardening.test.json'), { port: PORT, portMap: ownPort });
const AUTH = { authorization: 'Bearer TESTKEY' };

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

async function waitFor(fn, ms = 2000, step = 40) {
  const deadline = Date.now() + ms;
  for (;;) {
    let v = false;
    try { v = await fn(); } catch { v = false; }
    if (v) return true;
    if (Date.now() > deadline) return false;
    await wait(step);
  }
}

// ---- 上游命中计数（按 mock 名字）----
const COUNTER = new Map();
const bump = (name) => COUNTER.set(name, (COUNTER.get(name) || 0) + 1);
const hitsOf = (name) => COUNTER.get(name) || 0;
const resetHits = () => COUNTER.clear();

// ---- HTTP 小工具 ----
async function call(pathname, { method = 'GET', body, headers = {}, signal } = {}) {
  const r = await fetch(`${BASE}${pathname}`, {
    method,
    headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...AUTH, ...headers },
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  const text = await r.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* 非 JSON 响应（SSE / 纯文本） */ }
  return { status: r.status, text, json, headers: r.headers };
}

const chat = (body, extra = {}) => call('/v1/chat/completions', { method: 'POST', body, ...extra });
const messages = (body, extra = {}) => call('/v1/messages', { method: 'POST', body, ...extra });
const health = async () => (await call('/health')).json;
const recentTasks = async () => ((await call('/api/tasks?limit=80')).json?.recent) || [];
const channelState = async (name) => (((await call('/api/status')).json?.channels) || []).find((c) => c.name === name);

// ---- mock 上游 ----
const MSG = [{ role: 'user', content: '你好' }];
const GOOD_TEXT = '兜底渠道的健康回答：这是完整的正文内容，客户端能正常收到。';
const SILENCE_TEXT = '静默前的一段正文'.repeat(9); // 90 字，超过流式头部放行阈值 64

const sendJson = (res, code, obj) => {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body) });
  res.end(body);
};
const sendModels = (res, id) => sendJson(res, 200, { object: 'list', data: [{ id, object: 'model' }] });
const openaiBody = (model, text) => ({
  id: 'chatcmpl-t', object: 'chat.completion', created: 1, model,
  choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
  usage: { prompt_tokens: 3, completion_tokens: 5, total_tokens: 8 },
});
const chunk = (model, delta, finish = null) => `data: ${JSON.stringify({
  id: 'chatcmpl-s', object: 'chat.completion.chunk', created: 1, model,
  choices: [{ index: 0, delta, finish_reason: finish }],
})}\n\n`;
function streamOk(res, model, text = GOOD_TEXT) {
  res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' });
  res.write(chunk(model, { role: 'assistant', content: text }));
  res.write(chunk(model, {}, 'stop'));
  res.end('data: [DONE]\n\n');
}
const readBody = (req) => new Promise((resolve) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => { try { resolve(JSON.parse(raw || '{}')); } catch { resolve({}); } });
  req.on('error', () => resolve({}));
});

const servers = [];
function startMock(port, name, handler) {
  const server = http.createServer((req, res) => {
    res.on('error', () => { /* 客户端断开后写入会 EPIPE，忽略 */ });
    req.on('error', () => { /* 同上 */ });
    const url = new URL(req.url, 'http://127.0.0.1');
    bump(name);
    Promise.resolve(handler(req, res, url)).catch(() => { try { res.destroy(); } catch { /* ignore */ } });
  });
  server.on('clientError', (_err, socket) => { try { socket.destroy(); } catch { /* ignore */ } });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    // 逻辑端口 -> 动态实际端口（未登记的端口保持原样，方便以后加 mock）
    server.listen(ownPort[port] ?? port, '127.0.0.1', () => { servers.push(server); resolve(server); });
  });
}

async function startMocks() {
  // 9170 健康上游：非流式 JSON + 流式 SSE（rp-2 / rp-3 也指向它）
  await startMock(9170, 'ok-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'ok-model');
    const body = await readBody(req);
    if (body.stream === true) return streamOk(res, body.model || 'ok-model');
    return sendJson(res, 200, openaiBody(body.model || 'ok-model', GOOD_TEXT));
  });

  // 9171 慢上游（500ms 后成功）：bug1 用它占住全局槽位，bug3 用它验证中途断开
  await startMock(9171, 'slow', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'slow-model');
    const body = await readBody(req);
    await wait(500);
    if (res.writableEnded || res.destroyed) return undefined;
    return sendJson(res, 200, openaiBody(body.model || 'slow-model', '慢渠道回答'));
  });

  // 9183 更慢（2000ms 后成功）：bug3b 用它在全局闸门上压出排队超时
  await startMock(9183, 'overload', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'overload-model');
    const body = await readBody(req);
    await wait(2000);
    if (res.writableEnded || res.destroyed) return undefined;
    return sendJson(res, 200, openaiBody(body.model || 'overload-model', '压测渠道回答'));
  });

  // 9172 只回响应头、永不写 body（非流式方向）
  await startMock(9172, 'headers-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'headers-model');
    await readBody(req);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.flushHeaders();
    return undefined;
  });

  // 9173 只回响应头、永不写 body（流式方向）
  await startMock(9173, 'headers-stream-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'headers-stream-model');
    await readBody(req);
    res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
    res.flushHeaders();
    return undefined;
  });

  // 9174 零帧 200 流：响应头 + 立刻 end，一个数据帧都没有
  await startMock(9174, 'empty-stream-1', async (req, res) => {
    await readBody(req);
    res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
    res.end();
  });

  // 9175 200 但正文不是 SSE（非流式 JSON 错误体）
  await startMock(9175, 'non-sse-1', async (req, res) => {
    await readBody(req);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: { message: 'non-sse boom', type: 'server_error' } }));
  });

  // 9176 原生 Anthropic：200 + content:[]（OpenAI 形状里根本没有 choices）
  await startMock(9176, 'anth-empty-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'anth-empty-model');
    const body = await readBody(req);
    return sendJson(res, 200, {
      id: 'msg_1', type: 'message', role: 'assistant', model: body.model || 'anth-empty-model',
      content: [], stop_reason: 'end_turn', stop_sequence: null,
      usage: { input_tokens: 5, output_tokens: 1 },
    });
  });

  // 9177 余额不足：换家无意义，整个请求内判死
  await startMock(9177, 'dead-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'dead-model');
    await readBody(req);
    return sendJson(res, 400, {
      error: { message: 'Insufficient balance', type: 'insufficient_quota', code: 'insufficient_quota' },
    });
  });

  // 9178 流式：先发一帧 90 字正文，然后永久静默（验证中途静默也会被空闲看门狗收掉）
  await startMock(9178, 'midsilence-1', async (req, res) => {
    const body = await readBody(req);
    res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
    res.write(chunk(body.model || 'midsilence-model', { role: 'assistant', content: SILENCE_TEXT }));
    return undefined; // 不写、不 end
  });

  // 9179 rp-1：400 模型不存在（bug4 的"已打满预算的优先渠道"）。
  // 故意用"模型维度错误"而不是 500：markUnsupported 后 rp-1 会被从**候选池**里剔除，
  // 于是"注入候选"分支的 rest 必然非空——这样本用例不依赖 lib/channels.mjs 的候选排序策略
  // （冷却垫后 / 严格优先级 / 轮询），只依赖 proxy.mjs 自己的轮次语义。
  await startMock(9179, 'rp-1', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'round-model');
    await readBody(req);
    return sendJson(res, 400, {
      error: {
        message: 'The model `round-model` does not exist or you do not have access to it.',
        type: 'invalid_request_error',
        code: 'model_not_found',
      },
    });
  });
  await startMock(9182, 'rf-fail', async (req, res, url) => {
    if (url.pathname === '/v1/models') return sendModels(res, 'round-model');
    await readBody(req);
    return sendJson(res, 500, { error: { message: 'rf-fail boom' } });
  });

  // 9184 持续狂灌：让网关写回客户端时进入背压等待（bug7）
  await startMock(9184, 'flood-1', async (req, res) => {
    const body = await readBody(req);
    res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
    const frame = chunk(body.model || 'flood-model', { content: 'x'.repeat(4096) });
    let stopped = false;
    const stop = () => { stopped = true; clearInterval(timer); };
    const timer = setInterval(() => {
      if (stopped || res.writableEnded || res.destroyed) return stop();
      for (let i = 0; i < 16; i += 1) {
        if (res.write(frame) === false) break; // 上游侧背压：本 tick 先不发
      }
      return undefined;
    }, 2);
    res.on('close', stop);
    req.on('error', stop);
  });
}

// ---- 网关 ----
let gwLog = '';
function startGateway() {
  const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'info'], {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
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
  if (existsSync(LOG_DIR)) rmSync(LOG_DIR, { recursive: true, force: true });

  await startMocks();
  const gw = startGateway();

  // ---------- 单元：bug5 空响应形态 ----------
  console.log('\n[bug5] 空响应 / 不可用响应判定（单元）');
  const streamCases = [
    ['零字节 200 流', '', 'empty_stream'],
    ['只有 [DONE] 的 200 流', 'data: [DONE]\n\n', 'empty_stream'],
    ['Anthropic 收尾但零正文', 'event: message_start\ndata: {"type":"message_start"}\n\nevent: message_stop\ndata: {"type":"message_stop"}\n\n', 'empty_stream'],
    ['200 + 非 SSE JSON 错误体', '{"error":{"message":"boom"}}', 'non_sse_body'],
  ];
  for (const [name, raw, want] of streamCases) {
    const got = findUnusableStreamHead(raw)?.reason ?? null;
    ok(`流式：${name} -> ${want}`, got === want, `got=${got}`);
  }
  ok('流式：有正文的正常流不误伤', findUnusableStreamHead(chunk('m', { content: '你好' }, 'stop')) === null);
  ok('流式：正文未开始（无收尾信号）不误伤', findUnusableStreamHead(chunk('m', { reasoning_content: '思考中' })) === null);

  const respCases = [
    ['原生 Anthropic 空 content', { content: [], stop_reason: 'end_turn', usage: { output_tokens: 1 } }, 'empty_completion'],
    ['OpenAI->Anthropic 转换后的空 text 块', { content: [{ type: 'text', text: '' }], stop_reason: 'end_turn' }, 'empty_completion'],
    ['OpenAI choices 为空数组', { choices: [] }, 'empty_completion'],
    ['OpenAI 正文为空', { choices: [{ message: { content: '' }, finish_reason: 'stop' }] }, 'empty_completion'],
    ['200 却回 JSON 错误体', { error: { message: 'quota exceeded' } }, 'error_body'],
  ];
  for (const [name, obj, want] of respCases) {
    const got = findUnusableResponse(obj)?.reason ?? null;
    ok(`非流式：${name} -> ${want}`, got === want, `got=${got}`);
  }
  ok('非流式：正常 Anthropic 正文不误伤', findUnusableResponse({ content: [{ type: 'text', text: '你好' }], stop_reason: 'end_turn' }) === null);
  ok('非流式：tool_use 不误伤', findUnusableResponse({ content: [{ type: 'tool_use', id: 't1', name: 'f', input: {} }], stop_reason: 'tool_use' }) === null);
  ok('非流式：只有 thinking 不误伤', findUnusableResponse({ content: [{ type: 'thinking', thinking: '先想一下' }], stop_reason: 'end_turn' }) === null);
  ok('非流式：正常 OpenAI 正文不误伤', findUnusableResponse({ choices: [{ message: { content: '你好' }, finish_reason: 'stop' }] }) === null);
  ok('非流式：max_tokens 截断（有 token）不误伤', findUnusableResponse({ content: [], stop_reason: 'max_tokens', usage: { output_tokens: 12 } }) === null);

  // ---------- 单元：bug8 全局过载统计 ----------
  console.log('\n[bug8] 过载统计（单元）');
  {
    const lim = new GatewayLimiter({ maxConcurrent: 1, maxConcurrentPerChannel: 0, queueTimeoutMs: 60 });
    const rel = await lim.acquire('ch');
    let err = null;
    try { await lim.acquire('ch'); } catch (e) { err = e; }
    ok('全局排队超时计入 overloads', err?.code === 'CONCURRENCY_TIMEOUT' && lim.stats().overloads === 1,
      `code=${err?.code} overloads=${lim.stats().overloads}`);
    rel();
  }
  {
    const lim = new GatewayLimiter({ maxConcurrent: 4, maxConcurrentPerChannel: 1, queueTimeoutMs: 60 });
    const rel = await lim.acquire('ch');
    let err = null;
    try { await lim.acquire('ch'); } catch (e) { err = e; }
    ok('渠道级排队超时同样计入 overloads（无回归）', err?.code === 'CONCURRENCY_TIMEOUT' && lim.stats().overloads === 1,
      `code=${err?.code} overloads=${lim.stats().overloads}`);
    rel();
  }

  const ready = await waitReady();
  if (!ready) {
    console.log('\n网关未能就绪，终止');
    console.log(gwLog.slice(-2000));
    return 1;
  }

  // ---------- bug1：排队期间客户端断开 -> 许可归还 ----------
  console.log('\n[bug1] 排队期间客户端断开：并发许可必须归还');
  resetHits();
  {
    const pA = chat({ model: 'slow-model', messages: MSG });
    const inFlightSeen = await waitFor(async () => ((await health()).inFlight ?? 0) >= 1, 3000, 30);
    const acB = new AbortController();
    const pB = chat({ model: 'slow-model', messages: MSG }, { signal: acB.signal }).catch(() => null);
    const queuedSeen = await waitFor(async () => ((await health()).queued ?? 0) >= 1, 2000, 20);
    await wait(120);
    acB.abort();          // 客户端在排队期间断开
    const rA = await pA;
    await pB;
    const released = await waitFor(async () => ((await health()).inFlight ?? 1) === 0, 3000, 40);
    const before = (await health()).inFlight;
    const rC = await chat({ model: 'slow-model', messages: MSG });
    ok('前置：第一个请求在途、第二个请求确实排在队里', inFlightSeen && queuedSeen, `inFlight=${inFlightSeen} queued=${queuedSeen}`);
    ok('A（未断开）正常完成', rA.status === 200, `status=${rA.status}`);
    ok('断开后许可归还（global.active 回到 0）', released, `inFlight=${before}`);
    ok('后续请求仍能拿到槽位（未被 CONCURRENCY_TIMEOUT 挡住）', rC.status === 200, `status=${rC.status} ${rC.text.slice(0, 120)}`);
  }

  // ---------- bug2：只给响应头不给数据 ----------
  console.log('\n[bug2] 上游只回响应头、不吐数据：必须在超时内自行结束');
  {
    const t0 = Date.now();
    const r = await chat({ model: 'headers-model', messages: MSG });
    const ms = Date.now() - t0;
    ok('非流式：超时内结束并报错（不是永久挂住）', r.status === 503 && ms < 4000, `status=${r.status} ${ms}ms`);
    ok('非流式：错误里能看到"响应体读取超时"', /读取超时/.test(r.text), r.text.slice(0, 160));
  }
  {
    const t0 = Date.now();
    const r = await chat({ model: 'headers-stream-model', messages: MSG, stream: true });
    const ms = Date.now() - t0;
    ok('流式：超时内结束并报错（不是永久挂住）', r.status === 503 && ms < 4000, `status=${r.status} ${ms}ms`);
    ok('流式：错误里能看到"静默超时（响应头未发出，可换家）"', /静默超时/.test(r.text), r.text.slice(0, 160));
  }
  {
    const t0 = Date.now();
    const r = await chat({ model: 'midsilence-model', messages: MSG, stream: true });
    const ms = Date.now() - t0;
    ok('流式：发了一帧后静默，客户端在超时内收到收尾（不挂住）', r.status === 200 && ms < 4000 && r.text.includes(SILENCE_TEXT.slice(0, 20)),
      `status=${r.status} ${ms}ms bytes=${r.text.length}`);
  }

  // ---------- bug3a：连续 client_abort 不熔断 ----------
  console.log('\n[bug3] client_abort / overloaded 不计入熔断');
  resetHits();
  {
    for (let i = 0; i < 2; i += 1) {
      const ac = new AbortController();
      const p = chat({ model: 'abort-model', messages: MSG }, { signal: ac.signal }).catch(() => null);
      await waitFor(async () => ((await health()).inFlight ?? 0) >= 1, 3000, 30);
      await wait(120);
      ac.abort();
      await p;
      await waitFor(async () => ((await health()).inFlight ?? 1) === 0, 3000, 40);
    }
    const ch = await channelState('abort-1');
    ok('连续 2 次 client_abort 后渠道 failures 仍为 0', (ch?.failures ?? -1) === 0, `failures=${ch?.failures}`);
    ok('连续 2 次 client_abort 后渠道未熔断', ch?.coolingDown === false, `coolingDown=${ch?.coolingDown}`);
  }

  // ---------- bug3b + bug8：排队超时（overloaded）不熔断，且计入 overloads ----------
  {
    const beforeOverloads = ((await call('/api/metrics')).json?.concurrency?.overloads) ?? 0;
    const pA = chat({ model: 'overload-model', messages: MSG });
    await waitFor(async () => ((await health()).inFlight ?? 0) >= 1, 3000, 30);
    const rB = await chat({ model: 'overload-model', messages: MSG });   // 全局只有 1 个名额 -> 排队超时
    await pA;
    const afterOverloads = ((await call('/api/metrics')).json?.concurrency?.overloads) ?? 0;
    const ch = await channelState('overload-1');
    ok('overloads 统计包含了全局排队超时', afterOverloads > beforeOverloads, `before=${beforeOverloads} after=${afterOverloads}`);
    ok('排队超时（overloaded）后渠道 failures 仍为 0', (ch?.failures ?? -1) === 0, `failures=${ch?.failures} B.status=${rB.status}`);
    ok('排队超时（overloaded）后渠道未熔断', ch?.coolingDown === false, `coolingDown=${ch?.coolingDown}`);
  }

  // ---------- bug4：优先池用尽就地续跑同一轮 ----------
  // rp-1 用 400 模型不存在：它被 markUnsupported 后会从候选池里消失，于是"注入候选"必然发生，
  // 且不受 lib/channels.mjs 候选排序策略的影响（见 9179 mock 的注释）。
  console.log('\n[bug4] 优先池用尽：就地续跑同一轮，不重启整轮');
  resetHits();
  {
    const r = await chat({ model: 'round-model', messages: MSG });
    const ch = r.headers.get('x-gateway-channel');
    ok('注入的候选在本轮内被尝试并成功换家', r.status === 200 && ch === 'rp-2', `status=${r.status} channel=${ch}`);
    ok('本轮已失败的优先渠道没有被重新捶一遍', hitsOf('rp-1') === 1, `rp-1 hits=${hitsOf('rp-1')}`);
    ok('本轮已失败的随机池渠道没有被重新捶一遍', hitsOf('rf-fail') === 1, `rf-fail hits=${hitsOf('rf-fail')}`);
    ok('未被注入的候选没有被尝试（顺序未被整轮重启打乱）', hitsOf('rp-3') === 0, `rp-3 hits=${hitsOf('rp-3')}`);  }

  // ---------- bug5：真实链路上的换家 ----------
  console.log('\n[bug5] 不可用响应必须换家（真实链路）');
  {
    const r = await chat({ model: 'empty-stream-model', messages: MSG, stream: true });
    ok('零帧 200 流被判不可用并换家到健康渠道', r.status === 200 && r.headers.get('x-gateway-channel') === 'ok-1',
      `status=${r.status} channel=${r.headers.get('x-gateway-channel')}`);
    ok('客户端拿到的是健康渠道的正文', r.text.includes('健康回答'), `bytes=${r.text.length}`);
  }
  {
    const r = await chat({ model: 'non-sse-model', messages: MSG, stream: true });
    ok('200 + 非 SSE JSON 错误体被判不可用并换家', r.status === 200 && r.headers.get('x-gateway-channel') === 'ok-1',
      `status=${r.status} channel=${r.headers.get('x-gateway-channel')}`);
  }
  {
    const r = await messages({ model: 'anth-empty-model', max_tokens: 64, messages: MSG });
    const text = JSON.stringify(r.json?.content ?? '');
    ok('原生 Anthropic 空 content 被判不可用并换家', r.status === 200 && r.headers.get('x-gateway-channel') === 'ok-1',
      `status=${r.status} channel=${r.headers.get('x-gateway-channel')} body=${r.text.slice(0, 120)}`);
    ok('Anthropic 客户端拿到非空正文', /健康回答/.test(text), text.slice(0, 120));
  }

  // ---------- bug6：判死渠道跨轮次不再命中 ----------
  console.log('\n[bug6] 余额不足判死的渠道跨轮次不再命中');
  resetHits();
  {
    const t0 = Date.now();
    const r = await chat({ model: 'dead-model', messages: MSG });
    const ms = Date.now() - t0;
    ok('判死渠道全程只被打到 1 次（mock 计数）', hitsOf('dead-1') === 1, `hits=${hitsOf('dead-1')}`);
    ok('立刻回明确错误而不是空转到总预算', r.status === 503 && ms < 2500, `status=${r.status} ${ms}ms`);
    ok('错误里保留了失败原因（余额/鉴权）', /余额|鉴权|balance|auth/i.test(r.text), r.text.slice(0, 200));
  }

  // ---------- bug7 + bug9：背压等待与"中途断开不算成功" ----------
  // 这里用**裸 socket** 当客户端、并且全程一个字节都不读：内核接收缓冲填满后，
  // 网关必然停在"写回受阻 -> await waitDrain"。这样"断开那一刻网关正卡在 waitDrain 里"
  // 是确定性的，bug7（waitDrain 永不 resolve）与 bug9（正常返回被记成渠道成功）才能被稳定判定——
  // 用 fetch 的话客户端会自动读掉一部分 body，网关多半还在读上游，断开后走的是"上游迭代抛错"那条路，
  // 会把这两个 bug 掩盖掉。
  console.log('\n[bug7/bug9] 客户端中途断开：背压等待不挂死、且不记为渠道成功');
  resetHits();
  {
    const sock = net.connect(PORT, '127.0.0.1');
    await new Promise((resolve) => sock.once('connect', resolve));
    const reqBody = JSON.stringify({ model: 'flood-model', messages: MSG, stream: true });
    sock.write(
      `POST /v1/chat/completions HTTP/1.1\r\n`
      + `Host: 127.0.0.1:${PORT}\r\n`
      + 'content-type: application/json\r\n'
      + 'authorization: Bearer TESTKEY\r\n'
      + `content-length: ${Buffer.byteLength(reqBody)}\r\n\r\n${reqBody}`,
    );
    const flooding = await waitFor(async () => ((await health()).inFlight ?? 0) >= 1, 4000, 30);
    await wait(900);   // 全程不读：把网关顶进"写回受阻 -> waitDrain"
    sock.destroy();    // 客户端断开（服务端收到 close -> res.destroyed）
    const released = await waitFor(async () => ((await health()).inFlight ?? 1) === 0, 6000, 60);
    ok('断开后请求停止（许可归还，没有永久卡在 waitDrain）', flooding && released, `flooding=${flooding} released=${released}`);
    const rAfter = await chat({ model: 'ok-model', messages: MSG });
    ok('断开后网关仍能正常服务', rAfter.status === 200, `status=${rAfter.status}`);

    await wait(300);
    const rec = (await recentTasks()).find((t) => t.model === 'flood-model');
    ok('中途断开的中继流没有被记为渠道成功', !!rec && rec.clientAbort === true && rec.ok !== true,
      rec ? `ok=${rec.ok} clientAbort=${rec.clientAbort} kind=${rec.kind}` : '没有找到任务记录');
  }

  // ---------- 收尾：打印网关侧的加固日志证据 ----------
  const evidence = gwLog.split('\n').filter((l) => /降级到其余|判死|静默超时|读取超时|排队期间断开|中途断开|排队超时|不可用内容/.test(l));
  if (evidence.length) {
    console.log('\n网关日志证据：');
    for (const line of evidence.slice(0, 20)) console.log(`  | ${line.trim()}`);
  }

  gw.kill();
  return fail ? 1 : 0;
}

let code = 1;
try {
  code = await main();
} catch (err) {
  console.log(`\n测试异常: ${err?.stack || err}`);
  code = 1;
} finally {
  for (const s of servers) { try { s.close(); } catch { /* ignore */ } }
  console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
}
process.exit(code);
