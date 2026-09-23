// F6 回归：一个 agent 的排队请求不得预占全局/渠道名额（否则饿死其他 agent）
//
// 背景（代码审查 2026-09-20 F6/P1）：许可申请顺序是 global -> channel -> agent。
// 当某个 agent 自己的额度已满时，它后续的请求**已经先拿到 global（可能还有 channel）**才去等
// agent 名额。此时真正在跑的只有 1 个上游请求，但 global.active 已经是 2，
// 别的 agent 哪怕用一个空闲渠道也进不来（报告复现 F6_agent_starvation）。
//
// 修复：先把"每代理自己的"名额拿到手，再去抢全局/渠道 —— 积压请求在拿到自己名额前
// 不占用任何共享容量，"等待请求不计为执行中的全局/渠道负载"。
//
// 本套件断言：A 的积压请求排队时 global.active 不增加；B 用另一个空闲渠道能正常跑；
//            总上游并发不超过全局上限；全部完成后计数归零。
import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const M_A = 'f6-model-a'; // 走渠道 ch-a
const M_B = 'f6-model-b'; // 走渠道 ch-b（空闲渠道）

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'F6KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

// 上游：把流式响应挂住，直到测试调用 /release —— 这样才能精确观察并发与排队
let inFlight = 0;
let peakInFlight = 0;
const held = [];
const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => {
    if (req.url?.includes('/release')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ released: held.length }));
      for (const h of held.splice(0)) {
        h.res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: h.model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
        h.res.write('data: [DONE]\n\n');
        h.res.end();
        inFlight -= 1;
      }
      return;
    }
    let body = {};
    try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }
    const model = body.model || '';
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: model, object: 'model' }] }));
    }
    inFlight += 1;
    if (inFlight > peakInFlight) peakInFlight = inFlight;
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    // 首帧给足 64+ 字符：让网关的"头部守卫"放行并开始下发（否则响应头会一直被缓冲，
    // 客户端看不到任何东西，测试就观察不到"在途"状态）。之后挂住不结束。
    res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: { role: 'assistant', content: 'x'.repeat(80) } }] })}\n\n`);
    held.push({ res, model }); // 挂住，等 /release
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f6-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'f6.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
  taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200 },
  routing: {
    strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
    failThreshold: 99, cooldownMs: 1000, probeIntervalMs: 0, discoverIntervalMs: 0,
    timeoutMs: 20000, providerTimeoutMs: 20000, streamIdleTimeoutMs: 20000,
    // 报告的场景：全局 2 / 渠道 2 / 单代理 1
    maxConcurrent: 2, maxConcurrentPerChannel: 2, maxConcurrentPerAgent: 1,
    queueTimeoutMs: 4000,
  },
  channels: [
    { name: 'ch-a', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_A, priority: 10 },
    { name: 'ch-b', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_B, priority: 10 },
  ],
}, null, 2) + '\n', 'utf8');

let gw = null;
const chatRaw = (model, agent) => fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
  method: 'POST',
  headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json', 'x-agent-id': agent },
  body: JSON.stringify({ model, stream: true, messages: [{ role: 'user', content: 'hi' }] }),
});
const metrics = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/metrics`, { headers: { authorization: `Bearer ${API_KEY}` } });
  const j = await r.json().catch(() => null);
  return {
    global: j?.concurrency?.global?.active ?? null,
    chA: j?.concurrency?.channels?.['ch-a']?.active ?? 0,
    agents: j?.concurrency?.agents ?? {},
  };
};
const release = async () => {
  const r = await fetch(`http://127.0.0.1:${MOCK_PORT}/release`);
  return r.json();
};

try {
  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));
  gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
  let ready = false;
  for (let i = 0; i < 60; i += 1) {
    try { const r = await fetch(`http://127.0.0.1:${GW_PORT}/health`); if (r.ok) { ready = true; break; } } catch { /* retry */ }
    await wait(200);
  }
  ok('网关就绪', ready);
  if (!ready) throw new Error('网关未就绪');
  await wait(200);

  // A 的第 1 条：应占住 agent A 的名额 + 1 个全局名额
  const a1p = chatRaw(M_A, 'agent-A');
  const a1 = await a1p;
  await wait(300);
  const m1 = await metrics();
  ok('A 的第 1 条请求已准入（拿到响应头，global.active=1，上游在途 1）',
    a1.status === 200 && m1.global === 1, JSON.stringify({ status: a1.status, ...m1 }));

  // A 的第 2 条：agent A 额度已满 -> 排队。关键：**不得**预占第二个全局名额
  const a2p = chatRaw(M_A, 'agent-A');
  await wait(400);
  const m2 = await metrics();
  ok('A 的排队请求没有预占全局名额（global.active 仍为 1，旧实现是 2）',
    m2.global === 1, JSON.stringify(m2));
  ok('A 的排队请求没有预占渠道名额（ch-a active 仍为 1）', m2.chA === 1, JSON.stringify(m2));
  ok('A 的排队请求也还没有打到上游（上游在途仍为 1）', inFlight === 1, `上游在途=${inFlight}`);

  // B 用另一个空闲渠道：必须能进来
  const b1 = await chatRaw(M_B, 'agent-B');
  ok('B 向空闲渠道的请求被准入（拿到响应头，没被 A 的积压堵住）',
    b1.status === 200, `status=${b1.status}`);
  await wait(300);
  const m3 = await metrics();
  ok('此时 global.active=2（A1 + B1 在跑，A2 仍在等自己的 agent 名额）',
    m3.global === 2, JSON.stringify(m3));
  ok('上游实际并发峰值为 2（没有因为排队请求而超发）', peakInFlight === 2, `peak=${peakInFlight}`);

  // 放开上游：A1/B1 收尾，A2 被准入后（它是在释放之后才拿到名额的）再放开一次
  await release();
  const a2 = await a2p; // 排队的那条在名额释放后才被准入
  await release();
  const bodies = await Promise.all([a1.text(), a2.text(), b1.text()]);
  ok('三条请求全部正常收尾（排队的那条在名额释放后跑完）',
    a2.status === 200 && bodies.every((t) => t.includes('[DONE]')), JSON.stringify({ a2: a2.status, tails: bodies.map((t) => t.slice(-12)) }));
  await wait(500);
  const m4 = await metrics();
  ok('全部完成后计数归零（global.active=0，无残留名额）', m4.global === 0, JSON.stringify(m4));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  try { gw?.kill(); } catch { /* ignore */ }
  await wait(300);
  try { mock.close(); } catch { /* ignore */ }
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
