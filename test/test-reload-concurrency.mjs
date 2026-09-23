// F7 回归：并发上限热重载不得丢失在途计数
//
// 背景（代码审查 2026-09-20 F7/P2）：修改全局并发上限时会 new 一个 Semaphore、
// 修改渠道上限时会 clear() 掉 channels Map。新信号量的 active 从 0 开始，
// 正在执行的请求仍持有旧信号量的 release —— 于是在途请求从计数里消失：
// 报告复现"2 条在途时把上限从 2 调到 1，第三条仍立刻拿到许可（实际已获准 3 条），
// 而新 metrics 只报 active=1"。
//
// 修复：保留同一实例（setLimit），_handoff 按新上限决定能否移交；
//      调高上限时 _pump() 立即唤醒等待者。
//
// 本套件断言：① 调低上限后在途计数不丢、新请求不放行；② 调高上限后排队请求被立即唤醒；
//            ③ 收尾后计数归零。
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

const M = 'f7-model';
const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'F7KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

let inFlight = 0;
let peak = 0;
const held = [];
const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => {
    if (req.url?.includes('/release')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ released: held.length }));
      for (const h of held.splice(0)) {
        h.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: M, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
        h.write('data: [DONE]\n\n');
        h.end();
        inFlight -= 1;
      }
      return;
    }
    let body = {};
    try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: M, object: 'model' }] }));
    }
    inFlight += 1;
    if (inFlight > peak) peak = inFlight;
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: M, choices: [{ index: 0, delta: { role: 'assistant', content: 'x'.repeat(80) } }] })}\n\n`);
    held.push(res); // 挂住直到 /release
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f7-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'f7.test.json');
const writeCfg = (maxConcurrent, maxConcurrentPerChannel) => {
  writeFileSync(RUN_CFG, JSON.stringify({
    server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
    taskLog: { enabled: false, dir: LOGS_DIR },
    routing: {
      strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
      failThreshold: 99, probeIntervalMs: 0, discoverIntervalMs: 0,
      timeoutMs: 30000, providerTimeoutMs: 30000, streamIdleTimeoutMs: 30000,
      maxConcurrent, maxConcurrentPerChannel, maxConcurrentPerAgent: 0,
      queueTimeoutMs: 8000,
    },
    channels: [{ name: 'ch', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M, priority: 10 }],
  }, null, 2) + '\n', 'utf8');
};
writeCfg(2, 2);

let gw = null;
const chat = () => fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
  method: 'POST',
  headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
  body: JSON.stringify({ model: M, stream: true, messages: [{ role: 'user', content: 'hi' }] }),
});
const metrics = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/metrics`, { headers: { authorization: `Bearer ${API_KEY}` } });
  const j = await r.json().catch(() => null);
  return {
    global: j?.concurrency?.global?.active ?? null,
    limit: j?.concurrency?.global?.limit ?? null,
    chLimit: j?.concurrency?.channels?.['ch']?.limit ?? null,
  };
};
const reload = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/reload`, { method: 'POST', headers: { authorization: `Bearer ${API_KEY}` } });
  return r.status;
};
const release = async () => (await fetch(`http://127.0.0.1:${MOCK_PORT}/release`)).json();

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

  // 两条在途（上限 2）
  const r1 = await chat();
  const r2 = await chat();
  await wait(350);
  const m0 = await metrics();
  ok('两条请求已准入（active=2 / limit=2）', m0.global === 2 && m0.limit === 2 && inFlight === 2, JSON.stringify({ ...m0, inFlight }));

  // ---------- ① 调低上限 2 -> 1 ----------
  writeCfg(1, 1);
  const st = await reload();
  ok('热重载接口返回 200', st === 200, `status=${st}`);
  await wait(300);
  const m1 = await metrics();
  ok('调低上限后**在途计数不丢**（active 仍为 2，旧实现会归零）',
    m1.global === 2, JSON.stringify(m1));
  ok('新上限已生效（limit=1）', m1.limit === 1, JSON.stringify(m1));

  const r3 = chat(); // 不该被放行：active(2) 已高于新上限(1)
  await wait(400);
  ok('调低上限后不再放行新请求（上游在途仍为 2，旧实现会变成 3）',
    inFlight === 2, `上游在途=${inFlight}`);
  const m2 = await metrics();
  ok('排队请求不计入在途（active 仍为 2）', m2.global === 2, JSON.stringify(m2));

  // ---------- ② 调高上限 1 -> 3：等待者应被立即唤醒 ----------
  const tRaise = Date.now();
  writeCfg(3, 3);
  await reload();
  const r3res = await Promise.race([r3.then((r) => r), wait(1500).then(() => null)]);
  const raiseMs = Date.now() - tRaise;
  ok('调高上限后排队请求被立即唤醒（1.5s 内拿到响应头，不用等 8s 排队超时）',
    r3res !== null && raiseMs < 1500, `${raiseMs}ms`);
  await wait(200);
  ok('唤醒后上游在途变为 3', inFlight === 3, `上游在途=${inFlight}`);

  // ---------- ③ 收尾：全部完成、计数归零 ----------
  await release();
  const bodies = await Promise.all([r1, r3res, r2].map(async (r) => (r ? (await r.text()) : '')));
  ok('三条请求都正常收尾（都拿到 [DONE]）',
    bodies.every((t) => t.includes('[DONE]')), JSON.stringify(bodies.map((t) => t.slice(-12))));
  await wait(500);
  const m3 = await metrics();
  ok('全部完成后计数归零（active=0）', m3.global === 0, JSON.stringify(m3));
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
