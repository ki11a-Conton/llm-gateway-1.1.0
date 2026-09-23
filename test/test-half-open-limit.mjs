// F8 回归：半开窗口的并发上限必须真正拦在转发层，而不是只影响排序
//
// 背景（代码审查 2026-09-20 F8/P2）：`#halfOpenSaturated()` 只把占满的渠道排到候选末尾，
// 转发层遍历到它时**没有申请半开专用许可**——唯一渠道（或前面渠道都失败）时照样打过去。
// 报告复现：唯一渠道先失败进入冷却，冷却到期后设 halfOpenMaxInFlight=1，同时发 3 个请求，
// 上游实际观察到峰值并发 3。
//
// 本套件断言：半开窗口内并发试探数被强制为 1；多余请求被挡在网关侧（不落到上游）；
//            试探成功后渠道恢复、后续请求正常。
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

const M = 'f8-model';
const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'F8KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

// 上游行为：第一次请求回 500（触发冷却）；之后把流挂住（这样半开试探会一直占着名额，
// 便于观察"同时只有一个试探在跑"）。
let mode = 'fail';
let concurrent = 0;
let peak = 0;
let upstreamHits = 0;
const held = [];
const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => {
    if (req.url?.includes('/reset-peak')) {
      peak = 0;
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end('{"ok":true}');
    }
    if (req.url?.includes('/release')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ held: held.length }));
      for (const h of held.splice(0)) {
        h.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: M, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
        h.write('data: [DONE]\n\n');
        h.end();
        concurrent -= 1;
      }
      return;
    }
    let body = {};
    try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: M, object: 'model' }] }));
    }
    upstreamHits += 1;
    concurrent += 1;
    if (concurrent > peak) peak = concurrent;
    req.socket.on('close', () => { /* 观察半开是否提前断开 */ });
    if (mode === 'fail') {
      mode = 'hold'; // 第一条失败后，后续都挂住
      concurrent -= 1;
      res.writeHead(500, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: { message: 'boom' } }));
    }
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: M, choices: [{ index: 0, delta: { role: 'assistant', content: 'x'.repeat(80) } }] })}\n\n`);
    held.push(res);
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f8-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'f8.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
  taskLog: { enabled: false, dir: LOGS_DIR },
  routing: {
    strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
    // 失败 1 次即冷却 300ms；冷却到期后进入 6s 半开窗口，窗口内只允许 1 个试探
    failThreshold: 1, cooldownMs: 300, maxCooldownMs: 1000, halfOpenMs: 6000, halfOpenMaxInFlight: 1,
    probeIntervalMs: 0, discoverIntervalMs: 0,
    timeoutMs: 20000, providerTimeoutMs: 20000, streamIdleTimeoutMs: 20000,
    maxConcurrent: 8, maxConcurrentPerChannel: 0, queueTimeoutMs: 3000,
  },
  channels: [{ name: 'only', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M, priority: 10 }],
}, null, 2) + '\n', 'utf8');

let gw = null;
const chat = () => fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
  method: 'POST',
  headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
  body: JSON.stringify({ model: M, stream: true, messages: [{ role: 'user', content: 'hi' }] }),
});
const status = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/status`, { headers: { authorization: `Bearer ${API_KEY}` } });
  const j = await r.json().catch(() => null);
  const ch = (j?.channels || []).find((c) => c.name === 'only');
  return { cooling: ch?.coolingDown, lastError: ch?.lastError };
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

  // 第 1 条：失败 -> 渠道进入冷却
  const r0 = await chat();
  await r0.text();
  ok('第 1 条请求失败（触发冷却）', r0.status >= 400, `status=${r0.status}`);
  await wait(150);
  const s0 = await status();
  ok('渠道已进入冷却（冷却 300ms）', s0.cooling === true, JSON.stringify(s0));

  // 等冷却到期 -> 进入半开窗口
  await wait(300);
  // 从这一刻起单独统计"半开窗口内的上游并发峰值"（上一轮失败的请求已结束，不受它干扰）
  await fetch(`http://127.0.0.1:${MOCK_PORT}/reset-peak`);

  // 同时发 3 条：半开窗口内只应有 1 条打到上游
  const burst = [chat(), chat(), chat()];
  await wait(600);
  ok('半开窗口内上游实际并发被限制为 1（旧实现是 3）',
    peak === 1, `peak=${peak}`);
  ok('只有 1 条试探挂在上游（多余的请求被挡在网关侧，没落到上游）',
    held.length === 1 && upstreamHits === 2, JSON.stringify({ held: held.length, upstreamHits }));

  // 先放开试探，再收结果（否则被挂住的那条要等网关的空闲超时）
  await fetch(`http://127.0.0.1:${MOCK_PORT}/release`);
  const results = await Promise.all(burst.map(async (p) => {
    const r = await p;
    return { status: r.status, text: await r.text() };
  }));
  const okOnes = results.filter((x) => x.status === 200);
  const failed = results.filter((x) => x.status >= 400);
  ok('3 条里恰好 1 条被准入、2 条被挡下（不是 3 条一起试探）',
    okOnes.length === 1 && failed.length === 2,
    JSON.stringify(results.map((x) => x.status)));
  ok('被挡下的请求给出了可读原因（半开窗口名额已满）',
    failed.every((x) => /半开|overload|无法完成|503/.test(x.text) || x.status === 503),
    JSON.stringify(failed.map((x) => x.text.slice(0, 90))));

  await wait(400);
  const s1 = await status();
  ok('试探成功后渠道恢复（不再冷却）', s1.cooling === false, JSON.stringify(s1));
  const after = await chat();
  ok('恢复后新请求正常打到上游', after.status === 200 && upstreamHits === 3, JSON.stringify({ status: after.status, upstreamHits }));
  await after.text();
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
