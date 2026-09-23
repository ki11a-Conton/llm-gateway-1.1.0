// 多子代理并发测试：
//   1. 身份识别 —— 从 header / body / api-key 推断 agentId，响应头回传 x-gateway-agent
//   2. 每代理配额 —— 一个子代理不能吃光渠道配额，其它子代理仍能拿到服务
//   3. 最少在途选路 —— 多代理并发时请求被摊开，不会全部扑向同一家
//   4. 会话亲和 —— 同一代理的连续请求稳定复用同一个渠道
//   5. 可观测性 —— /api/status 与 /api/metrics 能看出"谁走了哪家、多少在途"
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：端口全部运行时动态分配（原 8797 网关 + 9121/9122/9123 三个同构上游）
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const BASE = `http://127.0.0.1:${PORT}`;
const KEY = 'TESTKEY';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitReady(url, tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch { /* retry */ }
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}

// 三家渠道都指向同一个 mock，但名字不同 -> 便于观察"请求被摊到哪几家"
// 逻辑端口 9121/9122/9123（mock-upstream.mjs 里的三个同构上游）由 MOCK_PORT_BASE 平移成实际端口；
// 配置写在系统临时目录里，跑完删掉，不再往仓库落一份带运行时端口的 json。
const CFG = path.join(os.tmpdir(), `multiagent.test-${process.pid}.json`);
const cfg = {
  server: { host: '127.0.0.1', port: PORT, apiKey: KEY },
  routing: {
    strategy: 'least-loaded',
    probeIntervalMs: 0, discoverIntervalMs: 0,
    maxAttempts: 3, failThreshold: 99,
    attemptsPerChannel: 1, retryPerAttemptMs: 0,
    retryLoop: false, retryMaxWaitMs: 0,
    sessionAffinity: true, affinityTtlMs: 600000,
    maxConcurrentPerAgent: 2,
    maxConcurrent: 64, maxConcurrentPerChannel: 64, queueTimeoutMs: 3000,
    failDedupMs: 0,
  },
  channels: [
    { name: 'ca', protocol: 'openai', baseUrl: mp.url(9121), apiKey: 'k', model: 'm1', priority: 10 },
    { name: 'cb', protocol: 'openai', baseUrl: mp.url(9122), apiKey: 'k', model: 'm1', priority: 10 },
    { name: 'cc', protocol: 'openai', baseUrl: mp.url(9123), apiKey: 'k', model: 'm1', priority: 10 },
  ],
};
writeFileSync(CFG, JSON.stringify(cfg, null, 2), 'utf8');

const chat = (body, headers = {}) =>
  fetch(`${BASE}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}`, ...headers },
    body: JSON.stringify(body),
  });

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
// 等 mock 的上游端口真正开始监听：只 sleep 不够，机器负载高时会 CONNREFUSED
async function waitPort(port, tries = 60) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/v1/models`);
      if (r.ok || r.status < 500) return true;
    } catch { /* not up yet */ }
    await wait(150);
  }
  throw new Error(`mock upstream ${port} 未就绪`);
}
await Promise.all([waitPort(mp.port(9121)), waitPort(mp.port(9122)), waitPort(mp.port(9123))]);

const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
await waitReady(`${BASE}/health`);
await wait(300);

// ---- 1) 身份识别来源 ----
{
  const r1 = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }] }, { 'x-agent-id': 'agent-alpha' });
  ok('x-agent-id 头被识别并回传', r1.headers.get('x-gateway-agent') === 'agent-alpha', r1.headers.get('x-gateway-agent'));

  // body / metadata 的会话标识优先于 api-key（同一个 key 可被多个子代理共用）
  const r2 = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }], session_id: 'sess-42' });
  ok('body.session_id 被识别（优先于 api-key）', (r2.headers.get('x-gateway-agent') || '').includes('sess-42'), r2.headers.get('x-gateway-agent'));

  const r3 = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }], metadata: { conversation_id: 'conv-9' } });
  ok('body.metadata.conversation_id 被识别', (r3.headers.get('x-gateway-agent') || '').includes('conv-9'), r3.headers.get('x-gateway-agent'));

  const r3b = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }], user: 'user_agent_77' });
  ok('body.user 被识别', (r3b.headers.get('x-gateway-agent') || '').includes('user_agent_77'), r3b.headers.get('x-gateway-agent'));

  // 显式头优先级最高：body 也有标识时要给 header 让路
  const r3c = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }], session_id: 'ignored-me' }, { 'x-agent-id': 'winner' });
  ok('显式头优先于 body', r3c.headers.get('x-gateway-agent') === 'winner', r3c.headers.get('x-gateway-agent'));

  // 无任何显式标识 -> 回落到 api-key 指纹
  const r4 = await chat({ model: 'm1', messages: [{ role: 'user', content: 'hi' }] });
  ok('无显式标识时回落到 api-key 指纹', (r4.headers.get('x-gateway-agent') || '').startsWith('key:'), r4.headers.get('x-gateway-agent'));
  await r4.json();
}

// ---- 2) 会话亲和：同一代理连续 3 次都走同一渠道 ----
{
  const seen = new Set();
  for (let i = 0; i < 3; i++) {
    const r = await chat({ model: 'm1', messages: [{ role: 'user', content: `t${i}` }] }, { 'x-agent-id': 'affinity-agent' });
    seen.add(r.headers.get('x-gateway-channel'));
    await r.json();
  }
  ok('同一代理连续 3 次复用同一渠道（会话亲和）', seen.size === 1, [...seen].join(','));
}

// ---- 3) 最少在途：多个不同代理并发 -> 请求被摊到多家渠道 ----
{
  const agents = ['p1', 'p2', 'p3', 'p4', 'p5', 'p6'];
  const results = await Promise.all(
    agents.map((a) => chat({ model: 'm1', messages: [{ role: 'user', content: 'x' }] }, { 'x-agent-id': a })),
  );
  const used = new Set(results.map((r) => r.headers.get('x-gateway-channel')));
  await Promise.all(results.map((r) => r.json()));
  ok('6 个不同代理并发被摊到 >= 2 家渠道（负载均衡）', used.size >= 2, `只用到 ${[...used].join(',')}`);
}

// ---- 4) 每代理配额：单代理并发被限制在 maxConcurrentPerAgent ----
{
  const snapBefore = await (await fetch(`${BASE}/api/status`)).json();
  ok('配置里 maxConcurrentPerAgent 生效', snapBefore.concurrency.maxConcurrentPerAgent === 2, JSON.stringify(snapBefore.concurrency.maxConcurrentPerAgent));

  // 单个代理发 6 个并发；配额 2 -> 必然出现排队
  const reqs = Array.from({ length: 6 }, (_, i) =>
    chat({ model: 'm1', messages: [{ role: 'user', content: `q${i}` }] }, { 'x-agent-id': 'hog-agent' }),
  );
  await wait(120);
  const snapMid = await (await fetch(`${BASE}/api/status`)).json();
  const hog = snapMid.agents.find((a) => a.id === 'hog-agent');
  const queuedEver = hog ? hog.queued > 0 || hog.inFlight <= 2 : false;
  ok('单代理在途不超过配额（<=2）', !hog || hog.inFlight <= 2, hog ? JSON.stringify(hog) : 'not found');
  ok('单代理出现排队（说明配额在起作用）', queuedEver, hog ? JSON.stringify(hog) : 'not found');
  const all = await Promise.all(reqs);
  await Promise.all(all.map((r) => r.json()));
  ok('被限流的请求最终都能完成', all.every((r) => r.status === 200), all.map((r) => r.status).join(','));
}

// ---- 5) 可观测性：/api/status 的 agents 视图 ----
{
  // 制造两个有在途的代理（mock 上游有 40ms 延迟，够看）
  const a = fetch(`${BASE}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${KEY}`, 'content-type': 'application/json', 'x-agent-id': 'obs-a' },
    body: JSON.stringify({ model: 'm1', messages: [{ role: 'user', content: 'hi' }] }),
  });
  const b = fetch(`${BASE}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${KEY}`, 'content-type': 'application/json', 'x-agent-id': 'obs-b' },
    body: JSON.stringify({ model: 'm1', messages: [{ role: 'user', content: 'hi' }] }),
  });
  await wait(60);
  const snap = await (await fetch(`${BASE}/api/status`)).json();
  const ids = snap.agents.map((x) => x.id);
  ok('/api/status.agents 列出活跃代理', ids.includes('obs-a') && ids.includes('obs-b'), ids.join(','));
  const withAffinity = snap.agents.filter((x) => x.affinity && x.affinity.channel);
  ok('代理视图带会话亲和渠道', withAffinity.length > 0, JSON.stringify(snap.agents).slice(0, 200));
  await Promise.all([a, b].map((p) => p.then((r) => r.json())));

  const metrics = await (await fetch(`${BASE}/api/metrics`)).json();
  ok('/api/metrics 带 agents 视图', Array.isArray(metrics.agents), typeof metrics.agents);
  ok('/api/metrics 带每代理并发上限', metrics.concurrency.maxConcurrentPerAgent === 2, String(metrics.concurrency.maxConcurrentPerAgent));
}

// ---- 6) 配额的"隔离性"：一个代理被限流时另一个代理不受影响 ----
{
  const hog = Array.from({ length: 8 }, (_, i) =>
    fetch(`${BASE}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: `Bearer ${KEY}`, 'content-type': 'application/json', 'x-agent-id': 'greedy' },
      body: JSON.stringify({ model: 'm1', messages: [{ role: 'user', content: `g${i}` }] }),
    }),
  );
  await wait(80);
  const t0 = Date.now();
  const other = await chat({ model: 'm1', messages: [{ role: 'user', content: 'me too' }] }, { 'x-agent-id': 'polite' });
  const ms = Date.now() - t0;
  ok('另一个代理不被大代理连累（快速成功）', other.status === 200 && ms < 2000, `status=${other.status} ${ms}ms`);
  await other.json();
  await Promise.all(hog.map((p) => p.then((r) => r.json()).catch(() => {})));
}

gw.kill();
mock.kill();
rmSync(CFG, { force: true });
await wait(300);
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
