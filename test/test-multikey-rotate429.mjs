// 测试「叠 Key rotate-429（Key 内部轮转 + 429 快切）」专项回归（PLAN.md TASK 13）：
//   单元级：环形游标 takeNextRequestKey() 的推进/回卷/渠道隔离 + Key 级错误分类
//   HTTP 级：Case 1~11（首个成功 / 两连 429 后成功 / 环形回卷 / 慢响应耐心等 /
//            整池 429 / 401 换 Key / 普通 400 不盲扫 / 并发摊开 / race 模式零回归 /
//            唯一候选渠道时整圈失败回 key#1 重扫 / 有兜底渠道时整圈失败立刻交回渠道级）
//   注意：本文件只验证"一条渠道内部的 Key 调度"，不触碰渠道路由（见 test-multikey-rotate-routing.mjs）。
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Channel } from '../lib/channels.mjs';
import { classifyStackedKeyResult } from '../lib/proxy.mjs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'multikey-rotate.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}

async function get(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text, headers: res.headers };
}

const chat = (model, extra = {}) => get('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }], max_tokens: 50, ...extra }),
});

const contentOf = (r) => r.json?.choices?.[0]?.message?.content || '';
const mockStats = async () => (await fetch(mp.url(9144, '/_stats'))).json();
const mockReset = async () => { await fetch(mp.url(9144, '/_reset')); };
const metrics = async () => (await get('/api/metrics')).json;

// ============================================================================
// 单元级：环形游标 + Key 级错误分类（不依赖服务器）
// ============================================================================
{
  const mkChannel = (cfg) => new Channel(
    { name: 'u', protocol: 'openai', baseUrl: 'http://127.0.0.1:1/v1', ...cfg }, {}, {},
  );

  const ch = mkChannel({ apiKeys: ['K1', 'K2', 'K3', 'K4', 'K5'], stackedKeyStrategy: 'rotate-429' });
  const seq = [];
  for (let i = 0; i < 8; i += 1) seq.push(ch.takeNextRequestKey().key);
  ok('环形游标：5 key 连取 8 次 = K1..K5,K1,K2,K3（正确回卷/不越界/不跳 key）',
    seq.join(',') === 'K1,K2,K3,K4,K5,K1,K2,K3', seq.join(','));
  const taken = ch.takeNextRequestKey();
  ok('takeNextRequestKey 返回 {key,index,count}（游标已在 K4）',
    taken && taken.key === 'K4' && taken.index === 3 && taken.count === 5, JSON.stringify(taken));

  const a = mkChannel({ apiKeys: ['A1', 'A2', 'A3'], stackedKeyStrategy: 'rotate-429' });
  const b = mkChannel({ apiKeys: ['B1', 'B2', 'B3'], stackedKeyStrategy: 'rotate-429' });
  a.takeNextRequestKey(); a.takeNextRequestKey(); a.takeNextRequestKey(); a.takeNextRequestKey();
  b.takeNextRequestKey();
  ok('每条渠道拥有独立游标（Channel A 不影响 Channel B）', a.nextKeyIndex === 1 && b.nextKeyIndex === 1,
    `a=${a.nextKeyIndex} b=${b.nextKeyIndex}`);

  let threw = '';
  try { mkChannel({ apiKeys: ['K1', 'K2'], stackedKeyStrategy: 'nope' }); } catch (e) { threw = e.message; }
  ok('非法 stackedKeyStrategy 直接报配置错误', /stackedKeyStrategy/.test(threw), threw);

  const single = mkChannel({ apiKey: 'K1', stackedKeyStrategy: 'rotate-429' });
  ok('单 key 渠道归一为 race 且不暴露 Key 策略',
    single.stackedKeyStrategy === 'race' && single.toJSON().stackedKeyStrategy === null,
    `${single.stackedKeyStrategy}/${single.toJSON().stackedKeyStrategy}`);

  const multi = mkChannel({ apiKeys: ['K1', 'K2'], stackedKeyStrategy: 'rotate-429' });
  ok('多 key 渠道 toJSON 暴露 keyCount + stackedKeyStrategy',
    multi.toJSON().keyCount === 2 && multi.toJSON().stackedKeyStrategy === 'rotate-429',
    JSON.stringify(multi.toJSON().stackedKeyStrategy));

  // Key 级错误分类（TASK 7）
  ok('分类：200 -> accept', classifyStackedKeyResult(200) === 'accept');
  ok('分类：429 -> next-key', classifyStackedKeyResult(429) === 'next-key');
  ok('分类：401/402/403 -> next-key',
    [401, 402, 403].every((s) => classifyStackedKeyResult(s) === 'next-key'));
  ok('分类：5xx -> next-key', [500, 502, 503, 504].every((s) => classifyStackedKeyResult(s) === 'next-key'));
  ok('分类：400 -> return-to-channel-layer', classifyStackedKeyResult(400) === 'return-to-channel-layer');
}

// ============================================================================
// HTTP 级
// ============================================================================
const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(300);

  // ---- Case 1：第一个 Key 就成功；下一条请求从下一个 Key 开始 ----
  await mockReset();
  const r1 = await chat('rot-first');
  ok('Case1 首个 Key 成功', r1.status === 200 && contentOf(r1) === 'fast-sk-fast-a', `${r1.status} ${contentOf(r1)}`);
  ok('Case1 只请求了 K1（K2 0 命中）', ((await mockStats()).hits['sk-r429-b'] || 0) === 0);
  ok('Case1 响应头 x-gateway-key=1/2', r1.headers.get('x-gateway-key') === '1/2', String(r1.headers.get('x-gateway-key')));
  ok('Case1 响应头策略为 rotate-429', r1.headers.get('x-gateway-key-strategy') === 'rotate-429', String(r1.headers.get('x-gateway-key-strategy')));
  ok('Case1 响应头尝试数=1', r1.headers.get('x-gateway-key-attempts') === '1', String(r1.headers.get('x-gateway-key-attempts')));
  ok('TASK10 响应头不泄露 Key 明文',
    !/sk-[a-z0-9-]{2,}/i.test([...r1.headers.entries()].map(([k, v]) => `${k}:${v}`).join(' ')));

  const r1b = await chat('rot-first'); // 游标已在 K2
  ok('Case1 下一请求从 K2 起步（K2 命中 1 次且 429）', ((await mockStats()).hits['sk-r429-b'] || 0) === 1);
  ok('Case1 下一请求回卷 K1 成功，尝试数=2',
    r1b.status === 200 && r1b.headers.get('x-gateway-key') === '1/2' && r1b.headers.get('x-gateway-key-attempts') === '2',
    `${r1b.headers.get('x-gateway-key')} attempts=${r1b.headers.get('x-gateway-key-attempts')}`);

  // ---- Case 2：连续两个 429 后成功（并把游标推到 K4）----
  await mockReset();
  const mBefore = await metrics();
  const chBefore = (mBefore.channels || []).find((c) => c.name === 'rot-two429') || {};
  const sBefore = mBefore.stackedKeys || {};
  const r2 = await chat('rot-two429');
  ok('Case2 两连 429 后 K3 成功', r2.status === 200 && contentOf(r2) === 'fast-sk-fast-3', `${r2.status} ${contentOf(r2)}`);
  ok('Case2 响应头 x-gateway-key=3/5 且 attempts=3',
    r2.headers.get('x-gateway-key') === '3/5' && r2.headers.get('x-gateway-key-attempts') === '3',
    `${r2.headers.get('x-gateway-key')} attempts=${r2.headers.get('x-gateway-key-attempts')}`);
  const s2 = await mockStats();
  ok('Case2 顺序 K1,K2,K3（各命中 1 次）',
    (s2.hits['sk-r429-1'] || 0) === 1 && (s2.hits['sk-r429-2'] || 0) === 1 && (s2.hits['sk-fast-3'] || 0) === 1,
    JSON.stringify(s2.hits));
  ok('Case2 K4/K5 未被调用', !s2.hits['sk-boom-4'] && !s2.hits['sk-boom-5'], JSON.stringify(s2.hits));

  // TASK 11：叠 Key 指标增量（渠道级只记成功 1 次，不污染 health）
  const mAfter = await metrics();
  const sAfter = mAfter.stackedKeys || {};
  const chAfter = (mAfter.channels || []).find((c) => c.name === 'rot-two429') || {};
  ok('TASK11 stackedKeyRequests +1', (sAfter.stackedKeyRequests || 0) - (sBefore.stackedKeyRequests || 0) === 1);
  ok('TASK11 stackedKeyAttempts +3', (sAfter.stackedKeyAttempts || 0) - (sBefore.stackedKeyAttempts || 0) === 3);
  ok('TASK11 stackedKey429s +2', (sAfter.stackedKey429s || 0) - (sBefore.stackedKey429s || 0) === 2);
  ok('TASK11 stackedKeySuccessAfterFailover +1',
    (sAfter.stackedKeySuccessAfterFailover || 0) - (sBefore.stackedKeySuccessAfterFailover || 0) === 1);
  ok('TASK11 渠道级 total +1 且 failed 不增加',
    (chAfter.total || 0) - (chBefore.total || 0) === 1 && (chAfter.failed || 0) === (chBefore.failed || 0),
    `total ${chBefore.total}->${chAfter.total} failed ${chBefore.failed}->${chAfter.failed}`);
  ok('TASK11 平均每请求 Key 尝试数可派生', Number.isFinite(sAfter.avgAttemptsPerRequest), String(sAfter.avgAttemptsPerRequest));

  // ---- Case 3：环形回卷（游标已在 K4，本请求扫到 K5 后回卷 K1，最终 K3 成功）----
  const r3 = await chat('rot-two429');
  ok('Case3 环形回卷 K5->K1 且整圈扫描后成功',
    r3.status === 200 && r3.headers.get('x-gateway-key') === '3/5' && r3.headers.get('x-gateway-key-attempts') === '5',
    `${r3.headers.get('x-gateway-key')} attempts=${r3.headers.get('x-gateway-key-attempts')}`);
  const s3 = await mockStats();
  ok('Case3 一次请求最多扫描一整圈（K4/K5 各只命中 1 次，不重复扫）',
    (s3.hits['sk-boom-4'] || 0) === 1 && (s3.hits['sk-boom-5'] || 0) === 1,
    JSON.stringify(s3.hits));

  // ---- Case 4：成功但慢（250ms）——必须耐心等，不启动后续 Key ----
  await mockReset();
  const r4 = await chat('rot-slow');
  ok('Case4 慢响应（250ms）耐心等待并成功', r4.status === 200 && contentOf(r4) === 'slow-sk-slow-a', `${r4.status} ${contentOf(r4)}`);
  ok('Case4 等待期间未启动 K2', ((await mockStats()).hits['sk-boom-b'] || 0) === 0);
  ok('Case4 尝试数=1', r4.headers.get('x-gateway-key-attempts') === '1', String(r4.headers.get('x-gateway-key-attempts')));

  // ---- Case 5：所有 Key 都 429 → 每个最多一次，不无限循环，交回渠道级降级 ----
  await mockReset();
  const r5 = await chat('rot-all429');
  ok('Case5 整池 429 后交回渠道级并降级到兜底渠道成功',
    r5.status === 200 && r5.headers.get('x-gateway-channel') === 'rot-all429-fb',
    `${r5.status} ${r5.headers.get('x-gateway-channel')}`);
  const s5 = await mockStats();
  ok('Case5 每个 Key 最多尝试一次（不无限循环）',
    (s5.hits['sk-r429-1'] || 0) === 1 && (s5.hits['sk-r429-2'] || 0) === 1,
    JSON.stringify(s5.hits));

  // ---- Case 6：401 后其他 Key 成功 → 不把整条渠道 auth 熔断 ----
  await mockReset();
  const r6 = await chat('rot-401');
  ok('Case6 K1 401 后 K2 成功', r6.status === 200 && r6.headers.get('x-gateway-key') === '2/2',
    `${r6.status} ${r6.headers.get('x-gateway-key')}`);
  const st = (await get('/api/status')).json;
  const ch401 = (st.channels || []).find((c) => c.name === 'rot-401') || {};
  ok('Case6 401 未把整条渠道熔断（coolingDown=false / failures=0）',
    ch401.coolingDown === false && ch401.failures === 0, JSON.stringify(ch401));
  const r6b = await chat('rot-401');
  ok('Case6 渠道仍可继续服务', r6b.status === 200, String(r6b.status));

  // ---- Case 7：普通 400（业务参数错误）不盲扫后续 Key ----
  await mockReset();
  const r7 = await chat('rot-400');
  const s7 = await mockStats();
  ok('Case7 普通 400 不盲扫 K2~KN', (s7.hits['sk-fast-y'] || 0) === 0, JSON.stringify(s7.hits));
  ok('Case7 保持既有业务错误语义（非 200）', r7.status !== 200, String(r7.status));

  // ---- Case 8：并发轮转 —— 首发 Key 明显摊开 ----
  await mockReset();
  const rs = await Promise.all(Array.from({ length: 10 }, () => chat('rot-conc')));
  ok('Case8 并发 10 个请求全部成功', rs.every((r) => r.status === 200), rs.map((r) => r.status).join(','));
  const s8 = await mockStats();
  const hit8 = ['sk-fast-c1', 'sk-fast-c2', 'sk-fast-c3', 'sk-fast-c4', 'sk-fast-c5'].map((k) => s8.hits[k] || 0);
  ok('Case8 首发 Key 明显摊开（每个 Key 至少命中 1 次）', hit8.every((n) => n >= 1), JSON.stringify(hit8));
  ok('Case8 10 次请求恰好 10 次 Key 尝试', hit8.reduce((x, y) => x + y, 0) === 10, JSON.stringify(hit8));
  const st8 = (await get('/api/status')).json;
  const conc8 = (st8.channels || []).find((c) => c.name === 'rot-conc') || {};
  ok('Case8 渠道路由计数仍在（渠道级语义未被改动）', conc8.keyCount === 5 && conc8.stackedKeyStrategy === 'rotate-429',
    JSON.stringify(conc8));

  // ---- Case 9：race 模式零回归（旧全量并发竞速仍在）----
  await mockReset();
  const r9 = await chat('rot-race');
  ok('Case9 race 模式仍是全量并发竞速（2/3 胜出）',
    r9.status === 200 && r9.headers.get('x-gateway-key') === '2/3' && r9.headers.get('x-gateway-key-strategy') === 'race',
    `${r9.headers.get('x-gateway-key')} ${r9.headers.get('x-gateway-key-strategy')}`);
  const s9 = await mockStats();
  ok('Case9 race 三个 Key 都被请求（真正并行）',
    ['sk-slow-r', 'sk-fast-r', 'sk-auth-r'].every((k) => (s9.hits[k] || 0) >= 1), JSON.stringify(s9.hits));
  // ---- Case 10：唯一候选渠道 → 整圈 429 后回 key#1 重扫（本次新增行为）----
  // mock 9106：前 N 次请求 429、之后成功。rot-lap-only 是唯一服务该 model 的渠道，
  // 所以"整圈失败就交回渠道级"毫无意义（交回去还是它自己）→ 改为回到第一个 Key 重扫。
  await fetch(mp.url(9106, '/_reset-flaky?fail=2'));
  const m10a = (await metrics()).stackedKeys || {};
  const r10 = await chat('rot-lap-only');
  const m10b = (await metrics()).stackedKeys || {};
  ok('Case10 唯一候选渠道：整圈 429 后重扫成功（agent 请求没有失败）',
    r10.status === 200 && contentOf(r10).includes('flaky ok'), `${r10.status} ${contentOf(r10)}`);
  ok('Case10 第 3 次尝试落在 key#1（真的回到第一个 Key，而不是停在 K2）',
    r10.headers.get('x-gateway-key') === '1/2' && r10.headers.get('x-gateway-key-attempts') === '3',
    `key=${r10.headers.get('x-gateway-key')} attempts=${r10.headers.get('x-gateway-key-attempts')}`);
  ok('Case10 恰好回 K1 重扫 1 圈（stackedKeyLapRetries +1）',
    (m10b.stackedKeyLapRetries || 0) - (m10a.stackedKeyLapRetries || 0) === 1,
    `${m10a.stackedKeyLapRetries} -> ${m10b.stackedKeyLapRetries}`);
  ok('Case10 总共只发了 3 次 Key 请求（第 1 圈 2 次 + 重扫 1 次）',
    (m10b.stackedKeyAttempts || 0) - (m10a.stackedKeyAttempts || 0) === 3,
    `${m10a.stackedKeyAttempts} -> ${m10b.stackedKeyAttempts}`);

  // ---- Case 11：还有别的候选渠道 → 整圈失败立刻交回渠道级快速降级（既有语义不许回退）----
  await fetch(mp.url(9106, '/_reset-flaky?fail=2'));
  const m11a = (await metrics()).stackedKeys || {};
  const r11 = await chat('rot-lap-multi');
  const m11b = (await metrics()).stackedKeys || {};
  ok('Case11 有兜底渠道时不重扫：直接降级到兜底渠道并成功',
    r11.status === 200 && r11.headers.get('x-gateway-channel') === 'rot-lap-multi-fb',
    `${r11.status} ${r11.headers.get('x-gateway-channel')}`);
  ok('Case11 本渠道只发了 2 次 Key 请求（每个 Key 一次，没有重扫）',
    (m11b.stackedKeyAttempts || 0) - (m11a.stackedKeyAttempts || 0) === 2,
    `${m11a.stackedKeyAttempts} -> ${m11b.stackedKeyAttempts}`);
  ok('Case11 没有发生回 K1 重扫（stackedKeyLapRetries 不变）',
    (m11b.stackedKeyLapRetries || 0) === (m11a.stackedKeyLapRetries || 0),
    `${m11a.stackedKeyLapRetries} -> ${m11b.stackedKeyLapRetries}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
