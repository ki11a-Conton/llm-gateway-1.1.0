// 测试「叠 Key rotate-429 对渠道路由零影响」专项回归（PLAN.md TASK 14）。
//
// 目标：证明本功能只作用于「一条已被选中的渠道内部的 Key 调度」，绝不参与渠道选路。
//
// 单元级（不依赖服务器，只 import lib/channels.mjs）：
//   构造两套除“Channel A 的 Key 个数”外逐位相同的配置——
//     A 多 Key + rotate-429 vs A 单 Key（对照）——
//   在 6 种渠道级策略（priority / round-robin / weighted / least-loaded / sticky /
//   sessionAffinity）以及两段式 tiered 下，candidatesFor() 的渠道顺序必须逐位一致；
//   并且把 A 的 Key 环形游标推进任意多格后，渠道顺序仍逐位不变
//   （=> Key 游标不参与渠道选路，不会改变渠道排序）。
//
// HTTP 级：
//   用一个 mock 上游 + 两个网关（主配置 A=5 Key rotate-429；对照配置 A=单 Key）跑同一串请求，
//   两者的 x-gateway-channel 序列必须逐位一致；同时主配置里 ra 的 Key 序号确实在轮转。
//   （=> K1 成功不会让下一条请求自动切到 Channel B；Key 轮转不改变渠道节奏。）
import { spawn } from 'node:child_process';
import path from 'node:path';
import { readFileSync, writeFileSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../lib/channels.mjs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const TMP = path.join(HERE, 'multikey-rotate-routing.unit.tmp.json');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================================================
// 单元级：渠道排序与 Key 游标解耦
// ============================================================================
const BASE = {
  sticky: false,
  sessionAffinity: false,
  fallbackShuffle: false,
  tiered: false,
  probeIntervalMs: 0,
  discoverIntervalMs: 0,
  maxAttempts: 6,
};

// Channel A：本次改造对象。多 Key 版 vs 单 Key 对照版**除 Key 个数外逐位相同**。
const A_MULTI = {
  name: 'ra', protocol: 'openai', baseUrl: 'https://routing-test.invalid/v1',
  apiKeys: ['K1', 'K2', 'K3', 'K4', 'K5'], stackedKeyStrategy: 'rotate-429',
  model: 'rr-model', priority: 10, tier: 'preferred',
};
const A_SINGLE = {
  name: 'ra', protocol: 'openai', baseUrl: 'https://routing-test.invalid/v1',
  apiKey: 'K1', model: 'rr-model', priority: 10, tier: 'preferred',
};
const B = {
  name: 'rb', protocol: 'openai', baseUrl: 'https://routing-test.invalid/v1',
  apiKey: 'kk', model: 'rr-model', priority: 10, tier: 'preferred',
};
const B_FALLBACK = { ...B, tier: 'fallback' };

function mgrWith(routing, channels) {
  writeFileSync(TMP, JSON.stringify({
    server: { host: '127.0.0.1', port: 1, apiKey: 'k' },
    routing: { ...BASE, ...routing },
    channels,
  }), 'utf8');
  return new ChannelManager(TMP).load();
}

/** 跑 n 次选路，记录每次的候选渠道顺序（"a>b" 字符串） */
function orderSeq(mgr, n, opts = {}) {
  const out = [];
  for (let i = 0; i < n; i += 1) out.push(mgr.candidatesFor('rr-model', opts).map((c) => c.name).join('>'));
  return out;
}

/**
 * 在指定策略下取一段候选顺序：可选先做 setup（粘性/会话亲和），再把 A 的 Key 游标推进 advance 格。
 * 返回 { seq, mgr, ra }。
 */
function runSeq({ routing, channels, setup = null, advance = 0, n = 12, opts = {} }) {
  const mgr = mgrWith(routing, channels);
  if (setup) setup(mgr);
  const ra = mgr.channels.find((c) => c.name === 'ra');
  for (let i = 0; i < advance; i += 1) ra.takeNextRequestKey();
  return { seq: orderSeq(mgr, n, opts), mgr, ra };
}

/** 断言：多 Key 版 / 单 Key 版 / 推进游标后的多 Key 版，三者渠道顺序完全一致 */
function assertDecoupled(label, routing, opts = {}, setup = null) {
  const multi = runSeq({ routing, channels: [A_MULTI, B], setup, n: 12, opts }).seq;
  const single = runSeq({ routing, channels: [A_SINGLE, B], setup, n: 12, opts }).seq;
  const advanced = runSeq({ routing, channels: [A_MULTI, B], setup, advance: 13, n: 12, opts }).seq;
  ok(`${label}：多 Key(rotate-429) 与单 Key 对照的渠道顺序逐位一致`,
    multi.join('|') === single.join('|'), `multi=${multi.join('|')} single=${single.join('|')}`);
  ok(`${label}：Key 游标推进 13 格后渠道顺序逐位不变（Key 游标不影响渠道路由）`,
    multi.join('|') === advanced.join('|'), `before=${multi.join('|')} after=${advanced.join('|')}`);
  ok(`${label}：候选里始终包含 ra 与 rb（没有因 Key 调度把渠道丢掉）`,
    multi.every((s) => s.includes('ra') && s.includes('rb')), multi.join('|'));
  return multi;
}

try {
  // ---- priority：同优先级稳定选 ra ----
  {
    const seq = assertDecoupled('priority', { strategy: 'priority' });
    ok('priority：稳定选 ra（无轮换）', seq.every((s) => s.startsWith('ra')), seq.join('|'));
  }

  // ---- round-robin：真·轮换起点 ----
  {
    const seq = assertDecoupled('round-robin', { strategy: 'round-robin' });
    ok('round-robin：候选首位确实在 ra/rb 之间轮换（不是永远同一家）',
      new Set(seq.map((s) => s.split('>')[0])).size === 2, seq.join('|'));
  }

  // ---- weighted：同权重平滑加权 ----
  {
    const seq = assertDecoupled('weighted', { strategy: 'weighted' }, {}, null);
    ok('weighted：两家同权重都真的参与', new Set(seq.map((s) => s.split('>')[0])).size === 2, seq.join('|'));
  }

  // ---- least-loaded：未开渠道级限流 -> 退化为 priority（只警告一次） ----
  {
    const seq = assertDecoupled('least-loaded(退化)', { strategy: 'least-loaded' });
    ok('least-loaded(退化)：退化为 priority 排序（首选 ra）', seq.every((s) => s.startsWith('ra')), seq.join('|'));
  }

  // ---- least-loaded：开渠道级限流 + rb 有在途 -> 在途少的 ra 优先 ----
  {
    const setup = (m) => { m.inFlightOf = (name) => (name === 'rb' ? 3 : 0); };
    const seq = assertDecoupled('least-loaded(限流开)', { strategy: 'least-loaded', maxConcurrentPerChannel: 4 }, {}, setup);
    ok('least-loaded(限流开)：在途为 0 的 ra 排首位', seq.every((s) => s.startsWith('ra')), seq.join('|'));
  }

  // ---- sticky：上次成功的 rb 被钉在最前，且不受 Key 游标影响 ----
  {
    const setup = (m) => m.rememberSuccess('rr-model', 'rb');
    const seq = assertDecoupled('sticky', { strategy: 'round-robin', sticky: true }, {}, setup);
    ok('sticky：rb 被钉在候选首位（跨请求粘性生效）', seq.every((s) => s.startsWith('rb')), seq.join('|'));
  }

  // ---- sessionAffinity：该代理上次成功的 rb 优先 ----
  {
    const setup = (m) => m.rememberAffinity('agent-1', 'rb');
    const seq = assertDecoupled('sessionAffinity', { strategy: 'round-robin', sessionAffinity: true }, { agentId: 'agent-1' }, setup);
    ok('sessionAffinity：该代理上次成功的 rb 被提到首位', seq.every((s) => s.startsWith('rb')), seq.join('|'));
  }

  // ---- 两段式 tiered：preferred 永远排在 fallback 之前 ----
  {
    const routing = { strategy: 'round-robin', tiered: true };
    const multi = runSeq({ routing, channels: [A_MULTI, B_FALLBACK], n: 8 }).seq;
    const single = runSeq({ routing, channels: [A_SINGLE, B_FALLBACK], n: 8 }).seq;
    const advanced = runSeq({ routing, channels: [A_MULTI, B_FALLBACK], advance: 11, n: 8 }).seq;
    ok('tiered：多 Key 与单 Key 对照的渠道顺序逐位一致', multi.join('|') === single.join('|'),
      `multi=${multi.join('|')} single=${single.join('|')}`);
    ok('tiered：Key 游标推进后渠道顺序不变', multi.join('|') === advanced.join('|'),
      `before=${multi.join('|')} after=${advanced.join('|')}`);
    ok('tiered：preferred(ra) 整体排在 fallback(rb) 之前',
      multi.every((s) => s.indexOf('ra') < s.indexOf('rb')), multi.join('|'));
  }

  // ---- 直接验证：takeNextRequestKey 只动本条渠道游标，不碰渠道级状态 ----
  {
    const m = mgrWith({ strategy: 'round-robin' }, [A_MULTI, B]);
    const ra = m.channels.find((c) => c.name === 'ra');
    const rb = m.channels.find((c) => c.name === 'rb');
    const before = { turn: m.turn, raRr: ra.rr, rbRr: rb.rr };
    for (let i = 0; i < 9; i += 1) ra.takeNextRequestKey();
    ok('takeNextRequestKey：只推进本条渠道的 Key 游标（ra.nextKeyIndex=9%5=4）',
      ra.nextKeyIndex === 4, String(ra.nextKeyIndex));
    ok('takeNextRequestKey：不触碰 Channel B 的游标', rb.nextKeyIndex === 0, String(rb.nextKeyIndex));
    ok('takeNextRequestKey：不触碰渠道级轮询游标 / 渠道 rr 计数',
      m.turn === before.turn && ra.rr === before.raRr && rb.rr === before.rbRr,
      JSON.stringify({ before, turn: m.turn, raRr: ra.rr, rbRr: rb.rr }));
  }
} catch (err) {
  console.error('UNIT ERROR', err);
  fail += 1;
} finally {
  rmSync(TMP, { force: true });
}

// ============================================================================
// HTTP 级：主配置（A=5 Key rotate-429）vs 对照配置（A=单 Key）渠道序列一致
// ============================================================================
const STATIC = path.join(HERE, 'multikey-rotate-routing.test.json');
const CTRL_TMP = path.join(HERE, 'multikey-rotate-routing.ctrl.tmp.json');
const PORT = await freePort();
const PORT_CTRL = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(STATIC, { port: PORT, mockBase: mp.base });

// 对照配置：把 Channel ra 换成单 Key，其余逐位相同
{
  const raw = JSON.parse(readFileSync(STATIC, 'utf8'));
  raw.channels = raw.channels.map((c) => (c.name === 'ra'
    ? {
      name: c.name, protocol: c.protocol, baseUrl: c.baseUrl, apiKey: c.apiKeys[0],
      model: c.model, priority: c.priority, tier: c.tier,
    }
    : c));
  writeFileSync(CTRL_TMP, JSON.stringify(raw), 'utf8');
}
const CFG_CTRL = materializeConfig(CTRL_TMP, { port: PORT_CTRL, mockBase: mp.base });

async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i += 1) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}

async function callOne(port) {
  const res = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'rr-model', messages: [{ role: 'user', content: 'hi' }], max_tokens: 20 }),
  });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return {
    status: res.status,
    channel: res.headers.get('x-gateway-channel'),
    key: res.headers.get('x-gateway-key'),
    keyStrategy: res.headers.get('x-gateway-key-strategy'),
    content: json?.choices?.[0]?.message?.content || '',
  };
}

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gwMain = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
const gwCtrl = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG_CTRL, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await waitReady(`http://127.0.0.1:${PORT_CTRL}/health`);
  await wait(300);

  const N = 8;
  const main = [];
  const ctrl = [];
  for (let i = 0; i < N; i += 1) {
    main.push(await callOne(PORT));
    ctrl.push(await callOne(PORT_CTRL));
  }

  const mainSeq = main.map((r) => r.channel);
  const ctrlSeq = ctrl.map((r) => r.channel);
  ok('HTTP：主配置与对照配置全部请求成功', [...main, ...ctrl].every((r) => r.status === 200),
    [...main, ...ctrl].map((r) => r.status).join(','));
  ok(`HTTP：渠道选择节奏与"ra 单 Key"对照逐位一致（${N} 次请求）`,
    mainSeq.join(',') === ctrlSeq.join(','), `main=${mainSeq.join(',')} ctrl=${ctrlSeq.join(',')}`);
  ok('HTTP：两家渠道都真的被用到（序列含 ra 与 rb）',
    new Set(mainSeq).size === 2, mainSeq.join(','));
  ok('HTTP：ra 内部确实在做 Key 轮转（出现多个不同的 x-gateway-key 序号）',
    new Set(main.filter((r) => r.channel === 'ra').map((r) => r.key)).size >= 2,
    main.filter((r) => r.channel === 'ra').map((r) => r.key).join(','));
  ok('HTTP：ra 命中时回 x-gateway-key-strategy=rotate-429，rb 命中时不回 Key 头',
    main.filter((r) => r.channel === 'ra').every((r) => r.keyStrategy === 'rotate-429')
    && main.filter((r) => r.channel === 'rb').every((r) => r.key === null),
    JSON.stringify(main.map((r) => ({ ch: r.channel, key: r.key, s: r.keyStrategy }))));
  // 注意：mock 上游故意把 key 名回显进**响应体**（便于测试识别是哪个 key 服务的），
  // 那不是网关泄露。这里只校验网关自己加的**响应头**不含 Key 明文。
  ok('HTTP：网关响应头不泄露 Key 明文（只回序号/总数/策略）',
    main.every((r) => !/sk-fast-c[0-9]/i.test(`${r.key}|${r.keyStrategy}`)) && main.every((r) => /^(\d+\/\d+|null)$/.test(String(r.key))),
    JSON.stringify(main.map((r) => ({ ch: r.channel, key: r.key, s: r.keyStrategy }))));
  ok('HTTP：对照配置（ra 单 Key）不产生叠 Key 响应头',
    ctrl.every((r) => (r.channel === 'ra' ? r.key === null && r.keyStrategy === null : true)),
    JSON.stringify(ctrl.map((r) => ({ ch: r.channel, key: r.key, s: r.keyStrategy }))));
} catch (err) {
  console.error('HTTP ERROR', err);
  fail += 1;
} finally {
  gwMain.kill();
  gwCtrl.kill();
  mock.kill();
  rmSync(CTRL_TMP, { force: true });
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
