// 思考档位（maxEffort）回归：目标档位解析 / per-channel 覆盖 / 客户端显式值尊重 /
// 上游拒绝档位时同渠道降档重试（不打穿渠道链）
//
// 背景（真实上游实测，2026-09-15）：
//   - 各家普遍接受的最高档是 xhigh，非法值 max 不在档位表里：
//     sensenova 原文 "field ReasoningEffort invalid, should be one of: low, medium, high, xhigh, none"
//   - xhigh 不是免费的：wxctf 开 xhigh 会把 max_tokens 全烧在思考上、正文返回空 -> 该渠道 pin 到 high
// 因此需要：全局默认档位 + 单渠道覆盖 + 配错时自动降档兜底。
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 上游端口都改成运行时动态分配，避免并行跑测试时抢端口。
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'effort-levels.test.json'), { port: PORT, mockBase: mp.base });
const MOCK = mp.url(9143);

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch { /* retry */ }
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}
async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', 'content-type': 'application/json', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
}
const chat = (body) => call('/v1/chat/completions', { method: 'POST', body: JSON.stringify(body) });
const seenEfforts = async () => (await (await fetch(MOCK + '/_seen')).json()).seen;
const contentOf = (r) => r.json?.choices?.[0]?.message?.content || '';

// ---------- 单元：档位拒绝识别 ----------
{
  const { isEffortRejection, MAX_EFFORT, EFFORT_FALLBACK } = await import('../lib/util.mjs');
  ok('单元：默认档位为 high', MAX_EFFORT === 'high', MAX_EFFORT);
  ok('单元：降档目标为 high', EFFORT_FALLBACK === 'high', EFFORT_FALLBACK);
  ok('单元：识别 sensenova 档位报错',
    isEffortRejection('HTTP 400 field ReasoningEffort invalid, should be one of: low, medium, high, xhigh, none'));
  ok('单元：识别下划线写法 reasoning_effort',
    isEffortRejection('reasoning_effort is not supported by this model'));
  ok('单元：识别中文报错（字段名仍带 ReasoningEffort）',
    isEffortRejection('思考强度 ReasoningEffort 不支持该取值'));
  ok('单元：不误伤无关 400',
    !isEffortRejection('model not found') && !isEffortRejection('max_tokens must be greater than 2'));
  ok('单元：不误伤档位无关的 invalid 报错',
    !isEffortRejection('invalid api key provided'));
}

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(400);

  // ---- ① 未配 maxEffort 的渠道：跟随 routing.maxEffort=high ----
  await fetch(MOCK + '/_reset');
  const r1 = await chat({ model: 'think-plain', messages: [{ role: 'user', content: 'hi' }] });
  ok('① 默认渠道注入 routing.maxEffort=high', r1.status === 200 && contentOf(r1) === 'effort=high', contentOf(r1));

  // ---- ② per-channel 覆盖：maxEffort=xhigh 的渠道拿到 xhigh ----
  await fetch(MOCK + '/_reset');
  const r2 = await chat({ model: 'think-xhigh', messages: [{ role: 'user', content: 'hi' }] });
  ok('② 渠道 maxEffort=xhigh 覆盖全局 high', r2.status === 200 && contentOf(r2) === 'effort=xhigh', contentOf(r2));

  // ---- ③ per-channel 覆盖：显式 pin 到 high 的渠道保持 high ----
  await fetch(MOCK + '/_reset');
  const r3 = await chat({ model: 'think-pin', messages: [{ role: 'user', content: 'hi' }] });
  ok('③ 渠道 maxEffort=high 不被拉高', r3.status === 200 && contentOf(r3) === 'effort=high', contentOf(r3));

  // ---- ④ 客户端显式给不低于目标的档位：原样尊重，不降档 ----
  await fetch(MOCK + '/_reset');
  const r4 = await chat({ model: 'think-plain', reasoning_effort: 'xhigh', messages: [{ role: 'user', content: 'hi' }] });
  ok('④ 客户端显式 xhigh 在 high 目标渠道上被尊重', r4.status === 200 && contentOf(r4) === 'effort=xhigh', contentOf(r4));

  // ---- ⑤ 非思考模型：不注入任何档位 ----
  await fetch(MOCK + '/_reset');
  const r5 = await chat({ model: 'qwen3.8-flash', messages: [{ role: 'user', content: 'hi' }] });
  ok('⑤ 非思考模型不注入档位', r5.status === 200 && contentOf(r5) === 'effort=none', contentOf(r5));

  // ---- ⑥ 上游拒绝 xhigh：同渠道降档 high 重试一次，请求仍然成功 ----
  await fetch(MOCK + '/_reset');
  const r6 = await chat({ model: 'think-reject', messages: [{ role: 'user', content: 'hi' }] });
  ok('⑥ 上游拒绝 xhigh 后同渠道降档成功', r6.status === 200 && contentOf(r6) === 'effort=high',
    `status=${r6.status} ${r6.text.slice(0, 200)}`);
  const seen6 = await seenEfforts();
  ok('⑥ 上游按顺序先收到 xhigh 再收到 high',
    seen6.length === 2 && seen6[0].effort === 'xhigh' && seen6[1].effort === 'high',
    JSON.stringify(seen6));
  const st6 = await call('/api/status');
  const chReject = (st6.json?.channels || []).find((c) => c.name === 'ch-reject');
  ok('⑥ 降档重试不算渠道失败（不计熔断）', (chReject?.failed ?? 0) === 0 && chReject?.coolingDown !== true,
    `failed=${chReject?.failed} cooling=${chReject?.coolingDown}`);
  ok('⑥ 降档后渠道仍健康', chReject?.healthy === true, JSON.stringify(chReject?.healthy));

  // ---- ⑦ 状态面板暴露 maxEffort ----
  const chX = (st6.json?.channels || []).find((c) => c.name === 'ch-xhigh');
  const chD = (st6.json?.channels || []).find((c) => c.name === 'ch-default');
  ok('⑦ /api/status 暴露渠道 maxEffort', chX?.maxEffort === 'xhigh', JSON.stringify(chX?.maxEffort));
  ok('⑦ 未覆盖的渠道 maxEffort 为空（跟随全局）', chD?.maxEffort === null, JSON.stringify(chD?.maxEffort));

  // ---- ⑧ Anthropic 原生路径：xhigh 折算 thinking budget（与 high 同为上限 32768）----
  const { anthropicAdapter } = await import('../lib/adapters/anthropic.mjs');
  const fakeChannel = { apiKey: 'k', baseUrl: 'https://api.anthropic.com/v1', protocol: 'anthropic' };
  const builtHigh = anthropicAdapter.buildRequest({
    channel: fakeChannel, model: 'claude-sonnet-4', stream: false,
    body: { model: 'claude-sonnet-4', messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'high' },
  });
  const builtXhigh = anthropicAdapter.buildRequest({
    channel: fakeChannel, model: 'claude-sonnet-4', stream: false,
    body: { model: 'claude-sonnet-4', messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'xhigh' },
  });
  ok('⑧ high -> thinking budget 32768', builtHigh.payload.thinking?.budget_tokens === 32768,
    JSON.stringify(builtHigh.payload.thinking));
  ok('⑧ xhigh -> thinking budget 不超 high 上限', builtXhigh.payload.thinking?.budget_tokens === 32768,
    JSON.stringify(builtXhigh.payload.thinking));
  ok('⑧ 开思考时 max_tokens 联动抬到 budget 之上', builtXhigh.payload.max_tokens > builtXhigh.payload.thinking.budget_tokens,
    `max_tokens=${builtXhigh.payload.max_tokens}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  // Windows/Node 竞态规避：kill 子进程后立即 process.exit() 会让 undici 池里指向已终止
  // 网关的死连接与 libuv 关闭流程竞态（async.c:76 断言，0xC0000409）。等连接错误传播完再退。
  await wait(500);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
