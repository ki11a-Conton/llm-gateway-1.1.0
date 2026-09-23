// C1 / C2 / D2 回归：选路韧性
//
// C1：被冷却的渠道在探活成功后被"救回"候选池（探活必须按 chat 可用性判断，不是 models）。
// C2：优先池（preferred）全挂时，同一轮内立刻降级到随机池，不白等一个 retryWaitMs。
// D2：流式"只发一帧就断流"时，响应头尚未发出 -> 允许整体换家；
//     反之若已下发过数据（响应头已发出）-> 绝不能换家（否则会往同一个 res 二次 writeHead）。
// 另外覆盖 B2 的 /api/tasks?since/until 时间窗。
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync, rmSync } from 'node:fs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 上游端口都改成运行时动态分配，避免并行跑测试时抢端口。
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'resilience.test.json'), { port: PORT, mockBase: mp.base });
const LOG_DIR = path.join(ROOT, 'logs-test');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

if (existsSync(LOG_DIR)) rmSync(LOG_DIR, { recursive: true, force: true });

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });

async function waitReady(tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch { /* retry */ }
    await wait(200);
  }
  throw new Error('gateway not ready');
}
const call = async (pathname, opts = {}) => {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
};
const chat = (body) => call('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});
const channelOf = async (name) => ((await call('/api/status')).json.channels || []).find((c) => c.name === name);

try {
  await waitReady();
  await wait(400);

  // ===== C1：冷却渠道被探活救回 =====
  await fetch(mp.url(9133, '/_kill'));
  const rC1 = await chat({ model: 'revive-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('C1 余额不足渠道被换家，请求成功', rC1.status === 200, `status=${rC1.status} ${rC1.text.slice(0, 160)}`);
  ok('C1 由健康兜底渠道完成', rC1.headers.get('x-gateway-channel') === 'revive-ok', String(rC1.headers.get('x-gateway-channel')));
  let rev = await channelOf('revive-1');
  ok('C1 该渠道进入冷却', rev?.coolingDown === true, `coolingDown=${rev?.coolingDown}`);
  ok('C1 该渠道 tier 正确（随机池）', rev?.tier === 'fallback', String(rev?.tier));

  // mock 变健康 -> 探活应把它救回来
  await fetch(mp.url(9133, '/_revive'));
  const probe = await call('/api/probe', { method: 'POST' });
  ok('C1 POST /api/probe 成功', probe.status === 200 && probe.json?.ok === true, probe.text.slice(0, 120));
  rev = await channelOf('revive-1');
  ok('C1 探活成功解除熔断', rev?.coolingDown === false && rev?.healthy === true,
    `coolingDown=${rev?.coolingDown} healthy=${rev?.healthy}`);
  ok('C1 失败计数被清零（uncool）', rev?.failures === 0, `failures=${rev?.failures}`);

  // 救回后应重新被选为候选
  const rC1b = await chat({ model: 'revive-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('C1 救回后重新成为候选并被使用',
    rC1b.status === 200 && rC1b.headers.get('x-gateway-channel') === 'revive-1',
    `status=${rC1b.status} ch=${rC1b.headers.get('x-gateway-channel')}`);

  // ===== C2：优先池全挂 -> 同轮落随机池 =====
  const t0 = Date.now();
  const rC2 = await chat({ model: 'tier-fb-model', messages: [{ role: 'user', content: 'hi' }] });
  const c2ms = Date.now() - t0;
  ok('C2 优先池全挂后请求仍成功', rC2.status === 200, `status=${rC2.status} ${rC2.text.slice(0, 160)}`);
  ok('C2 最终由随机池渠道完成', rC2.headers.get('x-gateway-channel') === 'fb-ok', String(rC2.headers.get('x-gateway-channel')));
  ok('C2 响应头标注 tier=fallback', rC2.headers.get('x-gateway-tier') === 'fallback', String(rC2.headers.get('x-gateway-tier')));
  // 同轮降级：没有等 retryWaitMs(300ms) 再重来一轮（否则 retryLoop=false 会直接失败）
  ok('C2 同轮降级（耗时 < 250ms，没有白等一轮）', c2ms < 250, `${c2ms}ms`);

  await wait(500);
  const tasksC2 = await call('/api/tasks?limit=20');
  const recC2 = (tasksC2.json?.recent || []).find((r) => r.model === 'tier-fb-model');
  ok('C2 任务日志记录了该请求', !!recC2, JSON.stringify((tasksC2.json?.recent || []).map((r) => r.model)));
  const chain = (recC2?.attempts || []).map((a) => a.channel);
  ok('C2 尝试链里优先池渠道排在前', chain.indexOf('pref-dead-1') === 0 && chain.includes('pref-dead-2'), JSON.stringify(chain));
  ok('C2 只走了一轮（rounds=1）', recC2?.rounds === 1, String(recC2?.rounds));

  // ===== D2a：只发一帧就断流（响应头未发出）-> 允许换家 =====
  const rD2a = await chat({ model: 'break-model', messages: [{ role: 'user', content: 'hi' }], stream: true });
  ok('D2a 断流渠道被换家，请求成功', rD2a.status === 200, `status=${rD2a.status} ${rD2a.text.slice(0, 160)}`);
  ok('D2a 由健康渠道完成', rD2a.headers.get('x-gateway-channel') === 'break-ok', String(rD2a.headers.get('x-gateway-channel')));
  ok('D2a 客户端拿到完整流（含 [DONE]）', rD2a.text.includes('[DONE]') && /你好/.test(rD2a.text), JSON.stringify(rD2a.text.slice(-120)));

  // ===== D2b：已下发过数据（响应头已发出）-> 绝不能换家 =====
  await fetch(mp.url(9137, '/_reset'));
  const rD2b = await chat({ model: 'latebreak-model', messages: [{ role: 'user', content: 'hi' }], stream: true });
  ok('D2b 已下发数据的断流：客户端仍拿到 HTTP 200', rD2b.status === 200, `status=${rD2b.status}`);
  ok('D2b 响应头回传的是断流渠道本身', rD2b.headers.get('x-gateway-channel') === 'latebreak-1', String(rD2b.headers.get('x-gateway-channel')));
  ok('D2b 客户端确实收到了断流前已下发的数据', rD2b.text.includes('AAAA'), JSON.stringify(rD2b.text.slice(0, 120)));
  ok('D2b 没有把兜底渠道的内容拼进来', !rD2b.text.includes('from late fallback'), JSON.stringify(rD2b.text.slice(-160)));
  const lateHits = (await (await fetch(mp.url(9137, '/_hits'))).json()).hits;
  ok('D2b 兜底渠道一次都没被尝试（绝不换家）', lateHits === 0, `hits=${lateHits}`);

  await wait(500);
  const tasksD2 = await call('/api/tasks?limit=30');
  const recD2b = (tasksD2.json?.recent || []).find((r) => r.model === 'latebreak-model');
  ok('D2b 任务日志标记了"响应已开始"的失败',
    recD2b?.channelError === true || recD2b?.kind === 'stream_break',
    JSON.stringify({ channelError: recD2b?.channelError, kind: recD2b?.kind }));

  // ===== B2：/api/tasks 时间窗 =====
  const future = new Date(Date.now() + 3600_000).toISOString();
  const past = new Date(Date.now() - 3600_000).toISOString();
  const winFuture = await call(`/api/tasks?since=${encodeURIComponent(future)}`);
  ok('B2 /api/tasks?since=未来 返回空', Array.isArray(winFuture.json?.recent) && winFuture.json.recent.length === 0,
    `len=${winFuture.json?.recent?.length}`);
  const winPast = await call(`/api/tasks?until=${encodeURIComponent(future)}`);
  ok('B2 /api/tasks?until=未来 返回全部', (winPast.json?.recent?.length ?? 0) >= 1, `len=${winPast.json?.recent?.length}`);
  const winRange = await call(`/api/tasks?since=${encodeURIComponent(past)}&until=${encodeURIComponent(future)}`);
  ok('B2 /api/tasks 时间窗区间内非空', (winRange.json?.recent?.length ?? 0) >= 1, `len=${winRange.json?.recent?.length}`);
  ok('B2 响应回显时间窗参数', winRange.json?.since === past && winRange.json?.until === future,
    JSON.stringify({ since: winRange.json?.since, until: winRange.json?.until }));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  await wait(400);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
