// 供应商钉选（sticky provider）回归测试
//
// 用户要求："一个 provider 的模型成功调用了，就反复调用直至调用失败；
//  失败了之后重试两次，如果都失败才切换到下一个 provider。"
//
// 对应实现：
//   - channels.mjs #orderFallback：lastGood(model) 健康时钉在随机池最前（sticky:true 开启）
//   - proxy.mjs：随机池每家预算 fallbackAttempts（默认 3 = 1 首次 + 2 次重试）
//   - 失败后渠道 failures>0 -> 粘性自动失效，换下一家
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
const CFG = materializeConfig(path.join(HERE, 'sticky.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

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
const call = async (pathname) => {
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { headers: { authorization: 'Bearer TESTKEY' } });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
};
const chat = async (body) => {
  const res = await fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
};
const chStat = (snap, name) => (snap.channels || []).find((c) => c.name === name);

try {
  await waitReady();
  await wait(400);

  // ===== ① 成功后反复复用同一家（连续 5 次都钉在 pin-a）=====
  await fetch(mp.url(9106, '/_reset-flaky?fail=0'));
  const vias = [];
  for (let i = 0; i < 5; i += 1) {
    const r = await chat({ model: 'pin-model', messages: [{ role: 'user', content: 'hi' }] });
    vias.push(r.headers.get('x-gateway-channel'));
    ok(`① 第${i + 1}次请求成功`, r.status === 200, `status=${r.status}`);
  }
  ok('① 成功后被钉住：5 次全部复用 pin-a', vias.every((v) => v === 'pin-a'), vias.join(','));

  // ===== ② 失败 -> 重试 2 次（共 3 次）后换下一家 =====
  const snap0 = (await call('/api/status')).json;
  const before = chStat(snap0, 'pin-a')?.failed ?? 0;
  await fetch(mp.url(9106, '/_reset-flaky?fail=99')); // 从此永远 429
  const t0 = Date.now();
  const r2 = await chat({ model: 'pin-model', messages: [{ role: 'user', content: 'hi' }] });
  const ms = Date.now() - t0;
  ok('② 坏掉的钉选渠道：请求仍成功（换家兜底，不挂住）', r2.status === 200, `status=${r2.status} ${ms}ms`);
  ok('② 切换到了下一家 provider pin-b', r2.headers.get('x-gateway-channel') === 'pin-b', String(r2.headers.get('x-gateway-channel')));

  const snap1 = (await call('/api/status')).json;
  const after = chStat(snap1, 'pin-a')?.failed ?? 0;
  ok('② 钉选渠道本次恰好被尝试 3 次（1 首次 + 2 重试）才放弃', after - before === 3, `delta=${after - before}`);
  ok('② 换家耗时含 2 次重试等待（>=100ms）', ms >= 100, `${ms}ms`);

  // ===== ③ 粘性已失效：下一次不会继续钉 pin-a 成功路（pin-a 已坏）=====
  const r3 = await chat({ model: 'pin-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('③ 失败过的渠道不再被钉住为"成功渠道"', r3.headers.get('x-gateway-channel') === 'pin-b', String(r3.headers.get('x-gateway-channel')));

  // ===== ④ pin-a 恢复后重新钉住（钉选记录只记"最近成功"）=====
  await fetch(mp.url(9106, '/_reset-flaky?fail=0'));
  // pin-b 现在是成功渠道（prio 1 也靠前者），先让它失败一次以让位：改用恢复后的 pin-a 观察钉选转移
  const vias2 = [];
  for (let i = 0; i < 3; i += 1) {
    const r = await chat({ model: 'pin-model', messages: [{ role: 'user', content: 'hi' }] });
    vias2.push(r.headers.get('x-gateway-channel'));
  }
  ok('④ 钉选渠道稳定复用同一家（无随机跳家）', new Set(vias2).size === 1, vias2.join(','));
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
