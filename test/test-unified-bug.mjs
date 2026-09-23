// 回归测试：统一池（unifiedModel）候选里，已标记「不支持」和刚被 429 限流的渠道不该被反复重试
// 对应真实场景：渠道返回 404 model not available 后，后续轮次仍被选中烧掉一次尝试；
//                  返回 429 TPM 限流后，下一轮轮转时又被选中再撞一次限流
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 端口块都运行时动态分配
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'bug.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitReady(tries = 50) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch {}
    await wait(200);
  }
  throw new Error('gateway not ready');
}
async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text, headers: res.headers };
}
const chat = () => call('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ model: 'auto', messages: [{ role: 'user', content: 'hi' }], max_tokens: 10 }),
});
const chByName = (list, name) => list.find((c) => c.name === name);

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady();

  // 请求 1：b-404(404) a-429(429) c-good(首次500) 第一轮全失败 -> 第二轮应只走 c-good 成功
  const r1 = await chat();
  ok('第1次请求最终成功', r1.status === 200, r1.text.slice(0, 160));
  ok('成功渠道是 c-good', r1.headers.get('x-gateway-channel') === 'c-good', String(r1.headers.get('x-gateway-channel')));

  const snap1 = (await call('/api/status')).json.channels || [];
  const b404 = chByName(snap1, 'b-404');
  const a429 = chByName(snap1, 'a-429');
  ok('b-404 只被尝试过 1 次（下一轮不再重试不支持的渠道）', b404 && b404.failed === 1, `failed=${b404?.failed}`);
  ok('a-429 只被尝试过 1 次（限流后下一轮跳过）', a429 && a429.failed === 1, `failed=${a429?.failed}`);
  ok('a-429 TPM 限流不触发冷却（渠道不降级，保持健康）', a429 && a429.coolingDown === false && a429.healthy === true, `coolingDown=${a429?.coolingDown} healthy=${a429?.healthy}`);

  // 请求 2：严格优先级下 a-429(p20) 每请求仍先被尝试其预算次数(1 次)，然后 c-good 成功；b-404 已被剔除不再出现
  const r2 = await chat();
  ok('第2次请求成功且走 c-good', r2.status === 200 && r2.headers.get('x-gateway-channel') === 'c-good', r2.text.slice(0, 160));
  const snap2 = (await call('/api/status')).json.channels || [];
  ok('b-404 累计失败数不变（未再被选中）', chByName(snap2, 'b-404')?.failed === 1, `failed=${chByName(snap2, 'b-404')?.failed}`);
  ok('a-429 每请求尝试其预算 1 次（累计 2）', chByName(snap2, 'a-429')?.failed === 2, `failed=${chByName(snap2, 'a-429')?.failed}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  await wait(300);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);