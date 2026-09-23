// 无上限重试测试：retryMaxWaitMs=0（或未配置）= 不设累计上限，固定 retryWaitMs 一直重试，
// 直到成功或客户端断开（旧语义：0 被当成"立即超限"，马上返回 503）
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
const CFG = materializeConfig(path.join(HERE, 'nocap.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  for (let i = 0; i < 50; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) break; } catch {}
    await wait(200);
  }
  await wait(300);

  // 唯一渠道永远 429：无上限时网关应每 retryWaitMs 重试一轮，不返回 503，直到客户端断开
  const ac = new AbortController();
  const p = fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'flaky-429', messages: [{ role: 'user', content: 'hi' }] }),
    signal: ac.signal,
  });
  const outcome = await Promise.race([
    p.then(async (r) => 'got-' + r.status).catch((e) => (e.name === 'AbortError' ? 'aborted' : 'err-' + e.message)),
    wait(1200).then(() => 'pending'),
  ]);
  ok('retryMaxWaitMs=0：1.2s 内不返回 503，持续固定间隔重试', outcome === 'pending', `outcome=${outcome}`);

  ac.abort(); // 客户端断开 -> 网关应停止循环
  await p.catch(() => {});
  await wait(500);
  const h = await fetch(`http://127.0.0.1:${PORT}/health`);
  ok('客户端断开后网关仍存活', h.ok, `health=${h.status}`);
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