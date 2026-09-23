// 路由策略 v2 测试：
//  ① 跨请求粘性：同一逻辑模型连续两次请求走同一个成功渠道（round-robin 下本应轮换）
//  ② 非 429 可重试错误：同渠道等 retryWaitMs 后重试一次，成功仍走该渠道
//  ③ 429/TPM：不等待、直接换下一个渠道
//  ④ 单元级：candidatesFor 的粘性排序与失效条件（失败计数 / 冷却）
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../lib/channels.mjs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const CFG = path.join(HERE, 'routing-policy.test.json');
// P5：网关端口与 mock 上游端口都改成运行时动态分配，避免并行跑测试时抢端口。
// 上面的单元用例只读渠道排序，与端口无关，继续用静态配置；下面起网关用平移后的临时配置。
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const GW_CFG = materializeConfig(CFG, { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 单元级：粘性排序（不依赖服务器）----
{
  const mgr = new ChannelManager(CFG);
  mgr.load();
  const first = (m) => mgr.candidatesFor(m)[0]?.name;
  // 优先池内：priority 生效
  ok('无粘性时按优先级排序（burn p10 最前）', first('auto') === 'burn', `first=${first('auto')}`);

  mgr.rememberSuccess('auto', 'g2');
  ok('粘性：成功过的渠道排最前（g2 覆盖优先级）', first('auto') === 'g2', `first=${first('auto')}`);

  const g2ch = mgr.channels.find((c) => c.name === 'g2');
  g2ch.markFailure('server_error', 'boom');
  ok('粘性失效：渠道有失败记录后不再优先', first('auto') !== 'g2', `first=${first('auto')}`);

  mgr.rememberSuccess('auto', 'burn');
  const burnCh = mgr.channels.find((c) => c.name === 'burn');
  burnCh.cool(60000);
  ok('粘性失效：渠道冷却中不再优先', first('auto') !== 'burn', `first=${first('auto')}`);

  // 两段选路：优先池永远排在随机池前面；随机池内部每次顺序打乱
  const tiers = mgr.candidatesFor('auto').map((c) => c.tier);
  const firstFallback = tiers.indexOf('fallback');
  ok('优先池整体排在随机池之前', firstFallback === -1 || tiers.slice(0, firstFallback).every((t) => t === 'preferred'), tiers.join(','));

  const orders = new Set();
  for (let i = 0; i < 40; i += 1) {
    orders.add(mgr.candidatesFor('auto').map((c) => c.name).join('>'));
  }
  ok('随机池每次顺序不同（随机路由生效）', orders.size > 1, `不同顺序数=${orders.size}`);
}

// ---- 集成级：起假上游 + 网关 ----
const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', GW_CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  for (let i = 0; i < 50; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) break; } catch {}
    await wait(200);
  }
  await wait(300);

  const call = async (model) => {
    const t0 = Date.now();
    const res = await fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
      body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }], max_tokens: 10 }),
    });
    const text = await res.text();
    return { status: res.status, elapsed: Date.now() - t0, channel: res.headers.get('x-gateway-channel'), text };
  };

  // ① 粘性（先跑，此时 rr 计数全为 0，保证确定性）：
  //   g1-model 只挂在优先池的 g1/g2 上，随机池不参与本用例
  const s1 = await call('g1-model');
  ok('粘性R1：走 g1', s1.status === 200 && s1.channel === 'g1', `status=${s1.status} ch=${s1.channel} ${s1.text.slice(0, 120)}`);
  const s2 = await call('g1-model');
  ok('粘性R2：仍走 g1（跨请求粘性，未被轮询轮走）', s2.status === 200 && s2.channel === 'g1', `status=${s2.status} ch=${s2.channel}`);
  ok('粘性R2：直走粘性渠道，无重试等待（<1000ms）', s2.elapsed < 1000, `elapsed=${s2.elapsed}ms`);

  // ② 同渠道重试：r-bad 首次 500 -> 等 retryWaitMs(2000) -> 同一渠道重试成功
  //    9111 的 "第一次 500" 是进程级一次性计数，必须显式复位，
  //    否则同一 mock 进程被复用（或上次测试残留）时本用例会静默失效
  await fetch(mp.url(9111, '/_reset-500v2'));
  const b = await call('rbad-model');
  ok('重试：成功走的是 r-bad（同渠道，不是跳过的 g1）', b.status === 200 && b.channel === 'r-bad', `status=${b.status} ch=${b.channel} ${b.text.slice(0, 120)}`);
  ok('重试：确实等了 retryWaitMs（elapsed >= 1500ms）', b.elapsed >= 1500, `elapsed=${b.elapsed}ms`);

  // ③ 429 直接换下一个：t-429 命中限流后不等待直接走 g1
  const a = await call('t429-model');
  ok('429跳过：最终走 g1', a.status === 200 && a.channel === 'g1', `status=${a.status} ch=${a.channel} ${a.text.slice(0, 120)}`);
  ok('429跳过：没有重试等待（elapsed < 1000ms）', a.elapsed < 1000, `elapsed=${a.elapsed}ms`);

  // ④ 统一池 sanity：auto 经过 burn(500×2->熔断) t-429(429->冷却) 后由 r-bad/g1 成功
  const u = await call('auto');
  ok('统一池 auto 最终成功', u.status === 200, `status=${u.status} ${u.text.slice(0, 160)}`);
  ok('统一池成功渠道在池内', ['r-bad', 'g1', 'g2'].includes(u.channel), `ch=${u.channel}`);
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