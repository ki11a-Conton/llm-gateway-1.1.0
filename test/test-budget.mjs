// 路由策略 v3（严格优先级 + 每渠道固定重试预算）测试：
//  ① 预算 6 耗尽后降级到下一个优先级渠道
//  ② 429/TPM 也计入预算（retries 3 -> 3 次后换）
//  ③ 预算内多次重试后成功（前 5 次 500，第 6 次成功）
//  ④ 全部渠道预算用尽后回到最高优先级循环（跨轮累计预算）
//  单元：严格优先级排序不被冷却插队
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../lib/channels.mjs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const CFG = path.join(HERE, 'budget.test.json');
// P5：网关端口与 mock 上游端口都改成运行时动态分配，避免并行跑测试时抢端口。
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

// ---- 单元：严格优先级排序 ----
{
  const mgr = new ChannelManager(CFG);
  mgr.load();
  const pA = mgr.channels.find((c) => c.name === 'pA');
  const names = mgr.candidatesFor('dead-model').map((c) => c.name);
  ok('严格优先级：pA(p1) 在 pB(p2) 前', names[0] === 'pA' && names[1] === 'pB', names.join(','));
  pA.cool(60000);
  const names2 = mgr.candidatesFor('dead-model').map((c) => c.name);
  // 冷却是"让位"而不是"隐身"：冷却中的渠道排到健康渠道之后，但仍留在候选列表里做半开试探。
  // 旧断言曾要求 pA 在冷却中仍排最前（"冷却不插优先级队"）——那等于每个请求都先撞一次已知故障的
  // 渠道再去试好的（线上实测 22% 的请求是这样白撞一次），既费时又浪费上游配额，而且与
  // README §2.3「熔断中的排最后（仍会做半开试探）」和 candidatesFor 文档「冷却后自然让位」都相反。
  ok('严格优先级：冷却中的 pA 让位给健康的 pB（不再先撞故障家）', names2[0] === 'pB', names2.join(','));
  ok('冷却渠道仍留在候选里（保留半开试探，不是剔除）', names2.includes('pA') && names2.indexOf('pA') > names2.indexOf('pB'), names2.join(','));
  ok('渠道默认 retries=2', new ChannelManager(CFG).load().channels.find((c) => c.name === 'pB').retries === 2);
}

// ---- 集成 ----
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
      body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }] }),
    });
    const text = await res.text();
    return { status: res.status, elapsed: Date.now() - t0, channel: res.headers.get('x-gateway-channel'), text };
  };
  const status = async () => (await fetch(`http://127.0.0.1:${PORT}/api/status`, { headers: { authorization: 'Bearer TESTKEY' } })).json();
  const ch = (s, n) => (s.channels || []).find((c) => c.name === n);

  // ① 预算 6 耗尽 -> 降级到 pB
  const r1 = await call('dead-model');
  ok('① 预算耗尽降级：pA 6 次全败后走 pB', r1.status === 200 && r1.channel === 'pB', `status=${r1.status} ch=${r1.channel} ${r1.text.slice(0, 120)}`);
  const s1 = await status();
  ok('① pA 被尝试 6 次（failed=6）', ch(s1, 'pA')?.failed === 6, `failed=${ch(s1, 'pA')?.failed}`);
  ok('① 每次间隔 200ms（elapsed >= 900ms）', r1.elapsed >= 900, `${r1.elapsed}ms`);

  // ② 429 计入预算（retries 3）
  const r2 = await call('flaky-429');
  ok('② 429 计入预算：pA429 3 次后走 pB2', r2.status === 200 && r2.channel === 'pB2', `status=${r2.status} ch=${r2.channel} ${r2.text.slice(0, 120)}`);
  const s2 = await status();
  ok('② pA429 被尝试 3 次（failed=3）', ch(s2, 'pA429')?.failed === 3, `failed=${ch(s2, 'pA429')?.failed}`);

  // ③ 预算内多次重试后成功（9118 前 5 次 500）
  await fetch(mp.url(9118, '/_reset-500v3'));
  const r3 = await call('budget-model');
  ok('③ 第 6 次尝试成功走 pC', r3.status === 200 && r3.channel === 'pC', `status=${r3.status} ch=${r3.channel} ${r3.text.slice(0, 120)}`);
  const s3 = await status();
  ok('③ pC 失败 5 次（第 6 次成功）', ch(s3, 'pC')?.failed === 5, `failed=${ch(s3, 'pC')?.failed}`);
  ok('③ 耗时 >= 5×200ms', r3.elapsed >= 900, `${r3.elapsed}ms`);

  // ④ 全部预算用尽 -> 回到最高优先级循环（跨轮：pE 2+2+1 次失败后成功；pF 两轮 × 2 次）
  await fetch(mp.url(9118, '/_reset-500v3'));
  const before = await status(); // failed 计数跨请求累计，用差值断言
  const pEBefore = ch(before, 'pE')?.failed ?? 0;
  const pFBefore = ch(before, 'pF')?.failed ?? 0;
  const r4 = await call('loop-model');
  ok('④ 循环回顶：跨轮耗尽后 pE 第 6 次尝试成功', r4.status === 200 && r4.channel === 'pE', `status=${r4.status} ch=${r4.channel} ${r4.text.slice(0, 120)}`);
  const s4 = await status();
  ok('④ pE 本轮失败 5 次（2+2+1 跨三轮）', (ch(s4, 'pE')?.failed ?? 0) - pEBefore === 5, `diff=${(ch(s4, 'pE')?.failed ?? 0) - pEBefore}`);
  ok('④ pF 本轮被尝试 4 次（两轮 × retries 2）', (ch(s4, 'pF')?.failed ?? 0) - pFBefore === 4, `diff=${(ch(s4, 'pF')?.failed ?? 0) - pFBefore}`);
  ok('④ 跨轮累计等待（elapsed >= 1200ms）', r4.elapsed >= 1200, `${r4.elapsed}ms`);

  // ⑤ 用户场景：sensenova×2 与 b.ai×2 全部 TPM 限流——各用尽 6 次预算（每 3s 一次）后依次降级，
  //    全程不触发冷却/熔断（不降级，保持 healthy），最终由下一个好渠道 ok1 成功
  const t0 = await status();
  const fBefore = {};
  for (const n of ['sA', 'sB', 'bA', 'bB']) fBefore[n] = ch(t0, n)?.failed ?? 0;
  const r5 = await call('tpm-chain');
  ok('⑤ TPM 全链：sA→sB→bA→bB 各 6 次后由 ok1 成功', r5.status === 200 && r5.channel === 'ok1', `status=${r5.status} ch=${r5.channel} ${r5.text.slice(0, 120)}`);
  const s5 = await status();
  ok('⑤ sA 用尽 6 次预算', (ch(s5, 'sA')?.failed ?? 0) - fBefore.sA === 6, `diff=${(ch(s5, 'sA')?.failed ?? 0) - fBefore.sA}`);
  ok('⑤ sB 用尽 6 次预算', (ch(s5, 'sB')?.failed ?? 0) - fBefore.sB === 6, `diff=${(ch(s5, 'sB')?.failed ?? 0) - fBefore.sB}`);
  ok('⑤ bA 用尽 6 次预算', (ch(s5, 'bA')?.failed ?? 0) - fBefore.bA === 6, `diff=${(ch(s5, 'bA')?.failed ?? 0) - fBefore.bA}`);
  ok('⑤ bB 用尽 6 次预算', (ch(s5, 'bB')?.failed ?? 0) - fBefore.bB === 6, `diff=${(ch(s5, 'bB')?.failed ?? 0) - fBefore.bB}`);
  ok('⑤ TPM 不降级：四个渠道全部 healthy 且未冷却', ['sA', 'sB', 'bA', 'bB'].every((n) => { const c = ch(s5, n); return c && c.healthy === true && c.coolingDown === false; }));
  ok('⑤ 每次间隔 200ms（elapsed >= 3800ms）', r5.elapsed >= 3800, `${r5.elapsed}ms`);

  // ⑤b 下一个请求仍从最高优先级 sA 开始（不降级 = 渠道保持榜首，继续先试它）
  const t1 = await status();
  const sABefore2 = ch(t1, 'sA')?.failed ?? 0;
  const r5b = await call('tpm-chain');
  const s5b = await status();
  ok('⑤b 第二请求仍先试 sA（sA 保持最高优先级，不降级）', r5b.status === 200 && r5b.channel === 'ok1' && (ch(s5b, 'sA')?.failed ?? 0) - sABefore2 === 6, `diff=${(ch(s5b, 'sA')?.failed ?? 0) - sABefore2}`);
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