// 测试两层重试：
//   第 1 层 —— 同一渠道最多 attemptsPerChannel 次，每次失败后固定等 retryPerAttemptMs
//   第 2 层 —— 全部渠道都废了，等 retryWaitMs 重新轮询（retryMaxWaitMs=0 时永不放弃）
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

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

// P5：端口全部运行时动态分配（原 8795 网关 + 9102/9104/9106 三个 mock 上游）；
// 每个场景的配置本来就是这个文件自己生成的，现在写到系统临时目录，不再覆盖仓库里的 retry.test.json。
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = path.join(os.tmpdir(), `retry.test-${process.pid}.json`);

async function runScenario(cfg, label, fn) {
  writeFileSync(CFG, JSON.stringify(cfg, null, 2), 'utf8');
  const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
  try {
    await waitReady(`http://127.0.0.1:${PORT}/health`);
    await wait(300);
    await fn();
  } catch (err) {
    console.error(`TEST ERROR [${label}]`, err);
    fail++;
  } finally {
    gw.kill();
    await wait(200);
  }
}

const chat = (body) => fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
  method: 'POST',
  headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);

const baseRouting = {
  probeIntervalMs: 0, discoverIntervalMs: 0, maxAttempts: 4,
  failThreshold: 50, // 调高，避免测试中熔断干扰
  retryLoop: true, retryWaitMs: 200, retryMaxWaitMs: 0,
  attemptsPerChannel: 6, retryPerAttemptMs: 300,
  // 本套测试验证的是"两层重试"语义（渠道内预算 + 跨渠道轮询），
  // 不是两段式随机选路。把两个渠道都放进优先池，保证降级顺序确定、断言可重复。
  tiered: false,
};

const flakyChannel = { name: 'flaky', protocol: 'openai', baseUrl: mp.url(9106), apiKey: 'k', model: 'flaky-model', priority: 10 };
const badChannel = { name: 'always-bad', protocol: 'openai', baseUrl: mp.url(9102), apiKey: 'k', model: 'bad-model', priority: 10 };

// ---- 场景 A：渠道内重试 —— 前 3 次失败，第 4 次成功（同一渠道内自愈）----
await runScenario(
  { server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' }, routing: baseRouting, channels: [flakyChannel] },
  '渠道内重试成功',
  async () => {
    await fetch(`${mp.url(9106)}/_reset-flaky?fail=3`);
    const t0 = Date.now();
    const res = await chat({ model: 'flaky-model', messages: [{ role: 'user', content: 'hi' }] });
    const ms = Date.now() - t0;
    const json = await res.json();
    ok('前 3 次 429 后，同一渠道内第 4 次成功', res.status === 200, JSON.stringify(json).slice(0, 200));
    ok('渠道内发生了 3 次 300ms 等待（>=900ms）', ms >= 900, `${ms}ms`);
    ok('内容来自第 4 次尝试', (json?.choices?.[0]?.message?.content || '').includes('flaky ok'), json?.choices?.[0]?.message?.content);
  },
);

// ---- 场景 B：渠道内耗尽（6 次全失败）后切换到下一家渠道 ----
await runScenario(
  {
    server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' },
    routing: baseRouting,
    channels: [
      { name: 'exhausted', protocol: 'openai', baseUrl: mp.url(9102), apiKey: 'k', model: 'shared-model', priority: 10 },
      { name: 'flaky', protocol: 'openai', baseUrl: mp.url(9106), apiKey: 'k', model: 'shared-model', priority: 20, alias: { 'shared-model': 'flaky-model' } },
    ],
  },
  '渠道耗尽后切换',
  async () => {
    await fetch(`${mp.url(9106)}/_reset-flaky?fail=1`);
    const t0 = Date.now();
    const res = await chat({ model: 'shared-model', messages: [{ role: 'user', content: 'hi' }] });
    const ms = Date.now() - t0;
    const json = await res.json();
    const via = res.headers.get('x-gateway-channel');
    ok('第一个渠道耗尽后切到第二个渠道成功', res.status === 200, `status=${res.status} via=${via}`);
    ok('最终由 flaky 渠道完成', via === 'flaky', `via=${via}`);
    // tiered:false -> 严格按 priority 降级：exhausted(6 次 × 300ms) 之后才轮到 flaky
    ok('第一个渠道的 6 次尝试确实都跑完了（>=1500ms）', ms >= 1500, `${ms}ms`);
  },
);

// ---- 场景 C：全池永远失败 + retryMaxWaitMs=0 → 只有客户端断开才停止 ----
await runScenario(
  {
    server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' },
    routing: { ...baseRouting, retryWaitMs: 150, retryPerAttemptMs: 100, attemptsPerChannel: 2 },
    channels: [badChannel],
  },
  '无限轮询',
  async () => {
    const ac = new AbortController();
    const req = chat({ model: 'bad-model', messages: [{ role: 'user', content: 'hi' }] });
    // 2000ms 内不应返回（说明没放弃，仍在轮询）
    const raceResult = await Promise.race([
      req.then((r) => ({ done: true, status: r.status })).catch(() => ({ done: true, status: 'err' })),
      wait(2000).then(() => ({ done: false })),
    ]);
    ok('retryMaxWaitMs=0 时 2s 内不放弃（仍在无限轮询）', raceResult.done === false, JSON.stringify(raceResult));
    ac.abort();
    req.catch(() => {});
  },
);

// ---- 场景 D：retryMaxWaitMs 有上限 → 超限返回 503 ----
await runScenario(
  {
    server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' },
    routing: { ...baseRouting, attemptsPerChannel: 2, retryPerAttemptMs: 100, retryWaitMs: 200, retryMaxWaitMs: 900 },
    channels: [badChannel],
  },
  '有上限超时',
  async () => {
    const t0 = Date.now();
    const res = await chat({ model: 'bad-model', messages: [{ role: 'user', content: 'hi' }] });
    const ms = Date.now() - t0;
    const json = await res.json();
    ok('超过 retryMaxWaitMs 后返回 503', res.status === 503, `status=${res.status} ${ms}ms`);
    ok('耗时约等于 retryMaxWaitMs（>=900ms 且 <6s）', ms >= 900 && ms < 6000, `${ms}ms`);
    ok('错误信息带尝试次数说明', /尝试|重试|轮询/i.test(json?.error?.message || ''), json?.error?.message?.slice(0, 200));
  },
);

// ---- 场景 E：不可重试错误（401）立即换家，不做渠道内 6 次重试 ----
await runScenario(
  {
    server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' },
    routing: { ...baseRouting, attemptsPerChannel: 6, retryPerAttemptMs: 500 },
    channels: [
      { name: 'bad-key', protocol: 'openai', baseUrl: mp.url(9104), apiKey: 'k', model: 'auth-model', priority: 10 },
      { name: 'flaky', protocol: 'openai', baseUrl: mp.url(9106), apiKey: 'k', model: 'auth-model', priority: 20, alias: { 'auth-model': 'flaky-model' } },
    ],
  },
  '鉴权错误不重试',
  async () => {
    await fetch(`${mp.url(9106)}/_reset-flaky?fail=0`);
    const t0 = Date.now();
    const res = await chat({ model: 'auth-model', messages: [{ role: 'user', content: 'hi' }] });
    const ms = Date.now() - t0;
    ok('401 不触发渠道内重试，直接换家成功', res.status === 200 && ms < 1500, `status=${res.status} ${ms}ms`);
  },
);

// ---- 场景 F：retryLoop=false → 单轮试完就返回 503 ----
await runScenario(
  {
    server: { host: '127.0.0.1', port: PORT, apiKey: 'TESTKEY' },
    routing: { ...baseRouting, retryLoop: false, attemptsPerChannel: 2, retryPerAttemptMs: 100 },
    channels: [badChannel],
  },
  '关闭跨渠道循环',
  async () => {
    const t0 = Date.now();
    const res = await chat({ model: 'bad-model', messages: [{ role: 'user', content: 'hi' }] });
    const ms = Date.now() - t0;
    ok('retryLoop=false 时不进入下一轮，返回 503', res.status === 503 && ms < 2000, `status=${res.status} ${ms}ms`);
  },
);

mock.kill();
rmSync(CFG, { force: true });
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
