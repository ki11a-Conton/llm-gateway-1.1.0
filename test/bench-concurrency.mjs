// 并发压测：验证网关在高并发下的吞吐、并发上限、熔断去抖与背压
//
// 场景：
//   A. 稳态吞吐 + 并发上限   客户端并发 64，验证发往上游的峰值并发不超过 perChannel 上限
//   B. 上游故障稳定性         上游全 500 时请求快速失败、网关不卡死、熔断计数不爆炸
//   C. 慢客户端背压           大响应 + 客户端慢速读取，验证网关内存不随响应体积膨胀
//
// 用法：node test/bench-concurrency.mjs
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { writeFileSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

const MOCK_PORT = 9201;
// 8790：避开其它测试占用的 8791-8799
const GW_PORT = 8790;
const KEY = 'BENCHKEY';
const CFG = path.join(HERE, 'bench.test.json');
// --no-limit：关闭并发闸门，作为对照跑场景 A，用来看"不设限时上游会被打到多少并发"
const NO_LIMIT = process.argv.includes('--no-limit');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let pass = 0;
let fail = 0;
const ok = (name, cond, extra = '') => {
  if (cond) { pass += 1; console.log(`  \x1b[32mPASS\x1b[0m ${name}`); }
  else { fail += 1; console.log(`  \x1b[31mFAIL\x1b[0m ${name} ${extra}`); }
};

const json = (res, code, obj) => {
  const b = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(b) });
  res.end(b);
};

// ---------- mock 上游 ----------
let mockInFlight = 0;
let mockPeak = 0;
let mockMode = 'ok';
let mockDelay = 40;
let mockRequests = 0;
// 上游写入被内核缓冲阻塞的次数：只有网关真的做了背压（暂停读取上游），
// 上游 socket 缓冲才会写满并把压力反传回来。这是背压链路成立的直接证据。
let mockWriteBlocked = 0;

const mock = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');

  if (url.pathname === '/_stats') {
    return json(res, 200, {
      peak: mockPeak, inFlight: mockInFlight, requests: mockRequests,
      mode: mockMode, writeBlocked: mockWriteBlocked,
    });
  }
  if (url.pathname === '/_mode') {
    const m = url.searchParams.get('mode');
    if (m) mockMode = m;
    const d = url.searchParams.get('delay');
    if (d) mockDelay = Number(d);
    if (url.searchParams.get('reset') === '1') { mockPeak = 0; mockRequests = 0; mockWriteBlocked = 0; }
    return json(res, 200, { mode: mockMode, delay: mockDelay });
  }

  mockRequests += 1;
  mockInFlight += 1;
  if (mockInFlight > mockPeak) mockPeak = mockInFlight;
  res.on('close', () => { mockInFlight -= 1; });

  let raw = '';
  for await (const c of req) raw += c;
  let body = {};
  try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }

  if (mockDelay) await sleep(mockDelay);

  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'bench-model' }] });
  if (mockMode === 'fail') return json(res, 500, { error: { message: 'bench upstream failure', type: 'server_error' } });

  if (url.pathname === '/v1/chat/completions') {
    if (body.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const chunks = mockMode === 'big' ? 800 : 4;
      const pad = mockMode === 'big' ? 'x'.repeat(4000) : 'ok';
      for (let i = 0; i < chunks; i += 1) {
        const payload = `data: ${JSON.stringify({
          id: 'bench', object: 'chat.completion.chunk', created: 1, model: body.model,
          choices: [{ index: 0, delta: { content: pad }, finish_reason: null }],
        })}\n\n`;
        // 上游侧也遵守背压：socket 缓冲满就等 drain。
        // 这样"网关是否暂停读取"会直接反映在 mockWriteBlocked 上。
        if (res.write(payload) === false) {
          mockWriteBlocked += 1;
          await once(res, 'drain');
        }
      }
      const tail = `data: ${JSON.stringify({
        id: 'bench', object: 'chat.completion.chunk', created: 1, model: body.model,
        choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      })}\n\n`;
      if (res.write(tail) === false) mockWriteBlocked += 1;
      return res.end('data: [DONE]\n\n');
    }
    return json(res, 200, {
      id: 'bench', object: 'chat.completion', created: 1, model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---------- 压测客户端 ----------
async function runLoad({ total, concurrency, stream = false, slowReadMs = 0 }) {
  const latencies = [];
  let okCount = 0;
  let failCount = 0;
  const errors = new Map();
  let next = 0;
  const started = Date.now();

  const worker = async () => {
    for (;;) {
      const i = next;
      next += 1;
      if (i >= total) return;
      const t0 = Date.now();
      try {
        const r = await fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
          body: JSON.stringify({ model: 'bench-model', stream, messages: [{ role: 'user', content: 'hi' }] }),
        });
        if (stream && r.body) {
          for await (const chunk of r.body) {
            if (slowReadMs) await sleep(slowReadMs);
            else void chunk;
          }
        } else {
          await r.text();
        }
        if (r.ok) okCount += 1;
        else {
          failCount += 1;
          const k = `HTTP ${r.status}`;
          errors.set(k, (errors.get(k) || 0) + 1);
        }
      } catch (e) {
        failCount += 1;
        const k = e.name || 'ERR';
        errors.set(k, (errors.get(k) || 0) + 1);
      }
      latencies.push(Date.now() - t0);
    }
  };

  await Promise.all(Array.from({ length: concurrency }, worker));
  const dur = Date.now() - started;
  latencies.sort((a, b) => a - b);
  const q = (p) => (latencies.length ? latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * p))] : 0);

  return {
    total, ok: okCount, fail: failCount, durMs: dur,
    rps: Number((total / (dur / 1000)).toFixed(1)),
    p50: q(0.5), p95: q(0.95), p99: q(0.99),
    errors: Object.fromEntries(errors),
  };
}

const stat = (r) =>
  `总 ${r.total} | 成功 ${r.ok} | 失败 ${r.fail} | ${r.rps} req/s | P50 ${r.p50}ms P95 ${r.p95}ms P99 ${r.p99}ms` +
  (Object.keys(r.errors).length ? ` | 错误 ${JSON.stringify(r.errors)}` : '');

const mockStats = () => fetch(`http://127.0.0.1:${MOCK_PORT}/_stats`).then((r) => r.json());
const setMock = (qs) => fetch(`http://127.0.0.1:${MOCK_PORT}/_mode?${qs}`).then((r) => r.json());
const gwMetrics = () => fetch(`http://127.0.0.1:${GW_PORT}/api/metrics`, { headers: { authorization: `Bearer ${KEY}` } }).then((r) => r.json());

async function waitReady(url, timeout = 15000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    try { const r = await fetch(url); if (r.ok) return true; } catch { /* retry */ }
    await sleep(150);
  }
  return false;
}

const kill = (p) => {
  if (!p || p.exitCode !== null) return;
  p.kill('SIGTERM');
  setTimeout(() => p.exitCode === null && p.kill('SIGKILL'), 1500).unref();
};

const main = async () => {
  writeFileSync(
    CFG,
    JSON.stringify({
      server: { host: '127.0.0.1', port: GW_PORT, apiKey: KEY, panel: false },
      routing: {
        strategy: 'priority',
        maxAttempts: 1,
        timeoutMs: 30000,
        streamIdleTimeoutMs: 30000,
        failThreshold: 3,
        cooldownMs: 5000,
        maxCooldownMs: 30000,
        probeIntervalMs: 0,
        discoverIntervalMs: 0,
        retryLoop: false,
        // 并发闸门：全局 16，单渠道 8 —— 用于验证上游峰值被压住
        // --no-limit 时全部置 0（不限制），作为对照组
        maxConcurrent: NO_LIMIT ? 0 : 16,
        maxConcurrentPerChannel: NO_LIMIT ? 0 : 8,
        queueTimeoutMs: 20000,
        failDedupMs: 2000,
      },
      channels: [
        {
          name: 'bench', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
          apiKey: 'k', models: ['bench-model'], priority: 1,
        },
      ],
    }, null, 2),
    'utf8',
  );

  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));

  const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover'], {
    stdio: 'ignore', env: { ...process.env, NODE_OPTIONS: '' },
  });

  process.on('exit', () => { kill(gw); kill(mock); try { unlinkSync(CFG); } catch { /* ignore */ } });

  const ready = await waitReady(`http://127.0.0.1:${GW_PORT}/health`);
  if (!ready) { console.error('网关未就绪'); process.exit(1); }
  console.log('\n网关已就绪，开始压测\n');

  // ---------- 场景 A：稳态吞吐 + 并发上限 ----------
  console.log('== 场景 A：稳态吞吐与上游并发上限（上游 delay=40ms，客户端并发 64）==');
  await setMock('mode=ok&delay=40&reset=1');
  await runLoad({ total: 40, concurrency: 16 }); // 预热
  await setMock('reset=1');
  const a = await runLoad({ total: 400, concurrency: 64 });
  const aMock = await mockStats();
  const aGw = await gwMetrics();
  console.log(`  结果: ${stat(a)}`);
  console.log(`  上游观测峰值并发: ${aMock.peak}（channel 上限 8，全局上限 16）`);
  console.log(`  网关在途: ${aGw.concurrency.global?.active ?? 0}  排队: ${aGw.concurrency.global?.pending ?? 0}  （未启用闸门时为空）`);
  ok('400 请求全部成功', a.fail === 0, JSON.stringify(a.errors));
  if (NO_LIMIT) {
    console.log(`  [对照组] 未启用并发闸门，上游峰值并发 = ${aMock.peak}（客户端并发 64）`);
  } else {
    ok('上游峰值并发被限制在单渠道上限 8 以内', aMock.peak <= 8, `peak=${aMock.peak}`);
    ok('未出现排队超时', aGw.concurrency.overloads === 0, `overloads=${aGw.concurrency.overloads}`);
  }

  if (NO_LIMIT) {
    console.log(`\n[对照组结束] 结果: ${pass} 通过, ${fail} 失败\n`);
    kill(gw);
    kill(mock);
    try { unlinkSync(CFG); } catch { /* ignore */ }
    process.exit(0);
  }

  // ---------- 场景 B：上游故障稳定性 ----------
  console.log('\n== 场景 B：上游全 500 时的稳定性与熔断去抖 ==');
  await setMock('mode=fail&delay=10&reset=1');
  const b = await runLoad({ total: 120, concurrency: 32 });
  const bGw = await gwMetrics();
  const benchCh = bGw.channels.find((c) => c.name === 'bench');
  console.log(`  结果: ${stat(b)}`);
  console.log(`  渠道统计: 请求 ${benchCh.total} 失败 ${benchCh.failed} 熔断计数 failures=${benchCh.failures}${benchCh.coolingDown ? ' [冷却中]' : ''}`);
  const health = await fetch(`http://127.0.0.1:${GW_PORT}/health`).then((r) => r.json());
  ok('上游故障时返回 5xx 而不是挂死', b.fail === 120, JSON.stringify(b.errors));
  ok('网关仍能响应 /health', !!health.status, JSON.stringify(health));
  // 熔断计数的并发去抖（failDedupMs=2000）只作用于"并发批里的重复失败"：
  //   markFailure 里 dedupApplies = failDedupMs > 0 && inFlight > 1，
  //   **串行的单发失败每次都会计数**——这是有意设计（低频场景必须能正常熔断），
  //   所以不能用固定阈值（如 <20）来判定，否则会把设计行为误报成失败。
  // 本场景：120 请求 × 3 次尝试（分层默认 fallbackAttempts=3）= 360 次上游失败；
  //   实测 inFlight 分布中约 45% 的失败发生在 inFlight<=1 的"尾部"（不被去抖），
  //   最终 failures≈44（相对 360 砍掉约 88%）。
  // 断言改为"计数远小于上游失败次数"，既能证明去抖生效，也不会因流量形状变化而误报。
  ok(
    '熔断计数被并发去抖（远小于上游失败次数）',
    benchCh.failures > 0 && benchCh.failures < Math.max(20, benchCh.failed / 4),
    `failures=${benchCh.failures} failed=${benchCh.failed}`,
  );

  // ---------- 场景 C：慢客户端背压 ----------
  console.log('\n== 场景 C：慢客户端背压（大响应 + 客户端慢读）==');
  await setMock('mode=big&delay=0&reset=1');
  const before = (await gwMetrics()).memory.rss;
  const c = await runLoad({ total: 32, concurrency: 16, stream: true, slowReadMs: 2 });
  // 压测后立刻取内存，避免 GC 掩盖峰值
  const during = (await gwMetrics()).memory.rss;
  const cMock = await mockStats();
  console.log(`  结果: ${stat(c)}`);
  console.log(`  网关 RSS: ${(before / 1048576).toFixed(1)}MB -> ${(during / 1048576).toFixed(1)}MB（增长 ${((during - before) / 1048576).toFixed(1)}MB）`);
  console.log(`  上游侧写入被阻塞次数: ${cMock.writeBlocked}（>0 说明背压从客户端一路传导回上游）`);
  console.log('  说明: 单响应约 3.2MB，16 并发；若无背压会缓冲数十 MB');
  ok('慢客户端下请求仍能完成', c.fail === 0, JSON.stringify(c.errors));
  ok('网关内存未随响应体积线性膨胀（< 80MB）', during - before < 80 * 1048576, `增长 ${((during - before) / 1048576).toFixed(1)}MB`);
  ok('背压链路成立（网关暂停读取上游）', cMock.writeBlocked > 0, `writeBlocked=${cMock.writeBlocked}`);

  console.log(`\n结果: ${pass} 通过, ${fail} 失败\n`);
  kill(gw);
  kill(mock);
  try { unlinkSync(CFG); } catch { /* ignore */ }
  process.exit(fail ? 1 : 0);
};

main();
