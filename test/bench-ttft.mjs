// A4 量测脚本：思考守卫对流式"首字延迟（TTFT）"的影响
//
// 背景：流式方向为了做"思维链混进正文"的拆分与空响应检测，会先把响应头部攒在内存里，
// 达到阈值（正文 64 字符 / 16KB / 收尾信号 / 已分离的思考）才判定并下发响应头。
// 攒头部 = 首字被推迟。本脚本用真实端到端请求量测这个代价，作为是否调整默认值的证据。
//
// 三个场景（都是流式，测量"请求发出 -> 收到响应头"的耗时）：
//   ① guard=on   + 上游先发一小段正文后停顿 400ms（9135）——头部要攒到收尾才放行，TTFT 被推迟
//   ② guard=off  + 同一上游（routing.reasoningGuardStream=false）——立即放行响应头
//   ③ guard=on   + 上游先发 reasoning_content（9128）——命中"已分离思考"提前放行，TTFT 应接近 ②
//
// 用法：node test/bench-ttft.mjs
// 产出：控制台表格 + test/bench-ttft.result.json
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync, writeFileSync, rmSync } from 'node:fs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const CFG = path.join(HERE, 'ttft.test.json');
const CFG_NOGUARD = path.join(HERE, 'ttft-noguard.tmp.json');
const OUT = path.join(HERE, 'bench-ttft.result.json');
const PORT = 8809;
const RUNS = 7;

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitReady(tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch { /* retry */ }
    await wait(200);
  }
  throw new Error('gateway not ready');
}

/** 一次流式请求的 TTFT（毫秒）：从发起请求到 fetch 解析出响应头 */
async function ttfbOnce(model) {
  const t0 = performance.now();
  const res = await fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
    body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }], stream: true }),
  });
  const ms = performance.now() - t0;
  // 必须读完，否则连接不释放，下一次测量会受干扰
  for await (const _ of res.body) { /* drain */ }
  return ms;
}

const median = (arr) => {
  const s = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
};

async function measure(label, model, runs = RUNS) {
  await ttfbOnce(model); // 预热（建连 + 首次路由）
  const samples = [];
  for (let i = 0; i < runs; i += 1) samples.push(await ttfbOnce(model));
  const stat = {
    label, model, runs,
    min: Math.round(Math.min(...samples)),
    median: Math.round(median(samples)),
    max: Math.round(Math.max(...samples)),
  };
  console.log(`  ${label.padEnd(46)} min=${String(stat.min).padStart(5)}ms  median=${String(stat.median).padStart(5)}ms  max=${String(stat.max).padStart(5)}ms`);
  return stat;
}

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore' });
await wait(1000);

const results = {};
try {
  // ---------- 场景 ①②：guard 开 / 关（同一上游、同一请求） ----------
  {
    const cfg = JSON.parse(readFileSync(CFG, 'utf8'));
    // ① guard 开（默认）
    let gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
    await waitReady(); await wait(300);
    console.log('\n[guard=on]  reasoningGuardStream=true');
    results.guardOn = await measure('① 攒头部（正文 <64 字符 + 上游停顿 400ms）', 'ttft-model');
    gw.kill(); await wait(500);

    // ② guard 关
    writeFileSync(CFG_NOGUARD, JSON.stringify({
      ...cfg,
      routing: { ...cfg.routing, reasoningGuardStream: false },
    }, null, 2), 'utf8');
    gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG_NOGUARD, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
    await waitReady(); await wait(300);
    console.log('\n[guard=off] reasoningGuardStream=false');
    results.guardOff = await measure('② 立即放行（不做流式思考守卫）', 'ttft-model');

    // ---------- 场景 ③：guard 开 + 上游已分离思考（提前放行路径） ----------
    console.log('\n[guard=on + 已分离思考]');
    results.guardOnSeparated = await measure('③ 命中 reasoning_content 提前放行', 'reason-model');
    gw.kill(); await wait(400);
  }

  const overhead = results.guardOn.median - results.guardOff.median;
  const earlyRelease = results.guardOnSeparated.median - results.guardOff.median;
  console.log('\n汇总（median，单位 ms）：');
  console.log(`  ① guard=on（攒头部）              : ${results.guardOn.median}`);
  console.log(`  ② guard=off（不守卫）             : ${results.guardOff.median}`);
  console.log(`  ③ guard=on + 已分离思考（提前放行）: ${results.guardOnSeparated.median}`);
  console.log(`  -> 攒头部带来的额外首字延迟       : ${overhead}ms（上游本身停顿 400ms）`);
  console.log(`  -> 提前放行相对 guard=off 的差距  : ${earlyRelease}ms`);

  const payload = {
    measuredAt: new Date().toISOString(),
    node: process.version,
    platform: process.platform,
    runs: RUNS,
    upstreamPauseMs: 400,
    results,
    overheadMedianMs: overhead,
    earlyReleaseDeltaMedianMs: earlyRelease,
  };
  writeFileSync(OUT, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  console.log(`\n结果已写入 ${path.relative(ROOT, OUT)}`);

  // 软断言：提前放行路径不应比"完全不守卫"慢太多（否则说明提前放行没生效）
  let fail = 0;
  if (earlyRelease > 150) { console.log('  [FAIL] 提前放行路径明显慢于 guard=off，检查 sawSeparatedReasoning 判定'); fail += 1; }
  else console.log('  [PASS] 已分离思考时提前放行生效（TTFT 接近不守卫）');
  if (overhead < 200) { console.log('  [WARN] 攒头部开销低于预期，检查上游停顿是否生效'); }
  process.exitCode = fail ? 1 : 0;
} catch (err) {
  console.error('BENCH ERROR', err);
  process.exitCode = 1;
} finally {
  mock.kill();
  rmSync(CFG_NOGUARD, { force: true });
  await wait(300);
}
