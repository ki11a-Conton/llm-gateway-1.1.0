// F3 回归：maxTotalWaitMs 必须是**请求级绝对总时限**
//
// 背景（代码审查 2026-09-20 F3/P1）：旧实现里 maxTotalWaitMs 比较的是 waitedMs，
// 而 waitedMs 只累加重试之间的等待——并发排队、上游生成、读体、背压等待统统不计；
// 检查还落在 `if (!retryLoop) break` 之后，不开整池重试时形同不存在。
// 另外 headerTimer 拿到响应头就被清掉，此后没有任何计时器能约束"持续有数据到来"的长流。
// 报告复现：maxTotalWaitMs=70 + timeoutMs=250，上游每 30ms 一个 chunk → 请求 367ms 后**成功**返回。
//
// 本套件断言：
//   ① 持续流超过总预算 -> 必须收尾（不当成功、不无限流下去）
//   ② 总预算到点后并发名额必须归还（在途回到 0）
//   ③ maxTotalWaitMs=0 = 不限总时长（慢而有限的正经回答仍成功）——配置语义必须明确
import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';

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

const M_SLOW = 'f3-slow';   // 每 60ms 一个 chunk，共 12 个（约 720ms）后正常收尾
const M_FAST = 'f3-fast';   // 立刻正常收尾

const MOCK_PORT = await freePort();
const GW_BUDGET_PORT = await freePort();
const GW_UNLIMITED_PORT = await freePort();
const API_KEY = 'F3KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

const chunk = (model, delta, extra = {}) => `data: ${JSON.stringify({
  id: 'c1', object: 'chat.completion.chunk', model,
  choices: [{ index: 0, delta, ...extra }],
})}\n\n`;

const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', async () => {
    let body = {};
    try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }
    const model = body.model || '';
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: model, object: 'model' }] }));
    }
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    if (model === M_SLOW) {
      res.write(chunk(model, { role: 'assistant', content: '起' }));
      for (let i = 0; i < 12; i += 1) {
        await wait(60); // 持续有数据 -> 既不触发 idle 也不触发首字节看门狗
        res.write(chunk(model, { content: `第${i}段` }));
      }
      res.write(chunk(model, {}, { finish_reason: 'stop' }));
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    res.write(chunk(model, { role: 'assistant', content: '快回答' }));
    res.write(chunk(model, {}, { finish_reason: 'stop' }));
    res.write('data: [DONE]\n\n');
    return res.end();
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f3-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const mkCfg = (file, port, maxTotalWaitMs) => {
  const p = path.join(tmpDir, file);
  writeFileSync(p, JSON.stringify({
    server: { host: '127.0.0.1', port, apiKey: API_KEY, panel: false },
    taskLog: { enabled: true, dir: LOGS_DIR, file: `tasks-${port}.jsonl`, ringMax: 200 },
    routing: {
      strategy: 'priority', attemptsPerChannel: 3, retryLoop: false, sessionAffinity: false,
      failThreshold: 99, cooldownMs: 1000, probeIntervalMs: 0, discoverIntervalMs: 0,
      // 关键：渠道超时（250ms）比总预算大得多，且流会一直有数据 -> 只有总预算能收场
      timeoutMs: 5000, firstByteTimeoutMs: 5000, streamIdleTimeoutMs: 5000, providerTimeoutMs: 5000,
      maxTotalWaitMs,
      maxConcurrent: 4, maxConcurrentPerChannel: 2, queueTimeoutMs: 5000,
    },
    channels: [
      { name: 'slow', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_SLOW, priority: 10 },
      { name: 'fast', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_FAST, priority: 10 },
    ],
  }, null, 2) + '\n', 'utf8');
  return p;
};
const CFG_BUDGET = mkCfg('f3-budget.json', GW_BUDGET_PORT, 300);
const CFG_UNLIMITED = mkCfg('f3-unlimited.json', GW_UNLIMITED_PORT, 0);

const gws = [];
async function startGw(cfg, port) {
  const g = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', cfg, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
  gws.push(g);
  for (let i = 0; i < 60; i += 1) {
    try { const r = await fetch(`http://127.0.0.1:${port}/health`); if (r.ok) return true; } catch { /* retry */ }
    await wait(200);
  }
  return false;
}
const chat = async (port, model) => {
  const r = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model, stream: true, messages: [{ role: 'user', content: 'hi' }] }),
  });
  return { status: r.status, text: await r.text() };
};
const getJson = async (port, p) => {
  const r = await fetch(`http://127.0.0.1:${port}${p}`, { headers: { authorization: `Bearer ${API_KEY}` } });
  return r.json().catch(() => null);
};
const looksClean = (text) => text.includes('[DONE]') && !/"error"/.test(text);

try {
  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));
  const okB = await startGw(CFG_BUDGET, GW_BUDGET_PORT);
  const okU = await startGw(CFG_UNLIMITED, GW_UNLIMITED_PORT);
  ok('两个网关（预算 300ms / 不限时长）都就绪', okB && okU);
  if (!okB || !okU) throw new Error('网关未就绪');
  await wait(200);

  // ---------- ① 持续流超总预算：必须在预算附近收尾，不能一路流到底还报成功 ----------
  {
    const t0 = Date.now();
    const r = await chat(GW_BUDGET_PORT, M_SLOW);
    const ms = Date.now() - t0;
    ok('持续流超总预算：请求在预算附近收尾（<1500ms，上游本来要流 ~900ms+ 并"成功"）',
      ms < 1500, `${ms}ms`);
    ok('持续流超总预算：不是干净的成功收尾（没有伪造 [DONE]）',
      !looksClean(r.text), `status=${r.status} tail=${r.text.slice(-140)}`);
    ok('持续流超总预算：客户端收到了明确的错误信号（流内 error 帧或非 200）',
      /"error"/.test(r.text) || r.status >= 400, `status=${r.status} text=${r.text.slice(0, 160)}`);

    await wait(400);
    // ② 名额释放：失败后不能把并发名额漏在里面
    const conc = await getJson(GW_BUDGET_PORT, '/api/metrics');
    const active = conc?.concurrency?.active ?? conc?.concurrency?.global?.active;
    ok('总预算收尾后并发名额归还（active 回到 0）', active === 0, JSON.stringify(conc?.concurrency));

    const tasks = await getJson(GW_BUDGET_PORT, '/api/tasks?limit=20');
    const rec = (tasks?.recent || []).find((x) => x.model === M_SLOW);
    ok('任务日志给出明确的总预算原因（kind=total_deadline）',
      rec?.kind === 'total_deadline' || /总预算|总时长/.test(rec?.error || ''), JSON.stringify(rec && { kind: rec.kind, error: rec.error }));
  }

  // ---------- ③ maxTotalWaitMs=0 = 不限总时长：慢而有限的正经回答必须成功 ----------
  {
    const t0 = Date.now();
    const r = await chat(GW_UNLIMITED_PORT, M_SLOW);
    const ms = Date.now() - t0;
    ok('不限总时长（0）：慢回答照常成功且拿到 [DONE]',
      r.status === 200 && looksClean(r.text), `status=${r.status} ${ms}ms tail=${r.text.slice(-80)}`);
    ok('不限总时长（0）：确实等到了完整流（>=600ms，没有被预算误杀）', ms >= 600, `${ms}ms`);
    await wait(200);
    const tasks = await getJson(GW_UNLIMITED_PORT, '/api/tasks?limit=20');
    const rec = (tasks?.recent || []).find((x) => x.model === M_SLOW);
    ok('不限总时长（0）：任务日志 ok:true', rec?.ok === true, JSON.stringify(rec && { ok: rec.ok, kind: rec.kind }));
  }

  // ---------- ④ 预算充裕时正常流不受影响 ----------
  {
    const r = await chat(GW_BUDGET_PORT, M_FAST);
    ok('预算充裕的正常流：照常成功', r.status === 200 && looksClean(r.text), `status=${r.status}`);
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  for (const g of gws) { try { g.kill(); } catch { /* ignore */ } }
  await wait(300);
  try { mock.close(); } catch { /* ignore */ }
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
