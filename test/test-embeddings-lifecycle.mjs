// F4 回归：embeddings 必须接入超时与客户端取消
//
// 背景（代码审查 2026-09-20 F4/P1）：`/v1/embeddings` 申请了全局/渠道/agent 三级并发许可，
// 但 fetch 不传 signal、r.text() 没有看门狗、也不监听客户端关闭。后果：
//   ① 上游不回响应头 / 正文中途停住 -> 请求永久挂住，finally 里的 release 永远走不到；
//   ② 客户端取消后请求继续跑，名额被永久占用，攒满 maxConcurrent 后连聊天一起卡死。
// 报告复现：配置超时 250ms，上游只回响应头不结束正文 —— 350ms 后 active 仍为 1；
//          取消客户端再等 150ms，active 仍为 1，上游连接没关。
//
// 本套件断言：三种挂死形态都能按期收尾；客户端取消能立刻释放名额；
//            随后 embeddings 与聊天都还能正常跑（容量没被漏掉）。
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

const M_HANG_HEADERS = 'emb-hang-headers'; // 只连不回（连响应头都没有）
const M_HANG_BODY = 'emb-hang-body';       // 回了响应头，正文写一半就再也不结束
const M_OK = 'emb-ok';                     // 正常 JSON
const M_CHAT = 'emb-chat-ok';              // 聊天用的正常模型

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'F4KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;
const TIMEOUT_MS = 400; // 配置的 embeddings 上游超时

let openedSockets = 0;
const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => {
    let body = {};
    try { body = JSON.parse(raw || '{}'); } catch { /* ignore */ }
    const model = body.model || '';
    if (req.url?.includes('/models')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ id: model, object: 'model' }] }));
    }
    if (req.url?.includes('/embeddings')) {
      openedSockets += 1;
      req.socket.on('close', () => { openedSockets -= 1; });
      if (model === M_HANG_HEADERS) {
        return; // 什么都不写，也不结束 —— 挂死
      }
      if (model === M_HANG_BODY) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.write('{"data":[{"embedding":'); // 正文只写一半，永不结束
        return;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ object: 'list', data: [{ object: 'embedding', embedding: [0.1], index: 0 }] }));
    }
    // 聊天：正常 SSE
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: { content: '好' } }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
    res.write('data: [DONE]\n\n');
    return res.end();
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f4-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'f4.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
  taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200 },
  routing: {
    strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
    failThreshold: 99, cooldownMs: 1000, probeIntervalMs: 0, discoverIntervalMs: 0,
    // 关键：embeddings 上游超时 400ms（旧实现完全不看这个值）
    embeddingsTimeoutMs: TIMEOUT_MS,
    timeoutMs: TIMEOUT_MS, providerTimeoutMs: TIMEOUT_MS, streamIdleTimeoutMs: TIMEOUT_MS,
    // 全局上限压小：名额一旦泄漏，后续请求立刻被挡住（症状放大，便于断言）
    maxConcurrent: 2, maxConcurrentPerChannel: 0, queueTimeoutMs: 3000,
  },
  channels: [
    { name: 'emb', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_OK, priority: 10 },
    { name: 'emb-head', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_HANG_HEADERS, priority: 10 },
    { name: 'emb-body', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_HANG_BODY, priority: 10 },
    { name: 'chat', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_CHAT, priority: 10 },
  ],
}, null, 2) + '\n', 'utf8');

let gw = null;
const emb = async (model, { abortAfterMs = 0 } = {}) => {
  const ac = new AbortController();
  if (abortAfterMs) setTimeout(() => ac.abort(), abortAfterMs);
  const t0 = Date.now();
  try {
    const r = await fetch(`http://127.0.0.1:${GW_PORT}/v1/embeddings`, {
      method: 'POST',
      headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
      body: JSON.stringify({ model, input: 'hi' }),
      signal: ac.signal,
    });
    return { status: r.status, text: await r.text(), ms: Date.now() - t0, aborted: false };
  } catch (err) {
    return { status: 0, text: String(err.message), ms: Date.now() - t0, aborted: true };
  }
};
const chat = async (model) => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model, stream: true, messages: [{ role: 'user', content: 'hi' }] }),
  });
  return { status: r.status, text: await r.text() };
};
const activeCount = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/metrics`, { headers: { authorization: `Bearer ${API_KEY}` } });
  const j = await r.json().catch(() => null);
  // 真实结构：/api/metrics -> concurrency.global.active（返回 null 表示读不到，绝不用 undefined 冒充 0）
  return j?.concurrency?.global?.active ?? null;
};

try {
  await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));
  gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
  let ready = false;
  for (let i = 0; i < 60; i += 1) {
    try { const r = await fetch(`http://127.0.0.1:${GW_PORT}/health`); if (r.ok) { ready = true; break; } } catch { /* retry */ }
    await wait(200);
  }
  ok('网关就绪', ready);
  if (!ready) throw new Error('网关未就绪');
  await wait(200);

  // ---------- ① 上游连响应头都不回 ----------
  {
    const r = await emb(M_HANG_HEADERS);
    ok('不回响应头：按期失败而不是永久挂住（<2s 返回 >=400）',
      r.status >= 400 && r.ms < 2000, `status=${r.status} ${r.ms}ms`);
    await wait(300);
    ok('不回响应头：并发名额已归还（active=0）', (await activeCount()) === 0, `active=${await activeCount()}`);
  }

  // ---------- ② 只回响应头、正文永不到来（报告原复现）----------
  {
    const r = await emb(M_HANG_BODY);
    ok('正文中途停住：按期失败（<2s 返回 >=400）',
      r.status >= 400 && r.ms < 2000, `status=${r.status} ${r.ms}ms`);
    await wait(300);
    ok('正文中途停住：并发名额已归还（active=0）', (await activeCount()) === 0, `active=${await activeCount()}`);
  }

  // ---------- ③ 客户端在读取期间取消：名额必须立刻释放 ----------
  {
    const r = await emb(M_HANG_BODY, { abortAfterMs: 150 });
    ok('客户端取消：客户端侧如期中断', r.aborted === true, JSON.stringify({ status: r.status, ms: r.ms }));
    await wait(500);
    const a = await activeCount();
    ok('客户端取消后名额归还（active=0，旧实现会永久占着）', a === 0, `active=${a}`);
  }

  // ---------- ④ 容量没被漏掉：embeddings 与聊天都还能跑 ----------
  {
    const r = await emb(M_OK);
    ok('后续 embeddings 正常成功（容量没被泄漏吃满）', r.status === 200 && /embedding/.test(r.text), `status=${r.status}`);
    const c = await chat(M_CHAT);
    ok('后续聊天正常成功（共用的并发池仍然可用）', c.status === 200 && c.text.includes('[DONE]'), `status=${c.status}`);
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  try { gw?.kill(); } catch { /* ignore */ }
  await wait(300);
  try { mock.close(); } catch { /* ignore */ }
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
