// F1 回归：流式"假成功"——上游流内报错 / 没有终止信号的 EOF 绝不能被当成正常结束
//
// 背景（代码审查 2026-09-20 F1/P1）：
//   a) anthropic→openai 适配器捕获流内错误后，只调日志回调，然后 yield `finish_reason: stop` + `[DONE]`；
//      其他读取异常与"未按协议收尾的 EOF"同样被补上正常收尾帧。
//   b) 同协议透传路径读完 HTTP body 就直接判成功，从不检查有没有终止信号。
//   两条路径都让路由层看到"正常返回"→ markSuccess + ok:true + 更新粘性路由，agent 收到半截回答。
//
// 本套件同时锁定**兼容形态**：上游给了有效 finish_reason 但没发最后的 [DONE]，必须照常成功。
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

// 模型名即 mock 行为开关
const M_OK = 'si-ok';                   // 正常：正文 + finish_reason + [DONE]
const M_TRUNC = 'si-truncated';         // 截断：正文后直接关流（无 finish_reason / 无 [DONE]）
const M_FIN_NO_DONE = 'si-finish-no-done'; // 兼容形态：有 finish_reason，但没有最后的 [DONE]
const M_ERR_ANTH = 'si-anth-error';     // Anthropic 流内 overloaded_error
const M_OK_ANTH = 'si-anth-ok';         // Anthropic 正常：message_stop 收尾

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'STREAMKEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

const sse = (obj) => `data: ${JSON.stringify(obj)}\n\n`;
const chunk = (model, delta, extra = {}) => sse({
  id: 'c1', object: 'chat.completion.chunk', model,
  choices: [{ index: 0, delta, ...extra }],
});

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

    // ---- Anthropic 协议上游（POST /v1/messages）----
    if (req.url?.includes('/messages')) {
      const head = { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' };
      if (model === M_ERR_ANTH) {
        res.writeHead(200, head);
        res.write('event: message_start\ndata: ' + JSON.stringify({ type: 'message_start', message: { id: 'm1', model, usage: { input_tokens: 9, output_tokens: 0 } } }) + '\n\n');
        res.write('event: content_block_start\ndata: ' + JSON.stringify({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) + '\n\n');
        res.write('event: content_block_delta\ndata: ' + JSON.stringify({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: '这是半截回答' } }) + '\n\n');
        // 流内错误：上游自己报了 overloaded_error，然后关流
        res.write('event: error\ndata: ' + JSON.stringify({ type: 'error', error: { type: 'overloaded_error', message: 'Overloaded' } }) + '\n\n');
        return res.end();
      }
      // 正常 Anthropic 流
      res.writeHead(200, head);
      res.write('event: message_start\ndata: ' + JSON.stringify({ type: 'message_start', message: { id: 'm2', model, usage: { input_tokens: 9, output_tokens: 0 } } }) + '\n\n');
      res.write('event: content_block_start\ndata: ' + JSON.stringify({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) + '\n\n');
      res.write('event: content_block_delta\ndata: ' + JSON.stringify({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: '完整的回答' } }) + '\n\n');
      res.write('event: content_block_stop\ndata: ' + JSON.stringify({ type: 'content_block_stop', index: 0 }) + '\n\n');
      res.write('event: message_delta\ndata: ' + JSON.stringify({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 7 } }) + '\n\n');
      res.write('event: message_stop\ndata: ' + JSON.stringify({ type: 'message_stop' }) + '\n\n');
      return res.end();
    }

    // ---- OpenAI 协议上游 ----
    const head = { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' };
    if (model === M_TRUNC) {
      // 只发正文增量就结束 HTTP body：没有 finish_reason、没有 usage、没有 [DONE]
      res.writeHead(200, head);
      res.write(chunk(model, { role: 'assistant', content: '半截' }));
      res.write(chunk(model, { content: '回答' }));
      return res.end();
    }
    if (model === M_FIN_NO_DONE) {
      // 兼容形态：有有效 finish_reason，但（部分上游）不发最后的 [DONE]
      res.writeHead(200, head);
      res.write(chunk(model, { role: 'assistant', content: '完整回答' }));
      res.write(chunk(model, {}, { finish_reason: 'stop' }));
      res.write(sse({ id: 'c1', model, choices: [], usage: { prompt_tokens: 3, completion_tokens: 4 } }));
      return res.end();
    }
    // 正常 OpenAI 流
    res.writeHead(200, head);
    res.write(chunk(model, { role: 'assistant', content: '正常回答' }));
    res.write(chunk(model, {}, { finish_reason: 'stop' }));
    res.write(chunk(model, {}, {}) === '' ? '' : '');
    res.write('data: [DONE]\n\n');
    return res.end();
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-stream-integrity-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'stream-integrity.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
  taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200 },
  modelMap: {},
  routing: {
    strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
    failThreshold: 99, cooldownMs: 1000, probeIntervalMs: 0, discoverIntervalMs: 0,
    timeoutMs: 20000, providerTimeoutMs: 5000, streamIdleTimeoutMs: 5000,
    maxConcurrent: 8, maxConcurrentPerChannel: 0, queueTimeoutMs: 5000,
  },
  channels: [
    { name: 'ok-oai', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_OK, priority: 10 },
    { name: 'trunc-oai', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_TRUNC, priority: 10 },
    { name: 'findone-oai', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_FIN_NO_DONE, priority: 10 },
    { name: 'err-anth', protocol: 'anthropic', baseUrl: BASE, apiKey: 'k', model: M_ERR_ANTH, priority: 10 },
    { name: 'ok-anth', protocol: 'anthropic', baseUrl: BASE, apiKey: 'k', model: M_OK_ANTH, priority: 10 },
  ],
}, null, 2) + '\n', 'utf8');

let gw = null;
const chat = async (model, opts = {}) => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model, stream: true, messages: [{ role: 'user', content: 'hi' }], ...opts }),
  });
  return { status: r.status, text: await r.text(), channel: r.headers.get('x-gateway-channel') };
};
const getJson = async (p) => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}${p}`, { headers: { authorization: `Bearer ${API_KEY}` } });
  return { status: r.status, json: await r.json().catch(() => null) };
};
/** 客户端看起来是不是"一次干净的正常收尾"（有 [DONE] 且没有错误帧）*/
const looksClean = (text) => text.includes('[DONE]') && !/"error"/.test(text) && !/^event: error/m.test(text);
/** 从任务日志里找这次请求的记录 */
async function taskOf(model) {
  const t = await getJson('/api/tasks?limit=50');
  const rec = (t.json?.recent || []).find((x) => x.model === model);
  return rec || null;
}

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
  await wait(150);

  // ---------- 基线：正常流必须成功（防止"修 F1 把正常流也判失败"）----------
  {
    const r = await chat(M_OK);
    ok('正常 OpenAI 流：客户端正常收尾', r.status === 200 && looksClean(r.text), `status=${r.status} tail=${r.text.slice(-80)}`);
    await wait(200);
    const rec = await taskOf(M_OK);
    ok('正常 OpenAI 流：任务日志 ok:true（渠道应被记成功）', rec?.ok === true, JSON.stringify(rec && { ok: rec.ok, kind: rec.kind }));
  }

  // ---------- F1-a：OpenAI 上游截断（无终止信号 EOF）----------
  {
    const r = await chat(M_TRUNC);
    ok('截断流：客户端拿到的不是"干净正常收尾"（没有伪造 [DONE]）',
      !looksClean(r.text), `status=${r.status} text=${r.text.slice(0, 160)}`);
    await wait(250);
    const rec = await taskOf(M_TRUNC);
    ok('截断流：任务日志 ok:false（不再被记成成功）', rec?.ok === false, JSON.stringify(rec && { ok: rec.ok, error: rec.error }));
    ok('截断流：错误信息指出缺少终止信号',
      /终止信号|截断/.test(rec?.error || ''), JSON.stringify(rec?.error));
    const st = await getJson('/api/status');
    const ch = (st.json?.channels || []).find((c) => c.name === 'trunc-oai');
    ok('截断流：渠道被记失败（lastError 含 stream_truncated）',
      /\[stream_truncated\]/.test(ch?.lastError || ''), JSON.stringify(ch?.lastError));
  }

  // ---------- F1-b：Anthropic 流内错误（原复现：客户端收到 200 + [DONE]，错误文本消失）----------
  {
    const r = await chat(M_ERR_ANTH);
    ok('流内错误：客户端拿到的不是"干净正常收尾"（不再伪造 stop + [DONE]）',
      !looksClean(r.text), `status=${r.status} text=${r.text.slice(0, 200)}`);
    ok('流内错误：上游错误信息没有凭空消失（或明确换成截断错误帧）',
      /error/i.test(r.text), `text=${r.text.slice(0, 200)}`);
    await wait(250);
    const rec = await taskOf(M_ERR_ANTH);
    ok('流内错误：任务日志 ok:false', rec?.ok === false, JSON.stringify(rec && { ok: rec.ok, error: rec.error }));
    const st = await getJson('/api/status');
    const ch = (st.json?.channels || []).find((c) => c.name === 'err-anth');
    ok('流内错误：渠道被记失败（不再 markSuccess）',
      /\[stream_truncated\]/.test(ch?.lastError || ''), JSON.stringify(ch?.lastError));
  }

  // ---------- 兼容形态：有 finish_reason 但没有最后的 [DONE] -> 必须照常成功 ----------
  {
    const r = await chat(M_FIN_NO_DONE);
    ok('兼容上游（有 finish_reason、无 [DONE]）：客户端正常收尾',
      r.status === 200 && looksClean(r.text), `status=${r.status} tail=${r.text.slice(-120)}`);
    await wait(250);
    const rec = await taskOf(M_FIN_NO_DONE);
    ok('兼容上游：任务日志 ok:true（不误判成截断）', rec?.ok === true, JSON.stringify(rec && { ok: rec.ok, error: rec.error }));
  }

  // ---------- 正常 Anthropic 流（转换方向）仍成功 ----------
  {
    const r = await chat(M_OK_ANTH);
    ok('正常 Anthropic 流：客户端正常收尾', r.status === 200 && looksClean(r.text), `status=${r.status} tail=${r.text.slice(-120)}`);
    await wait(200);
    const rec = await taskOf(M_OK_ANTH);
    ok('正常 Anthropic 流：任务日志 ok:true', rec?.ok === true, JSON.stringify(rec && { ok: rec.ok, error: rec.error }));
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
