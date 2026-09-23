// F5 回归：带嵌套字段的 usage 不能整体漏记
//
// 背景（代码审查 2026-09-20 F5/P2）：采集用 `/"usage"\s*:\s*\{[^{}]*\}/g` 在原始字节里找 usage，
// `[^{}]*` 明确排除内部对象 —— 只要 usage 含 prompt_tokens_details / completion_tokens_details
// 这类嵌套字段，整个匹配就失败，外层本来存在的 prompt/completion 总数也跟着一起丢。
// 报告复现：3 个都返回 100/20 的成功请求（其中一个普通、一个非流式带 details、一个流式带 details），
// 应累计 360 token，面板接口实际只返回 120。
//
// 修复：改为**括号配对**扫描（lib/proxy.mjs 的 findUsageObjects），支持任意嵌套，
// 仍然只认上游原始字节里真实回报的 usage（不采信适配器兜的 0）。
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

const M_PLAIN = 'u5-plain';               // 普通 usage：100/20
const M_NESTED = 'u5-nested';             // 非流式 + 嵌套 details：100/20
const M_NESTED_STREAM = 'u5-nested-stream'; // 流式 + 嵌套 details：100/20
const M_PROSE = 'u5-prose';               // 正文里"谈到" usage，不得被误当成统计

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const API_KEY = 'F5KEY';
const BASE = `http://127.0.0.1:${MOCK_PORT}/v1`;

const NESTED_USAGE = {
  prompt_tokens: 100,
  completion_tokens: 20,
  total_tokens: 120,
  prompt_tokens_details: { cached_tokens: 64, audio_tokens: 0 },
  completion_tokens_details: { reasoning_tokens: 12, audio_tokens: 0 },
};

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
    const base = { id: 'o1', object: 'chat.completion', model, choices: [{ index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' }] };

    if (model === M_PROSE) {
      // 正文里"谈到" usage：这是**字符串内容**，不是真实 usage 字段，绝不能被采到
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({
        ...base,
        choices: [{ index: 0, message: { role: 'assistant', content: '示例：{"usage":{"prompt_tokens":9999,"completion_tokens":9999}}' }, finish_reason: 'stop' }],
      }));
    }
    if (model === M_NESTED) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ...base, usage: NESTED_USAGE }));
    }
    if (model === M_NESTED_STREAM) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
      res.write(`data: ${JSON.stringify({ id: 's', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: { role: 'assistant', content: 'ok' } }] })}\n\n`);
      res.write(`data: ${JSON.stringify({ id: 's', object: 'chat.completion.chunk', model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
      // 嵌套 details 的流末 usage 帧（跨 chunk 拆开，顺便验证拼接）
      const usageFrame = `data: ${JSON.stringify({ id: 's', object: 'chat.completion.chunk', model, choices: [], usage: NESTED_USAGE })}\n\n`;
      res.write(usageFrame.slice(0, 40));
      res.write(usageFrame.slice(40));
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ...base, usage: { prompt_tokens: 100, completion_tokens: 20, total_tokens: 120 } }));
  });
});

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-f5-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
mkdirSync(LOGS_DIR, { recursive: true });
const RUN_CFG = path.join(tmpDir, 'f5.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: false },
  taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200, usageKeepDays: 120 },
  routing: {
    strategy: 'priority', attemptsPerChannel: 1, retryLoop: false, sessionAffinity: false,
    failThreshold: 99, probeIntervalMs: 0, discoverIntervalMs: 0,
    timeoutMs: 20000, providerTimeoutMs: 20000, streamIdleTimeoutMs: 20000,
    maxConcurrent: 8, maxConcurrentPerChannel: 0, queueTimeoutMs: 5000,
  },
  channels: [
    { name: 'plain', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_PLAIN, priority: 10 },
    { name: 'nested', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_NESTED, priority: 10 },
    { name: 'nested-stream', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_NESTED_STREAM, priority: 10 },
    { name: 'prose', protocol: 'openai', baseUrl: BASE, apiKey: 'k', model: M_PROSE, priority: 10 },
  ],
}, null, 2) + '\n', 'utf8');

let gw = null;
const chat = async (model, stream = false) => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { authorization: `Bearer ${API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model, stream, messages: [{ role: 'user', content: 'hi' }] }),
  });
  return { status: r.status, text: await r.text() };
};
const usage = async () => {
  const r = await fetch(`http://127.0.0.1:${GW_PORT}/api/usage`, { headers: { authorization: `Bearer ${API_KEY}` } });
  return r.json();
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

  const a = await chat(M_PLAIN);
  const b = await chat(M_NESTED);
  const c = await chat(M_NESTED_STREAM, true);
  const d = await chat(M_PROSE);
  ok('四个请求都成功返回', a.status === 200 && b.status === 200 && c.status === 200 && d.status === 200,
    `${a.status}/${b.status}/${c.status}/${d.status}`);
  await wait(400);

  const u = await usage();
  const today = u?.today || {};
  ok('三个请求都进了统计（requests=3，嵌套 usage 不再被整体漏掉）',
    today.requests === 3, JSON.stringify({ requests: today.requests }));
  ok('Token 总数正确：输入 300 / 输出 60 / 合计 360（旧实现只有 120）',
    today.inputTokens === 300 && today.outputTokens === 60 && today.totalTokens === 360,
    JSON.stringify({ i: today.inputTokens, o: today.outputTokens, t: today.totalTokens }));

  const byModel = today.byModel || {};
  ok('非流式嵌套 usage 被采到（100/20）',
    byModel[M_NESTED]?.inputTokens === 100 && byModel[M_NESTED]?.outputTokens === 20,
    JSON.stringify(byModel[M_NESTED]));
  ok('流式嵌套 usage（跨 chunk 拼接）被采到（100/20）',
    byModel[M_NESTED_STREAM]?.inputTokens === 100 && byModel[M_NESTED_STREAM]?.outputTokens === 20,
    JSON.stringify(byModel[M_NESTED_STREAM]));
  ok('正文里"谈到"usage 的响应不产生记录（不误采字符串里的字面量）',
    byModel[M_PROSE] === undefined, JSON.stringify(byModel[M_PROSE]));
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
