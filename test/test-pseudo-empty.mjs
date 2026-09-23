// D1 回归：拦截"伪空"响应
//
// 现象：上游 HTTP 200，正文里只有空白 / 单个标点（"."） / 空代码围栏（```）——
// 对 agent 而言与完全空响应一样不可用，但此前会被当成功返回，agent 表现为"卡住不动"。
//
// 本用例把 D1 的手写自测固化成回归：
//   单元：findUnusableResponse / findUnusableStreamHead 的判定矩阵（不误伤正常短回答）
//   集成：伪空渠道被跳过、请求由健康渠道完成
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 上游端口都改成运行时动态分配，避免并行跑测试时抢端口。
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'pseudo-empty.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 单元：非流式判定矩阵 ----------
{
  const { findUnusableResponse, findUnusableStreamHead } = await import('../lib/proxy.mjs');

  const nonStream = (content, extra = {}) => ({
    choices: [{ index: 0, message: { role: 'assistant', content, ...extra }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1 },
  });
  const reasonOf = (content, extra) => findUnusableResponse(nonStream(content, extra))?.reason ?? null;

  ok('单元：空字符串 -> empty_completion', reasonOf('') === 'empty_completion', String(reasonOf('')));
  ok('单元：纯空白 -> empty_completion', reasonOf('   \n ') === 'empty_completion', String(reasonOf('   \n ')));
  ok('单元：单个句点 -> pseudo_empty', reasonOf('.') === 'pseudo_empty', String(reasonOf('.')));
  ok('单元：单个引号 -> pseudo_empty', reasonOf('"') === 'pseudo_empty', String(reasonOf('"')));
  ok('单元：空代码围栏 -> pseudo_empty', reasonOf('```\n\n```') === 'pseudo_empty', String(reasonOf('```\n\n```')));
  ok('单元：纯标点串 -> pseudo_empty', reasonOf('，。！？') === 'pseudo_empty', String(reasonOf('，。！？')));

  ok('单元：数字短回答不误伤', reasonOf('391') === null, String(reasonOf('391')));
  ok('单元：单字回答不误伤', reasonOf('好') === null, String(reasonOf('好')));
  ok('单元：有内容的代码块不误伤', reasonOf('```js\nconst a=1;\n```') === null, String(reasonOf('```js\nconst a=1;\n```')));
  ok('单元：只有 tool_calls 不误伤',
    findUnusableResponse({ choices: [{ index: 0, message: { role: 'assistant', content: '', tool_calls: [{ id: 't', function: { name: 'f', arguments: '{}' } }] }, finish_reason: 'tool_calls' }] }) === null);
  ok('单元：只有 reasoning_content 不误伤',
    reasonOf('', { reasoning_content: '我在思考' }) === null, String(reasonOf('', { reasoning_content: '我在思考' })));
  ok('单元：finish_reason=length 且有 token 不算空',
    findUnusableResponse({ choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'length' }], usage: { completion_tokens: 5 } }) === null);

  // minChars 可配：抬高阈值后短回答也算不可用（保守默认 1 不会走到这里）
  ok('单元：emptyMinChars=5 时 3 字回答被拦',
    findUnusableResponse(nonStream('391'), { minChars: 5 })?.reason === 'pseudo_empty');
  ok('单元：emptyMinChars=5 时 6 字回答放行',
    findUnusableResponse(nonStream('abcdef'), { minChars: 5 }) === null);

  // ---------- 单元：流式判定矩阵 ----------
  const sse = (delta, finish = null) => `data: ${JSON.stringify({
    id: 'x', object: 'chat.completion.chunk', created: 1, model: 'm',
    choices: [{ index: 0, delta, finish_reason: finish }],
  })}\n\n`;
  const streamReason = (raw) => findUnusableStreamHead(raw)?.reason ?? null;

  ok('单元：流式空响应 -> empty_stream', streamReason(sse({}, 'stop')) === 'empty_stream', String(streamReason(sse({}, 'stop'))));
  ok('单元：流式只有围栏壳 -> pseudo_empty_stream',
    streamReason(sse({ content: '```\n\n```' }, 'stop')) === 'pseudo_empty_stream',
    String(streamReason(sse({ content: '```\n\n```' }, 'stop'))));
  ok('单元：流式有正文 -> 放行', streamReason(sse({ content: 'hello world' }, 'stop')) === null);
  ok('单元：流式有 reasoning_content -> 放行', streamReason(sse({ reasoning_content: '思考' }, 'stop')) === null);
  ok('单元：流式未收尾（无 finish_reason）-> 放行', streamReason(sse({ content: '' })) === null);
  ok('单元：流式有 tool_calls -> 放行',
    streamReason(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 't' }] }, finish_reason: 'tool_calls' }] })}\n\n`) === null);
}

// ---------- 集成 ----------
const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });

async function waitReady(tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch { /* retry */ }
    await wait(200);
  }
  throw new Error('gateway not ready');
}
const call = async (pathname, opts = {}) => {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
};
const chat = (body) => call('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

try {
  await waitReady();
  await wait(400);

  // 非流式伪空：'." 只有一个标点 -> 必须换家
  const r1 = await chat({ model: 'pseudo-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('集成：伪空渠道被跳过，请求成功', r1.status === 200, `status=${r1.status} ${r1.text.slice(0, 160)}`);
  ok('集成：成功渠道是 ok-1（不是只回 "." 的 pseudo-1）',
    r1.headers.get('x-gateway-channel') === 'ok-1', String(r1.headers.get('x-gateway-channel')));
  ok('集成：agent 拿到的正文非空', (r1.json?.choices?.[0]?.message?.content || '').length > 0);

  const ch = ((await call('/api/status')).json.channels || []).find((c) => c.name === 'pseudo-1');
  ok('集成：伪空被记为渠道失败', (ch?.failed ?? 0) >= 1, `failed=${ch?.failed}`);

  // 流式伪空：只有代码围栏壳 -> 同样换家
  const r2 = await chat({ model: 'pseudo-model', messages: [{ role: 'user', content: 'hi' }], stream: true });
  ok('集成：流式伪空也换家成功',
    r2.status === 200 && r2.headers.get('x-gateway-channel') === 'ok-1',
    `status=${r2.status} ch=${r2.headers.get('x-gateway-channel')}`);
  ok('集成：流式正文非空', /你好/.test(r2.text), JSON.stringify(r2.text.slice(0, 160)));

  // 任务日志里应能看到 pseudo_empty 指纹
  await wait(500);
  const errs = await call('/api/errors?limit=50');
  const fp = errs.json?.byFingerprint || {};
  ok('集成：伪空被归类为 empty_response', (fp.empty_response ?? 0) >= 1, JSON.stringify(fp));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  await wait(400);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
