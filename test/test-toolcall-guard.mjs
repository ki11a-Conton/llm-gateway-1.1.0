// 回归测试：工具调用正文泄漏守卫 —— 上游把工具调用"手写"进正文（HTTP 200）时，
// 守卫视为该渠道失败并自动换下一个渠道；只作用于请求带 tools 的调用；渠道可配 guardToolCallText:false 关闭
import { spawn } from 'node:child_process';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：mock 上游端口整体平移 + 网关端口运行时分配（避免并行跑测试时抢固定端口）
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'toolcall.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 守卫模块单元检查（不起网关）----
{
  const { findToolCallTextLeak, extractStreamContentText, hitStreamLeak } = await import(
    pathToFileURL(path.join(ROOT, 'lib', 'toolcall-guard.mjs')).href
  );
  const openaiGarbage = { choices: [{ message: { role: 'assistant', content: '<｜tool_calls｜> <invoke name="pwsh">x</invoke>' } }] };
  ok('单元: openai 正文垃圾命中 <｜tool_calls｜>', findToolCallTextLeak(openaiGarbage)?.marker === '<｜tool_calls｜>');
  ok('单元: openai 带原生 tool_calls 不算泄漏', findToolCallTextLeak({
    choices: [{ message: { role: 'assistant', content: '用 <tool_call> 格式', tool_calls: [{ id: '1', function: { name: 'f', arguments: '{}' } }] } }],
  }) === null);
  ok('单元: anthropic 文本块垃圾命中 <invoke name=', findToolCallTextLeak({
    content: [{ type: 'text', text: 'ok <invoke name="pwsh">' }],
  })?.marker === '<invoke name=');
  ok('单元: anthropic 带 tool_use 不算泄漏', findToolCallTextLeak({
    content: [{ type: 'text', text: 'ok <tool_call>' }, { type: 'tool_use', id: '1', name: 'f', input: {} }],
  }) === null);
  ok('单元: 流式 \\uFF5C 转义 + chunk 拆分可拼接识别', hitStreamLeak(extractStreamContentText(
    'data: {"delta":{"content":"<\\uFF5Ctool"}}\n\ndata: {"delta":{"content":"_calls\\uFF5C>"}}\n\n',
  )) === '<｜tool_calls｜>');
  ok('单元: 正常正文不误报', hitStreamLeak(extractStreamContentText('data: {"delta":{"content":"你好世界"}}\n\n')) === null);
  ok('单元: reasoning_content 不参与匹配', extractStreamContentText('{"reasoning_content":"<tool_call>"}') === '');
}

async function waitReady(tries = 50) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch {}
    await wait(200);
  }
  throw new Error('gateway not ready');
}
async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text, headers: res.headers };
}
const TOOLS = [{ type: 'function', function: { name: 'get_time', description: 'Get time.', parameters: { type: 'object', properties: {} } } }];
const chat = (extra) => call('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ model: 'auto', messages: [{ role: 'user', content: 'hi' }], max_tokens: 10, ...extra }),
});
const chByName = (list, name) => list.find((c) => c.name === name);

/** 把网关下发的 SSE 拼回正文文本 */
function sseContent(text) {
  let out = '';
  for (const m of text.matchAll(/^data: (.+)$/gm)) {
    if (m[1] === '[DONE]') continue;
    try { out += JSON.parse(m[1]).choices?.[0]?.delta?.content || ''; } catch {}
  }
  return out;
}

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady();

  // 1) 非流式 + 无 tools：守卫不生效，泄漏响应原样透传（文本协议 agent 场景不受影响）
  //    注意：leaky 是本模型的唯一候选渠道，所以这里必然走它
  const r1 = await chat({});
  ok('无 tools：请求走高优先级 leaky（守卫不生效）', r1.status === 200 && r1.headers.get('x-gateway-channel') === 'leaky', r1.text.slice(0, 160));
  ok('无 tools：正文垃圾原样透传', r1.json?.choices?.[0]?.message?.content?.includes('<｜tool_calls｜>') === true);

  // 2) 非流式 + tools：leaky 命中守卫 -> 自动换 good 成功
  const r2 = await chat({ tools: TOOLS });
  ok('带 tools：泄漏渠道被跳过，成功渠道是 good', r2.status === 200 && r2.headers.get('x-gateway-channel') === 'good', r2.text.slice(0, 160));
  ok('带 tools：响应正文干净', r2.json?.choices?.[0]?.message?.content === 'hello from good');
  const snap2 = (await call('/api/status')).json.channels || [];
  ok('leaky 记录 1 次失败（tool_call_text）', chByName(snap2, 'leaky')?.failed === 1, `failed=${chByName(snap2, 'leaky')?.failed}`);

  // 3) 流式 + tools：同样换到 good，客户端收到干净流
  const r3 = await chat({ tools: TOOLS, stream: true });
  ok('流式带 tools：成功渠道是 good', r3.status === 200 && r3.headers.get('x-gateway-channel') === 'good', r3.text.slice(0, 160));
  ok('流式带 tools：拼接后正文干净', sseContent(r3.text) === '你好世界!', JSON.stringify(sseContent(r3.text)));
  const snap3 = (await call('/api/status')).json.channels || [];
  // leaky 是 auto 池里的候选，第二轮重试会因为已累计 2 次失败触发熔断而不再被选中
  ok('leaky 累计 2 次失败（守卫持续生效）', (chByName(snap3, 'leaky')?.failed ?? 0) === 2, `failed=${chByName(snap3, 'leaky')?.failed}`);

  // 4) 渠道级关闭：leaky-off 配 guardToolCallText:false，泄漏响应原样透传
  const r4 = await call('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'off-model', messages: [{ role: 'user', content: 'hi' }], max_tokens: 10, tools: TOOLS }),
  });
  ok('guardToolCallText:false：垃圾原样透传', r4.status === 200 && r4.headers.get('x-gateway-channel') === 'leaky-off'
    && r4.json?.choices?.[0]?.message?.content?.includes('<｜tool_calls｜>') === true, r4.text.slice(0, 160));
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
