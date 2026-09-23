// A1 + A3 回归：OpenAI 上游 -> Anthropic 客户端（/v1/messages）方向的思考块与帧合法性
//
// A1（P0）：上游用 reasoning_content 承载思考时，走 /v1/messages 的客户端（Claude Code /
//   Anthropic SDK）必须能看到 thinking 块，且 thinking 块排在正文块之前。
//   非流式断言 content[0].type==='thinking'；流式断言 content_block_start 声明 thinking、
//   content_block_delta 是 thinking_delta、块索引从 0 起连续。
//
// A3（P0）：Anthropic 客户端方向**不做**流式正文思维链拆分（方案②：不拆、只归一），
//   否则会出现"content_block_start 说 text、delta.type 却是 thinking_delta"的非法帧。
//   本用例断言该方向下帧结构自洽、流完整收尾。
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：mock 上游端口整体平移 + 网关端口运行时分配（避免并行跑测试时抢固定端口）
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'anthropic-reasoning.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// 解析 Anthropic SSE 事件流为 [{event, data}]
function parseAnthropicSSE(raw) {
  const out = [];
  for (const block of String(raw).split('\n\n')) {
    let event = null;
    const dataLines = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
    }
    if (!dataLines.length) continue;
    try { out.push({ event, data: JSON.parse(dataLines.join('\n')) }); } catch { /* 忽略非 JSON 帧 */ }
  }
  return out;
}

// 校验一帧流里的内容块结构是否自洽：start 声明的类型必须与后续 delta 的类型匹配
function frameAudit(events) {
  const declared = new Map();   // index -> 声明的块类型
  const started = [];           // 按出现顺序的 [{index, type}]
  const deltas = [];            // [{index, deltaType}]
  const mismatches = [];
  let sawStop = false;
  const typeToDelta = { thinking: 'thinking_delta', text: 'text_delta', tool_use: 'input_json_delta' };
  for (const { data } of events) {
    if (data.type === 'content_block_start') {
      declared.set(data.index, data.content_block?.type);
      started.push({ index: data.index, type: data.content_block?.type });
    } else if (data.type === 'content_block_delta') {
      const dt = data.delta?.type;
      deltas.push({ index: data.index, deltaType: dt });
      const want = declared.get(data.index);
      if (!want || typeToDelta[want] !== dt) {
        mismatches.push({ index: data.index, declared: want, deltaType: dt });
      }
    } else if (data.type === 'message_stop') {
      sawStop = true;
    }
  }
  return { declared, started, deltas, mismatches, sawStop };
}

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
const messages = (body) => fetch(`http://127.0.0.1:${PORT}/v1/messages`, {
  method: 'POST',
  headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
  body: JSON.stringify(body),
}).then(async (res) => ({ status: res.status, headers: res.headers, text: await res.text() }));

try {
  await waitReady();
  await wait(400);

  // ===== A1 非流式：thinking 块必须在正文块之前 =====
  const r1 = await messages({ model: 'reason-model', max_tokens: 64, messages: [{ role: 'user', content: '17*23' }] });
  ok('A1 非流式：请求成功', r1.status === 200, r1.text.slice(0, 200));
  let j1 = null; try { j1 = JSON.parse(r1.text); } catch { /* ignore */ }
  const blocks = Array.isArray(j1?.content) ? j1.content : [];
  ok('A1 非流式：content 是块数组', blocks.length > 0, JSON.stringify(blocks).slice(0, 160));
  ok('A1 非流式：content[0] 是 thinking 块', blocks[0]?.type === 'thinking', JSON.stringify(blocks[0]).slice(0, 160));
  ok('A1 非流式：thinking 内容非空', typeof blocks[0]?.thinking === 'string' && blocks[0].thinking.length > 0,
    JSON.stringify(blocks[0]?.thinking || '').slice(0, 120));
  const textIdx = blocks.findIndex((b) => b.type === 'text');
  const thinkIdx = blocks.findIndex((b) => b.type === 'thinking');
  ok('A1 非流式：正文块排在 thinking 之后', thinkIdx === 0 && textIdx === 1, `think=${thinkIdx} text=${textIdx}`);
  ok('A1 非流式：正文保留了正式回答', /391/.test(blocks[textIdx]?.text || ''), JSON.stringify(blocks[textIdx]?.text));
  ok('A1 非流式：Anthropic 结构完整（type=message）', j1?.type === 'message' && j1?.role === 'assistant');

  // ===== A1 流式：thinking 块 -> 正文块，索引连续 =====
  const r2 = await messages({ model: 'reason-model', max_tokens: 64, stream: true, messages: [{ role: 'user', content: '17*23' }] });
  ok('A1 流式：请求成功', r2.status === 200, r2.text.slice(0, 200));
  const ev2 = parseAnthropicSSE(r2.text);
  ok('A1 流式：有 message_start', ev2.some((e) => e.data.type === 'message_start'));
  const a2 = frameAudit(ev2);
  ok('A1 流式：第一个内容块是 thinking', a2.started[0]?.type === 'thinking', JSON.stringify(a2.started));
  ok('A1 流式：内容块索引从 0 起连续',
    a2.started.every((b, i) => b.index === i), JSON.stringify(a2.started.map((b) => b.index)));
  ok('A1 流式：出现 thinking_delta（index 0）',
    a2.deltas.some((d) => d.deltaType === 'thinking_delta' && d.index === 0), JSON.stringify(a2.deltas));
  ok('A1 流式：出现 text_delta（index 1）',
    a2.deltas.some((d) => d.deltaType === 'text_delta' && d.index === 1), JSON.stringify(a2.deltas));
  ok('A1 流式：没有"start 说 text 但 delta 是 thinking_delta"的错配帧',
    a2.mismatches.length === 0, JSON.stringify(a2.mismatches));
  ok('A1 流式：以 message_stop 收尾', a2.sawStop);
  const thinkText = ev2.filter((e) => e.data.type === 'content_block_delta' && e.data.delta?.type === 'thinking_delta')
    .map((e) => e.data.delta.thinking).join('');
  const textText = ev2.filter((e) => e.data.type === 'content_block_delta' && e.data.delta?.type === 'text_delta')
    .map((e) => e.data.delta.text).join('');
  ok('A1 流式：thinking 文本完整', /17×23/.test(thinkText), JSON.stringify(thinkText).slice(0, 120));
  ok('A1 流式：正文文本完整（不含思维链）', /391/.test(textText) && !/17×23/.test(textText), JSON.stringify(textText).slice(0, 120));
  // thinking 块只开一次（增量连续，不应反复开关）
  ok('A1 流式：thinking 块只开启一次',
    a2.started.filter((b) => b.type === 'thinking').length === 1, JSON.stringify(a2.started));

  // ===== A3 流式 + 思维链混正文（Anthropic 客户端方向）：帧结构必须自洽 =====
  const r3 = await messages({ model: 'leak-think', max_tokens: 256, stream: true, messages: [{ role: 'user', content: '17*23' }] });
  ok('A3 流式：请求成功', r3.status === 200, r3.text.slice(0, 200));
  const ev3 = parseAnthropicSSE(r3.text);
  const a3 = frameAudit(ev3);
  ok('A3 流式：没有类型错配帧（start 声明 text 却发 thinking_delta 之类）',
    a3.mismatches.length === 0, JSON.stringify(a3.mismatches).slice(0, 240));
  ok('A3 流式：该方向不做正文拆分（不出现 thinking_delta）',
    a3.deltas.every((d) => d.deltaType !== 'thinking_delta'), JSON.stringify(a3.deltas).slice(0, 200));
  ok('A3 流式：以 message_stop 收尾', a3.sawStop);
  const text3 = ev3.filter((e) => e.data.type === 'content_block_delta' && e.data.delta?.type === 'text_delta')
    .map((e) => e.data.delta.text).join('');
  ok('A3 流式：正文完整送达（含最终答案 391）', /391/.test(text3), JSON.stringify(text3).slice(0, 160));
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
