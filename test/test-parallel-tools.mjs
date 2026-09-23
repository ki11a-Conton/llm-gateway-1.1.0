// F2 回归：OpenAI → Anthropic 流式转换必须正确处理**交错的并行工具调用**
//
// 背景（代码审查 2026-09-20 F2/P1）：转换器只维护一个 `current` 内容块，工具 index 一变就
// 关掉当前块、另开一个新块。而 OpenAI 允许两个工具的参数增量交错到达
// （第一帧同时含 index 0/1，第二帧继续补 index 0/1，且第二帧通常不再带 id/name）——
// 于是 2 个工具被拆成 4 个残缺 tool_use 块：前两个只剩 `{"q":`，后两个函数名为空、
// id 被重新生成、参数只剩 `"a"}` / `"b"}`。
//
// 本套件断言：工具数量、id、名称、JSON 参数与上游一致，且每块只 start/stop 一次。
import { openaiAdapter } from '../lib/adapters/openai.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

/** 把 OpenAI chunk 数组包成 SSE 事件流（末尾补 [DONE]，模拟正常收尾） */
function eventsOf(chunks, { done = true } = {}) {
  return (async function* () {
    for (const c of chunks) yield { event: 'message', data: JSON.stringify(c) };
    if (done) yield { event: 'message', data: '[DONE]' };
  })();
}

/** 解析 Anthropic 帧，重建块状态：{ index -> { type, id, name, json, starts, stops } } */
function parseFrames(frames) {
  const blocks = new Map();
  const events = [];
  for (const f of frames) {
    const m = String(f).match(/^event: (\S+)\ndata: (.*)$/m);
    if (!m) continue;
    const [, evName, dataRaw] = m;
    let json = null;
    try { json = JSON.parse(dataRaw); } catch { continue; }
    events.push({ event: evName, json });
    if (evName === 'content_block_start') {
      const i = json.index;
      const b = blocks.get(i) || { starts: 0, stops: 0, json: '' };
      b.starts += 1;
      b.type = json.content_block?.type;
      b.id = json.content_block?.id;
      b.name = json.content_block?.name;
      blocks.set(i, b);
    } else if (evName === 'content_block_stop') {
      const b = blocks.get(json.index) || { starts: 0, stops: 0, json: '' };
      b.stops += 1;
      blocks.set(json.index, b);
    } else if (evName === 'content_block_delta') {
      const b = blocks.get(json.index) || { starts: 0, stops: 0, json: '' };
      if (json.delta?.type === 'input_json_delta') b.json += json.delta.partial_json || '';
      blocks.set(json.index, b);
    }
  }
  return { blocks: [...blocks.entries()].sort((a, b) => a[0] - b[0]).map(([, b]) => b), events };
}

async function convert(chunks, opts) {
  const frames = [];
  for await (const f of openaiAdapter.streamToAnthropic(eventsOf(chunks, opts), 'test-model')) frames.push(f);
  return parseFrames(frames);
}

console.log('— F2：交错并行工具调用 —');

// ---------- 1) 两个工具交错增量（审查报告的原复现）----------
{
  const c1 = {
    id: 's1', model: 'test-model',
    choices: [{ index: 0, delta: { tool_calls: [
      { index: 0, id: 'call_a', type: 'function', function: { name: 'fa', arguments: '{"q":' } },
      { index: 1, id: 'call_b', type: 'function', function: { name: 'fb', arguments: '{"q":' } },
    ] } }],
  };
  const c2 = {
    id: 's1', model: 'test-model',
    choices: [{ index: 0, delta: { tool_calls: [
      { index: 0, function: { arguments: '"a"}' } },
      { index: 1, function: { arguments: '"b"}' } },
    ] } }],
  };
  const { blocks } = await convert([c1, c2]);
  ok('两个工具 => 恰好两个 tool_use 块（不是四个残缺块）', blocks.length === 2, `blocks=${blocks.length} ${JSON.stringify(blocks)}`);
  ok('每块只 start 一次、stop 一次',
    blocks.every((b) => b.starts === 1 && b.stops === 1 && b.type === 'tool_use'),
    JSON.stringify(blocks));
  ok('id 与上游一致（call_a / call_b，未被重新生成）',
    blocks[0]?.id === 'call_a' && blocks[1]?.id === 'call_b', JSON.stringify(blocks.map((b) => b.id)));
  ok('函数名与上游一致（fa / fb，没有空名）',
    blocks[0]?.name === 'fa' && blocks[1]?.name === 'fb', JSON.stringify(blocks.map((b) => b.name)));
  ok('参数完整还原成 {"q":"a"} / {"q":"b"}',
    blocks[0]?.json === '{"q":"a"}' && blocks[1]?.json === '{"q":"b"}', JSON.stringify(blocks.map((b) => b.json)));
}

// ---------- 2) 三个工具交错 + 单工具跨多帧 ----------
{
  const mk = (deltas) => ({ id: 's2', model: 'test-model', choices: [{ index: 0, delta: { tool_calls: deltas } }] });
  const chunks = [
    mk([
      { index: 0, id: 'c0', function: { name: 'f0', arguments: '{"a"' } },
      { index: 1, id: 'c1', function: { name: 'f1', arguments: '{"b"' } },
    ]),
    mk([
      { index: 0, function: { arguments: ':1}' } },
      { index: 1, function: { arguments: ':2}' } },
      { index: 2, id: 'c2', function: { name: 'f2', arguments: '{"c":3}' } },
    ]),
    mk([{ index: 1, function: { arguments: '' } }]),
  ];
  const { blocks } = await convert(chunks);
  ok('三个工具 => 恰好三个 tool_use 块', blocks.length === 3, `blocks=${blocks.length}`);
  ok('三个工具的参数分别完整',
    blocks.map((b) => b.json).join('|') === '{"a":1}|{"b":2}|{"c":3}', JSON.stringify(blocks.map((b) => b.json)));
  ok('三个工具的 id/name 正确',
    blocks.map((b) => `${b.id}/${b.name}`).join('|') === 'c0/f0|c1/f1|c2/f2',
    JSON.stringify(blocks.map((b) => `${b.id}/${b.name}`)));
  ok('每块只 start/stop 一次（空增量帧不另开块）',
    blocks.every((b) => b.starts === 1 && b.stops === 1), JSON.stringify(blocks));
}

// ---------- 3) 工具与正文相邻：块顺序正确且不互相破坏 ----------
{
  const chunks = [
    { id: 's3', model: 'm', choices: [{ index: 0, delta: { content: '先说一句。' } }] },
    { id: 's3', model: 'm', choices: [{ index: 0, delta: { tool_calls: [
      { index: 0, id: 'tx', function: { name: 'fx', arguments: '{"k"' } },
    ] } }] },
    { id: 's3', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: ':1}' } }] } }] },
    { id: 's3', model: 'm', choices: [{ index: 0, delta: { content: '收尾。' }, finish_reason: 'tool_calls' }] },
  ];
  const { blocks, events } = await convert(chunks);
  const types = blocks.map((b) => b.type);
  // 取舍说明：Anthropic 的 content_block 必须顺序 start→delta→stop，无法同时开着两个块。
  // 为满足 F2 的核心要求（工具数量/id/名称/参数与上游一致），工具块统一延后到收尾按 index 输出；
  // 代价是"工具调用前后各有一段正文"会合并进同一个 text 块——信息不丢，只是块分段合并。
  ok('正文与工具块顺序合法（正文 -> tool_use；相邻正文合并为一个 text 块）',
    types.join(',') === 'text,tool_use', JSON.stringify(types));
  const tool = blocks.find((b) => b.type === 'tool_use');
  ok('工具参数完整（{"k":1}）', tool?.json === '{"k":1}', JSON.stringify(tool));
  ok('message_delta 的 stop_reason 是 tool_use（不是 stop）',
    events.some((e) => e.event === 'message_delta' && e.json?.delta?.stop_reason === 'tool_use'),
    JSON.stringify(events.filter((e) => e.event === 'message_delta').map((e) => e.json?.delta?.stop_reason)));
}

// ---------- 4) 缺 id/name 的上游（部分中转只发 arguments）：按 index 归属，不重复开块 ----------
{
  const chunks = [
    { id: 's4', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '{"x":' } }] } }] },
    { id: 's4', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '1}' } }] } }] },
    { id: 's4', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ index: 1, function: { arguments: '{"y":2}' } }] } }] },
  ];
  const { blocks } = await convert(chunks);
  ok('缺 id/name 的两条按 index 归成两块', blocks.length === 2, `blocks=${blocks.length}`);
  ok('缺 id 时兜了 id（非空）且参数不丢',
    blocks.every((b) => typeof b.id === 'string' && b.id.length > 0) && blocks[0].json === '{"x":1}',
    JSON.stringify(blocks));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
