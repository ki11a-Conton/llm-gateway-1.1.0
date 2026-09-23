// openai 适配器兼容性回归：直接调适配器（不起网关，纯单元），覆盖 6 个已确认 bug
//   1) stream_options 被整条删除 -> 流式永远拿不到 usage
//   2) enable_thinking 只在恰好小写 'high' 时才加（生产档位是 xhigh）-> 等于从没生效
//   3) OpenAI->Anthropic 有 tool_use 却回 end_turn，且 tool_use 缺 id 兜底
//   4) 流式 tool_calls 缺 index 时整条被丢弃 -> 零个 tool_use 块
//   5) Anthropic 流式客户端的 input_tokens 恒为 0
//   6) parseModels 不排序 -> 自动发现渠道的"默认模型"不确定
import { openaiAdapter, toAnthropicStopReason } from '../lib/adapters/openai.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const chan = (extra = {}) => ({ apiKey: 'k', baseUrl: 'http://127.0.0.1:9999/v1', headers: {}, ...extra });
const build = (body, stream, channel = chan()) => openaiAdapter.buildRequest({ channel, model: 'm', body, stream }).payload;

/** 把 OpenAI chunk 数组喂给 streamToAnthropic，把 SSE 帧解析成 { event, data } */
async function collect(chunks, model = 'm') {
  const source = (async function* () {
    for (const c of chunks) yield { event: 'message', data: typeof c === 'string' ? c : JSON.stringify(c) };
  })();
  const out = [];
  for await (const frame of openaiAdapter.streamToAnthropic(source, model)) {
    const nl = frame.indexOf('\n');
    const event = frame.slice('event: '.length, nl);
    const data = JSON.parse(frame.slice(nl + 1 + 'data: '.length).trim());
    out.push({ event, data });
  }
  return out;
}
const firstOf = (evs, type) => evs.find((e) => e.data.type === type)?.data;

// ---------- Bug 1：stream_options 必须原样透传 ----------
{
  const so = { include_usage: true };
  const streamed = build({ messages: [], stream_options: so }, true);
  ok('Bug1 stream:true 保留 stream_options.include_usage', streamed.stream_options?.include_usage === true,
    JSON.stringify(streamed.stream_options));
  ok('Bug1 stream:true 保留客户端给的整个 stream_options 对象（原样透传）',
    JSON.stringify(streamed.stream_options) === JSON.stringify(so), JSON.stringify(streamed.stream_options));
  ok('Bug1 回归 stream:false 行为不变（本来就没删）',
    build({ messages: [], stream_options: so }, false).stream_options?.include_usage === true);
  ok('Bug1 回归 客户端没传时不会凭空添加 stream_options',
    !('stream_options' in build({ messages: [] }, true)));
  ok('Bug1 forceStream 渠道也不会把 stream_options 删掉',
    build({ messages: [], stream_options: so }, true, chan({ forceStream: true })).stream_options?.include_usage === true);
}

// ---------- Bug 2：归一化后的思考档位都要带 enable_thinking ----------
{
  const cases = [
    ['high', true], ['HIGH', true], ['High', true],
    ['xhigh', true], ['XHIGH', true], ['XHigh', true], [' xhigh ', true],
    ['medium', true], ['low', true],
    ['off', false], ['none', false], ['NoNe', false],
  ];
  for (const [effort, expected] of cases) {
    const p = build({ messages: [], reasoning_effort: effort }, false);
    ok(`Bug2 reasoning_effort=${JSON.stringify(effort)} -> enable_thinking=${expected}`,
      (p.enable_thinking === true) === expected, `enable_thinking=${JSON.stringify(p.enable_thinking)}`);
  }
  const mixed = build({ messages: [], reasoning_effort: '  XHigh  ' }, false);
  ok('Bug2 混合大小写/空白归一后 reasoning.effort=xhigh', mixed.reasoning?.effort === 'xhigh',
    JSON.stringify(mixed.reasoning));
  ok('Bug2 非思考模型（网关不注入 reasoning_effort）绝不带 enable_thinking',
    !('enable_thinking' in build({ messages: [] }, false)));
  ok('Bug2 客户端显式 enable_thinking:false 不被覆盖',
    build({ messages: [], reasoning_effort: 'xhigh', enable_thinking: false }, false).enable_thinking === false);
  ok('Bug2 客户端显式 reasoning 对象不被覆盖',
    JSON.stringify(build({ messages: [], reasoning_effort: 'xhigh', reasoning: { effort: 'custom' } }, false).reasoning)
      === JSON.stringify({ effort: 'custom' }));
}

// ---------- Bug 3：tool_use 语义（stop_reason + id 兜底）----------
{
  const noId = openaiAdapter.toAnthropicResponse({
    id: 'chatcmpl-1',
    choices: [{
      index: 0,
      message: { role: 'assistant', content: null, tool_calls: [{ type: 'function', function: { name: 'f', arguments: '{}' } }] },
      finish_reason: null,
    }],
  }, 'm');
  ok('Bug3 有 tool_call 且缺 finish_reason -> stop_reason=tool_use', noId.stop_reason === 'tool_use', noId.stop_reason);
  ok('Bug3 产出 tool_use 块且 name/input 正确',
    noId.content.length === 1 && noId.content[0].type === 'tool_use' && noId.content[0].name === 'f'
      && JSON.stringify(noId.content[0].input) === '{}', JSON.stringify(noId.content));
  ok('Bug3 缺 id 时生成非空兜底 id',
    typeof noId.content[0].id === 'string' && noId.content[0].id.length > 0, JSON.stringify(noId.content[0].id));

  const mixed = openaiAdapter.toAnthropicResponse({
    choices: [{
      index: 0,
      message: {
        role: 'assistant',
        tool_calls: [
          { id: 'call_1', function: { name: 'a', arguments: '{"x":1}' } },
          { function: { name: 'b', arguments: '{}' } },
        ],
      },
      finish_reason: 'tool_calls',
    }],
  }, 'm');
  const ids = mixed.content.filter((b) => b.type === 'tool_use').map((b) => b.id);
  ok('Bug3 每个 tool_use 都有非空 id', ids.length === 2 && ids.every((i) => typeof i === 'string' && i.length > 0),
    JSON.stringify(ids));
  ok('Bug3 上游自带 id 原样保留，兜底 id 不撞车', ids[0] === 'call_1' && ids[1] !== 'call_1', JSON.stringify(ids));

  ok('Bug3 有 tool_call 但上游报 stop -> 仍是 tool_use',
    openaiAdapter.toAnthropicResponse({ choices: [{ message: { tool_calls: [{ id: 'c', function: { name: 'f', arguments: '{}' } }] }, finish_reason: 'stop' }] }, 'm').stop_reason === 'tool_use');
  ok('Bug3 回归 length 截断语义不被工具兜底覆盖',
    openaiAdapter.toAnthropicResponse({ choices: [{ message: { tool_calls: [{ id: 'c', function: { name: 'f', arguments: '{}' } }] }, finish_reason: 'length' }] }, 'm').stop_reason === 'max_tokens');
  ok('Bug3 回归 无 tool_call 时 stop -> end_turn',
    openaiAdapter.toAnthropicResponse({ choices: [{ message: { content: 'hi' }, finish_reason: 'stop' }] }, 'm').stop_reason === 'end_turn');
  ok('Bug3 回归 无 tool_call 且缺 finish_reason -> end_turn',
    openaiAdapter.toAnthropicResponse({ choices: [{ message: { content: 'hi' }, finish_reason: null }] }, 'm').stop_reason === 'end_turn');
  ok('Bug3 兜底与 anthropic 侧 mapStopReason 口径一致（工具 + 缺原因）',
    toAnthropicStopReason(null, true) === 'tool_use' && toAnthropicStopReason(null, false) === 'end_turn');
}

// ---------- Bug 4：流式 tool_calls 缺 index ----------
{
  const evs = await collect([
    { id: 'c1', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'f', arguments: '{}' } }] }, finish_reason: null }] },
    { id: 'c1', model: 'm', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] },
    '[DONE]',
  ]);
  const starts = evs.filter((e) => e.data.type === 'content_block_start');
  ok('Bug4 缺 index 的流式 tool_calls 也能产出 tool_use 块',
    starts.length === 1 && starts[0].data.content_block.type === 'tool_use', JSON.stringify(starts.map((s) => s.data)));
  ok('Bug4 缺 index 时 tool_use 块有非空 id',
    typeof starts[0]?.data.content_block.id === 'string' && starts[0].data.content_block.id.length > 0,
    JSON.stringify(starts[0]?.data.content_block));
  ok('Bug4 arguments 落到 input_json_delta（不再整条丢弃）',
    evs.some((e) => e.data.type === 'content_block_delta' && e.data.delta?.type === 'input_json_delta'
      && e.data.delta.partial_json === '{}'), JSON.stringify(evs.map((e) => e.data.type)));
  ok('Bug4 收尾 stop_reason=tool_use',
    firstOf(evs, 'message_delta')?.delta?.stop_reason === 'tool_use', JSON.stringify(firstOf(evs, 'message_delta')?.delta));

  // 跨 chunk 延续（上游把 arguments 拆成多帧、都不带 index）：必须落在同一个工具块里
  const evs2 = await collect([
    { id: 'c2', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ id: 'call_9', function: { name: 'f', arguments: '{"a":' } }] }, finish_reason: null }] },
    { id: 'c2', model: 'm', choices: [{ index: 0, delta: { tool_calls: [{ function: { arguments: '1}' } }] }, finish_reason: 'tool_calls' }] },
    '[DONE]',
  ]);
  const starts2 = evs2.filter((e) => e.data.type === 'content_block_start');
  const json2 = evs2.filter((e) => e.data.delta?.type === 'input_json_delta').map((e) => e.data.delta.partial_json).join('');
  ok('Bug4 缺 index 的后续增量延续同一个工具块（不重复开块）',
    starts2.length === 1 && startedTypes(starts2) === 'tool_use', JSON.stringify(starts2.map((s) => s.data)));
  ok('Bug4 续帧 arguments 拼接完整', json2 === '{"a":1}', JSON.stringify(json2));

  // 回归：带 index 的两条并行工具调用仍按 index 各开一块
  const evs3 = await collect([
    { id: 'c3', model: 'm', choices: [{ index: 0, delta: { tool_calls: [
      { index: 0, id: 'call_a', function: { name: 'a', arguments: '{}' } },
      { index: 1, id: 'call_b', function: { name: 'b', arguments: '{}' } },
    ] }, finish_reason: null }] },
    { id: 'c3', model: 'm', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] },
    '[DONE]',
  ]);
  const starts3 = evs3.filter((e) => e.data.type === 'content_block_start' && e.data.content_block.type === 'tool_use');
  ok('Bug4 回归 带 index 的双工具调用仍产出两个块（索引连续）',
    starts3.length === 2 && starts3[0].data.index === 0 && starts3[1].data.index === 1,
    JSON.stringify(starts3.map((s) => s.data)));
}

function startedTypes(starts) {
  return starts.length === 1 ? starts[0].data.content_block.type : starts.map((s) => s.data.content_block.type).join(',');
}

// ---------- Bug 5：流式 input_tokens 不再恒 0 ----------
{
  const evs = await collect([
    { id: 'c', model: 'm', choices: [{ index: 0, delta: { content: 'hi' }, finish_reason: null }] },
    { id: 'c', model: 'm', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 5, completion_tokens: 6 } },
    '[DONE]',
  ]);
  const startMsg = firstOf(evs, 'message_start')?.message;
  const finalDelta = firstOf(evs, 'message_delta');
  ok('Bug5 message_delta 带真实 input_tokens（不是恒 0）', finalDelta?.usage?.input_tokens === 5,
    JSON.stringify(finalDelta?.usage));
  ok('Bug5 message_delta 的 output_tokens 仍正确', finalDelta?.usage?.output_tokens === 6,
    JSON.stringify(finalDelta?.usage));
  ok('Bug5 message_start 仍是合法 Anthropic 结构（usage 字段齐全）',
    startMsg?.usage && 'input_tokens' in startMsg.usage && 'output_tokens' in startMsg.usage,
    JSON.stringify(startMsg?.usage));

  // 上游把 usage 放在首帧：message_start 就该是真值，不能是占位 0
  const evs2 = await collect([
    { id: 'c', model: 'm', usage: { prompt_tokens: 7, completion_tokens: 2 }, choices: [{ index: 0, delta: { content: 'x' }, finish_reason: null }] },
    { id: 'c', model: 'm', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] },
    '[DONE]',
  ]);
  ok('Bug5 首帧带 usage 时 message_start.input_tokens 直接是真值',
    firstOf(evs2, 'message_start')?.message?.usage?.input_tokens === 7,
    JSON.stringify(firstOf(evs2, 'message_start')?.message?.usage));
  ok('Bug5 首帧 usage 不会被随后的帧覆盖回 0',
    firstOf(evs2, 'message_delta')?.usage?.input_tokens === 7,
    JSON.stringify(firstOf(evs2, 'message_delta')?.usage));

  // 空流兜底：只有 usage 帧、没有 choices
  const evs3 = await collect([
    { id: 'c', model: 'm', choices: [], usage: { prompt_tokens: 9, completion_tokens: 1 } },
    '[DONE]',
  ]);
  ok('Bug5 只有 usage 帧的流：兜底 message_start 也带真实 input_tokens',
    firstOf(evs3, 'message_start')?.message?.usage?.input_tokens === 9,
    JSON.stringify(firstOf(evs3, 'message_start')?.message?.usage));
}

// ---------- Bug 6：parseModels 确定性排序 ----------
{
  const shuffled = {
    object: 'list',
    data: [{ id: 'text-embedding-3-small' }, { id: 'whisper-1' }, { id: 'gpt-4o' }, { id: 'gpt-4o-mini' }],
  };
  const a = openaiAdapter.parseModels(shuffled);
  const b = openaiAdapter.parseModels({ object: 'list', data: [...shuffled.data].reverse() });
  const c = openaiAdapter.parseModels(['whisper-1', 'gpt-4o', 'text-embedding-3-small', 'gpt-4o-mini']);
  const expected = ['gpt-4o', 'gpt-4o-mini', 'text-embedding-3-small', 'whisper-1'];
  ok('Bug6 同一份乱序输入输出稳定排序', JSON.stringify(a) === JSON.stringify(b), JSON.stringify([a, b]));
  ok('Bug6 排序为 id 升序（字符串数组同样适用）', JSON.stringify(a) === JSON.stringify(expected)
    && JSON.stringify(c) === JSON.stringify(expected), JSON.stringify([a, c]));
  ok('Bug6 渠道默认模型不再由上游顺序决定（首个是 gpt-4o，不是 embedding/whisper）', a[0] === 'gpt-4o', a[0]);
  ok('Bug6 回归 不修改上游原始数组', shuffled.data[0].id === 'text-embedding-3-small' && Array.isArray(shuffled.data) && shuffled.data.length === 4);
  ok('Bug6 回归 非列表输入仍返回 null', openaiAdapter.parseModels({}) === null && openaiAdapter.parseModels(null) === null);
  ok('Bug6 回归 name/model 兜底字段仍生效',
    JSON.stringify(openaiAdapter.parseModels({ data: [{ name: 'b-model' }, { model: 'a-model' }] })) === JSON.stringify(['a-model', 'b-model']));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
