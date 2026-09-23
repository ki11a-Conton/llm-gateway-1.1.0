// W2 回归：Anthropic 适配器的三处已确认 bug
//
// Bug 1：开了 thinking 却没清掉互斥参数（top_p/temperature + 强制 tool_choice）→ 上游 400。
//        官方约束：thinking 与 temperature/top_p/top_k 的非默认值、强制工具选择（any/tool）、
//        尾部 assistant prefill 三者都不兼容。
// Bug 2：max_tokens 被强行顶到 budget+1024，越过渠道 maxTokens 上限 → 400。
//        （实测 high 档把客户端 max_tokens:100 顶成 33792，而渠道上限只有 8192。）
// Bug 3：并行工具结果没合并 → 连续两条 user → 400 "roles must alternate"。
//
// 本用例直接 import 适配器调 buildRequest / toAnthropicMessages（纯单元，不起网关、不占端口），
// 因此可与其它并行测试同时跑，不会撞 mock 端口。
//
// P4 追加（本文件末尾）：top_k 转发（此前从来不转发）+ routing.thinkingMaxTokensPolicy
// （raise 默认必须与改动前逐位一致 / drop-thinking 宁可放弃思考也不放大 max_tokens）。
import { anthropicAdapter, toAnthropicMessages } from '../lib/adapters/anthropic.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BASE = { apiKey: 'k', baseUrl: 'https://api.anthropic.com/v1', protocol: 'anthropic' };
const chNoCap = { ...BASE };                                  // 上限未知（回归用）
const ch8192 = { ...BASE, maxTokens: 8192 };                  // config.json 的 anthropic 渠道
const chOpus = { ...BASE, maxTokens: 4096 };                  // Claude 3 Opus 上限
const chTiny = { ...BASE, maxTokens: 1024 };                  // 小到塞不下 thinking

const build = (channel, body) =>
  anthropicAdapter.buildRequest({ channel, model: 'claude-sonnet-4', body, stream: false }).payload;

const user = (text) => ({ role: 'user', content: text });
const isForcedToolChoice = (tc) => tc?.type === 'any' || tc?.type === 'tool';

console.log('\n== Bug 1：thinking 互斥参数清理 ==');
{
  // 复现任务书里的实测输入：xhigh + top_p:0.9 + tool_choice:'required' + tools
  const p = build(chNoCap, {
    messages: [user('hi')],
    reasoning_effort: 'xhigh',
    temperature: 0.5,
    top_p: 0.9,
    tool_choice: 'required',
    tools: [{ type: 'function', function: { name: 'f', parameters: { type: 'object', properties: {} } } }],
  });
  console.log('  payload 关键字段:', JSON.stringify({
    temperature: p.temperature, top_p: p.top_p, top_k: p.top_k,
    tool_choice: p.tool_choice, tools: p.tools?.length, thinking: p.thinking,
  }));
  ok('开 thinking', p.thinking?.type === 'enabled', JSON.stringify(p.thinking));
  ok('top_p 被清掉', !('top_p' in p), `top_p=${p.top_p}`);
  ok('top_k 被清掉', !('top_k' in p), `top_k=${p.top_k}`);
  ok('temperature 被清掉', !('temperature' in p), `temperature=${p.temperature}`);
  ok('强制 tool_choice（any/tool）不再出现', !isForcedToolChoice(p.tool_choice), JSON.stringify(p.tool_choice));
  ok('降级 tool_choice 但仍保留 tools（模型仍看得到工具）',
    Array.isArray(p.tools) && p.tools.length === 1 && p.tools[0].name === 'f', JSON.stringify(p.tools));
}
{
  // auto / none 与 thinking 兼容，不应被动过
  const pAuto = build(chNoCap, { messages: [user('hi')], reasoning_effort: 'high', tool_choice: 'auto' });
  ok('tool_choice=auto 与 thinking 兼容，保持 auto', pAuto.tool_choice?.type === 'auto', JSON.stringify(pAuto.tool_choice));
  const pFn = build(chNoCap, {
    messages: [user('hi')], reasoning_effort: 'high',
    tool_choice: { type: 'function', function: { name: 'f' } },
  });
  ok('指定函数（tool）与 thinking 互斥，降级为不发送', !('tool_choice' in pFn), JSON.stringify(pFn.tool_choice));
}
{
  // 反向回归：没开 thinking 时，采样参数与强制 tool_choice 必须原样保留
  const p = build(chNoCap, { messages: [user('hi')], temperature: 0.4, top_p: 0.9, tool_choice: 'required' });
  ok('未开 thinking：temperature 原样保留', p.temperature === 0.4, String(p.temperature));
  ok('未开 thinking：top_p 原样保留', p.top_p === 0.9, String(p.top_p));
  ok('未开 thinking：强制 tool_choice 保持 any', p.tool_choice?.type === 'any', JSON.stringify(p.tool_choice));
  ok('未开 thinking：不出现 thinking 字段', !('thinking' in p));
  const pOff = build(ch8192, { messages: [user('hi')], reasoning_effort: 'off', temperature: 0.3, top_p: 0.8 });
  ok('effort=off 视为不开 thinking（采样参数/不清理，也不顶 max_tokens）',
    !('thinking' in pOff) && pOff.temperature === 0.3 && pOff.top_p === 0.8, JSON.stringify(pOff));
}

console.log('\n== Bug 1b：尾部 assistant（prefill）不注入 thinking ==');
{
  const p = build(ch8192, {
    messages: [user('写一个函数'), { role: 'assistant', content: 'function f() {' }],
    reasoning_effort: 'high',
    top_p: 0.9,
  });
  console.log('  payload 关键字段:', JSON.stringify({
    roles: p.messages.map((m) => m.role), thinking: p.thinking, top_p: p.top_p, max_tokens: p.max_tokens,
  }));
  ok('prefill 时不注入 thinking', !('thinking' in p), JSON.stringify(p.thinking));
  ok('prefill 时不报错，尾部 assistant 消息保留',
    p.messages.length === 2 && p.messages[1].role === 'assistant'
      && p.messages[1].content[0].text === 'function f() {', JSON.stringify(p.messages));
  ok('prefill 时 max_tokens 不被 thinking 联动抬升（保持客户端/兜底值）',
    p.max_tokens === 8192, String(p.max_tokens));
  ok('prefill 时不做互斥清理（本就没开 thinking，top_p 原样透传）', p.top_p === 0.9, String(p.top_p));
}

console.log('\n== Bug 2：budget_tokens / max_tokens 的上限夹取 ==');
{
  // 任务书实测输入：渠道上限 8192，客户端只要 100
  const p = build(ch8192, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 });
  console.log('  high + max_tokens:100 + 上限 8192 ->', JSON.stringify({
    max_tokens: p.max_tokens, thinking: p.thinking,
  }));
  ok('budget_tokens < max_tokens（Anthropic 硬要求）',
    p.thinking?.budget_tokens < p.max_tokens, JSON.stringify({ b: p.thinking?.budget_tokens, m: p.max_tokens }));
  ok('max_tokens 不超过渠道上限 8192', p.max_tokens <= 8192, String(p.max_tokens));
  ok('客户端 max_tokens:100 没被顶到 33792 那种量级', p.max_tokens === 8192, String(p.max_tokens));
  ok('目标档位塞不进上限时降 budget（32768 -> 7168）',
    p.thinking?.budget_tokens === 7168, String(p.thinking?.budget_tokens));

  const px = build(ch8192, { messages: [user('hi')], reasoning_effort: 'xhigh', max_tokens: 100 });
  ok('xhigh 同样被夹到上限内', px.thinking?.budget_tokens === 7168 && px.max_tokens === 8192,
    JSON.stringify({ b: px.thinking?.budget_tokens, m: px.max_tokens }));

  const pOpus = build(chOpus, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 });
  ok('上限 4096（Claude 3 Opus）同样合法',
    pOpus.thinking?.budget_tokens === 3072 && pOpus.max_tokens === 4096 && pOpus.thinking.budget_tokens < pOpus.max_tokens,
    JSON.stringify({ b: pOpus.thinking?.budget_tokens, m: pOpus.max_tokens }));

  const pLow = build(ch8192, { messages: [user('hi')], reasoning_effort: 'low', max_tokens: 100 });
  ok('low 档（budget 4096）在上限 8192 内不降档，max_tokens 抬到 budget+1024',
    pLow.thinking?.budget_tokens === 4096 && pLow.max_tokens === 5120,
    JSON.stringify({ b: pLow.thinking?.budget_tokens, m: pLow.max_tokens }));

  const pBig = build(ch8192, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 32768 });
  ok('客户端要的 32768 超过渠道上限时也夹到 8192（绝不发超上限请求）',
    pBig.max_tokens === 8192 && pBig.thinking.budget_tokens < pBig.max_tokens, String(pBig.max_tokens));

  const pTiny = build(chTiny, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 600 });
  ok('上限小到放不下 thinking（1024）时不注入 thinking，也不放大 max_tokens',
    !('thinking' in pTiny) && pTiny.max_tokens === 600, JSON.stringify({ t: pTiny.thinking, m: pTiny.max_tokens }));
}
{
  // 回归：上限未知时保持改动前的行为
  const p = build(chNoCap, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 });
  console.log('  上限未知 + high + max_tokens:100 ->', JSON.stringify({ max_tokens: p.max_tokens, thinking: p.thinking }));
  ok('上限未知：budget 维持档位值 32768', p.thinking?.budget_tokens === 32768, String(p.thinking?.budget_tokens));
  ok('上限未知：max_tokens 仍按旧公式抬到 budget+1024 = 33792',
    p.max_tokens === 33792, String(p.max_tokens));

  const pMed = build(chNoCap, { messages: [user('hi')], reasoning_effort: 'medium' });
  ok('上限未知：无 max_tokens 时用兜底 4096 参与取大（medium -> 17408）',
    pMed.thinking?.budget_tokens === 16384 && pMed.max_tokens === 17408,
    JSON.stringify({ b: pMed.thinking?.budget_tokens, m: pMed.max_tokens }));

  const pMin = build({ ...chNoCap, minMaxTokens: 8 }, { messages: [user('hi')], reasoning_effort: 'medium', max_tokens: 1 });
  ok('上限未知：minMaxTokens 下限改写仍生效（1 -> 8 后再参与取大）',
    pMin.max_tokens === 17408, String(pMin.max_tokens));
}

console.log('\n== Bug 3：并行 tool_result 合并进同一条 user 消息 ==');
const rolesAlternate = (msgs) =>
  msgs.length > 0 && msgs[0].role === 'user' && msgs.every((m, i) => i === 0 || m.role !== msgs[i - 1].role);
{
  // 实测非法输出：[user(text), assistant([tool_use,tool_use]), user(tr_a), user(tr_b)]
  const { messages } = toAnthropicMessages([
    user('hi'),
    {
      role: 'assistant', content: '',
      tool_calls: [
        { id: 'call_a', type: 'function', function: { name: 'f', arguments: '{"x":1}' } },
        { id: 'call_b', type: 'function', function: { name: 'g', arguments: '{}' } },
      ],
    },
    { role: 'tool', tool_call_id: 'call_a', content: 'ra' },
    { role: 'tool', tool_call_id: 'call_b', content: 'rb' },
  ]);
  console.log('  roles:', JSON.stringify(messages.map((m) => m.role)),
    'tool_results:', JSON.stringify((messages[2]?.content || []).filter((b) => b.type === 'tool_result').map((b) => b.tool_use_id)));
  ok('两个并行 tool_result 合并成一条 user 消息', messages.length === 3 && messages[2].role === 'user',
    JSON.stringify(messages.map((m) => m.role)));
  const trs = (messages[2]?.content || []).filter((b) => b.type === 'tool_result');
  ok('两个 tool_result 都在同一条 user 里', trs.length === 2, JSON.stringify(messages[2]?.content));
  ok('tool_result 保持调用顺序（call_a 在前）', trs[0]?.tool_use_id === 'call_a' && trs[1]?.tool_use_id === 'call_b',
    JSON.stringify(trs.map((b) => b.tool_use_id)));
  ok('内容与 tool_call_id 未串台', trs[0]?.content === 'ra' && trs[1]?.content === 'rb',
    JSON.stringify(trs.map((b) => b.content)));
  ok('相邻角色严格 user/assistant 交替', rolesAlternate(messages), JSON.stringify(messages.map((m) => m.role)));
  ok('assistant 里两个 tool_use 都在', (messages[1]?.content || []).filter((b) => b.type === 'tool_use').length === 2,
    JSON.stringify(messages[1]?.content));
}
{
  // 常见形态：tool_result 后面紧跟一条普通 user 文本
  const { messages } = toAnthropicMessages([
    user('hi'),
    { role: 'assistant', content: '按你说的做了', tool_calls: [{ id: 'call_a', type: 'function', function: { name: 'f', arguments: '{}' } }] },
    { role: 'tool', tool_call_id: 'call_a', content: 'ra' },
    user('再顺便做第二件事'),
  ]);
  console.log('  roles:', JSON.stringify(messages.map((m) => m.role)),
    'last:', JSON.stringify(messages[messages.length - 1]?.content));
  ok('tool_result + 紧跟的普通 user 文本合成一条 user', messages.length === 3, JSON.stringify(messages.map((m) => m.role)));
  const last = messages[messages.length - 1]?.content || [];
  ok('合成后仍含 tool_result 与文本两块',
    last.length === 2 && last.some((b) => b.type === 'tool_result') && last.some((b) => b.type === 'text' && b.text === '再顺便做第二件事'),
    JSON.stringify(last));
  ok('tool_result 排在文本块之前（Anthropic 要求）', last[0]?.type === 'tool_result', JSON.stringify(last.map((b) => b.type)));
  ok('相邻角色严格交替', rolesAlternate(messages), JSON.stringify(messages.map((m) => m.role)));
}
{
  // 三个并行结果 + 更长的会话：整段角色必须交替、首条为 user
  const { messages } = toAnthropicMessages([
    { role: 'system', content: 'sys' },
    user('hi'),
    {
      role: 'assistant', content: '',
      tool_calls: [
        { id: 'c1', type: 'function', function: { name: 'a', arguments: '{}' } },
        { id: 'c2', type: 'function', function: { name: 'b', arguments: '{}' } },
        { id: 'c3', type: 'function', function: { name: 'c', arguments: '{}' } },
      ],
    },
    { role: 'tool', tool_call_id: 'c1', content: '1' },
    { role: 'tool', tool_call_id: 'c2', content: '2' },
    { role: 'tool', tool_call_id: 'c3', content: '3' },
    user('继续'),
    { role: 'assistant', content: '好' },
  ]);
  ok('三个并行 tool_result + 追加文本合成一条 user（共 4 条消息）', messages.length === 4,
    JSON.stringify(messages.map((m) => m.role)));
  const trs = (messages[2]?.content || []).filter((b) => b.type === 'tool_result');
  ok('三个 tool_result 顺序完整', trs.map((b) => b.tool_use_id).join(',') === 'c1,c2,c3',
    JSON.stringify(trs.map((b) => b.tool_use_id)));
  ok('全部角色严格交替且首条为 user', rolesAlternate(messages), JSON.stringify(messages.map((m) => m.role)));
}
{
  // 回归：普通"连续同角色文本合并"的语义没有被破坏
  const { messages } = toAnthropicMessages([user('第一句'), user('第二句'), { role: 'assistant', content: '好' }]);
  const merged = messages[0]?.content || [];
  ok('连续普通 user 消息仍然合并且顺序不变',
    messages.length === 2 && merged.length === 2 && merged[0].text === '第一句' && merged[1].text === '第二句',
    JSON.stringify(messages));
  const noTool = toAnthropicMessages([{ role: 'assistant', content: '先说的' }, user('hi')]);
  ok('首条非 user 仍被丢弃（原有语义）', noTool.messages.length === 1 && noTool.messages[0].role === 'user',
    JSON.stringify(noTool.messages.map((m) => m.role)));
}

console.log('\n== P4-a：top_k 转发（此前从来不转发，客户端传了等于白丢）==');
{
  const p = build(chNoCap, { messages: [user('hi')], top_k: 40 });
  ok('未开 thinking：客户端 top_k:40 到达 payload', p.top_k === 40, String(p.top_k));
  ok('未显式传 top_k：payload 不凭空多出该字段', !('top_k' in build(chNoCap, { messages: [user('hi')] })));

  const pThinking = build(chNoCap, { messages: [user('hi')], reasoning_effort: 'high', top_k: 40 });
  ok('开 thinking：显式传入的 top_k:40 被清掉（官方互斥，既有断言同口径）',
    !('top_k' in pThinking), String(pThinking.top_k));
}

console.log('\n== P4-b：routing.thinkingMaxTokensPolicy ==');
{
  // 回归锁：默认（不配策略）必须是 raise，且与本文件 Bug 2 的数字逐位一致
  const pDefault = build(ch8192, { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 });
  const pRaise = build(
    { ...ch8192, routing: { thinkingMaxTokensPolicy: 'raise' } },
    { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 },
  );
  console.log('  未配置 vs 显式 raise:', JSON.stringify({
    d: { max_tokens: pDefault.max_tokens, thinking: pDefault.thinking },
    r: { max_tokens: pRaise.max_tokens, thinking: pRaise.thinking },
  }));
  ok('默认策略 = raise：{max_tokens:8192, budget_tokens:7168}（逐位一致）',
    pDefault.max_tokens === 8192 && pDefault.thinking?.type === 'enabled'
      && pDefault.thinking?.budget_tokens === 7168,
    JSON.stringify({ m: pDefault.max_tokens, t: pDefault.thinking }));
  ok('显式 raise 与不配置的 payload 完全一致', JSON.stringify(pRaise) === JSON.stringify(pDefault));

  // drop-thinking：客户端 max_tokens 是硬意图
  const pDrop = build(
    { ...ch8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } },
    {
      messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100,
      temperature: 0.5, top_p: 0.9, top_k: 40,
    },
  );
  console.log('  drop-thinking（上限 8192）:', JSON.stringify({
    max_tokens: pDrop.max_tokens, thinking: pDrop.thinking,
    temperature: pDrop.temperature, top_p: pDrop.top_p, top_k: pDrop.top_k,
  }));
  ok('drop-thinking：不注入 thinking', !('thinking' in pDrop), JSON.stringify(pDrop.thinking));
  ok('drop-thinking：max_tokens 保持客户端 100（不放大）', pDrop.max_tokens === 100, String(pDrop.max_tokens));
  ok('drop-thinking：没有 thinking 就不做互斥清理（temperature/top_p/top_k 原样保留）',
    pDrop.temperature === 0.5 && pDrop.top_p === 0.9 && pDrop.top_k === 40,
    JSON.stringify({ t: pDrop.temperature, p: pDrop.top_p, k: pDrop.top_k }));

  const pDropNoCap = build(
    { ...chNoCap, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } },
    { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 },
  );
  ok('drop-thinking：上限未知时也不放大（100，而不是 33792）',
    pDropNoCap.max_tokens === 100 && !('thinking' in pDropNoCap),
    JSON.stringify({ m: pDropNoCap.max_tokens, t: pDropNoCap.thinking }));

  const pDropFits = build(
    { ...ch8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } },
    { messages: [user('hi')], reasoning_effort: 'low', max_tokens: 8192 },
  );
  ok('drop-thinking：客户端 max_tokens 放得下 budget 时照常思考（low -> 4096 / 8192）',
    pDropFits.thinking?.budget_tokens === 4096 && pDropFits.max_tokens === 8192,
    JSON.stringify({ m: pDropFits.max_tokens, t: pDropFits.thinking }));

  const pDropClamp = build(
    { ...ch8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } },
    { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 20000 },
  );
  ok('drop-thinking：上限把客户端值夹小不算"放大"，思考保留（20000 -> 8192）',
    pDropClamp.thinking?.type === 'enabled' && pDropClamp.max_tokens === 8192,
    JSON.stringify({ m: pDropClamp.max_tokens, t: pDropClamp.thinking }));

  // 未知策略值不得改变既有行为（回落 raise）
  const pJunk = build(
    { ...ch8192, routing: { thinkingMaxTokensPolicy: 'no-such-policy' } },
    { messages: [user('hi')], reasoning_effort: 'high', max_tokens: 100 },
  );
  ok('未知策略值回落 raise（行为与默认一致）', JSON.stringify(pJunk) === JSON.stringify(pDefault));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
