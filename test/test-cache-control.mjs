// P4 验收：prompt caching（cache_control）保真透传 + top_k 转发 + 思考预算策略
//
// 纯适配器单元测试：只 import 两个适配器，不起网关、不连上游、**不占用任何端口**
// （因此可与其它测试并行跑，不存在端口冲突；对接 P5 的动态端口改造也无需求）。
//
// 背景（改动前的代码事实）：
//   - `grep -r cache_control lib/` 零命中：客户端标在内容块上的 cache_control 在协议转换时
//     被直接丢弃（Anthropic 客户端 -> 内部表示：anthropic.mjs 的 fromAnthropicBody 把块折叠成
//     字符串；内部表示 -> Anthropic 上游：toAnthropicMessages 重建块时只搬 text）。
//   - `top_k`：anthropic.mjs 只在 stripThinkingIncompatible 里 delete，从来不转发。
//   - 思考预算：客户端 max_tokens 一定被抬到 budget+1024（或渠道上限），没有可配策略。
//
// 验收口径来自 OPTIMIZATION-PLAN.md §P4：cache_control 不丢（内容逐字不变 + 标记仍在同一块 +
// 标记数量不变）、多标记、top_k 到达 payload、开 thinking 时 top_k 被清、策略 raise 逐位一致
// （回归锁）、策略 drop-thinking 不放大 max_tokens。
import {
  anthropicAdapter,
  toAnthropicMessages,
  fromAnthropicBody,
  carryCacheControl,
  resolveThinkingMaxTokensPolicy,
} from '../lib/adapters/anthropic.mjs';
import { openaiAdapter } from '../lib/adapters/openai.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// 每个标记都新建对象：避免"共享同一个引用"掩盖搬运错误（例如把标记挂错块时仍然相等）
const cc = (extra = {}) => ({ type: 'ephemeral', ...extra });

const BASE = { apiKey: 'k', baseUrl: 'https://api.anthropic.com/v1', protocol: 'anthropic' };
const ch = (raw = {}) => ({ ...BASE, ...raw });
const build = (channel, body) =>
  anthropicAdapter.buildRequest({ channel, model: 'claude-sonnet-4', body, stream: false }).payload;

// 标记总数（结构化计数，不看字符串，避免文本里恰好出现 cache_control 字样时误判）
function countCC(node, seen = new Set()) {
  if (Array.isArray(node)) return node.reduce((n, x) => n + countCC(x, seen), 0);
  if (!node || typeof node !== 'object') return 0;
  if (seen.has(node)) return 0;
  seen.add(node);
  let n = node.cache_control != null ? 1 : 0;
  for (const v of Object.values(node)) n += countCC(v, seen);
  return n;
}

// 内容块指纹：把"块类型 + 正文/图片源 + id/name + input + 标记"整块序列化成一行。
// 前后两个指纹序列逐项相等 == (a) 内容逐字不变 (b) 标记仍在同一个块上 (c) 标记数量不变。
function blockFp(b) {
  return JSON.stringify({
    type: b?.type,
    text: b?.text ?? null,
    source: b?.source ?? b?.image_url ?? null,
    id: b?.id ?? b?.tool_use_id ?? null,
    name: b?.name ?? null,
    input: b?.input ?? null,
    cc: b?.cache_control ?? null,
  });
}
const contentBlocks = (m) => (Array.isArray(m?.content) ? m.content : []);

/** Anthropic 形状请求体的块指纹序列（system 块在前，然后按消息顺序）。 */
function anthropicFp(body) {
  const out = [];
  const sys = Array.isArray(body.system) ? body.system : typeof body.system === 'string' ? [{ type: 'text', text: body.system }] : [];
  for (const b of sys) out.push(blockFp(b));
  for (const m of body.messages || []) for (const b of contentBlocks(m)) out.push(blockFp(b));
  return out;
}

/** 从已转换的 {system, messages} 结果里取同样的块指纹序列。 */
function builtFp(system, messages) {
  const out = [];
  const sys = Array.isArray(system) ? system : system ? [{ type: 'text', text: system }] : [];
  for (const b of sys) out.push(blockFp(b));
  for (const m of messages || []) for (const b of contentBlocks(m)) out.push(blockFp(b));
  return out;
}

const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// 贯穿全用例的输入：system 多块、user 文本+图片、assistant 文本+tool_use、user tool_result，
// 其中 6 个块各带一个标记（覆盖 Anthropic 允许挂 cache_control 的全部块类型）。
const richAnthropicBody = () => ({
  model: 'claude-sonnet-4',
  max_tokens: 512,
  system: [
    { type: 'text', text: 'SYSTEM-1' },
    { type: 'text', text: 'SYSTEM-2 前缀（长文档）', cache_control: cc() },
  ],
  messages: [
    {
      role: 'user',
      content: [
        { type: 'text', text: '第 一 段 带 空 格 / 换行\n和 emoji 🚀', cache_control: cc({ ttl: '1h' }) },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAB' }, cache_control: cc() },
        { type: 'text', text: '第二段（无标记）' },
      ],
    },
    {
      role: 'assistant',
      content: [
        { type: 'text', text: 'assistant 带标记', cache_control: cc() },
        { type: 'tool_use', id: 'toolu_1', name: 'f', input: { a: 1 }, cache_control: cc() },
      ],
    },
    {
      role: 'user',
      content: [
        { type: 'tool_result', tool_use_id: 'toolu_1', content: 'result-text', cache_control: cc() },
      ],
    },
  ],
});

// ---------------------------------------------------------------------------
console.log('\n== 统一入口：carryCacheControl（唯一搬运点）==');
{
  ok('carryCacheControl 已导出（供以后新增块类型时复用，避免又漏一处）',
    typeof carryCacheControl === 'function');
  const src = { type: 'text', text: 'x', cache_control: { type: 'ephemeral', ttl: '5m' } };
  const dst = carryCacheControl({ type: 'text', text: 'x' }, src);
  ok('标记原样搬到目标块（含 ttl 等额外字段）',
    eq(dst.cache_control, { type: 'ephemeral', ttl: '5m' }), JSON.stringify(dst.cache_control));
  ok('源块没有标记时目标块不凭空多出 cache_control',
    !('cache_control' in carryCacheControl({ type: 'text', text: 'x' }, { type: 'text', text: 'x' })));

  console.log('  策略解析:', JSON.stringify({
    unset: resolveThinkingMaxTokensPolicy({}),
    missingRouting: resolveThinkingMaxTokensPolicy({ routing: undefined }),
    raise: resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: 'raise' } }),
    drop: resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }),
    upper: resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: 'DROP-THINKING' } }),
    junk: resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: '???' } }),
  }));
  ok('未配置/未知值一律回落 raise（默认零漂移）',
    resolveThinkingMaxTokensPolicy({}) === 'raise'
      && resolveThinkingMaxTokensPolicy({ routing: undefined }) === 'raise'
      && resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: 'raise' } }) === 'raise'
      && resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: '???' } }) === 'raise');
  ok('drop-thinking 大小写不敏感',
    resolveThinkingMaxTokensPolicy({ routing: { thinkingMaxTokensPolicy: 'DROP-THINKING' } }) === 'drop-thinking');
}

// ---------------------------------------------------------------------------
console.log('\n== §1 cache_control 不丢：Anthropic 客户端 -> 内部 -> Anthropic 上游 ==');
{
  const body = richAnthropicBody();
  const before = anthropicFp(body);
  const internal = fromAnthropicBody(body);
  const internalCC = countCC(internal.messages);

  const { system, messages } = toAnthropicMessages(internal.messages);
  const after = builtFp(system, messages);

  console.log('  原始块指纹:', before.length, '个块 /', countCC(body), '个标记');
  console.log('  内部表示标记数:', internalCC);
  console.log('  往返后块数:', after.length, '/ 标记数:', countCC({ system, messages }));
  if (!eq(before, after)) {
    console.log('  before:', JSON.stringify(before));
    console.log('  after :', JSON.stringify(after));
  }
  ok('往返后块数与块内容指纹逐项一致（文本逐字不变 + 标记仍在同一块 + 位置不变）',
    eq(before, after));
  ok('标记数量不变（6 个）', countCC(body) === 6 && countCC({ system, messages }) === 6,
    `before=${countCC(body)} after=${countCC({ system, messages })}`);
  ok('内部表示同样不丢标记（Anthropic -> OpenAI 协议渠道时上游忽略即可，网关不能丢）',
    internalCC === 6, String(internalCC));
}

console.log('\n== §1b cache_control 保留在内部表示里（专供 Anthropic 协议渠道）==');
{
  const body = richAnthropicBody();
  const internal = fromAnthropicBody(body);
  console.log('  内部表示标记数:', countCC(internal));
  ok('内部表示里标记一个不少（Anthropic 协议渠道靠它按原块位置重新发出）',
    countCC(internal) === 6, String(countCC(internal)));
}

console.log('\n== §1c cache_control 不丢：OpenAI 客户端 content 块 -> Anthropic 上游 ==');
{
  const openaiBody = {
    model: 'm',
    messages: [
      {
        role: 'system',
        content: [{ type: 'text', text: 'SYS' }, { type: 'text', text: 'SYS2', cache_control: cc() }],
      },
      {
        role: 'user',
        content: [
          { type: 'text', text: 'U1', cache_control: cc() },
          { type: 'image_url', image_url: { url: 'data:image/png;base64,AAAB' }, cache_control: cc() },
        ],
      },
    ],
  };
  const payload = build(ch(  { maxTokens: 8192 }), openaiBody);
  console.log('  payload.system:', JSON.stringify(payload.system));
  console.log('  payload.messages[0]:', JSON.stringify(payload.messages[0]));
  console.log('  标记数:', countCC(payload));
  ok('system 块上的标记保住了（system 退化为文本块数组，而不是被拼成字符串）',
    Array.isArray(payload.system) && payload.system.length === 2
      && payload.system[1].text === 'SYS2' && eq(payload.system[1].cache_control, cc()));
  ok('user content 块上的标记保住了（文本 + 图片各自在同一块上）',
    countCC(payload) === 3
      && eq(payload.messages[0].content[0].cache_control, cc())
      && eq(payload.messages[0].content[1].cache_control, cc()),
    JSON.stringify(payload.messages[0].content));
  ok('未带标记的块不多出 cache_control', !('cache_control' in payload.system[0]));
}

// ---------------------------------------------------------------------------
console.log('\n== §2 多标记：两个块各带一个 -> 仍是一个块一个 ==');
{
  const body = {
    model: 'claude-sonnet-4',
    max_tokens: 128,
    messages: [
      {
        role: 'user',
        content: [
          { type: 'text', text: 'A', cache_control: cc() },
          { type: 'text', text: 'B', cache_control: cc() },
        ],
      },
    ],
  };
  const { system, messages } = toAnthropicMessages(fromAnthropicBody(body).messages);
  const blocks = messages[0]?.content || [];
  console.log('  转换后 content:', JSON.stringify(blocks));
  ok('两个块仍是两个块、各自带标记、顺序与文本不变',
    blocks.length === 2
      && blocks[0].text === 'A' && blocks[1].text === 'B'
      && eq(blocks[0].cache_control, cc()) && eq(blocks[1].cache_control, cc()));
  ok('标记数量 = 2', countCC({ system, messages }) === 2, String(countCC({ system, messages })));
}

// ---------------------------------------------------------------------------
console.log('\n== §3 top_k 能到达 payload ==');
{
  const p = build(ch(), { messages: [{ role: 'user', content: 'hi' }], top_k: 40 });
  console.log('  payload.top_k =', p.top_k);
  ok('客户端传 top_k:40 -> payload 里有 top_k:40', p.top_k === 40, String(p.top_k));

  const p0 = build(ch(), { messages: [{ role: 'user', content: 'hi' }], top_k: 0 });
  ok('top_k:0 也算显式传值（不被当 falsy 丢掉）', p0.top_k === 0, String(p0.top_k));

  const pn = build(ch(), { messages: [{ role: 'user', content: 'hi' }] });
  ok('没传 top_k 时 payload 不出现 top_k', !('top_k' in pn));
}

// ---------------------------------------------------------------------------
console.log('\n== §4 开 thinking 时 top_k 被清（temperature/top_p 同口径，既有断言不许退化）==');
{
  const p = build(ch(), {
    messages: [{ role: 'user', content: 'hi' }],
    reasoning_effort: 'high',
    top_k: 40,
    top_p: 0.9,
    temperature: 0.5,
  });
  console.log('  payload 关键字段:', JSON.stringify({
    thinking: p.thinking, top_k: p.top_k, top_p: p.top_p, temperature: p.temperature,
  }));
  ok('开 thinking', p.thinking?.type === 'enabled');
  ok('top_k 被清', !('top_k' in p), String(p.top_k));
  ok('top_p 被清', !('top_p' in p), String(p.top_p));
  ok('temperature 被清', !('temperature' in p), String(p.temperature));
}

// ---------------------------------------------------------------------------
console.log('\n== §5 策略 raise（默认）：max_tokens:100 + 上限 8192 + high -> {8192, 7168}（回归锁）==');
{
  const body = { messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'high', max_tokens: 100 };
  const dflt = build(ch({ maxTokens: 8192 }), body);                                  // 完全没配策略
  const explicit = build(ch({ maxTokens: 8192, routing: { thinkingMaxTokensPolicy: 'raise' } }), body);
  const noCap = build(ch(), body);
  console.log('  未配置    :', JSON.stringify({ max_tokens: dflt.max_tokens, thinking: dflt.thinking }));
  console.log('  显式 raise:', JSON.stringify({ max_tokens: explicit.max_tokens, thinking: explicit.thinking }));
  console.log('  上限未知  :', JSON.stringify({ max_tokens: noCap.max_tokens, thinking: noCap.thinking }));
  ok('默认 = {max_tokens:8192, budget_tokens:7168}（逐位一致）',
    dflt.max_tokens === 8192 && dflt.thinking?.budget_tokens === 7168 && dflt.thinking?.type === 'enabled',
    JSON.stringify({ m: dflt.max_tokens, t: dflt.thinking }));
  ok('显式 raise 与不配置完全一致',
    eq({ m: explicit.max_tokens, t: explicit.thinking }, { m: dflt.max_tokens, t: dflt.thinking }));
  ok('上限未知时旧公式不变（budget 32768 / max_tokens 33792）',
    noCap.max_tokens === 33792 && noCap.thinking?.budget_tokens === 32768,
    JSON.stringify({ m: noCap.max_tokens, t: noCap.thinking }));
}

// ---------------------------------------------------------------------------
console.log('\n== §6 策略 drop-thinking：不放大 max_tokens ==');
{
  const body = { messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'high', max_tokens: 100 };
  const p = build(ch({ maxTokens: 8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }), body);
  console.log('  上限 8192 + high + max_tokens:100 ->', JSON.stringify({ max_tokens: p.max_tokens, thinking: p.thinking }));
  ok('无 thinking 字段', !('thinking' in p), JSON.stringify(p.thinking));
  ok('max_tokens 保持 100（不放大）', p.max_tokens === 100, String(p.max_tokens));

  const noCap = build(ch({ routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }), body);
  ok('上限未知时同样不放大（保持 100，而不是 33792）', noCap.max_tokens === 100 && !('thinking' in noCap),
    JSON.stringify({ m: noCap.max_tokens, t: noCap.thinking }));

  // 没有 thinking 就没有互斥：采样参数与 top_k 必须原样保留（否则 drop-thinking 反而改坏了请求）
  const withSampling = build(ch({ maxTokens: 8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }), {
    ...body, temperature: 0.5, top_p: 0.9, top_k: 40,
  });
  ok('放弃思考后不误清采样参数（temperature/top_p/top_k 原样）',
    withSampling.temperature === 0.5 && withSampling.top_p === 0.9 && withSampling.top_k === 40,
    JSON.stringify({ t: withSampling.temperature, p: withSampling.top_p, k: withSampling.top_k }));

  // 上限把客户端值夹小（20000 -> 8192）不是"放大"：思考照常保留
  const clamped = build(ch({ maxTokens: 8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }), {
    messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'high', max_tokens: 20000,
  });
  ok('上限夹小客户端值时保留思考（不算放大）',
    clamped.thinking?.type === 'enabled' && clamped.max_tokens === 8192,
    JSON.stringify({ m: clamped.max_tokens, t: clamped.thinking }));

  // budget 足够小、无需放大时也不该放弃思考
  const fits = build(ch({ maxTokens: 8192, routing: { thinkingMaxTokensPolicy: 'drop-thinking' } }), {
    messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'low', max_tokens: 8192,
  });
  ok('客户端 max_tokens 已够放下 budget 时不放弃思考（low -> budget 4096, max_tokens 8192）',
    fits.thinking?.budget_tokens === 4096 && fits.max_tokens === 8192,
    JSON.stringify({ m: fits.max_tokens, t: fits.thinking }));
}

// ---------------------------------------------------------------------------
console.log('\n== 验收 7：不泄漏到 OpenAI 线格式（§4.4）==');
{
  const body = richAnthropicBody();
  const internal = fromAnthropicBody(body);
  const before = JSON.stringify(internal);
  const payload = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: internal,
    stream: false,
  }).payload;
  const leaks = (JSON.stringify(payload).match(/cache_control/g) || []).length;
  console.log('  OpenAI payload:', JSON.stringify(payload.messages));
  console.log('  payload 里 cache_control 出现次数:', leaks);
  ok('payload 里 cache_control 出现 0 次（23 个 OpenAI 协议渠道是主力路径，不能把未知字段发出去）',
    leaks === 0, String(leaks));
  ok('剥字段不污染内部表示（buildRequest 不改入参）',
    JSON.stringify(internal) === before);

  // 纯 text 块 -> 折叠成字符串（标准 OpenAI 形态，也是改动前的出站形态）
  // 注意：richAnthropicBody() 的 user 消息**含图片块**，按设计必须保持数组（见下面 362 起的用例），
  // 所以"纯文本折叠"必须用独立的纯文本 body 来验，不能从这个 payload 里找 user:string。
  console.log('  上面 payload 的 content 形态:', payload.messages.map((m) => `${m.role}:${Array.isArray(m.content) ? 'array' : typeof m.content}`).join(' '));
  const textOnly = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: { messages: [{ role: 'user', content: [{ type: 'text', text: 'A', cache_control: cc() }, { type: 'text', text: 'B' }] }] },
    stream: false,
  }).payload.messages[0].content;
  ok('全是 text 块的数组被折叠成字符串', typeof textOnly === 'string', JSON.stringify(textOnly));
  ok('折叠后文本逐字不变（A + B，分隔符与改动前的 texts.join(\'\') 一致）',
    textOnly === 'AB', JSON.stringify(textOnly));

  // 含图片 -> 保持数组，只剥字段、不改结构
  const withImage = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: {
      messages: [{
        role: 'user',
        content: [
          { type: 'text', text: '看图', cache_control: cc() },
          { type: 'image_url', image_url: { url: 'data:image/png;base64,AAAB' }, cache_control: cc() },
        ],
      }],
    },
    stream: false,
  }).payload.messages[0].content;
  console.log('  含图片的 content:', JSON.stringify(withImage));
  ok('含非 text 块时保持数组结构（不折叠）', Array.isArray(withImage) && withImage.length === 2,
    JSON.stringify(withImage));
  ok('数组内文本与图片内容逐字不变',
    withImage[0].type === 'text' && withImage[0].text === '看图'
      && withImage[1].type === 'image_url'
      && withImage[1].image_url.url === 'data:image/png;base64,AAAB',
    JSON.stringify(withImage));
  ok('数组内每个块的 cache_control 都被剥掉',
    !('cache_control' in withImage[0]) && !('cache_control' in withImage[1]),
    JSON.stringify(withImage));

  // content 是字符串 -> 不动
  const plain = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: { messages: [{ role: 'user', content: 'plain-string' }] },
    stream: false,
  }).payload.messages[0].content;
  ok('content 是字符串时不动它', plain === 'plain-string', JSON.stringify(plain));

  // system 块数组：剥字段 + 折叠，分隔符按改动前的 join('\n')（Claude Code 正是这种输入）
  const sys = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: { messages: [{ role: 'system', content: [{ type: 'text', text: 'S1' }, { type: 'text', text: 'S2', cache_control: cc() }] }, { role: 'user', content: 'u' }] },
    stream: false,
  }).payload;
  console.log('  system 消息:', JSON.stringify(sys.messages[0]));
  ok('system 块数组剥字段后按 \\n 折叠（与改动前 fromAnthropicBody 的 join(\'\\n\') 一致）',
    sys.messages[0].content === 'S1\nS2', JSON.stringify(sys.messages[0].content));
  ok('system 消息上不再有 cache_control', (JSON.stringify(sys).match(/cache_control/g) || []).length === 0);
}

console.log('\n== 验收 8：不误伤内部表示（§4.4 反向锁：剥的是 OpenAI 线格式，不是内部表示）==');
{
  const body = richAnthropicBody();
  const internal = fromAnthropicBody(body);
  const internalBefore = JSON.stringify(internal);

  // (1) 内部表示本体的标记数
  ok('内部表示标记数 = 6（未被 OpenAI 净化流程动过）', countCC(internal) === 6, String(countCC(internal)));

  // (2) 同一个内部 body 走 OpenAI 路径（会被剥）之后，再走 Anthropic 路径 —— 标记必须仍在
  const payload = openaiAdapter.buildRequest({
    channel: { apiKey: 'k', baseUrl: 'https://api.openai.com/v1' },
    model: 'm',
    body: internal,
    stream: false,
  }).payload;
  ok('OpenAI payload 已被剥干净（对照组）', (JSON.stringify(payload).match(/cache_control/g) || []).length === 0);

  ok('经过 OpenAI buildRequest 后内部表示仍未被改动（共享 dict 不被污染）',
    JSON.stringify(internal) === internalBefore);

  const built = build(ch({ maxTokens: 8192 }), internal);
  const builtCC = countCC(built);
  const { system, messages } = toAnthropicMessages(internal.messages);
  const anthCC = countCC({ system, messages });
  console.log('  Anthropic 上游 payload 标记数:', builtCC, '/ toAnthropicMessages 标记数:', anthCC);
  ok('同一内部 body 走 Anthropic 路径时标记仍在（数量不变：6）', builtCC === 6, String(builtCC));
  ok('且块位置不变（与原始输入指纹逐项一致）', eq(anthropicFp(body), builtFp(system, messages)), '');

  // 逐个块核对"标记落在原来那个块上"
  const user0 = built.messages[0]?.content || [];
  console.log('  Anthropic payload user[0].content:', JSON.stringify(user0));
  ok('标记位置精确：标记落在原来那两个带标记的块上、无标记的块仍无标记',
    eq(user0[0]?.cache_control, cc({ ttl: '1h' }))
      && eq(user0[1]?.cache_control, cc())
      && !('cache_control' in (user0[2] || {})),
    JSON.stringify(user0));
}
console.log('\n== 验收 9（附加）零漂移：没有 cache_control 的请求形态一字不变 ==');
{
  // 无标记请求：system 单块 -> 仍是字符串；user/assistant 文本块 -> 仍是单个文本块且不带标记。
  const { system, messages } = toAnthropicMessages(
    fromAnthropicBody({
      system: [{ type: 'text', text: 'SYS-A' }],
      messages: [
        { role: 'user', content: [{ type: 'text', text: 'U-A' }] },
        { role: 'assistant', content: [{ type: 'text', text: 'A-A' }] },
      ],
    }).messages,
  );
  console.log('  system:', JSON.stringify(system), 'messages:', JSON.stringify(messages));
  ok('system 仍是字符串（只有带标记时才升级为块数组）', system === 'SYS-A', JSON.stringify(system));
  ok('user 仍是文本块（形态与改动前一致）',
    messages[0].content.length === 1 && messages[0].content[0].type === 'text'
      && messages[0].content[0].text === 'U-A' && !('cache_control' in messages[0].content[0]),
    JSON.stringify(messages[0]));
  ok('assistant 文本块无 cache_control', !('cache_control' in messages[1].content[0]));

  const internal = fromAnthropicBody({
    system: [{ type: 'text', text: 'S1' }, { type: 'text', text: 'S2' }],
    messages: [{ role: 'user', content: [{ type: 'text', text: 'T1' }] }],
  });
  ok('无标记时 Anthropic -> 内部仍折叠成字符串（形态与改动前一致）',
    internal.messages[0].content === 'S1\nS2' && internal.messages[1].content === 'T1',
    JSON.stringify(internal.messages));

  const multiSystem = toAnthropicMessages([
    { role: 'system', content: 'S1' },
    { role: 'system', content: 'S2' },
    { role: 'user', content: 'U' },
  ]);
  ok('无标记的多条 system 仍用 \\n\\n 拼接（原语义）', multiSystem.system === 'S1\n\nS2',
    JSON.stringify(multiSystem.system));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
