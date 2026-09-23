// util.mjs 加固回归测试
//  ① parseSSE 必须支持 CRLF：只找 '\n\n' 时，合法的 CRLF 流（'\r\n\r\n'）永远切不开，
//     整条流会被攒到 EOF 当成"一个"事件，数据全部丢失 -> 客户端拿到空回答。
//  ② classifyUpstreamFailure 必须把"普通参数 400"和"模型不存在"分开。
//     判错代价极大：误判成 modelIssue 会让该渠道对该模型【被拉黑】；
//     误判成 auth 会让每个被撞到的渠道冷却 10 分钟、且请求卡满 maxTotalWaitMs 才回 503。
//  ③ 429 正文里同时出现"额度"字样时，以"限流"为准（否则白冷却 900s 且跳过限流的特殊处理）。
import { parseSSE, classifyUpstreamFailure, isEffortRejection } from '../lib/util.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// ---------- ① parseSSE ----------
async function* bytes(list) { for (const s of list) yield Buffer.from(s, 'utf8'); }
async function collect(list) {
  const out = [];
  for await (const e of parseSSE(bytes(list))) out.push(e);
  return out;
}

{
  const lf = [
    'event: message_start\ndata: {"a":1}\n\n',
    'event: content_block_delta\ndata: {"b":2}\n\n',
    'event: message_stop\ndata: {"c":3}\n\n',
  ];
  const a = await collect(lf);
  ok('LF 流解析出 3 个事件', a.length === 3, `实际=${a.length}`);
  ok('LF 流事件名正确', a.map((e) => e.event).join(',') === 'message_start,content_block_delta,message_stop', a.map((e) => e.event).join(','));

  // 同一条流改成 CRLF：这正是回归点
  const crlf = lf.map((f) => f.replace(/\n/g, '\r\n'));
  const b = await collect(crlf);
  ok('CRLF 流同样解析出 3 个事件（回归：原先只有 1 个）', b.length === 3, `实际=${b.length}`);
  ok('CRLF 流事件名正确', b.map((e) => e.event).join(',') === 'message_start,content_block_delta,message_stop', b.map((e) => e.event).join(','));
  ok('CRLF 流的 data 仍可 JSON 解析', (() => { try { JSON.parse(b[0]?.data ?? ''); return true; } catch { return false; } })(), JSON.stringify(b[0]?.data));

  // 分隔符被 TCP 分片切开
  const split = await collect(['event: message_start\r\ndata: {"a":1}\r', '\n\r\nevent: message_stop\r\ndata: {"c":3}\r\n\r\n']);
  ok('CRLF 分隔符跨 chunk 也能正确切帧', split.length === 2 && split[0].event === 'message_start', `实际=${split.length}`);

  // 裸 CR 分隔（SSE 规范也允许）
  const cr = await collect(['event: message_start\rdata: {"a":1}\r\r', 'event: message_stop\rdata: {"c":3}\r\r']);
  ok('裸 CR 分隔也能切帧', cr.length === 2, `实际=${cr.length}`);
}

// ---------- ② 400 分类 ----------
{
  const cases = [
    // [status, body, 期望 kind, 期望 modelIssue, 说明]
    [400, '{"error":{"message":"max_tokens is too large: 33792. This model supports at most 8192 completion tokens","type":"invalid_request_error"}}', 'bad_request', false, 'max_tokens 超上限（曾误判为模型问题 -> 永久拉黑）'],
    [400, '{"error":{"message":"This model\'s maximum context length is 8192 tokens. However your messages have 20000 tokens","type":"invalid_request_error","code":"context_length_exceeded"}}', 'bad_request', false, '上下文超长（曾误判为模型问题 -> 永久拉黑）'],
    [400, '{"error":{"message":"Invalid \'temperature\': this model does not support temperature != 1","type":"invalid_request_error"}}', 'bad_request', false, '参数不被模型支持（曾误判为模型问题）'],
    [400, '{"error":{"message":"The model `gpt-4o` does not exist","type":"invalid_request_error","code":"model_not_found"}}', 'model_unsupported', true, '真的模型不存在（必须仍能识别）'],
    [400, '{"error":{"message":"unknown model: foo-bar"}}', 'model_unsupported', true, 'unknown model 仍要识别'],
    [400, '{"error":{"message":"no provider supported for this model"}}', 'model_unsupported', true, 'no provider supported 仍要识别'],
    [404, '{"error":{"message":"The model `x` does not exist"}}', 'model_not_found', true, '404 模型不存在'],
    [404, '{"detail":"Not Found"}', 'model_not_found', false, '404 但没提模型（路径写错）不得拉黑'],
    [400, '{"error":{"message":"Invalid API key provided","type":"invalid_request_error"}}', 'auth', false, '真的鉴权失败仍要识别'],
    [401, '{"error":{"message":"Unauthorized"}}', 'auth', false, '401 -> auth'],
    [403, 'forbidden', 'auth', false, '403 -> auth'],
    [400, '{"error":{"message":"invalid_token"}}', 'auth', false, 'OAuth 的 invalid_token 仍要识别'],
    [429, '{"error":{"message":"rpm exhausted, please retry later"}}', 'rate_limit', false, '429 限流文案（曾因额度字样被误判为余额不足）'],
    [429, '{"error":{"message":"inference exceeds tpm/rpm limit"}}', 'rate_limit', false, '429 tpm/rpm 限流'],
    [429, '{"error":{"message":"You exceeded your current quota, please check your plan and billing details"}}', 'insufficient_balance', false, '429 但明确是欠费 -> 仍按余额不足'],
    [402, 'anything', 'insufficient_balance', false, '402 -> 余额不足'],
    [500, 'boom', 'server_error', false, '5xx -> 可重试服务错误'],
  ];
  for (const [status, body, kind, modelIssue, desc] of cases) {
    const got = classifyUpstreamFailure(status, body);
    ok(`分类：${desc} -> ${kind}`, got.kind === kind && got.modelIssue === modelIssue, `实际 kind=${got.kind} modelIssue=${got.modelIssue}`);
  }

  // ④ 误判成 auth 的代价特别大（全渠道冷却 10 分钟），单独钉死这一条
  const authish = classifyUpstreamFailure(400, '{"error":{"message":"Invalid \'max_tokens\': integer below minimum value","type":"invalid_request_error","param":"max_tokens","code":"integer_below_min_value"}}');
  ok('参数 400 绝不能被判成 auth（曾导致全渠道冷却 10 分钟 + 卡满预算才回 503）', authish.kind !== 'auth', `实际 kind=${authish.kind}`);
  ok('参数 400 也不应被判成 modelIssue', authish.modelIssue === false, `modelIssue=${authish.modelIssue}`);
}

// ---------- ⑤ 既有语义不得被改坏 ----------
{
  ok('isEffortRejection 仍能识别档位非法', isEffortRejection('field ReasoningEffort invalid, should be one of: low, medium, high') === true);
  ok('isEffortRejection 不误伤普通报错', isEffortRejection('max_tokens is too large') === false);
  ok('5xx 仍可重试', classifyUpstreamFailure(503, 'bad gateway').retryable === true);
  ok('余额不足仍不可重试', classifyUpstreamFailure(402, 'payment required').retryable === false);
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
