// 回归测试：思考过程泄漏进正文 + 空调用
//
// 两类线上现象：
//   ① 思考过程直接显现在正文内容里（上游字段名不统一，或干脆把思维链写进 content）
//   ② 空调用：HTTP 200 但 content/tool_calls/reasoning 全空，agent 收不到任何东西
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
const CFG = materializeConfig(path.join(HERE, 'reasoning.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 单元：思考字段归一化 ----------
{
  const {
    flattenReasoning, normalizeReasoning, splitInlineReasoning,
    looksLikeReasoning, cleanMessageReasoning,
  } = await import('../lib/reasoning-guard.mjs');

  // ① 字段名归一化：各家叫法不同，统一到 reasoning_content
  const m1 = { role: 'assistant', content: '答案是 391', reasoning: '让我算一下 17*23……' };
  normalizeReasoning(m1);
  ok('单元：reasoning 字段归一到 reasoning_content',
    m1.reasoning_content === '让我算一下 17*23……' && m1.reasoning === undefined, JSON.stringify(m1));

  const m2 = { role: 'assistant', content: 'x', thinking: '思考中' };
  normalizeReasoning(m2);
  ok('单元：thinking 字段归一到 reasoning_content', m2.reasoning_content === '思考中' && m2.thinking === undefined);

  const m3 = { role: 'assistant', content: 'x', reasoning_details: [{ text: '第一段' }, { text: '第二段' }] };
  normalizeReasoning(m3);
  ok('单元：reasoning_details 数组展开成文本', m3.reasoning_content === '第一段\n第二段', String(m3.reasoning_content));

  ok('单元：flattenReasoning 支持 {text}', flattenReasoning({ text: 'A' }) === 'A');
  ok('单元：flattenReasoning 支持嵌套 summary', flattenReasoning([{ summary: { text: 'S' } }]) === 'S');

  // ② 正文里的思维链识别（用实测拿到的真实文本）
  const realLeakEn = `Sure! Let's break this down step by step.

**Step 1: Break down the multiplication using distributive property**  
We can think of 23 as 20 + 3, so:
17 x 23 = (17 x 20) + (17 x 3)

**Step 2: Multiply each part**
- 17 x 20 = 340
- 17 x 3 = 51

So the final answer is 391.`;
  ok('单元：识别出正文是一段思维链', looksLikeReasoning(realLeakEn));
  ok('单元：短回答不误判为思维链', looksLikeReasoning('答案是 391') === false);

  const split1 = splitInlineReasoning(realLeakEn);
  ok('单元：思维链被切分出最终答案', split1.split === true && /391/.test(split1.content), JSON.stringify(split1).slice(0, 200));
  ok('单元：切出的思考段落进 reasoning', /Step 1/.test(split1.reasoning), split1.reasoning.slice(0, 80));

  // 中文思维链 + "综上/因此答案"分界
  const realLeakZh = `我需要先计算 17 乘以 23。首先把 23 拆成 20 加 3，然后分别相乘再相加。
17 乘 20 等于 340，17 乘 3 等于 51，两者相加得到 391。
综上，答案是 391。`;
  const split2 = splitInlineReasoning(realLeakZh);
  ok('单元：中文思维链被切分', split2.split === true && /391/.test(split2.content), JSON.stringify(split2).slice(0, 200));

  // 正常长回答不能被误切（没有思维链自述特征）
  const normal = `以下是三种常见方案：

1. 方案 A：使用连接池，适合高并发读场景，成本低。
2. 方案 B：分库分表，适合数据量极大的场景，但改造成本高。
3. 方案 C：引入缓存层，适合读多写少的场景。

建议优先从方案 A 开始，验证后再考虑其它方案。`;
  ok('单元：正常长回答不被误切成思考过程', splitInlineReasoning(normal).split === false);

  // 带原生 tool_calls 时绝不动正文
  const m4 = { role: 'assistant', content: realLeakEn, tool_calls: [{ id: '1', function: { name: 'f', arguments: '{}' } }] };
  const r4 = cleanMessageReasoning(m4);
  ok('单元：有原生 tool_calls 时不切正文', r4.split === false && m4.content === realLeakEn);
}

// ---------- 单元：annotations 是恒为 null 的占位键，必须原样透传（A2）----------
// 实测（channel 17 / ai.hyper.nyc.mn）确认 annotations 既不承载思考也不承载 citations，
// 所以故意不放进 REASONING_FIELDS。这里锁定该决策，避免以后有人"顺手"把它加进去。
{
  const { normalizeReasoning, cleanMessageReasoning } = await import('../lib/reasoning-guard.mjs');
  const { openaiAdapter } = await import('../lib/adapters/openai.mjs');

  const mAnn = { role: 'assistant', content: '正式回答', annotations: null };
  const rAnn = normalizeReasoning(mAnn);
  ok('单元：annotations 不被当作思考字段归一',
    rAnn.changed === false && 'annotations' in mAnn && mAnn.annotations === null && mAnn.reasoning_content === undefined,
    JSON.stringify(mAnn));

  const cAnn = cleanMessageReasoning({ role: 'assistant', content: '正式回答', annotations: null });
  ok('单元：annotations 不会被拆成 reasoning_content',
    cAnn.changed === false && cAnn.reasoningChars === 0, JSON.stringify(cAnn));

  const annResp = {
    id: 'chatcmpl-ann', object: 'chat.completion', created: 1, model: 'm',
    choices: [{ index: 0, message: { role: 'assistant', content: '正式回答', annotations: null }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 2 },
  };
  const outAnn = openaiAdapter.toOpenAIResponse(annResp, 'm');
  ok('单元：annotations 原样透传（OpenAI 方向）',
    'annotations' in outAnn.choices[0].message
      && outAnn.choices[0].message.annotations === null
      && outAnn.choices[0].message.reasoning_content === undefined,
    JSON.stringify(outAnn.choices[0].message));

  const outAnnAnthropic = openaiAdapter.toAnthropicResponse(annResp, 'm');
  ok('单元：annotations 不会变成 Anthropic thinking 块',
    outAnnAnthropic.content.every((b) => b.type !== 'thinking'),
    JSON.stringify(outAnnAnthropic.content));
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
const sseText = (raw) => {
  let out = '';
  for (const m of raw.matchAll(/^data: (.+)$/gm)) {
    if (m[1] === '[DONE]') continue;
    try { out += JSON.parse(m[1]).choices?.[0]?.delta?.content || ''; } catch { /* skip */ }
  }
  return out;
};
const sseReasoning = (raw) => {
  let out = '';
  for (const m of raw.matchAll(/^data: (.+)$/gm)) {
    if (m[1] === '[DONE]') continue;
    try { out += JSON.parse(m[1]).choices?.[0]?.delta?.reasoning_content || ''; } catch { /* skip */ }
  }
  return out;
};

try {
  await waitReady();
  await wait(400);

  // ===== ① 思考过程泄漏进正文：非流式 =====
  const r1 = await chat({ model: 'leak-think', messages: [{ role: 'user', content: '17*23' }] });
  ok('① 请求成功', r1.status === 200, r1.text.slice(0, 160));
  const msg1 = r1.json?.choices?.[0]?.message || {};
  ok('① 正文里不再包含思维链自述（Step 1）', !/Step\s*1/i.test(msg1.content || ''), JSON.stringify((msg1.content || '').slice(0, 160)));
  ok('① 正文保留了最终答案', /391/.test(msg1.content || ''), JSON.stringify((msg1.content || '').slice(0, 160)));
  ok('① 思维链被移入 reasoning_content', /Step\s*1/i.test(msg1.reasoning_content || ''), JSON.stringify((msg1.reasoning_content || '').slice(0, 120)));

  // ===== ①b 思考过程泄漏进正文：流式 =====
  const r1s = await chat({ model: 'leak-think', messages: [{ role: 'user', content: '17*23' }], stream: true });
  ok('①b 流式请求成功', r1s.status === 200, r1s.text.slice(0, 160));
  const c1s = sseText(r1s.text);
  ok('①b 流式正文不含思维链（Step 1）', !/Step\s*1/i.test(c1s), JSON.stringify(c1s.slice(0, 160)));
  ok('①b 流式正文保留最终答案', /391/.test(c1s), JSON.stringify(c1s.slice(0, 160)));

  // ===== ①c 上游用非标准字段名（reasoning / thinking）承载思考 =====
  const r2 = await chat({ model: 'reason-field', messages: [{ role: 'user', content: 'hi' }] });
  const msg2 = r2.json?.choices?.[0]?.message || {};
  ok('①c 非标准 reasoning 字段被归一化到 reasoning_content', /让我想想/.test(msg2.reasoning_content || ''), JSON.stringify(msg2.reasoning_content));
  ok('①c 正文保持干净', msg2.content === '这是正式回答', JSON.stringify(msg2.content));

  // ===== ② 空调用：HTTP 200 但什么都没有 -> 换家，绝不把空响应丢给 agent =====
  const r3 = await chat({ model: 'empty-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('② 空响应渠道被跳过，请求最终成功', r3.status === 200, `status=${r3.status} ${r3.text.slice(0, 160)}`);
  ok('② 成功渠道是有内容的 ok-1', r3.headers.get('x-gateway-channel') === 'ok-1', String(r3.headers.get('x-gateway-channel')));
  ok('② agent 拿到的正文非空', (r3.json?.choices?.[0]?.message?.content || '').length > 0);

  // ②b 流式空调用
  const r3s = await chat({ model: 'empty-model', messages: [{ role: 'user', content: 'hi' }], stream: true });
  ok('②b 流式空响应也被跳过', r3s.status === 200 && r3s.headers.get('x-gateway-channel') === 'ok-1',
    `status=${r3s.status} ch=${r3s.headers.get('x-gateway-channel')}`);
  ok('②b 流式正文非空', sseText(r3s.text).length > 0, JSON.stringify(sseText(r3s.text).slice(0, 120)));

  // ②c 只有思考没有正文：对 agent 也算可用（不能当空响应丢掉）
  const r4 = await chat({ model: 'reason-only', messages: [{ role: 'user', content: 'hi' }] });
  ok('②c 只有 reasoning_content 时不算空响应',
    r4.status === 200 && r4.headers.get('x-gateway-channel') === 'reason-only-1',
    `status=${r4.status} ch=${r4.headers.get('x-gateway-channel')}`);
  ok('②c reasoning_content 被保留', /我在思考/.test(r4.json?.choices?.[0]?.message?.reasoning_content || ''));
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
