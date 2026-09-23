// 回归测试（W5）：思考守卫的"误切"与"去重/丢内容"两类问题
//
// lib/reasoning-guard.mjs 的原则是"宁可不动，也不能把正式回答误切成思考过程"。
// 这个文件专门守住三件事：
//   ① 普通连接词（首先 / First / 好的 / Sure）不能单独当作 CoT 证据——
//      否则一段正常的结构化回答会被整段搬进 reasoning_content，不渲染思考的客户端只看到被截断的答案；
//   ② 真正的 CoT 泄漏仍然必须切（别修着修着把功能修没了）；
//   ③ 重复别名字段要去重、展不开的思考字段不能无声丢内容。
//
// 纯单元测试：不起网关、不起 mock 上游，直接 import 目标函数断言，因此可以单独快速跑。
import {
  normalizeReasoning, splitInlineReasoning, looksLikeReasoning,
  cleanMessageReasoning, REASONING_FALLBACK_FIELD,
} from '../lib/reasoning-guard.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// ============ ① 已复核的误切样例：必须不切，正文一字不少 ============
{
  // 中文：普通连接词"首先…" + 收尾语"结论：" 的正常结构化回答
  const fpZh = '首先，我们需要确认版本兼容性。依赖 v2 与 v3 的接口签名不兼容，直接升级会编译失败。\n\n结论：建议先升级 v3 适配层，再迁移业务代码，预计工作量两天。';
  const splitZh = splitInlineReasoning(fpZh);
  ok('误切①：中文"首先…结论："不切', splitZh.split === false, JSON.stringify(splitZh).slice(0, 200));
  ok('误切①：正文字节不变（完整保留）', splitZh.content === fpZh, JSON.stringify(splitZh.content).slice(0, 200));

  const msgZh = { role: 'assistant', content: fpZh };
  const cleanZh = cleanMessageReasoning(msgZh);
  ok('误切①：cleanMessageReasoning 同样不切（proxy 实际调用的路径）',
    cleanZh.split === false && msgZh.content === fpZh && !msgZh.reasoning_content, JSON.stringify(cleanZh));

  // 英文：普通连接词"First…" + 收尾语"the answer is"
  const fpEn = 'First, install the package with npm. Then configure the port in config.json.\n\nTherefore the answer is to restart the service.';
  const splitEn = splitInlineReasoning(fpEn);
  ok('误切②：英文"First… the answer is"不切', splitEn.split === false, JSON.stringify(splitEn).slice(0, 200));
  ok('误切②：正文完整保留', splitEn.content === fpEn, JSON.stringify(splitEn.content).slice(0, 200));

  // 英文加强版：同样的结构里出现 "we need to check"（正常表述，不是自述推理）
  const fpEn2 = 'First, we need to check the version compatibility matrix. The v2 and v3 interface signatures differ, so a direct upgrade fails to compile.\n\nTherefore the answer is to upgrade the adapter layer first, then migrate the business code, roughly two days of work.';
  ok('误切③：英文"we need to check…"不切（只认认知类动词）',
    splitInlineReasoning(fpEn2).split === false, JSON.stringify(splitInlineReasoning(fpEn2)).slice(0, 200));

  // 中文加强版：写法很像但通篇没有自述推理（没有"我/让我 + 推理动作"）
  const fpZh2 = '首先确认版本兼容性：v2 与 v3 的接口签名不兼容，直接升级会编译失败。其次评估改动范围，涉及 12 个文件。然后给出迁移顺序。\n\n结论：建议先升级 v3 适配层，再迁移业务代码。';
  ok('误切④：中文"首先…其次…然后…结论："不切（无自述推理证据）',
    splitInlineReasoning(fpZh2).split === false, JSON.stringify(splitInlineReasoning(fpZh2)).slice(0, 200));

  // looksLikeReasoning 层面直接锁死：普通连接词单独出现不算证据
  ok('误切⑤：单独"首先/First"不算 CoT 证据',
    looksLikeReasoning('First, install the package with npm. Then configure the port in config.json so it listens on 8080.') === false);
  ok('误切⑤：单独"好的/首先"不算 CoT 证据',
    looksLikeReasoning('好的，下面我把三种方案分别说明一下：方案 A 成本最低，方案 B 容量最大，方案 C 改造最少，请按场景选择。') === false);
}

// ============ ② 真正的 CoT 泄漏：必须照切不误 ============
{
  // 中文真泄漏：自述"我需要先计算…" + "综上，答案是"
  const leakZh = '我需要先计算 17 乘以 23。首先把 23 拆成 20 加 3，然后分别相乘再相加。\n17 乘 20 等于 340，17 乘 3 等于 51，两者相加得到 391。\n综上，答案是 391。';
  const s1 = splitInlineReasoning(leakZh);
  ok('真泄漏①：中文思维链被切分', s1.split === true, JSON.stringify(s1).slice(0, 200));
  ok('真泄漏①：正文只留最终答案', /391/.test(s1.content) && !/我需要先计算/.test(s1.content), JSON.stringify(s1.content).slice(0, 120));
  ok('真泄漏①：思考段落进了 reasoning', /我需要先计算/.test(s1.reasoning), JSON.stringify(s1.reasoning).slice(0, 120));

  // 英文真泄漏：Let's break this down + Step 1 + the final answer
  const leakEn = `Sure! Let's break this down step by step.

**Step 1: Break down the multiplication using distributive property**
17 x 23 = (17 x 20) + (17 x 3)

**Step 2: Multiply each part**
- 17 x 20 = 340
- 17 x 3 = 51

So the final answer is 391.`;
  const s2 = splitInlineReasoning(leakEn);
  ok('真泄漏②：英文思维链被切分', s2.split === true, JSON.stringify(s2).slice(0, 200));
  ok('真泄漏②：正文不含 Step 1 / 含 391', !/Step\s*1/i.test(s2.content) && /391/.test(s2.content), JSON.stringify(s2.content).slice(0, 120));
  ok('真泄漏②：思考段落进了 reasoning', /Step\s*1/i.test(s2.reasoning), JSON.stringify(s2.reasoning).slice(0, 120));

  // 英文真泄漏（另一种口吻）：We need to compute + The user is asking
  const leakEn2 = `We need to compute 17 times 23 for the user.

The user is asking for a single number, so work it out step by step:
17 * 20 = 340
17 * 3 = 51

Therefore, the final answer is 391.`;
  const s3 = splitInlineReasoning(leakEn2);
  ok('真泄漏③：英文"We need to compute / The user is asking"被切分', s3.split === true, JSON.stringify(s3).slice(0, 200));
  ok('真泄漏③：正文只留最终答案', /391/.test(s3.content) && !/We need to compute/.test(s3.content), JSON.stringify(s3.content).slice(0, 120));

  // 中文真泄漏（"让我想想"口吻）
  const leakZh2 = '让我想想这个问题该怎么回答。用户问的是 17 乘 23，我需要先算 17 乘 20 等于 340，再算 17 乘 3 等于 51，两者相加是 391。\n\n综上，答案是 391。';
  const s4 = splitInlineReasoning(leakZh2);
  ok('真泄漏④：中文"让我想想"口吻被切分', s4.split === true, JSON.stringify(s4).slice(0, 200));

  // 走 proxy 实际调用的入口，确认拆分结果真的落到 reasoning_content
  const msgLeak = { role: 'assistant', content: leakEn };
  const cleanLeak = cleanMessageReasoning(msgLeak);
  ok('真泄漏：cleanMessageReasoning 把思维链挪进 reasoning_content',
    cleanLeak.split === true && /Step\s*1/i.test(msgLeak.reasoning_content || '') && !/Step\s*1/i.test(msgLeak.content),
    JSON.stringify({ split: cleanLeak.split, content: (msgLeak.content || '').slice(0, 80) }));

  // 有原生 tool_calls 时绝不动正文（原有行为，回归）
  const msgTool = { role: 'assistant', content: leakEn, tool_calls: [{ id: '1', function: { name: 'f', arguments: '{}' } }] };
  ok('真泄漏：有原生 tool_calls 时不切正文',
    cleanMessageReasoning(msgTool).split === false && msgTool.content === leakEn);
}

// ============ ③ 别名重复：去重后只保留一份文本 ============
{
  const dup1 = { role: 'assistant', content: '正式回答', reasoning_content: '同一段文字', thinking: '同一段文字' };
  const r1 = normalizeReasoning(dup1);
  ok('去重①：reasoning_content + thinking 同文本只留一份',
    r1.text === '同一段文字' && dup1.reasoning_content === '同一段文字' && dup1.thinking === undefined,
    JSON.stringify({ text: r1.text, msg: dup1 }));

  // OpenRouter 风格：reasoning + reasoning_details:[{text}] 同一段
  const dup2 = { role: 'assistant', content: '正式回答', reasoning: '同一段文字', reasoning_details: [{ text: '同一段文字' }] };
  const r2 = normalizeReasoning(dup2);
  ok('去重②：reasoning + reasoning_details 同文本只留一份',
    r2.text === '同一段文字' && dup2.reasoning_content === '同一段文字' && dup2.reasoning === undefined && dup2.reasoning_details === undefined,
    JSON.stringify({ text: r2.text, msg: dup2 }));

  // 三份同文本
  const dup3 = { role: 'assistant', reasoning_content: 'X', reasoning: 'X', analysis: 'X' };
  const r3 = normalizeReasoning(dup3);
  ok('去重③：三份同文本仍只留一份', r3.text === 'X', JSON.stringify(r3));

  // 不同文本必须按 REASONING_FIELDS 的优先级顺序保留，不能因去重丢内容
  const ord = { role: 'assistant', reasoning: '第一段', thinking: '第二段', analysis: '第三段' };
  const rOrd = normalizeReasoning(ord);
  ok('去重④：不同文本按优先级顺序拼接、不丢段',
    ord.reasoning_content === '第一段\n第二段\n第三段', JSON.stringify(ord.reasoning_content));

  // 重复内容 + 拆分同时发生时，正文长度不虚高
  const dupLeak = { role: 'assistant', content: 'First, install the package with npm. Then configure the port in config.json.\n\nTherefore the answer is to restart the service.', reasoning: '已有思考', thinking: '已有思考' };
  const rDupLeak = cleanMessageReasoning(dupLeak);
  ok('去重⑤：重复思考字段不会让 reasoning 变长/正文被切',
    dupLeak.reasoning_content === '已有思考' && rDupLeak.split === false, JSON.stringify({ rc: dupLeak.reasoning_content, split: rDupLeak.split }));
}

// ============ ④ 展不开的思考字段：不能无声丢内容 ============
{
  // 实测样例：{reasoning:{signature:'abc'}} —— flattenReasoning 展不开，旧实现直接 delete
  const unexp = { role: 'assistant', content: '正式回答', reasoning: { signature: 'abc' } };
  const r1 = normalizeReasoning(unexp);
  ok('不丢内容①：{reasoning:{signature}} 被转存到降级字段',
    Array.isArray(unexp[REASONING_FALLBACK_FIELD])
      && unexp[REASONING_FALLBACK_FIELD].length === 1
      && unexp[REASONING_FALLBACK_FIELD][0].from === 'reasoning'
      && unexp[REASONING_FALLBACK_FIELD][0].value.signature === 'abc',
    JSON.stringify(unexp));
  ok('不丢内容①：字段没有被静默删除（原字段被归并，但内容还在）',
    unexp.reasoning === undefined && r1.changed === true && r1.text === '', JSON.stringify(unexp));

  // 走 proxy 实际调用的入口（cleanMessageReasoning -> normalizeReasoning）
  const unexp2 = { role: 'assistant', content: '正式回答', reasoning: { signature: 'sig-1', id: 'rs_1' } };
  cleanMessageReasoning(unexp2);
  ok('不丢内容②：cleanMessageReasoning 路径上内容仍可查',
    JSON.stringify(unexp2[REASONING_FALLBACK_FIELD] || '').includes('sig-1'), JSON.stringify(unexp2));

  // 多个展不开的来源按出现顺序累积
  const unexp3 = { role: 'assistant', reasoning: { signature: 's1' }, analysis: { id: 'a1' } };
  normalizeReasoning(unexp3);
  ok('不丢内容③：多个展不开来源都保留',
    Array.isArray(unexp3[REASONING_FALLBACK_FIELD])
      && unexp3[REASONING_FALLBACK_FIELD].map((x) => x.from).join(',') === 'reasoning,analysis',
    JSON.stringify(unexp3));

  // 空壳字段（空串 / 空对象 / 空数组）本来就没内容，不该制造降级噪声
  const empty = { role: 'assistant', content: '正式回答', reasoning: '', thinking: {}, reasoning_details: [] };
  const rEmpty = normalizeReasoning(empty);
  ok('不丢内容④：空壳字段不产生降级字段',
    empty[REASONING_FALLBACK_FIELD] === undefined && rEmpty.text === '', JSON.stringify(empty));

  // 展得开的内容照旧走 reasoning_content，不进降级字段
  const okMsg = { role: 'assistant', content: '正式回答', reasoning: { text: '正常思考' } };
  const rOk = normalizeReasoning(okMsg);
  ok('不丢内容⑤：可展开的思考仍归一到 reasoning_content',
    okMsg.reasoning_content === '正常思考' && okMsg[REASONING_FALLBACK_FIELD] === undefined && rOk.text === '正常思考',
    JSON.stringify(okMsg));
}

// ============ ⑤ 原有长度保护回归 ============
{
  // 短回复（整体 < 60 字符）一律不动
  ok('长度保护①：整体过短不判为思维链', looksLikeReasoning('答案是 391') === false);
  const short = '让我想想。答案是 42。';
  ok('长度保护②：短回复不切', splitInlineReasoning(short).split === false && splitInlineReasoning(short).content === short);

  // 命中强证据 + 有分界语，但前缀不足 40 字符 -> 不切
  const shortHead = '让我想想：先算 17×20。答案是 这是一段足够长的说明文字，用来把尾部撑过最小长度限制，并让整体长度超过六十个字符，确保判定会走到前缀长度这一关。';
  const rHead = splitInlineReasoning(shortHead);
  ok('长度保护③：前缀 < 40 字符不动', rHead.split === false && rHead.content === shortHead,
    JSON.stringify({ split: rHead.split, len: shortHead.length }));

  // 命中强证据 + 前缀够长，但分界语之后没有实质内容（tail < 6）-> 不切
  const shortTail = '让我想想这个问题的解法，先把乘数拆成整十数和个位数，分别相乘之后再相加，就能得到最终结果。结论：';
  const rTail = splitInlineReasoning(shortTail);
  ok('长度保护④：尾部太短不动', rTail.split === false && rTail.content === shortTail,
    JSON.stringify({ split: rTail.split, len: shortTail.length }));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
