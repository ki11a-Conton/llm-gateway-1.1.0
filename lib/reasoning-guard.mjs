// 思考过程（reasoning）与空调用守卫
//
// 解决两类线上现象：
//
//  ① 思考过程直接显现在正文内容里
//     不同中转对"思考"的字段名不统一：reasoning_content / reasoning / thinking /
//     reasoning_details / annotations，甚至有上游干脆把整段思维链写进 content
//     （形如 "Step 1: ... Let's break this down ... Therefore the answer is ..."）。
//     agent 拿到这种正文会把它当成正式回答，表现为"模型在自言自语"。
//     处理：a) 把已知的思考字段归一化到 reasoning_content（OpenAI 口径）；
//           b) 识别正文里的思维链范式，分离成 reasoning_content 与干净正文。
//
//  ② 空调用（HTTP 200 但什么都没有）
//     content 为空、没有 tool_calls、没有 reasoning —— agent 收不到任何东西，
//     表现为"网关不理我"。这个判定放在 proxy.mjs（需要拿整个响应判断 finish_reason），
//     本模块只提供文本提取工具。
//
// ---------- 为什么 `annotations` 不在 REASONING_FIELDS 里（实测结论，别再猜） ----------
// 曾怀疑某些中转用 `annotations` 承载思考内容或 citations，于是抓了真实上游的响应体核对
// （channel 17 / ai.hyper.nyc.mn，模型 deepseek-ai/DeepSeek-V4-Flash-0731，非流式与 effort=high 各一次）：
//   message keys = role, content, annotations
//   annotations = null            <- 恒为 null，是个占位键，既无思考也无 citations
//   usage.completion_tokens_details.reasoning_tokens = 0
//   content = "好的，我们一步步来推理这个问题。**第一步：明确已知条件** …"（整段思维链直接写在 content 里）
// 结论：`annotations` 不承载任何思考文本，把它并进 REASONING_FIELDS 只会制造空字段、
// 让"归一化"误报改动，反而掩盖真正的问题。因此**故意不收录**，保持原样透传
// （toOpenAIResponse 的 {...json} 已经是这样）。
// 这条链路上真正要处理的是"思维链混进正文"，由下面的正文拆分逻辑负责——这也反向验证了守卫的必要性。

/** 已知承载思考内容的字段名（按优先级）。值是字符串或对象/数组时都会被展开成文本。 */
export const REASONING_FIELDS = [
  'reasoning_content',
  'reasoning',
  'thinking',
  'reasoning_details',
  'analysis',
];

/**
 * 把任意形状的思考字段值展开成纯文本。
 * 兼容：字符串 / { text } / { content } / [{ text }] / [{ summary }]（OpenAI Responses 风格）
 */
export function flattenReasoning(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.map(flattenReasoning).filter(Boolean).join('\n');
  if (typeof value === 'object') {
    for (const k of ['text', 'content', 'summary', 'thinking', 'reasoning']) {
      if (typeof value[k] === 'string' && value[k]) return value[k];
      if (value[k] && typeof value[k] === 'object') {
        const nested = flattenReasoning(value[k]);
        if (nested) return nested;
      }
    }
  }
  return '';
}

/**
 * 展不开的思考字段的降级落点（约定：数组，元素形如 { from: 'reasoning', value: <原始值> }）。
 *
 * 为什么需要它：{reasoning:{signature:'abc'}} 这种形态（加密签名、只带 id 的思考块）
 * flattenReasoning 展不开，旧实现会把这个字段直接 delete 掉、也不产出 reasoning_content，
 * 内容就无声消失了——排查时完全看不到"上游其实给过东西"。降级保留比丢弃安全。
 */
export const REASONING_FALLBACK_FIELD = 'reasoning_unexpanded';

/** 展不开的字段里是否还装着内容（只有空串/空数组/空对象才允许直接丢） */
function hasUnexpandablePayload(value) {
  if (value == null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  return Boolean(value); // 数字/布尔这类标量：保留原值也比丢掉好
}

/**
 * 从消息对象里取出思考文本，并归一化到 reasoning_content。
 * 返回是否发生了改写（供调用方决定要不要落日志）。
 *
 * 两个实测过的坑都在这里修：
 *  - 不同别名可能装着同一段文字（reasoning_content + thinking、reasoning + reasoning_details…），
 *    旧写法 `${text}\n${t}` 会把同一段拼两遍，思考内容重复、上报的思考长度翻倍；
 *    现在按文本去重，只保留首次出现的那一份（顺序稳定）。
 *  - 展不开的形态以前被 delete 且不产 reasoning_content，内容无声丢失；
 *    现在转存到 REASONING_FALLBACK_FIELD。
 */
export function normalizeReasoning(message) {
  if (!message || typeof message !== 'object') return { changed: false, text: '' };
  const segments = [];
  const seen = new Set(); // 去重键：相同文本只收一份，顺序即 REASONING_FIELDS 的优先级顺序
  let changed = false;

  for (const field of REASONING_FIELDS) {
    if (message[field] == null) continue;
    const value = message[field];
    const t = flattenReasoning(value);
    if (t) {
      if (!seen.has(t)) {
        seen.add(t);
        segments.push(t);
      }
      if (field !== 'reasoning_content') changed = true;
    } else if (field !== 'reasoning_content' && hasUnexpandablePayload(value)) {
      // 展不开但有内容：转存到约定的降级字段，绝不静默丢
      const bucket = Array.isArray(message[REASONING_FALLBACK_FIELD])
        ? message[REASONING_FALLBACK_FIELD]
        : [];
      bucket.push({ from: field, value });
      message[REASONING_FALLBACK_FIELD] = bucket;
      changed = true;
    }
    // 归并后删掉非标准字段，避免下游看到重复内容
    // （reasoning_content 自身展不开时保持原样，既不丢也不制造第二份副本）
    if (field !== 'reasoning_content') {
      delete message[field];
      changed = true;
    }
  }

  const text = segments.join('\n');
  if (text && message.reasoning_content !== text) {
    message.reasoning_content = text;
    changed = true;
  }
  return { changed, text };
}

// ---------- 正文里的思维链识别 ----------

/**
 * 思维链"强证据"：命中任意一条（且正文里没有工具调用）才怀疑是思考过程混进正文。
 *
 * 这些措辞只有在"模型对自己说话"时才会出现：第一人称的推理计划、把提问者写成第三人称、
 * 明确的 "Step N:" 分步。来自实测（channel 17 / vyei 等把整段推理写进 content）：
 *   "We need to compute ...", "Let's break this down", "Step 1:", "The user is asking ..."
 *
 * 为什么不再收录普通连接词（实测误切，别再退回去）：
 *   旧实现把 `首先`/`First`/`好的`/`Sure` 这类普通连接词也算证据，于是只要正文里还能找到
 *   `结论：`/`the answer is` 这类收尾语，整段前缀就会被当成思考切走：
 *     - "首先，我们需要确认版本兼容性。……\n\n结论：建议先升级 v3 适配层……"
 *       -> 正文只剩 "结论：建议先升级 v3 适配层……"
 *     - "First, install the package with npm. ……\n\nTherefore the answer is to restart the service."
 *       -> 正文只剩 "the answer is to restart the service."
 *   这两条都是正常的结构化回答，不渲染 reasoning_content 的客户端看到的就是被截断的答案。
 *   所以现在的判据是：**必须命中强证据**，普通连接词单独出现一律不立案。
 *
 * 强证据的边界刻意收得很窄，避免换个姿势又误切：
 *   - 英文只认 `let me/let's + 推理动作` 与 `I/We need|have|want|should|must|will + to + 认知类动词`
 *     （let's install the package / we need to check compatibility 这类正常表述不命中）；
 *   - 中文只认单数第一人称 + 明确推理动作（我/让我 + 想/思考/计算/分析…）；
 *     "我们需要确认……" 这种极常见的正式表述刻意不算证据（实测误切样例 1 就长这样）。
 */
const COT_MARKERS = [
  // 英文：第一人称"我要开始推理了"的自述
  /\b(?:let me|let's)\s+(?:think|reason|analy[sz]e|work\s+through|work\s+(?:this|it)\s+out|figure\s+(?:this|it)\s+out|break\s+(?:this|it|the\s+problem)\s+down|walk\s+through|go\s+step\s+by\s+step)\b/i,
  /^\s*(?:i|we)\s+(?:need|have|want|should|must|will)\s+to\s+(?:think|reason|analy[sz]e|figure|compute|calculate|determine|solve|derive|work\s+out|decide|recall|consider|understand|parse)\b/im,
  // 把提问者当第三人称来写，是思维链的典型口吻（正式回答不会这么写）
  /^\s*the user\s+(?:is|was|wants|wanted|asked|asks|requested|wrote|said|gave|has)\b/im,
  /\bstep\s*\d+\s*[:.)]/i,
  /\bchain\s+of\s+thought\b/i,
  /\b(?:my|the)\s+reasoning\b/i,
  // 中文：单数第一人称 + 明确推理动作（"让我想想" / "我需要先计算" / "我先分析一下"）。
  // 刻意不收录"我们……"与单独的"首先/第一步/好的"。
  /让我(?:先|来)?(?:想|想想|思考|推理|梳理|分析|计算|算|推导|拆解|琢磨|看看)/,
  /我(?:需要|得|要|想)(?:先|来)?(?:想|思考|推理|梳理|分析|计算|算|推导|拆解|琢磨|确认一下|弄清楚)/,
  /我(?:先|来)(?:想|思考|推理|梳理|分析|计算|算|推导|拆解|琢磨)/,
  /(?:让我们|咱们)(?:先|来)?(?:想|思考|推理|梳理|分析|计算|推导|拆解)/,
];

/** 正文以这些"最终答案"引导语开头时，之前的整段都可以视作思维链。
 *  允许出现在句子中间（中文常写成"……，综上，答案是 391"），因此不强求行首。 */
const FINAL_MARKERS = [
  /the\s+(?:final\s+)?answer\s+is\b/i,
  /final\s+answer\s*[:：]/i,
  /答案\s*(?:就)?\s*(?:是|为)\s*[:：]?/,
  /(?:综上|总而言之|总的来说)\s*[,，]?/,
  /(?:因此|所以)\s*[,，]?\s*(?:最终)?答案/,
  /结论\s*[:：]/,
];

/**
 * 这段正文像不像"思维链自述"（而不是正式回答）。
 *
 * 判据：长度够（短回复一律不动）**且**命中至少一条强证据（见 COT_MARKERS）。
 * 普通连接词（首先/First/好的/Sure）不再作数——它们既可能是思考也可能是正常写作，
 * 单凭一条"首先…结论："就把正文搬进 reasoning 是本模块最贵的误伤。
 */
export function looksLikeReasoning(text) {
  const t = String(text || '');
  if (t.length < 60) return false; // 太短的不做处理，避免误伤正常短回答
  return COT_MARKERS.some((re) => re.test(t));
}

/**
 * 尝试把"正文里混入的思维链"分离出来。
 *
 * 策略保守且可解释——只在同时满足以下条件时才动手：
 *   - 正文确实呈现思维链范式（looksLikeReasoning：必须命中强证据，普通连接词不算）
 *   - 能找到明确的"最终答案"分界点（FINAL_MARKERS）
 *   - 分界点之后还留有实质内容
 * 否则原样返回（宁可不动，也不能把正式回答误切成思考过程）。
 *
 * @returns {{reasoning:string, content:string, split:boolean, marker:string|null}}
 */
export function splitInlineReasoning(text) {
  const original = String(text ?? '');
  if (!original.trim() || !looksLikeReasoning(original)) {
    return { reasoning: '', content: original, split: false, marker: null };
  }
  for (const re of FINAL_MARKERS) {
    const m = re.exec(original);
    if (!m) continue;
    const head = original.slice(0, m.index).trim();
    const tail = original.slice(m.index).trim();
    // 两段都要有实质内容才认；tail 太短说明"答案"只是句尾客套，不切
    if (head.length >= 40 && tail.length >= 6) {
      return { reasoning: head, content: tail, split: true, marker: m[0].trim().slice(0, 40) };
    }
  }
  return { reasoning: '', content: original, split: false, marker: null };
}

/**
 * 就地处理一条非流式 OpenAI 形状的响应消息：
 *   1) 归一化各家的思考字段到 reasoning_content
 *   2) 若正文是混入的思维链，拆出 reasoning_content 与干净 content
 * @returns {{changed:boolean, split:boolean, marker:string|null, reasoningChars:number}}
 */
export function cleanMessageReasoning(message, { splitInline = true } = {}) {
  if (!message || typeof message !== 'object') {
    return { changed: false, split: false, marker: null, reasoningChars: 0 };
  }
  const norm = normalizeReasoning(message);
  let split = false;
  let marker = null;

  const content = typeof message.content === 'string' ? message.content : '';
  if (splitInline && content && !hasNativeToolCalls(message)) {
    const { reasoning, content: clean, split: did, marker: mk } = splitInlineReasoning(content);
    if (did) {
      message.content = clean;
      message.reasoning_content = [message.reasoning_content, reasoning].filter(Boolean).join('\n');
      split = true;
      marker = mk;
    }
  }

  return {
    changed: norm.changed || split,
    split,
    marker,
    reasoningChars: (message.reasoning_content || '').length,
  };
}

function hasNativeToolCalls(message) {
  return Array.isArray(message.tool_calls) && message.tool_calls.length > 0;
}

/**
 * 流式：把"正文里混入的思维链"按同样的规则切分。
 * 返回 { reasoning, content, split }，调用方据此决定发哪些 delta。
 * 未命中时 content 原样返回。
 */
export function splitStreamChunkReasoning(text) {
  const { reasoning, content, split, marker } = splitInlineReasoning(text);
  return { reasoning, content, split, marker };
}
