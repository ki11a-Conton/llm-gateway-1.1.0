// OpenAI 兼容协议适配器：请求与响应都是 OpenAI 格式，流式直接透传
// 覆盖：OpenAI 官方 / DeepSeek / 智谱 / Moonshot / 硅基流动 / 阿里百炼 / 火山方舟 / OpenRouter / 各类中转

import { joinUrl, genId, safeJsonParse } from '../util.mjs';
import { flattenReasoning, REASONING_FIELDS } from '../reasoning-guard.mjs';

/**
 * 从 OpenAI 消息里取出思考文本（兼容各家字段名：reasoning_content / reasoning / thinking …）。
 * 用于把 OpenAI 兼容上游的思考内容转成 Anthropic 的 thinking 块——否则走 /v1/messages
 * 的客户端（Claude Code / Anthropic SDK）完全看不到思考过程。
 */
function extractReasoningText(msg) {
  if (!msg || typeof msg !== 'object') return '';
  for (const field of REASONING_FIELDS) {
    const t = flattenReasoning(msg[field]);
    if (t) return t;
  }
  return '';
}

export const openaiAdapter = {
  id: 'openai',
  /** true 表示上游 SSE 可以直接字节透传给客户端 */
  passthroughStream: true,

  buildRequest({ channel, model, body, stream }) {
    const headers = {
      'content-type': 'application/json',
      authorization: `Bearer ${channel.apiKey}`,
      ...(channel.headers || {}),
    };
    // OpenCode Zen 免费档：伪装成 opencode 客户端（服务端靠请求头识别客户端指纹放行免费额度）
    if (channel.opencodeFree) {
      const rnd = genId('');
      headers['user-agent'] = channel.headers?.['user-agent'] || 'opencode/1.14.28 ai-sdk/provider-utils/4.0.23 runtime/node/22';
      headers['x-opencode-client'] = 'cli';
      headers['x-opencode-project'] = 'global';
      headers['x-opencode-session'] = `ses_${rnd}`;
      headers['x-opencode-request'] = `req_${rnd}`;
    }
    // 部分网关需要额外的鉴权头（如 OpenRouter 的 HTTP-Referer）
    const payload = { ...body, model, stream };
    // 上游只认流式（WorkBuddy 国际版等）：出站强制 stream=true，非流式客户端由网关 SSE 聚合
    if (channel.forceStream) payload.stream = true;
    // 思考强度：客户端（或网关的"能思考就最高强度"策略）给了 reasoning_effort 时，
    // 除了标准字段外再补一份兼容写法——不同中转对"思考"的参数名不统一
    // （reasoning / enable_thinking / thinking），只补缺失的那些，避免覆盖上游已有语义。
    //
    // "模型能不能思考"不在这一层判断：调用方（lib/proxy.mjs 的 resolveEffort，按
    // channel.supportsThinking / modelSupportsThinking 逐渠道判定）只会给"确实要思考"的
    // 请求带上 reasoning_effort。所以这里的 reasoning_effort 存在性本身就是能力判断，
    // 非思考模型走不到这个分支，不会被白加 enable_thinking。
    if (typeof payload.reasoning_effort === 'string' && payload.reasoning_effort.trim()) {
      const effort = payload.reasoning_effort.trim().toLowerCase();
      if (payload.reasoning === undefined) payload.reasoning = { effort };
      // 用归一化后的 effort 判断，不再拿原始字符串比对大小写/档位：
      // 原来写的是 payload.reasoning_effort === 'high'，于是 'HIGH'/'High' 全漏掉，
      // 而生产实际注入的档位是 'xhigh'（config.json 的 routing.maxEffort）——
      // 这个 Qwen/DashScope 兼容字段在默认部署里等于从来没被设置过。
      // 只有显式关思考（off/none）的档位才不加。
      if (effort !== 'off' && effort !== 'none' && payload.enable_thinking === undefined) {
        payload.enable_thinking = true;
      }
    }
    // 请求改写：渠道配置 minMaxTokens 时，把低于下限的 max_tokens / max_completion_tokens 抬到下限
    // （部分上游对极小值直接 400，如 "max_completion_tokens must be greater than 2"）
    if (channel.minMaxTokens) {
      const floor = Number(channel.minMaxTokens);
      if (Number.isFinite(floor) && floor > 0) {
        for (const key of ['max_completion_tokens', 'max_tokens']) {
          const v = payload[key];
          if (typeof v === 'number' && v < floor) payload[key] = floor;
        }
      }
    }
    // OpenAI 线格式上剥掉 cache_control（见 OPTIMIZATION-PLAN §P4 4.4）。
    //
    // 调用链：server.mjs 用 fromAnthropicBody(body) 把 Anthropic 客户端的请求体转成"内部 OpenAI
    // 形状"再交给路由，所以目标渠道是 OpenAI 协议时，内部 body 会被直接铺进这里的 payload。
    // Anthropic 的 cache_control 是**它自己协议里**的 prompt caching 标记，OpenAI 官方与绝大多数
    // 中转都不认它：发过去零收益，且有让严格上游 400 的风险（未知字段）。
    //
    // 分工：cache_control 只活在**内部表示**里，专供 Anthropic 协议渠道（anthropic.mjs 按原块位置
    // 重新发出）。"在网关这一层不丢"与"不把不支持的字段发给上游"是两件事，这里负责后者。
    // 注意本函数不改入参 body（沿用原有约定），所以剥字段前先深拷贝被改动的 content。
    stripOpenAIWireCacheControl(payload);
    // stream_options 原样透传，不再删除：上游只有收到 stream_options.include_usage
    // 才会在流末补一帧带 usage 的 chunk，删掉它 = 客户端永远拿不到 token 统计
    // （也和 README「流式响应带 stream_options.include_usage」的承诺矛盾）。
    // 这里曾按"透传模式不篡改上游统计选项"删过，但删掉的正是客户端显式要的东西。
    // 多出来的 usage 帧（choices 为空）不会被网关下游路径误判：OpenAI 客户端方向是
    // 字节透传；forceStream 的非流式聚合按帧取 usage（lib/workbuddy.mjs
    // aggregateStreamToCompletion）；Anthropic 方向由 streamToAnthropic 的 chunk.usage
    // 分支吸收，该帧没有 finish_reason，不会触发空响应/finish_reason 判定。
    return { url: joinUrl(channel.baseUrl, channel.chatPath || '/chat/completions'), headers, payload };
  },

  buildModelsRequest(channel) {
    const headers = { authorization: `Bearer ${channel.apiKey}`, ...(channel.headers || {}) };
    if (channel.opencodeFree) {
      const rnd = genId('');
      headers['user-agent'] = channel.headers?.['user-agent'] || 'opencode/1.14.28 ai-sdk/provider-utils/4.0.23 runtime/node/22';
      headers['x-opencode-client'] = 'cli';
      headers['x-opencode-project'] = 'global';
      headers['x-opencode-session'] = `ses_${rnd}`;
      headers['x-opencode-request'] = `req_${rnd}`;
    }
    return {
      url: joinUrl(channel.baseUrl, channel.modelsPath || '/models'),
      headers,
    };
  },

  parseModels(json) {
    const list = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : null;
    if (!list) return null;
    return list
      .map((m) => (typeof m === 'string' ? m : m?.id ?? m?.name ?? m?.model))
      .filter(Boolean)
      // 确定性排序：上游 /models 的返回顺序并不稳定（往往把 embedding/whisper 排前面），
      // 而 channels.defaultModel() 取 [...models][0] 当渠道默认模型——不排序的话
      // unified 模式的流量会随上游返回顺序漂移。按 id 升序，用码点比较而不是
      // localeCompare，避免不同环境 locale 不同导致同一份输入排出不同结果。
      .sort((a, b) => {
        const x = String(a);
        const y = String(b);
        return x < y ? -1 : x > y ? 1 : 0;
      });
  },

  // 上游已是 OpenAI 格式，只做字段兜底
  toOpenAIResponse(json, model) {
    if (!json || typeof json !== 'object') return json;
    const out = { ...json, model: json.model || model };
    if (!out.id) out.id = genId('chatcmpl-');
    if (!out.object) out.object = 'chat.completion';
    if (!out.created) out.created = Math.floor(Date.now() / 1000);
    return out;
  },

  toOpenAIEmbeddings(json, model) {
    return json;
  },

  // ---------- 反向：OpenAI 输出 -> Anthropic 输出（客户端走 /v1/messages 时用）----------

  toAnthropicResponse(json, model) {
    const msg = json?.choices?.[0]?.message || {};
    const content = [];
    const text = typeof msg.content === 'string' ? msg.content : (msg.content || []).map((c) => c?.text ?? '').join('');
    // 思考块必须排在正文块之前（Anthropic 协议要求），否则客户端解析顺序错乱。
    // 上游用哪家字段名承载思考都行——extractReasoningText 会归一。
    const reasoning = extractReasoningText(msg);
    if (reasoning) content.push({ type: 'thinking', thinking: reasoning });
    if (text) content.push({ type: 'text', text });
    const hasToolCalls = Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0;
    for (const tc of msg.tool_calls || []) {
      content.push({
        type: 'tool_use',
        // 缺 id 的上游（实测有中转根本不发 id）必须兜一个：没有 id 的 tool_use 块
        // 下一轮无法和 tool_result 配对，Anthropic 客户端会直接 400。
        id: tc.id || genId('toolu_'),
        name: tc.function?.name,
        input: safeJsonParse(tc.function?.arguments, {}),
      });
    }
    if (!content.length) content.push({ type: 'text', text: '' });

    const inTok = json?.usage?.prompt_tokens ?? 0;
    const outTok = json?.usage?.completion_tokens ?? 0;
    return {
      id: json?.id ?? genId('msg_'),
      type: 'message',
      role: 'assistant',
      model: json?.model || model,
      content,
      // hasToolCalls 兜底：已经有 tool_use 块时，上游没给 finish_reason（或报的是
      // stop）也不能回 end_turn——Anthropic 客户端看不到 tool_use 就不会执行工具，
      // agent 会卡死在等结果。口径与 anthropic.mjs 的 mapStopReason 一致。
      stop_reason: toAnthropicStopReason(json?.choices?.[0]?.finish_reason, hasToolCalls),
      stop_sequence: null,
      usage: { input_tokens: inTok, output_tokens: outTok },
    };
  },

  /** OpenAI chunk 流 -> Anthropic SSE 事件流 */
  async *streamToAnthropic(events, model) {
    const id = genId('msg_');
    const usage = { input_tokens: 0, output_tokens: 0 };
    let started = false;
    // 上游最后给的 finish_reason 原样留着，收尾时再连同"有没有 tool_use 块"一起折算 stop_reason
    let finishReason = null;
    let sawToolUse = false;
    let sawDone = false; // 上游是否发过终止帧 [DONE]（F1：协议完成判定的一半）

    // 内容块索引必须从 0 开始连续分配（Anthropic 协议要求），且 thinking 块必须排在正文之前。
    // 因此不能写死"正文块 = index 0"——思考块可能先出现，把正文挤到 index 1。
    let nextIndex = 0;
    let current = null; // { index, type, toolIndex? }

    const emit = (event, payload) => `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    const openBlock = (type, extra = {}) => {
      const index = nextIndex;
      nextIndex += 1;
      current = { index, type };
      return emit('content_block_start', {
        type: 'content_block_start',
        index,
        content_block: { type, ...extra },
      });
    };
    const closeBlock = () => {
      if (!current) return null;
      const ev = emit('content_block_stop', { type: 'content_block_stop', index: current.index });
      current = null;
      return ev;
    };
    const delta = (index, d) => emit('content_block_delta', { type: 'content_block_delta', index, delta: d });

    // 工具块按 index 缓冲、收尾统一输出：index -> { id, name, args }
    // （原因见循环里 tool_calls 分支的注释：交错增量无法用"一个 current 块"表达）
    const toolSlots = new Map();
    const toolBlocks = function* () {
      if (!toolSlots.size) return;
      if (current) yield closeBlock(); // 协议要求块顺序：先收掉正在流的正文/思考块
      for (const [, slot] of [...toolSlots.entries()].sort((a, b) => a[0] - b[0])) {
        // 缺 id 也要兜一个：没有 id 的 tool_use 块无法与下一轮 tool_result 配对
        yield openBlock('tool_use', { id: slot.id || genId('toolu_'), name: slot.name ?? '', input: {} });
        if (slot.args) yield delta(current.index, { type: 'input_json_delta', partial_json: slot.args });
        yield closeBlock();
      }
      toolSlots.clear(); // 幂等：收尾路径可能再调用一次
    };

    for await (const ev of events) {
      const data = ev.data;
      if (!data || data === '[DONE]') {
        if (data === '[DONE]') sawDone = true;
        yield* toolBlocks();
        if (started && current) yield closeBlock();
        break;
      }
      const chunk = safeJsonParse(data, null);
      if (!chunk) continue;

      // usage 必须在 message_start 之前吸收：有的上游把 usage 放在首帧，
      // 先发 message_start 再解析 usage 就会把首帧真实的 input_tokens 写成 0。
      if (chunk.usage) {
        if (chunk.usage.prompt_tokens) usage.input_tokens = chunk.usage.prompt_tokens;
        if (chunk.usage.completion_tokens) usage.output_tokens = chunk.usage.completion_tokens;
      }

      if (!started) {
        started = true;
        yield emit('message_start', {
          type: 'message_start',
          message: {
            id,
            type: 'message',
            role: 'assistant',
            model: chunk.model || model,
            content: [],
            stop_reason: null,
            stop_sequence: null,
            // 已知就带真实值（多数上游 usage 随末帧才到，这里是 0 占位；
            // 收尾的 message_delta 会用真实值再报一次，不让占位的 0 成为最终结果）
            usage: { input_tokens: usage.input_tokens, output_tokens: usage.output_tokens },
          },
        });
      }

      const choice = chunk.choices?.[0];
      const d = choice?.delta || {};

      // 思考增量（OpenAI 兼容上游用 reasoning_content / reasoning 承载思考）-> Anthropic thinking 块。
      // 缺了这一步，走 /v1/messages 的客户端就完全看不到思考过程。
      const reasoningDelta = typeof d.reasoning_content === 'string'
        ? d.reasoning_content
        : typeof d.reasoning === 'string' ? d.reasoning : '';
      if (reasoningDelta) {
        if (current?.type !== 'thinking') {
          if (current) yield closeBlock();
          yield openBlock('thinking', { thinking: '' });
        }
        yield delta(current.index, { type: 'thinking_delta', thinking: reasoningDelta });
      }

      if (typeof d.content === 'string' && d.content) {
        if (current?.type !== 'text') {
          if (current) yield closeBlock();
          yield openBlock('text', { text: '' });
        }
        yield delta(current.index, { type: 'text_delta', text: d.content });
      }

      // 缺 index 的上游（部分中转只发 id/name/arguments）按数组位置兜底：单条时就是位置 0，
      // 于是它先开块，后续同位置的增量自然"延续当前工具块"。不兜底的话这些 chunk 会被
      // 整条丢掉（实测：只有 message_start/message_delta，零个 tool_use 块）。
      const toolCalls = d.tool_calls || [];
      for (let pos = 0; pos < toolCalls.length; pos++) {
        const tc = toolCalls[pos];
        const tcIndex = tc.index ?? pos;
        // 工具调用：OpenAI 允许多个工具的参数增量**交错到达**（一帧同时含 index 0/1，
        // 下一帧继续补 0/1，且后续帧通常不再重复 id/name）。而 Anthropic 的 content_block
        // 必须严格顺序（start -> deltas -> stop），无法同时开着两个工具块——
        // 所以这里按 index 分别缓冲元数据与完整参数，收尾时按 index 顺序一次性输出
        // （见 toolBlocks）。旧实现"index 一变就关旧开新"会把 2 个交错工具拆成 4 个残缺块。
        const slot = toolSlots.get(tcIndex) || { id: null, name: null, args: '' };
        if (typeof tc.id === 'string' && tc.id) slot.id = tc.id;
        if (typeof tc.function?.name === 'string' && tc.function.name) slot.name = tc.function.name;
        if (typeof tc.function?.arguments === 'string') slot.args += tc.function.arguments;
        toolSlots.set(tcIndex, slot);
        sawToolUse = true;
      }

      if (choice?.finish_reason) finishReason = choice.finish_reason;
    }

    if (!started) {
      // 空流兜底：走到这里流已经读完，usage 若出现过也已吸收，直接带上真实值
      yield emit('message_start', {
        type: 'message_start',
        message: {
          id, type: 'message', role: 'assistant', model,
          content: [], stop_reason: null, stop_sequence: null,
          usage: { input_tokens: usage.input_tokens, output_tokens: usage.output_tokens },
        },
      });
    }
    if (current) yield closeBlock();
    // 正常收尾路径也可能有缓冲的工具块（上游给了 finish_reason 但没发 [DONE]）
    yield* toolBlocks();

    // F1（代码审查 2026-09-20）：协议完成判定。
    // 只有拿到终止信号才算"模型说完了"：① [DONE]  ② 有效 finish_reason（部分兼容上游不发 [DONE]）。
    // 两者都没有 = 上游被截断，绝不能合成一个 message_stop 让路由层当成成功——
    // 否则 agent 收下半截回答、故障渠道还被记成功并继续获得优先路由。
    if (!sawDone && !finishReason) {
      const e = new Error('上游流在给出终止信号前结束（缺少 finish_reason 与 [DONE]）');
      e.code = 'STREAM_TRUNCATED';
      throw e;
    }

    yield emit('message_delta', {
      type: 'message_delta',
      delta: {
        // 和 toAnthropicResponse 同一口径：有 tool_use 块时不能回 end_turn，
        // 否则 Anthropic 客户端不会执行工具，agent 卡死。
        stop_reason: toAnthropicStopReason(finishReason, sawToolUse),
        stop_sequence: null,
      },
      // input_tokens 通常随末帧才到，message_start 里那个值只是占位；这里必须再报一次
      // 真实值，否则客户端最终拿到的 input_tokens 恒为 0（与 anthropic->openai 方向不一致）。
      usage: { input_tokens: usage.input_tokens, output_tokens: usage.output_tokens },
    });
    yield emit('message_stop', { type: 'message_stop' });
  },
};

// ---------------------------------------------------------------------------
// OpenAI 线格式净化：剥掉 Anthropic 专有的 cache_control
//
// cache_control 是 Anthropic 自己协议里的 prompt caching 标记，OpenAI 官方与绝大多数中转都不认它。
// 网关内部表示会保留它（专供 Anthropic 协议渠道按原块位置重新发出，见 anthropic.mjs 的
// carryCacheControl），但在**发往 OpenAI 协议上游**之前必须剥掉：发过去零收益，纯风险
// （严格上游可能因未知字段 400）。"在网关这一层不丢"与"不把上游不支持的字段发出去"是两件事。
//
// 顺带把"全是 text 块的数组"折叠回字符串：这是标准 OpenAI 形态，也是网关内部表示在
// OpenAI 渠道上的既有出站形状（避免把自己的内部块结构泄漏给上游）。
// 含图片等非 text 块时保持数组结构不变，只剥字段。
//
// 折叠用的分隔符按角色区分，目的是"零漂移"而不是随手拼：
//   - system / developer 消息（含顶层 system）：改动前的 fromAnthropicBody 就是 join('\n')，
//     Claude Code 这类客户端正是把 system 发成块数组，用 '\n' 才能让上游收到与改动前一致的文本。
//   - 其余消息：Anthropic 的 content 块语义是直接拼接，改动前的 fromAnthropicBody 也是
//     texts.join('')，所以用 ''。
//
// 不修改调用方对象：需要改动的 message/content/block 一律先浅拷贝。
// ---------------------------------------------------------------------------

const blockHasCacheControl = (b) =>
  !!b && typeof b === 'object' && !Array.isArray(b) && b.cache_control != null;

const isTextBlock = (b) => !!b && typeof b === 'object' && !Array.isArray(b) && b.type === 'text';

const isSystemRole = (role) => role === 'system' || role === 'developer';

/**
 * 归一一个 content 值：数组 -> 剥字段（全 text 时进一步折叠成字符串）；字符串 -> 原样。
 * @param {any} content
 * @param {string} sep 折叠纯文本块时用的分隔符（见上方说明）
 * @returns {{value: any, changed: boolean}}
 */
function toOpenAIWireContent(content, sep) {
  if (!Array.isArray(content)) return { value: content, changed: false };
  const needsStrip = content.some(blockHasCacheControl);
  // 空数组不折叠（保持原结构，不做无中生有的改写）
  const foldable = content.length > 0 && content.every(isTextBlock);
  if (!needsStrip && !foldable) return { value: content, changed: false };
  const blocks = needsStrip
    ? content.map((b) => {
        if (!blockHasCacheControl(b)) return b;
        const copy = { ...b };
        delete copy.cache_control;
        return copy;
      })
    : content;
  return {
    value: foldable ? blocks.map((b) => (typeof b.text === 'string' ? b.text : '')).join(sep) : blocks,
    changed: true,
  };
}

function stripOpenAIWireCacheControl(payload) {
  if (!payload || typeof payload !== 'object') return payload;

  // 顶层 system（Anthropic 方向的 fromAnthropicBody 会把它删掉，这里只是防御性覆盖）
  if (payload.system !== undefined) {
    const { value, changed } = toOpenAIWireContent(payload.system, '\n');
    if (changed) payload.system = value;
  }

  if (!Array.isArray(payload.messages)) return payload;
  let changedAny = false;
  const next = payload.messages.map((msg) => {
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) return msg;
    const sep = isSystemRole(msg.role) ? '\n' : '';
    const { value: content, changed: contentChanged } = toOpenAIWireContent(msg.content, sep);
    const msgLevel = msg.cache_control != null;
    const tcLevel = Array.isArray(msg.tool_calls) && msg.tool_calls.some(blockHasCacheControl);
    if (!contentChanged && !msgLevel && !tcLevel) return msg;
    changedAny = true;
    const out = contentChanged ? { ...msg, content } : { ...msg };
    if (msgLevel) delete out.cache_control;
    if (tcLevel) {
      out.tool_calls = msg.tool_calls.map((tc) => {
        if (!blockHasCacheControl(tc)) return tc;
        const copy = { ...tc };
        delete copy.cache_control;
        return copy;
      });
    }
    return out;
  });
  if (changedAny) payload.messages = next;
  return payload;
}

/**
 * OpenAI finish_reason -> Anthropic stop_reason。
 *
 * hasToolCalls 兜底必须和 anthropic.mjs 的 mapStopReason(reason, hasToolCalls) 同口径：
 * 已经产出了 tool_use 块，而上游漏给 finish_reason（实测中转会返回 null），或干脆把
 * 工具调用也报成 'stop' 时，绝不能回 end_turn——Anthropic 客户端（Claude Code）看不到
 * tool_use 停止原因就不会执行工具，agent 会卡死。
 * 注意：finish_reason='length' 的截断语义要保留，不能被工具兜底覆盖。
 */
export function toAnthropicStopReason(reason, hasToolCalls = false) {
  if (hasToolCalls && (reason == null || reason === '' || reason === 'stop' || reason === 'tool_calls')) {
    return 'tool_use';
  }
  switch (reason) {
    case 'length': return 'max_tokens';
    case 'tool_calls': return 'tool_use';
    case 'content_filter': return 'refusal';
    case 'stop':
    default: return 'end_turn';
  }
}
