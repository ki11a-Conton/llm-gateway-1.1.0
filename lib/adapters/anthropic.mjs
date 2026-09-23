// Anthropic 原生协议适配器：对外 OpenAI 格式，对内 /v1/messages 原生格式
// 负责请求转换、非流式响应转换、流式 SSE 事件流转换

import { joinUrl, genId, safeJsonParse } from '../util.mjs';
import { log } from '../logger.mjs';

const ANTHROPIC_VERSION = '2023-06-01';

// 思考强度 -> Anthropic thinking budget（budget_tokens 必须小于 max_tokens，buildRequest 里已联动抬升）
// xhigh 与 high 同为 32768：Anthropic 的 budget 上限本来就贴着 max_tokens 天花板，
// 再往上加只会把 max_tokens 抬到不合法区间，不设更高值。
const EFFORT_BUDGET = { low: 4096, medium: 16384, high: 32768, xhigh: 32768 };

export const anthropicAdapter = {
  id: 'anthropic',
  passthroughStream: false, // Anthropic 的 SSE 结构与 OpenAI 不同，必须转译

  buildRequest({ channel, model, body, stream }) {
    const { system, messages } = toAnthropicMessages(body.messages || []);
    let maxTokens = body.max_tokens ?? body.max_completion_tokens ?? channel.maxTokens ?? 4096;
    // 请求改写：渠道配置 minMaxTokens 时，把低于下限的输出上限抬到下限（部分上游对极小值直接 400）
    if (channel.minMaxTokens && Number(maxTokens) < Number(channel.minMaxTokens)) {
      maxTokens = Number(channel.minMaxTokens);
    }

    const payload = {
      model,
      messages,
      max_tokens: Number(maxTokens) || 4096,
      stream: !!stream,
    };
    if (system) payload.system = system;
    if (body.temperature != null) payload.temperature = clamp(body.temperature, 0, 1);
    if (body.top_p != null) payload.top_p = body.top_p;
    // top_k：Anthropic 原生采样参数。客户端显式传了就透传——此前从来不转发，等于白丢客户端意图。
    // 开 thinking 时官方要求它保持默认值，由下面的 stripThinkingIncompatible 统一清掉（守卫不变）。
    if (body.top_k != null) payload.top_k = body.top_k;
    if (body.stop) payload.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];
    if (body.metadata?.user_id) payload.metadata = { user_id: String(body.metadata.user_id) };

    // OpenAI 的 reasoning_effort -> Anthropic 的 thinking budget
    const effort = typeof body.reasoning_effort === 'string' ? body.reasoning_effort.toLowerCase() : null;
    const wantsThinking = !!effort && effort !== 'off' && effort !== 'none';
    // 尾部是 assistant = 客户端显式 prefill（续写）。Anthropic 不允许"开 thinking + prefill"共存，
    // 但 prefill 是客户端主动表达的续写意图，优先级高于网关注入的 effort：
    // 这时宁可不注入 thinking（请求仍合法、仍能续写），也不要因此报错或悄悄改掉客户端语义。
    const isPrefill = messages.length > 0 && messages[messages.length - 1].role === 'assistant';
    // 渠道 maxTokens 是模型/渠道的硬上限（如 config.json 的 anthropic 渠道 8192、Claude 3 Opus 4096）。
    // 只有明确配了才当上限用：没配就不猜，保持"上限未知"的旧行为（见下方分支）。
    const cap = Number(channel.maxTokens) > 0 ? Number(channel.maxTokens) : null;
    // 上限小到连最小 budget(1024) + 1024 正文余量都放不下时，该渠道根本开不了 thinking：
    // 放弃注入，保留客户端原本的 max_tokens（发一个必定 400 的请求比不思考更糟）。
    const canFitThinking = cap === null || cap >= 2048;
    const thinkingEnabled = wantsThinking && !isPrefill && canFitThinking;
    // 思考预算策略（routing.thinkingMaxTokensPolicy，经 channel.routing 透传，见 channels.mjs:220）：
    //   "raise"（默认）= 为满足官方 budget_tokens < max_tokens，把 max_tokens 抬到 budget + 1024
    //                    （必要时夹在渠道上限内）——这是既有行为，默认值必须与改动前逐位一致。
    //   "drop-thinking" = 客户端给的 max_tokens 是硬意图：宁可这次不思考，也绝不放大 max_tokens；
    //                    代价是本次请求失去思考能力，所以 warn 一次留痕。
    const thinkingPolicy = resolveThinkingMaxTokensPolicy(channel);
    let thinkingApplied = false;

    if (thinkingEnabled) {
      const want = EFFORT_BUDGET[effort] ?? EFFORT_BUDGET.medium;
      // Anthropic 要求 budget_tokens < max_tokens，且 budget 自身下限是 1024。
      // 已知上限时优先保证请求合法：档位塞不进上限就降 budget（思考短一点），
      // 绝不为塞下目标档位把 max_tokens 顶到上限之外。
      const budget = cap === null ? want : Math.max(1024, Math.min(want, cap - 1024));
      // max_tokens = max(客户端/渠道要求值, budget + 1024)，已知上限时再夹到上限内。
      // body.max_tokens / max_completion_tokens 是客户端"想要多少输出"（两者语义等价，前者优先），
      // 不是渠道硬上限，所以只能参与取大，不能当成上限用。
      const withHeadroom = budget + 1024;
      const raisedMaxTokens = cap === null
        ? Math.max(payload.max_tokens, withHeadroom)
        : Math.min(cap, Math.max(payload.max_tokens, withHeadroom));
      // drop-thinking 只在"确实需要放大"时放弃思考。上限把客户端值改小了（如 20000 夹到 8192）
      // 不算放大：那是"请求本来就不合法"的规范化，与思考无关，此时思考照常保留。
      if (thinkingPolicy === 'drop-thinking' && raisedMaxTokens > payload.max_tokens) {
        warnThinkingDroppedOnce(channel, payload.max_tokens, raisedMaxTokens);
      } else {
        payload.thinking = { type: 'enabled', budget_tokens: budget };
        payload.max_tokens = raisedMaxTokens;
        thinkingApplied = true;
      }
    }

    if (Array.isArray(body.tools) && body.tools.length) {
      payload.tools = body.tools
        .filter((t) => t?.type === 'function' || t?.function)
        .map((t) => ({
          name: t.function?.name ?? t.name,
          description: t.function?.description ?? t.description ?? '',
          input_schema: t.function?.parameters ?? { type: 'object', properties: {} },
        }));
    }
    if (body.tool_choice) payload.tool_choice = toAnthropicToolChoice(body.tool_choice);

    // 互斥参数清理必须放在 tools/tool_choice 落好之后（要能看到 tool_choice），
    // 且统一走 stripThinkingIncompatible：以后再加采样类参数时只改那一处，不会再漏。
    // 注意用 thinkingApplied 而不是 thinkingEnabled：drop-thinking 策略下 thinkingEnabled 为真
    // 但实际没注入 thinking，此时绝不能清采样参数（没有 thinking 就无互斥可言）。
    if (thinkingApplied) stripThinkingIncompatible(payload);

    const headers = {
      'content-type': 'application/json',
      'x-api-key': channel.apiKey,
      'anthropic-version': channel.anthropicVersion || ANTHROPIC_VERSION,
      ...(channel.headers || {}),
    };
    return { url: joinUrl(channel.baseUrl, channel.chatPath || '/messages'), headers, payload };
  },

  buildModelsRequest(channel) {
    return {
      url: joinUrl(channel.baseUrl, channel.modelsPath || '/models'),
      headers: {
        'x-api-key': channel.apiKey,
        'anthropic-version': channel.anthropicVersion || ANTHROPIC_VERSION,
        ...(channel.headers || {}),
      },
    };
  },

  parseModels(json) {
    const list = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : null;
    if (!list) return null;
    return list.map((m) => (typeof m === 'string' ? m : m?.id ?? m?.name)).filter(Boolean);
  },

  toOpenAIResponse(json, model) {
    const blocks = Array.isArray(json?.content) ? json.content : [];
    const text = blocks.filter((b) => b.type === 'text').map((b) => b.text).join('');
    const thinking = blocks.filter((b) => b.type === 'thinking').map((b) => b.thinking).join('');
    const toolCalls = blocks
      .filter((b) => b.type === 'tool_use')
      .map((b) => ({
        id: b.id,
        type: 'function',
        function: { name: b.name, arguments: JSON.stringify(b.input ?? {}) },
      }));

    const inTok = json?.usage?.input_tokens ?? 0;
    const outTok = json?.usage?.output_tokens ?? 0;

    const message = {
      role: 'assistant',
      content: text || null,
      ...(toolCalls.length ? { tool_calls: toolCalls } : {}),
      ...(thinking ? { reasoning_content: thinking } : {}),
    };

    return {
      id: json?.id ?? genId('chatcmpl-'),
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: json?.model || model,
      choices: [
        {
          index: 0,
          message,
          finish_reason: mapStopReason(json?.stop_reason, toolCalls.length > 0),
        },
      ],
      usage: {
        prompt_tokens: inTok,
        completion_tokens: outTok,
        total_tokens: inTok + outTok,
      },
    };
  },

  /**
   * 把 Anthropic 的 SSE 事件流转换成 OpenAI 的 chat.completion.chunk 流
   * @param {AsyncIterable<{event:string,data:string}>} events
   * @param {string} model
   * @param {{onMidStreamError?:Function}} [opts]
   */
  async *streamToOpenAI(events, model, opts = {}) {
    const id = genId('chatcmpl-');
    const created = Math.floor(Date.now() / 1000);
    const usage = { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };
    let sentRole = false;
    let toolIndex = -1;
    let stopReason = null;
    let sawTerminal = false; // F1：是否收到过有效终止信号（message_stop / stop_reason）

    const frame = (delta, finishReason = null, withUsage = false) => {
      const chunk = {
        id,
        object: 'chat.completion.chunk',
        created,
        model,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      };
      if (withUsage) chunk.usage = usage;
      return `data: ${JSON.stringify(chunk)}\n\n`;
    };

    const ensureRole = () => {
      if (sentRole) return null;
      sentRole = true;
      return frame({ role: 'assistant', content: '' });
    };

    try {
      for await (const ev of events) {
        if (ev.event === 'ping') continue;
        const msg = safeJsonParse(ev.data, null);
        if (!msg || typeof msg !== 'object') continue;

        switch (msg.type) {
          case 'message_start': {
            if (msg.message?.usage?.input_tokens) usage.prompt_tokens = msg.message.usage.input_tokens;
            const r = ensureRole();
            if (r) yield r;
            break;
          }
          case 'content_block_start': {
            const b = msg.content_block;
            if (b?.type === 'tool_use') {
              toolIndex += 1;
              const r = ensureRole();
              if (r) yield r;
              yield frame({
                tool_calls: [
                  {
                    index: toolIndex,
                    id: b.id,
                    type: 'function',
                    function: { name: b.name, arguments: '' },
                  },
                ],
              });
            }
            break;
          }
          case 'content_block_delta': {
            const d = msg.delta;
            if (!d) break;
            if (d.type === 'text_delta' && d.text) {
              const r = ensureRole();
              if (r) yield r;
              yield frame({ content: d.text });
            } else if (d.type === 'thinking_delta' && d.thinking) {
              // 思考过程转成 DeepSeek 风格的 reasoning_content 增量
              const r = ensureRole();
              if (r) yield r;
              yield frame({ reasoning_content: d.thinking });
            } else if (d.type === 'input_json_delta' && d.partial_json) {
              yield frame({
                tool_calls: [
                  { index: Math.max(toolIndex, 0), function: { arguments: d.partial_json } },
                ],
              });
            }
            break;
          }
          case 'message_delta': {
            if (msg.delta?.stop_reason) {
              stopReason = msg.delta.stop_reason;
              // 有的兼容上游用 message_delta.stop_reason 收尾、不发 message_stop：
              // 这算有效的协议终止信号（F1 允许这种兼容形态）
              sawTerminal = true;
            }
            if (msg.usage?.output_tokens) usage.completion_tokens = msg.usage.output_tokens;
            break;
          }
          case 'message_stop': {
            sawTerminal = true;
            usage.total_tokens = usage.prompt_tokens + usage.completion_tokens;
            yield frame({}, mapStopReason(stopReason), true);
            yield 'data: [DONE]\n\n';
            return;
          }
          case 'error': {
            const e = new Error(msg.error?.message || 'anthropic stream error');
            e.code = 'MID_STREAM';
            throw e;
          }
          default:
            break;
        }
      }
    } catch (err) {
      if (err?.code === 'MID_STREAM') {
        opts.onMidStreamError?.(err);
        // F1（代码审查 2026-09-20）：上游流内报错**绝不能包装成正常结束**。
        // 原来这里 yield 一个 finish_reason=stop + [DONE]，于是路由层看到"正常返回"，
        // 执行 markSuccess、更新成功渠道记忆并记 ok:true —— agent 把半截回答当完整结果，
        // 故障渠道还继续享受优先路由。改为打标记抛出，由转发层按"响应是否已提交"
        // 决定换家（未提交）或按下游协议报流内错误并记失败（已提交）。
        const e = new Error(`上游流内错误: ${err.message}`);
        e.code = 'STREAM_TRUNCATED';
        throw e;
      }
      if (err?.code === 'STREAM_TRUNCATED') throw err;
      // 上游字节流中断（socket 断开 / abort）：同样不能补一个"优雅收尾"骗过路由层
      const e = new Error(`上游字节流中断: ${err.message}`);
      e.code = 'STREAM_TRUNCATED';
      throw e;
    }

    // 流未按预期收尾：没有拿到任何有效终止信号 = 上游被截断
    if (!sawTerminal) {
      const e = new Error('上游流在给出终止信号前结束（缺少 message_stop / stop_reason）');
      e.code = 'STREAM_TRUNCATED';
      throw e;
    }
    usage.total_tokens = usage.prompt_tokens + usage.completion_tokens;
    yield frame({}, mapStopReason(stopReason), true);
    yield 'data: [DONE]\n\n';
  },
};

// ---------- 内部辅助 ----------

export function mapStopReason(reason, hasToolCalls = false) {
  if (hasToolCalls && (reason === 'tool_use' || !reason)) return 'tool_calls';
  switch (reason) {
    case 'end_turn':
    case 'stop_sequence':
    case 'pause_turn':
    case 'refusal':
      return 'stop';
    case 'max_tokens':
      return 'length';
    case 'tool_use':
      return 'tool_calls';
    default:
      return 'stop';
  }
}

function clamp(v, lo, hi) {
  const n = Number(v);
  if (Number.isNaN(n)) return undefined;
  return Math.min(hi, Math.max(lo, n));
}

/**
 * 思考预算策略解析：channel.routing.thinkingMaxTokensPolicy。
 * Channel 构造时把 manager 的 routing 挂在 channel.routing 上（channels.mjs:220），
 * 所以适配器不需要额外传参就能读到路由级配置。
 * 未知值 / 未配置一律回落 "raise"（保持改动前的默认行为，零漂移）。
 * @returns {'raise'|'drop-thinking'}
 */
export function resolveThinkingMaxTokensPolicy(channel) {
  const raw = channel?.routing?.thinkingMaxTokensPolicy;
  if (raw == null) return 'raise';
  const v = String(raw).trim().toLowerCase().replace(/_/g, '-');
  return v === 'drop-thinking' || v === 'dropthinking' ? 'drop-thinking' : 'raise';
}

// drop-thinking 的 warn 只发一次（按渠道），避免每个请求刷屏。
const warnedDropThinking = new Set();

function warnThinkingDroppedOnce(channel, clientMaxTokens, raisedMaxTokens) {
  const key = channel?.name || channel?.baseUrl || '(unknown)';
  if (warnedDropThinking.has(key)) return;
  warnedDropThinking.add(key);
  log.warn(
    `thinkingMaxTokensPolicy=drop-thinking：客户端 max_tokens=${clientMaxTokens} 放不下思考所需 ${raisedMaxTokens}，`
    + '已放弃思考（不放大 max_tokens）',
    channel?.name || 'anthropic',
  );
}

// ---------------------------------------------------------------------------
// prompt caching（cache_control）保真透传
//
// cache_control 是请求侧标记，网关只负责"搬运"：不解释、不新增、不改位置。
// "透传但上游忽略"与"在网关这一层丢弃"是两种语义：前者上游一旦支持缓存前缀就自动生效，
// 后者等于永久白丢（本文件此前就是后者）。因此所有"生成/重建内容块"的地方都必须走
// carryCacheControl —— 这是唯一入口，以后再加块类型时照着调用，就不会又漏一处。
// 参考：https://platform.claude.com/docs/en/build-with-claude/prompt-caching
// ---------------------------------------------------------------------------

/** 把 source 上的 cache_control 原样搬到 target 块上（source 没有就不动 target）。 */
export function carryCacheControl(target, source) {
  if (!target || typeof target !== 'object') return target;
  const cc = source && typeof source === 'object' ? source.cache_control : undefined;
  if (cc != null) target.cache_control = cc;
  return target;
}

/** 该块（或消息/工具调用）上有没有 cache_control 标记。 */
const hasCacheControl = (x) => x != null && typeof x === 'object' && x.cache_control != null;

/** Anthropic image 块 -> OpenAI image_url part（无 cache_control 时不产生标记，形态与改动前一致）。 */
function toOpenAIImagePart(im) {
  if (im?.source?.type === 'base64') {
    return {
      type: 'image_url',
      image_url: { url: `data:${im.source.media_type || 'image/png'};base64,${im.source.data}` },
    };
  }
  if (im?.source?.type === 'url') {
    return { type: 'image_url', image_url: { url: im.source.url } };
  }
  return null;
}

/**
 * 清掉与 Anthropic thinking 互斥的出站参数（只作用于 payload，不改客户端 body）。
 * 官方约束（https://platform.claude.com/docs/en/build-with-claude/thinking）：
 *   - temperature / top_p / top_k 必须保持默认值；
 *   - 不能用强制工具选择（tool_choice = any / tool），auto / none 可以；
 *   - 不能同时给尾部 assistant prefill（这一条在 buildRequest 里靠"不注入 thinking"解决）。
 * 这些约束是一组、随参数增加最容易漏掉某一个，所以抽成一处集中处理。
 */
function stripThinkingIncompatible(payload) {
  delete payload.temperature;
  delete payload.top_p;
  // buildRequest 现在会转发客户端显式传的 top_k，所以这里不再是"防御性删除"，
  // 而是官方互斥规则的实际执行点：开 thinking 时 top_k 必须保持默认值。
  delete payload.top_k;
  // 强制工具选择与 thinking 互斥：降级为"整个字段不发送"，等价于 Anthropic 的 auto 默认值。
  // 不降级成 auto 而是删掉，是为了不改写客户端没显式要求的语义；也不删 tools，
  // 否则模型会完全看不到工具，比降级更偏离客户端意图。
  if (payload.tool_choice?.type === 'any' || payload.tool_choice?.type === 'tool') {
    delete payload.tool_choice;
  }
}

function toAnthropicToolChoice(tc) {
  if (!tc) return undefined;
  if (tc === 'auto') return { type: 'auto' };
  if (tc === 'none') return { type: 'none' };
  if (tc === 'required' || tc === 'any') return { type: 'any' };
  if (typeof tc === 'object' && tc.function?.name) {
    return { type: 'tool', name: tc.function.name };
  }
  return { type: 'auto' };
}

function normalizeUserContent(content) {
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  if (!Array.isArray(content)) return [{ type: 'text', text: String(content ?? '') }];

  const out = [];
  for (const part of content) {
    if (part?.type === 'text') {
      // cache_control 跟着"同一个块"走：文本逐字不变，标记不搬家也不丢。
      out.push(carryCacheControl({ type: 'text', text: part.text ?? '' }, part));
    } else if (part?.type === 'image_url') {
      const url = part.image_url?.url;
      if (typeof url === 'string' && url.startsWith('data:')) {
        const m = /^data:([^;]+);base64,(.*)$/s.exec(url);
        if (m) {
          out.push(carryCacheControl({
            type: 'image',
            source: { type: 'base64', media_type: m[1], data: m[2] },
          }, part));
        }
      } else if (typeof url === 'string') {
        out.push(carryCacheControl({ type: 'image', source: { type: 'url', url } }, part));
      }
    }
  }
  return out.length ? out : [{ type: 'text', text: '' }];
}

// Anthropic 请求体 -> OpenAI 请求体（客户端走 /v1/messages、上游是 OpenAI 协议时用）
export function fromAnthropicBody(body) {
  const messages = [];

  if (body.system) {
    const sysBlocks = typeof body.system === 'string'
      ? [{ type: 'text', text: body.system }]
      : Array.isArray(body.system) ? body.system : [];
    if (sysBlocks.some(hasCacheControl)) {
      // 带标记时不能塌成一个字符串：标记必须留在原来那个块上（块位置本身就是缓存语义）。
      // 内部表示用"文本块数组"（OpenAI 的 content parts 形态之一），标记跟着块一起进数组。
      const content = [];
      for (const b of sysBlocks) {
        const text = typeof b === 'string' ? b : b?.text ?? '';
        if (text || hasCacheControl(b)) content.push(carryCacheControl({ type: 'text', text }, b));
      }
      if (content.length) messages.push({ role: 'system', content });
    } else {
      const sys =
        typeof body.system === 'string'
          ? body.system
          : Array.isArray(body.system)
            ? body.system.map((s) => (typeof s === 'string' ? s : s?.text ?? '')).join('\n')
            : '';
      if (sys) messages.push({ role: 'system', content: sys });
    }
  }

  for (const m of body.messages || []) {
    if (m?.role === 'user') {
      const parts = Array.isArray(m.content) ? m.content : [{ type: 'text', text: m.content ?? '' }];
      const texts = parts.filter((p) => p.type === 'text').map((p) => p.text ?? '');
      const images = parts.filter((p) => p.type === 'image');
      const toolResults = parts.filter((p) => p.type === 'tool_result');

      for (const tr of toolResults) {
        const c =
          typeof tr.content === 'string'
            ? tr.content
            : Array.isArray(tr.content)
              ? tr.content.map((x) => (typeof x === 'string' ? x : x?.text ?? '')).join('')
              : JSON.stringify(tr.content ?? '');
        // tool_result 的标记挂到内部 tool 消息上（块本身在这里被拆成了独立消息，没地方放），
        // 回到 Anthropic 渠道时由 toAnthropicMessages 重新放回 tool_result 块。
        const toolMsg = { role: 'tool', tool_call_id: tr.tool_use_id, content: c };
        if (hasCacheControl(tr)) toolMsg.cache_control = tr.cache_control;
        messages.push(toolMsg);
      }

      const text = texts.join('');
      // 只有"标记落在会被折叠的块上"（text/image）时才需要保留块结构；tool_result/tool_use 的
      // 标记另走消息/工具调用字段，不必因此改变 content 形态（少扰动上游）。
      const inlineMarked = parts.some((p) => (p?.type === 'text' || p?.type === 'image') && hasCacheControl(p));
      if (inlineMarked) {
        const content = [];
        for (const p of parts) {
          if (p?.type === 'text') {
            const t = p.text ?? '';
            if (t || hasCacheControl(p)) content.push(carryCacheControl({ type: 'text', text: t }, p));
          } else if (p?.type === 'image') {
            const block = toOpenAIImagePart(p);
            if (block) content.push(carryCacheControl(block, p));
          }
        }
        if (content.length) messages.push({ role: 'user', content });
      } else if (images.length) {
        const content = [];
        if (text) content.push({ type: 'text', text });
        for (const im of images) {
          const block = toOpenAIImagePart(im);
          if (block) content.push(block);
        }
        messages.push({ role: 'user', content });
      } else if (text) {
        messages.push({ role: 'user', content: text });
      }
      continue;
    }

    if (m?.role === 'assistant') {
      const parts = Array.isArray(m.content) ? m.content : [{ type: 'text', text: m.content ?? '' }];
      const text = parts.filter((p) => p.type === 'text').map((p) => p.text ?? '').join('');
      const toolUses = parts.filter((p) => p.type === 'tool_use');
      const msg = { role: 'assistant', content: text || null };
      // 同上：只有文本块上有标记时才保留块结构；否则维持改动前的字符串形态。
      if (parts.some((p) => p?.type === 'text' && hasCacheControl(p))) {
        const content = [];
        for (const p of parts) {
          if (p?.type !== 'text') continue;
          const t = p.text ?? '';
          if (t || hasCacheControl(p)) content.push(carryCacheControl({ type: 'text', text: t }, p));
        }
        msg.content = content.length ? content : null;
      }
      if (toolUses.length) {
        msg.tool_calls = toolUses.map((tu) => {
          const tc = {
            id: tu.id,
            type: 'function',
            function: { name: tu.name, arguments: JSON.stringify(tu.input ?? {}) },
          };
          // tool_use 块上的标记挂到 tool_call 上，回到 Anthropic 渠道时再放回块上。
          if (hasCacheControl(tu)) tc.cache_control = tu.cache_control;
          return tc;
        });
      }
      messages.push(msg);
    }
  }

  const out = { ...body, messages };
  delete out.system;
  delete out.anthropic_version;

  // Anthropic 的 thinking 参数折算成统一的 reasoning_effort（供强度路由与下游转换使用）
  if (body.thinking && body.thinking.type === 'enabled' && Number(body.thinking.budget_tokens) > 0) {
    const b = Number(body.thinking.budget_tokens);
    out.reasoning_effort = b >= 24000 ? 'high' : b >= 10000 ? 'medium' : 'low';
  }
  delete out.thinking;

  if (body.stop_sequences) {
    out.stop = body.stop_sequences;
    delete out.stop_sequences;
  }
  if (Array.isArray(body.tools) && body.tools.length) {
    out.tools = body.tools.map((t) => ({
      type: 'function',
      function: {
        name: t.name,
        description: t.description ?? '',
        parameters: t.input_schema ?? { type: 'object', properties: {} },
      },
    }));
  }
  if (body.tool_choice) out.tool_choice = fromAnthropicToolChoice(body.tool_choice);
  if (body.metadata?.user_id) out.user = String(body.metadata.user_id);

  return out;
}

function fromAnthropicToolChoice(tc) {
  if (typeof tc === 'string') return tc;
  switch (tc.type) {
    case 'auto': return 'auto';
    case 'none': return 'none';
    case 'any': return 'required';
    case 'tool': return { type: 'function', function: { name: tc.name } };
    default: return 'auto';
  }
}

// OpenAI messages -> Anthropic { system, messages }
// 额外处理：抽取 system、合并相邻同角色消息、tool 消息转 tool_result
export function toAnthropicMessages(messages) {
  const systemParts = [];
  const out = [];

  // 把 content 归一成块数组（合并时要按块拼接，字符串得先包成 text 块）
  const toBlocks = (content) => (Array.isArray(content) ? content : [{ type: 'text', text: content ?? '' }]);

  const push = (msg) => {
    const prev = out[out.length - 1];
    // Anthropic 不接受连续同角色消息，做一次合并。
    // tool_result 也是装在 user 消息里的，所以必须一起参与合并：一次 assistant 里有 N 个并行
    // tool_use 时客户端会发回 N 条 role:'tool'，若各自占一条 user 就变成连续 N 条 user，
    // 上游直接 400 "roles must alternate"。合并成同一条 user 后，N 个 tool_result 都在里面，
    // 且保持调用顺序（并行结果本就该在同一条 user 消息里一起回）。
    // 同一处逻辑也覆盖"tool_result 后面紧跟一条普通 user 文本"的常见形态（客户端把工具结果
    // 和追加指令分两条发），它同样会被合成一条 user，不会留下连续同角色。
    if (prev && prev.role === msg.role) {
      const blocks = [...toBlocks(prev.content), ...toBlocks(msg.content)];
      // Anthropic 要求 user 消息里 tool_result 块必须排在所有文本块之前
      // （见 handle-tool-calls：tool_result blocks must come FIRST in the content array）。
      // 按类型做一次稳定分组：tool_result 之间、文本块之间各自的先后顺序都不变，
      // 因此普通"连续同角色文本合并"的行为与合并前完全一致。
      prev.content = [
        ...blocks.filter((b) => b?.type === 'tool_result'),
        ...blocks.filter((b) => b?.type !== 'tool_result'),
      ];
      return;
    }
    out.push(msg);
  };

  for (const m of messages) {
    if (!m || typeof m !== 'object') continue;
    const role = m.role;

    if (role === 'system' || role === 'developer') {
      const blocks = typeof m.content === 'string'
        ? [{ type: 'text', text: m.content }]
        : Array.isArray(m.content)
          ? m.content
          : [{ type: 'text', text: String(m.content ?? '') }];
      if (blocks.some(hasCacheControl)) {
        // 带标记的 system 块不能塌成字符串：Anthropic 的 system 接受"文本块数组"，
        // 数组里每个块各自带 cache_control（位置=语义）。逐块登记，收尾时再决定形态。
        for (const b of blocks) {
          const text = typeof b === 'string' ? b : b?.text ?? '';
          if (text || hasCacheControl(b)) systemParts.push({ text, cache_control: b?.cache_control });
        }
        continue;
      }
      const text =
        typeof m.content === 'string'
          ? m.content
          : Array.isArray(m.content)
            ? m.content.map((c) => c?.text ?? '').join('\n')
            : String(m.content ?? '');
      if (text) systemParts.push(text);
      continue;
    }

    if (role === 'user') {
      push({ role: 'user', content: normalizeUserContent(m.content) });
      continue;
    }

    if (role === 'assistant') {
      const content = [];
      if (typeof m.content === 'string') {
        if (m.content) content.push({ type: 'text', text: m.content });
      } else if (Array.isArray(m.content)) {
        // 文本块上的 cache_control 必须跟着块一起进 content（上游是否命中缓存由它决定）
        for (const c of m.content) {
          if (c?.type === 'text' && (c.text || hasCacheControl(c))) {
            content.push(carryCacheControl({ type: 'text', text: c.text ?? '' }, c));
          }
        }
      }
      if (Array.isArray(m.tool_calls)) {
        for (const tc of m.tool_calls) {
          content.push(carryCacheControl({
            type: 'tool_use',
            id: tc?.id || genId('toolu_'),
            name: tc?.function?.name || '',
            input: safeJsonParse(tc?.function?.arguments, {}),
          }, tc));
        }
      }
      if (!content.length) content.push({ type: 'text', text: '' });
      push({ role: 'assistant', content });
      continue;
    }

    if (role === 'tool') {
      const text =
        typeof m.content === 'string'
          ? m.content
          : Array.isArray(m.content)
            ? m.content.map((c) => c?.text ?? '').join('\n')
            : JSON.stringify(m.content ?? '');
      // Anthropic 把 tool_result 放在 user 消息里
      push({
        role: 'user',
        isToolResult: true,
        content: [
          carryCacheControl({
            type: 'tool_result',
            tool_use_id: m.tool_call_id,
            content: text || '',
          }, m),
        ],
      });
      continue;
    }
  }

  // Anthropic 要求首条必须是 user
  while (out.length && out[0].role !== 'user') out.shift();

  // 没有任何块带标记时，system 保持原来的单字符串形态（不配 cache_control 的请求零漂移）。
  const keptSystem = systemParts.filter((p) => (typeof p === 'string' ? p : p.text));
  const systemMarked = keptSystem.some((p) => typeof p !== 'string' && p.cache_control != null);

  return {
    system: !keptSystem.length
      ? undefined
      : systemMarked
        ? keptSystem.map((p) => carryCacheControl(
            { type: 'text', text: typeof p === 'string' ? p : p.text },
            typeof p === 'string' ? null : p,
          ))
        : keptSystem.join('\n\n'),
    messages: out,
  };
}
