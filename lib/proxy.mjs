// 转发代理：选路 -> 并发闸门 -> 逐个尝试上游（分池、分预算）-> 整池循环重试 -> 流式/非流式写回客户端
//
// 路由分两段（tier），见 channels.mjs#candidatesFor：
//   第 1 段 preferred（sensenova / api.b.ai）：按 priority 轮询，渠道内重试用渠道自己的 retries
//   第 2 段 fallback（其余供应商）：上次成功的渠道钉在最前反复复用；一旦失败按
//     fallbackAttempts 重试（默认 1 + 2 次，每次 3s），仍失败才换下一家；
//     余额不足 / 鉴权失败 / 空响应立刻换下一家
//
// 可用性铁律：任何代码路径都必须给客户端一个 HTTP 响应。绝不允许请求被"挂着不处理"
// ——每个循环、每次等待、每次探活都受客户端断开信号与总预算约束。

import { Readable } from 'node:stream';
import { log } from './logger.mjs';
import {
  parseSSE, readAllText, truncate, safeJsonParse, estimatePromptTokens,
  classifyUpstreamFailure, modelSupportsThinking, MAX_EFFORT, EFFORT_FALLBACK, isEffortRejection,
} from './util.mjs';
import { openaiAdapter } from './adapters/openai.mjs';
import { anthropicAdapter } from './adapters/anthropic.mjs';
import { findToolCallTextLeak, extractStreamContentText, hitStreamLeak } from './toolcall-guard.mjs';
import { cleanMessageReasoning, splitInlineReasoning } from './reasoning-guard.mjs';
import { isOverloadError } from './concurrency.mjs';
import { shouldRetryInChannel, waitBeforeRetry, DEFAULT_CHANNEL_RETRY } from './retry.mjs';
import { shortLabel } from './agent.mjs';
import { getTaskLog, errorFingerprint } from './tasklog.mjs';
import { getUsage } from './usage.mjs';

const MAX_BODY_BYTES = 32 * 1024 * 1024;

// ---------- Token 用量采集（设计文档 2026-09-17 §4.1）----------
// 只认上游**真实回报**的 usage：OpenAI 口径 prompt_tokens/completion_tokens、
// Anthropic 口径 input_tokens/output_tokens。适配层在转换时会把缺失的 usage 兜成 0，
// 所以这里扫的是**上游原始响应字节**（非流式 JSON / 流式 SSE 都适用），
// 两套字段名都认，取各字段出现过的最大值（Anthropic 的 input 在 message_start、
// output 在 message_delta，分两帧到达；流末 usage 帧只有一次）。
// 全程没有任何 usage（或 usage 全为 0）时返回 null —— 调用方据此**不产生**记录，绝不记成"用了 0 token"。
const USAGE_SCAN_TAIL = 4096; // 只留尾部窗口：usage 对象很小，跨 chunk 也能拼回来
// F5（代码审查 2026-09-20）：旧实现用 /"usage"\s*:\s*\{[^{}]*\}/g 扫 usage，
// `[^{}]*` 明确排除了内部对象 —— 只要 usage 含 prompt_tokens_details /
// completion_tokens_details 这类嵌套字段就整体匹配失败，外层 prompt/completion 总数也一起丢，
// 于是"成功请求"的 Token 与请求数双双漏记（报告复现 F5_nested_usage）。
// 现在改为**括号配对**扫描：找到 "usage" 键后按嵌套深度取出完整对象再 JSON 解析。

/** 从 `{` 起做括号配对，跳过字符串内的括号与转义；返回配对 `}` 的下标，失败返回 -1 */
function matchBraces(s, start) {
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let k = start; k < s.length; k += 1) {
    const c = s[k];
    if (inStr) {
      if (esc) esc = false;
      else if (c === '\\') esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') inStr = true;
    else if (c === '{') depth += 1;
    else if (c === '}') {
      depth -= 1;
      if (depth === 0) return k;
    }
  }
  return -1;
}

/** 遍历文本里所有 `"usage": {...}` 并解析（支持嵌套对象；跳过被转义的 \"usage\"） */
function* findUsageObjects(text) {
  const KEY = '"usage"';
  let i = text.indexOf(KEY);
  while (i !== -1) {
    // 被转义的 \"usage\" 是正文里的字面量（不是真实字段），跳过
    if (i > 0 && text[i - 1] === '\\') {
      i = text.indexOf(KEY, i + 1);
      continue;
    }
    let j = i + KEY.length;
    while (j < text.length && (text[j] === ' ' || text[j] === '\t')) j += 1;
    if (text[j] === ':') {
      j += 1;
      while (j < text.length && (text[j] === ' ' || text[j] === '\t')) j += 1;
    }
    if (text[j] === '{') {
      const end = matchBraces(text, j);
      if (end > j) {
        const obj = safeJsonParse(text.slice(j, end + 1), null);
        if (obj) yield obj;
        i = text.indexOf(KEY, end);
        continue;
      }
    }
    i = text.indexOf(KEY, i + 1);
  }
}

/** 从 usage 对象里取第一个有效的整数字段（缺失 / 非数字 -> null） */
function usageField(obj, keys) {
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v === 'number' && Number.isFinite(v) && v >= 0) return Math.floor(v);
  }
  return null;
}

/** 创建一个"边转发边扫 usage"的旁路扫描器（不改变字节流） */
function createUsageScan() {
  const seen = { input: 0, output: 0, found: false };
  let tail = '';
  return {
    push(item) {
      const text = Buffer.isBuffer(item) ? item.toString('utf8') : String(item ?? '');
      const buf = tail + text;
      for (const json of findUsageObjects(buf)) {
        const i = usageField(json, ['prompt_tokens', 'input_tokens']);
        const o = usageField(json, ['completion_tokens', 'output_tokens']);
        if (i == null && o == null) continue;
        if (i != null) seen.input = Math.max(seen.input, i);
        if (o != null) seen.output = Math.max(seen.output, o);
        if ((i ?? 0) > 0 || (o ?? 0) > 0) seen.found = true;
      }
      tail = buf.slice(-USAGE_SCAN_TAIL);
    },
    /** @returns {{input:number,output:number}|null} 上游真的回报过非零 usage 才有值 */
    result() {
      return seen.found ? { input: seen.input, output: seen.output } : null;
    },
  };
}

/** 旁路扫描包装：原样 yield 每个 chunk（字节透明），只顺手把文本喂给扫描器 */
async function* tapUsage(source, scan) {
  for await (const item of source) {
    scan.push(item);
    yield item;
  }
}

export class UpstreamError extends Error {
  /**
   * @param {object} opts
   * @param {boolean} [opts.responseStarted] 响应是否已经开始写回客户端。
   *   true = 已下发过数据，路由层必须停止换家（否则会往同一个 res 二次 writeHead）；
   *   false/未给 = 客户端一个字节都没收到，允许整体让给下一个渠道。
   *   ⚠️ 必须显式赋值：这个字段曾被传进构造器却没有保存，导致路由层
   *   `if (err.responseStarted)` 恒为假、"已下发不换家"的守卫成了死代码。
   */
  constructor(message, { status = 502, kind = 'upstream', retryable = true, detail = null, modelIssue = false, responseStarted = false } = {}) {
    super(message);
    this.status = status;
    this.kind = kind;
    this.retryable = retryable;
    this.detail = detail;
    this.modelIssue = modelIssue;
    this.responseStarted = responseStarted === true;
  }
}

export async function readJsonBody(req) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > MAX_BODY_BYTES) throw new UpstreamError('请求体过大', { status: 413, retryable: false });
    chunks.push(c);
  }
  if (!chunks.length) return {};
  const text = Buffer.concat(chunks).toString('utf8');
  const parsed = safeJsonParse(text, null);
  if (!parsed || typeof parsed !== 'object') {
    throw new UpstreamError('请求体不是合法的 JSON', { status: 400, retryable: false });
  }
  return parsed;
}

export function openaiError(message, type = 'server_error', code = null, status = 502) {
  return {
    error: {
      message,
      type,
      ...(code ? { code } : {}),
      param: null,
    },
  };
}

/**
 * 三段耗时的字段名（P2 §2.5，**冻结**）：排队 / 首字节 / 正文。
 * 数据源在这里（lib/proxy.mjs），消费方是 lib/tasklog.mjs + P3 的面板。
 */
const TIMING_KEYS = ['queueMs', 'ttfbMs', 'bodyMs'];

/** 只挑"实测到"的段：缺的键直接不写（绝不补 0，否则面板会把"没测到"显示成"等了 0ms"） */
function pickTimingFields(timing) {
  const out = {};
  if (!timing) return out;
  for (const k of TIMING_KEYS) {
    const v = Number(timing[k]);
    if (Number.isFinite(v) && v >= 0) out[k] = Math.round(v);
  }
  return out;
}

/**
 * 请求级耗时分解：把实测到的段**平铺**在任务记录上（P3 的 pickTimings / normalizeTimings
 * 读的就是平铺键），另附一份 `timings` 嵌套镜像（P2 §2.5 要求的请求级暴露）。
 * 一段都没测到 -> 不写任何键 —— 成功路径也必须带出来（否则最常见的"第一家就成功"
 * 在面板上完全看不到耗时分解）。
 */
function withTimings(timing) {
  const flat = pickTimingFields(timing);
  if (!Object.keys(flat).length) return {};
  return { ...flat, timings: { ...flat } };
}

/**
 * 上游响应体的"看门狗可达"包装（P2 §2.3）。
 *
 * 全局 fetch 的 body 会随 signal 一起被 abort；但**出站代理路径**不是：
 * lib/outbound-proxy.mjs 的 roundTrip 在"响应头解析完成"的那一刻就摘掉了 abort 监听
 *（`signal.removeEventListener('abort', onAbort)`，见该文件 roundTrip/onData），
 * 之后 socket / body 都不再理会 abort。后果是：上游只回响应头不给数据时，
 * 首字节/空闲看门狗虽然准时 abort 了，body 读取却永远不返回 —— 请求挂死，
 * 连带并发许可一起泄漏（这正是 P2 §2.3 要测的那条路径）。
 *
 * 转发层不能假设出站通道会兑现 abort，所以这里自己把 signal 接到 body 上：
 * destroy 会让 for-await 抛错（交给既有的错误分类处理），同时取消底层 web 流。
 * 无论走 fetch 还是走本机代理，看门狗都一定能打断 body 读取。
 *
 * 注：出站代理路径被中止后，代理那一跳的 socket 要等对端关闭才回收——那是
 * lib/outbound-proxy.mjs 内部的事（abort 没有传达到 socket），不在本文件归属内。
 */
function watchdogBody(webBody, signal) {
  if (!webBody) return null;
  const node = Readable.fromWeb(webBody);
  const onAbort = () => node.destroy(new Error('上游响应体读取被中止（看门狗）'));
  // 中止时可能没有活跃的 reader（例如正卡在写回背压等待里），
  // 挂一个空监听避免 Unhandled 'error' 直接把进程带走；for-await 自己的监听不受影响。
  node.on('error', () => { /* 由调用方按 abort 标志分类，见 throwIfBodyAborted */ });
  if (signal.aborted) onAbort();
  else signal.addEventListener('abort', onAbort, { once: true });
  node.once('close', () => signal.removeEventListener('abort', onAbort));
  return node;
}

/**
 * 等待内核发送缓冲排空：当 res.write() 返回 false 时必须等待 drain 再继续写，
 * 否则慢客户端会把上游数据无限堆在进程内存里（内存膨胀甚至 OOM）。
 * 客户端中途断开时 drain 不会到来，所以同时监听 close 以解除等待。
 *
 * P2 §2.2：只有 drain/close 两条解除路径还不够——客户端"既不读也不断开"时
 *（挂起的 agent、被 SIGSTOP 的进程、卡在网络中间态的对端），drain 永远不会到来，
 * `await` 会把请求连同并发许可**永久**挂住，攒满 maxConcurrent 之后整个网关假死。
 * 所以再补两条解除路径：
 *   - signal：请求级 abort（客户端断开 / 首字节超时 / 空闲看门狗）立即解除；
 *   - timeoutMs：自身超时，到点**抛错**而不是静默 resolve——把它当成"写完了"
 *     会让调用方把一次没送完的响应记成成功。
 * abort 只解除等待、不在这里定性：由调用方区分"客户端断开 / 首字节超时 / 静默超时"，
 * 否则空闲看门狗超时会被误报成 client_abort。
 */
function waitDrain(res, { signal = null, timeoutMs = 0 } = {}) {
  return new Promise((resolve, reject) => {
    let timer = null;
    let settled = false;
    function cleanup() {
      res.off('drain', onDrain);
      res.off('close', onClose);
      signal?.removeEventListener('abort', onAbort);
      clearTimeout(timer);
    }
    function done() {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    }
    function onDrain() { done(); }
    function onClose() { done(); }
    function onAbort() { done(); }
    // 客户端已经断开（destroyed / 已终结）时 drain 永远不会到来：必须立刻返回，
    // 否则 await 会把整个请求永久挂住（上游还在流，数据一直堆在进程内存里）。
    if (res.destroyed || res.writableEnded) {
      resolve();
      return;
    }
    if (signal?.aborted) {
      resolve();
      return;
    }
    res.once('drain', onDrain);
    res.once('close', onClose);
    signal?.addEventListener('abort', onAbort, { once: true });
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        cleanup();
        // 客户端停止消费：按"客户端已断开"处理（与 res.on('close') 同一条记账路径），
        // 既不算渠道失败（上游是好的），也绝不算成功。
        reject(new UpstreamError(`客户端停止读取响应（写回背压等待 >${timeoutMs}ms）`, {
          status: 499, kind: 'client_abort', retryable: false,
        }));
      }, timeoutMs);
    }
  });
}

/** reasoning_effort 档位排序：实测 xhigh 是各家普遍接受的最高档，非法值 max 不在其中 */
const EFFORT_RANK = { low: 1, medium: 2, high: 3, xhigh: 4 };

/**
 * 最高强度思考：能思考的模型补上目标档位的 reasoning_effort。
 * 目标档位 = channel.maxEffort（per-channel 覆盖，用于"同一档位各家支持度不同"）
 *          > routing.maxEffort > MAX_EFFORT(high)。
 * 客户端显式要求不低于目标档位时原样尊重，不降档也不重复注入。
 * @returns {{effort:string|null, injected:boolean}} effort 为实际使用的档位
 */
function resolveEffort(body, channel, model, routing) {
  const asked = typeof body?.reasoning_effort === 'string' && body.reasoning_effort.trim()
    ? body.reasoning_effort.trim().toLowerCase()
    : null;
  const force = routing.forceMaxEffort !== false;
  if (!force) return { effort: asked, injected: false };

  const target = channel.maxEffort || routing.maxEffort || MAX_EFFORT;
  if (asked && EFFORT_RANK[asked] >= (EFFORT_RANK[target] || 0)) {
    return { effort: asked, injected: false };
  }

  // 渠道显式声明支持思考 -> 强制；显式声明不支持 -> 尊重声明；未声明 -> 按模型名判断
  const capable = channel.supportsThinking === null || channel.supportsThinking === undefined
    ? modelSupportsThinking(model)
    : channel.supportsThinking === true;
  if (!capable) return { effort: asked, injected: false };
  return { effort: target, injected: true };
}

export async function handleChatCompletions({ manager, req, res, body, requestId, clientProtocol = 'openai', agentId = null, agentSource = 'none' }) {
  let model = body?.model;
  if (!model) {
    return sendJson(res, 400, openaiError('缺少 model 字段', 'invalid_request_error', 'missing_model', 400));
  }

  const stream = body.stream === true;
  const who = agentId ? shortLabel(agentId) : null;
  const tasklog = getTaskLog();
  const startedAt = Date.now();
  let recorded = false;

  // 任务日志记录器：无论从哪条路径返回，都保证恰好写一条任务记录
  const finish = (rec) => {
    if (recorded) return;
    recorded = true;
    tasklog.write({
      requestId,
      agent: who,
      agentSource,
      model: rec.model ?? model,
      stream,
      protocol: clientProtocol,
      elapsedMs: Date.now() - startedAt,
      ...rec,
    });
  };

  // 客户端传 reasoning_effort（low/medium/high）时按强度路由；能思考的模型统一拉到最高强度
  const askedEffort = typeof body.reasoning_effort === 'string' && body.reasoning_effort.trim()
    ? body.reasoning_effort.trim().toLowerCase()
    : null;
  // effortMap：同一供应商不同强度往往是不同模型名（如 deepseek-chat vs deepseek-reasoner），按强度改写模型名。
  //
  // 只在客户端"显式"要求了强度时才改写模型名：
  // 客户端点名 deepseek-chat（不带 effort）= 它明确要的就是非思考模型，网关不能因为
  // "能思考就拉到最高强度"这条策略，把它偷偷换成 deepseek-reasoner——那是改语义，不是加强度。
  // 强制最高强度只作用于"已经是思考模型"的请求（在下面按渠道/模型名逐次判定后注入参数）。
  const effortMap = manager.config?.effortMap?.[model];
  if (askedEffort && effortMap && effortMap[askedEffort]) {
    log.info(`强度映射 ${model} + ${askedEffort} -> ${effortMap[askedEffort]}`, requestId);
    body.model = effortMap[askedEffort];
    model = body.model;
  }
  // 上下文感知路由：估算请求 prompt token 数，传给候选过滤（渠道 contextWindow 装不下的被跳过）
  const promptTokens = estimatePromptTokens(body);
  const candidates = manager.candidatesFor(model, { effort: askedEffort, promptTokens, agentId, requestId });

  // 兜底：客户端传来的模型名池子里没有时（agent 常发自己的默认名如 "auto"），
  // 自动改调配置的 fallbackModel —— 让"随便什么模型名都能被服务"成立
  let fallbackFrom = null;
  const fallbackTarget = manager.config?.fallbackModel || manager.config?.unifiedModel;
  if (!candidates.length && fallbackTarget && fallbackTarget !== model) {
    const fbCandidates = manager.candidatesFor(fallbackTarget, { effort: askedEffort, promptTokens, agentId, requestId });
    if (fbCandidates.length) {
      fallbackFrom = model;
      log.info(`模型 "${model}" 无可用渠道，兜底到 "${fallbackTarget}"`, requestId);
      body.model = fallbackTarget;
      model = body.model;
      candidates.push(...fbCandidates);
    }
  }
  if (fallbackFrom) {
    try { res.setHeader?.('x-gateway-fallback-from', fallbackFrom); } catch { /* headers 已发送 */ }
  }

  // 本次请求是不是"只有这一家候选渠道"——叠 Key 用它决定整圈 Key 都失败之后的走向：
  //   唯一候选  → 交回渠道级也没别人可换，所以改为回 key#1 继续重扫（见 rotate429Upstream 的 allowLapRetry）
  //   还有别的  → 保持既有语义：立刻交回渠道级快速降级换家
  // 为什么必须区分：无条件重扫会把一个"整池 429"的渠道一直拖到首字节超时，
  // 反而错过本可以成功的兜底渠道（test-multikey-rotate429 Case 5 守的就是这条）。
  // 判据用"候选列表长度"，而不是"渠道池里同模型的渠道数"——候选已含两段分层与冷却过滤，
  // 正是"这次请求实际还能换谁"的答案。
  const onlyCandidate = candidates.length === 1;

  // 池子总入口：unifiedModel 请求时，每家用自己绑定的默认模型
  const isUnified = !!(manager.config?.unifiedModel && model === manager.config.unifiedModel);

  const errBody = (msg, type, code) =>
    clientProtocol === 'anthropic'
      ? { type: 'error', error: { type: type === 'upstream_unavailable' ? 'api_error' : 'invalid_request_error', message: msg } }
      : openaiError(msg, type, code);

  if (!candidates.length) {
    log.error(`没有可用渠道支持模型 ${model}`, requestId);
    finish({ ok: false, kind: 'model_not_found', error: `没有任何已启用渠道可提供模型 "${model}"`, tries: 0 });
    return sendJson(
      res,
      400,
      errBody(
        `没有任何已启用渠道可提供模型 "${model}"。请检查 config.json 的 channels.models 白名单或 alias 映射。`,
        'invalid_request_error',
        'model_not_found',
      ),
    );
  }

  // ---- 两层重试 ----
  // 第 1 层（渠道内）：同一渠道最多尝试 attemptsPerChannel 次（渠道可用 retries 覆盖），
  //   每次失败后等 retryPerAttemptMs（未配置回落 retryWaitMs）再试；全废才换下一家渠道。
  //   第二段（随机池）的渠道另有独立的、更小的预算 fallbackAttempts × fallbackRetryIntervalMs（2 × 3s）。
  // 第 2 层（跨渠道循环）：所有候选渠道都废了，等 retryWaitMs 重新轮询一遍；
  //   retryMaxWaitMs / maxTotalWaitMs 决定什么时候放弃并回一个明确错误给客户端。
  const routing = manager.routing;
  // 客户端原始 body 的快照：每次尝试都要基于它改写（否则上一次尝试塞进去的
  // reasoning_effort / 模型名会污染下一次尝试，导致路由到不一致的上游参数）
  const baseBody = { ...body };
  const defaultAttempts = Math.max(1, Number(routing.attemptsPerChannel) || DEFAULT_CHANNEL_RETRY.attemptsPerChannel);
  const perAttemptWaitMs = Math.max(
    0,
    Number(routing.retryPerAttemptMs ?? routing.retryWaitMs ?? DEFAULT_CHANNEL_RETRY.retryPerAttemptMs) || 0,
  );
  const fallbackAttempts = Math.max(1, Number(routing.fallbackAttempts) || 2);
  const fallbackWaitMs = Math.max(0, Number(routing.fallbackRetryIntervalMs ?? routing.retryWaitMs) || 3000);
  const retryLoop = !!routing.retryLoop;
  const retryWaitMs = Math.max(250, routing.retryWaitMs ?? 3000);
  const retryMaxWaitMs = routing.retryMaxWaitMs > 0 ? routing.retryMaxWaitMs : Infinity;
  // 总预算：这是"网关绝不无限挂着"的最后一道保险。
  // 只有在 retryLoop 且 retryMaxWaitMs 也没上限时才交给 maxTotalWaitMs 兜底（默认 10 分钟）。
  const maxTotalWaitMs = routing.maxTotalWaitMs > 0 ? routing.maxTotalWaitMs : Infinity;
  // F3（代码审查 2026-09-20）：总预算必须是**请求级绝对 deadline**，而不是只累计"重试之间的等待"。
  // 旧实现只累加 waitedMs（重试等待），并发排队、上游生成、读体、背压等待全都不计入，
  // 而且检查落在 `if (!retryLoop) break` 之后 —— 不开整池重试时这道保险等于不存在。
  // 现在从请求入口起算绝对时刻，一路贯穿排队 / 单次上游 / 读体 / 背压 / 重试等待。
  const requestStartedAt = Date.now();
  const deadlineAt = Number.isFinite(maxTotalWaitMs) ? requestStartedAt + maxTotalWaitMs : Infinity;
  /** 距总预算到期的剩余毫秒（无预算时返回 Infinity） */
  const remainingBudgetMs = () => (Number.isFinite(deadlineAt) ? Math.max(0, deadlineAt - Date.now()) : Infinity);
  let budgetExhausted = false;

  // 客户端断开后所有等待都要能立刻结束，否则会白白占着定时器和上游配额
  const clientGone = new AbortController();
  const onClientClose = () => clientGone.abort();
  res.on('close', onClientClose);

  let waitedMs = 0;
  let round = 0;
  let totalTries = 0;
  let attempts = [];
  // 上一轮的失败明细（本轮一次上游都没打到时用它给出可读的 503 说明）
  let lastAttempts = [];
  let lastErr = null;
  // 最近一次尝试的三段耗时（P2 §2.5）：判死/断连等提前收摊的分支也要能带出耗时分解
  let lastTiming = {};
  let sawNonRetryable = false;
  let triedFallback = false;
  // 本轮内已经"彻底失败"的渠道（失败的失败/余额不足/鉴权不过）：本轮不再回头撞
  const deadThisRound = new Set();
  // 整个请求内已被"判死"的渠道（余额不足 / 鉴权失败）：markFailure 只把它们冷却并排到候选末尾，
  // 并不会把它们从候选里移除，所以必须自己记住——否则跨渠道轮次（retryLoop）每轮都会再捶一遍。
  const deadChannels = new Set();

  // 某渠道在本轮"实际生效"的尝试预算与失败间隔：
  // 渠道级显式配置 > 路由级显式配置 > 分层默认（随机池 = fallbackAttempts × fallbackRetryIntervalMs）。
  // 运维在 config 里写死的数字是明确意图，不能被分层默认值悄悄覆盖。
  // 抽成函数是为了让"优先池已用尽"的降级日志打印的数字和循环里真正使用的数字永远是同一个。
  const planFor = (channel) => {
    const isFallback = channel.tier !== 'preferred' && routing.tiered !== false;
    const budgetExplicit = channel.retries != null || manager.routingExplicit?.has('attemptsPerChannel');
    const budget = isFallback && !budgetExplicit
      ? fallbackAttempts
      : Math.max(1, channel.retries ?? defaultAttempts);
    const waitExplicit = channel.retryPerAttemptMs != null || manager.routingExplicit?.has('retryPerAttemptMs');
    const waitMs = isFallback && !waitExplicit
      ? fallbackWaitMs
      : (channel.retryPerAttemptMs ?? perAttemptWaitMs);
    return { budget, waitMs };
  };

  try {
    while (true) {
      round += 1;
      // 同一个 requestId 贯穿所有轮次（retryLoop）：轮询游标对同一请求只前进一格，
      // 否则"同一请求多轮选路"会因为游标多跳而每轮换一家（P1 §1.4 / channels.mjs#nextTurn）
      const cands = round === 1
        ? candidates
        : manager.candidatesFor(model, { effort: askedEffort, promptTokens, agentId, requestId });
      // 上一轮的失败明细留一份：本轮若所有候选都已判死、一次上游都没打到，
      // 最终 503 的 detail 才不会变成空字符串。
      if (attempts.length) lastAttempts = attempts;
      attempts = [];
      lastErr = null;
      let stopLoop = false;
      sawNonRetryable = false;
      deadThisRound.clear();
      // 本轮真正打到上游的次数：全 0 说明剩下的候选都已判死，继续轮询只是白等到总预算耗尽
      let roundTries = 0;

      // 按降级链逐个渠道尝试；每个渠道用尽自己的重试预算（含首次）才换下一个。
      // 用 while + 下标遍历（而不是 for..of）：候选被追加时可以就地**继续同一轮**（见下方注入点）。
      let ci = 0;
      for (;;) {
        if (ci >= cands.length) {
          // 优先池在本轮已经用尽：立刻补上"还没上场的候选"（随机池 / 被 maxAttempts 截断的剩余渠道），
          // 不要让客户端白等一个 retryWaitMs。
          // 关键：这里只能续跑同一轮。以前是 `cands.push(...rest); round -= 1; continue;` 重启外层
          // while ——那会重新取候选、清空 attempts 与 deadThisRound，把本轮已经打满预算、
          // 甚至已经判死（余额/鉴权）的渠道再捶一遍（同一轮同一个渠道被重复计费、重复打上游）。
          if (triedFallback || routing.tiered === false) break;
          triedFallback = true;
          const tried = new Set(cands.map((c) => c.name));
          const rest = manager
            .candidatesFor(model, { effort: askedEffort, promptTokens, agentId, requestId })
            .filter((c) => !tried.has(c.name) && !deadChannels.has(c.name));
          if (!rest.length) break;
          const plan = planFor(rest[0]);
          log.info(
            `优先池本轮已用尽，立即降级到其余 ${rest.length} 家供应商（随机顺序，每家 ${plan.budget} 次 × ${Math.round(plan.waitMs / 1000)}s）`,
            requestId,
          );
          cands.push(...rest);
          continue;
        }
        const channel = cands[ci];
        ci += 1;
        if (deadThisRound.has(channel.name) || deadChannels.has(channel.name)) continue;
        channel.rr += 1;
        // 第二段（随机池）的渠道本次请求只给 fallbackAttempts 次（默认 2 次 × 3s），
        // 除非渠道/路由显式写了 retries / retryPerAttemptMs（见 planFor）。
        const { budget, waitMs: attemptWaitMs } = planFor(channel);
        const upstreamModel = isUnified ? channel.defaultModel() || model : channel.resolveModel(model);

        // 能思考的模型：统一拉到目标档位（渠道/模型不支持时自动退回客户端原值）
        const resolved = resolveEffort(baseBody, channel, upstreamModel, routing);
        let useEffort = resolved.effort;
        const injected = resolved.injected;
        // 上游拒绝档位（400 "ReasoningEffort invalid"）时同渠道降一档重试一次，而不是直接换家
        let effortDowngraded = false;
        // 每次尝试都在原始 body 的副本上改写：上家的改写结果绝不带到下一家
        const attemptBody = { ...baseBody };
        if (useEffort) attemptBody.reasoning_effort = useEffort;
        else delete attemptBody.reasoning_effort;

        let a = 0;
        while (a < budget) {
          a += 1;
          if (res.destroyed || res.writableEnded) { stopLoop = true; break; }
          // F3：总预算到点就不再开新的上游请求（也不切渠道）——单次尝试内部的超时
          // 由 attemptChannelInner 的 budgetAbort 负责，这里管的是"还没发出去的"。
          if (Date.now() >= deadlineAt) { budgetExhausted = true; stopLoop = true; break; }
          totalTries += 1;
          roundTries += 1;
          const started = Date.now();
          // 本次尝试的三段耗时（P2 §2.5）：由 attemptChannel / attemptChannelInner 就地填充。
          // 失败换家时随 attempts[] 带出，成功时随请求级记录带出。
          const timing = {};
          lastTiming = timing;
          log.info(
            `尝试 #${a}/${budget} -> ${channel.name} [${channel.tier}] (${upstreamModel})` +
              `${who ? ` [${who}]` : ''}${round > 1 ? ` 第${round}轮` : ''}${a > 1 ? '（重试）' : ''}` +
              `${channel.coolingDown ? ' [冷却中]' : ''}${isUnified ? ' [池子总入口]' : ''}` +
              `${injected ? ` [思考=${useEffort}]` : ''}`,
            requestId,
          );

          try {
            const result = await attemptChannel({
              channel, body: attemptBody, model: upstreamModel, stream, res, requestId, clientProtocol,
              limiter: manager.limiter, agentId, routing, timing, deadlineAt, onlyCandidate,
            });
            const latency = Date.now() - started;
            channel.markSuccess(latency);
            manager.rememberSuccess(model, channel.name);
            manager.rememberAffinity(agentId, channel.name);
            log.ok(
              `成功 via ${channel.name} (${upstreamModel}) ${latency}ms（第${a}次尝试${round > 1 ? `，第${round}轮` : ''}${who ? `，代理 ${who}` : ''}）`,
              requestId,
            );
            // Token 用量（§4.1）：只记"成功交付给客户端"的那次 attempt 的 usage；
            // 上游没回报 usage 时 usage 为 null —— 不记录、不估算、不造假 0。
            const usage = result?.usage ?? null;
            finish({
              ok: true,
              channel: channel.name,
              tier: channel.tier,
              upstreamModel,
              effort: useEffort,
              attempts,
              tries: totalTries,
              rounds: round,
              waitedMs,
              // usage 随任务记录进 tasklog（请求级 usage: { input_tokens, output_tokens }）
              ...(usage ? { usage: { input_tokens: usage.input, output_tokens: usage.output } } : {}),
              // 成功路径同样带出耗时分解（最常见的"第一家就成功"在面板上必须能看到）
              ...withTimings(timing),
            });
            if (usage) {
              getUsage().record({
                ts: new Date().toISOString(),
                model,
                channel: channel.name,
                input: usage.input,
                output: usage.output,
              });
            }
            return result;
          } catch (err) {
            const latency = Date.now() - started;
            lastErr = err;

            // 档位被上游拒绝（如 "field ReasoningEffort invalid, should be one of: low, medium, high, xhigh"）：
            // 同渠道降到 EFFORT_FALLBACK 重试一次——既不消耗渠道重试预算，也不计熔断失败，
            // 否则一个配错的 maxEffort 会把整条渠道链打穿。
            if (!effortDowngraded
              && useEffort && EFFORT_RANK[useEffort] > EFFORT_RANK[EFFORT_FALLBACK]
              && isEffortRejection(err.message)) {
              effortDowngraded = true;
              log.warn(`上游拒绝思考档位 ${useEffort}（${truncate(err.message, 100)}），同渠道降级为 ${EFFORT_FALLBACK} 重试`, requestId);
              useEffort = EFFORT_FALLBACK;
              attemptBody.reasoning_effort = EFFORT_FALLBACK;
              a -= 1; // 降档重试不计入渠道重试预算
              continue;
            }

            const fp = errorFingerprint(err.kind || 'error', err.message);
            const lastAttempt = a === budget || !shouldRetryInChannel(err, a, budget);
            if (lastAttempt) {
              attempts.push({
                channel: channel.name, tier: channel.tier, error: err.message,
                status: err.status ?? null, kind: err.kind ?? null, fingerprint: fp,
                tries: a, ms: latency,
                // 三段耗时（P2 §2.5）：只带实测到的段，缺的键不写
                ...pickTimingFields(timing),
              });
            }

            if (err.modelIssue) channel.markUnsupported(isUnified ? upstreamModel : model);
            // 熔断只统计"上游自己的错"：客户端取消（client_abort）与本地并发闸门排队超时
            // （overloaded）都不是渠道的问题。算进去的后果是——用户两次 Ctrl-C、或一次高并发
            // 排队超时，就把健康渠道熔断 30s（越忙越熔断的正反馈，503 风暴）。
            // rate_limit 在 Channel#markFailure 内部已经同样豁免，这里和它对齐。
            if (err.kind !== 'client_abort' && err.kind !== 'overloaded') {
              // inFlight：失败发生时该渠道的在途数（本次尝试的槽位已释放，剩下的都是并发兄弟请求），
              // 供 markFailure 做并发感知去抖，避免一批请求同时撞上同一个上游故障瞬间打穿熔断阈值
              channel.markFailure(err.kind || 'error', err.message, {
                inFlight: manager.limiter?.channelInFlight(channel.name) ?? 1,
              });
            }

            log.warn(`失败 via ${channel.name}（第${a}/${budget}次）[${err.kind || 'error'}]: ${truncate(err.message, 160)}`, requestId);

            // 客户端断开：直接收摊（客户端已经不听了，再重试没有意义）。
            // 必须排在"响应已开始"之前：流式中途断开时响应头早已发出，但它既不是渠道失败、
            // 也不能算渠道成功（否则被中断的流会被记成成功并钉成 lastGood 影响粘性路由）。
            if (err.kind === 'client_abort') {
              finish({ ok: false, clientAbort: true, kind: 'client_abort', error: err.message, attempts, tries: totalTries, rounds: round, waitedMs, ...withTimings(lastTiming) });
              stopLoop = true;
              break;
            }
            // 响应已开始写入客户端，无法再切换
            if (err.responseStarted) {
              log.error('响应已开始下发，无法切换到其它渠道', requestId);
              finish({
                ok: false, channel: channel.name, channelError: true, kind: err.kind,
                fingerprint: fp, error: err.message, attempts, tries: totalTries, rounds: round, waitedMs,
                ...withTimings(lastTiming),
              });
              return;
            }
            // 余额不足 / 鉴权失败：换家重试没有意义，这家直接判死——
            // 本轮不再撞，后续轮次（retryLoop）也不要再撞（冷却只把它排到候选末尾，不会移除）。
            if (err.kind === 'insufficient_balance' || err.kind === 'auth') {
              deadThisRound.add(channel.name);
              deadChannels.add(channel.name);
              break;
            }
            if (!shouldRetryInChannel(err, a, budget)) {
              // 参数错误：整池重来无意义；鉴权/模型不存在等渠道级问题不设标志（继续降级链）
              if (err.kind === 'bad_request') sawNonRetryable = true;
              break;
            }

            // 渠道内固定等待后重试（F3：等待时间不得超过总预算剩余）
            const waitClock = Date.now();
            const done = await waitBeforeRetry(Math.min(attemptWaitMs, remainingBudgetMs()), clientGone.signal);
            waitedMs += Date.now() - waitClock;
            if (!done || res.destroyed || res.writableEnded) { stopLoop = true; break; }
            if (Date.now() >= deadlineAt) { budgetExhausted = true; stopLoop = true; break; }
          }
        }
        if (stopLoop) break;
      }

      // 本轮结束：参数类不可重试错误直接失败；否则等固定间隔从最高优先级重新开始
      if (stopLoop) break;
      // 本轮一次上游都没打到：剩下的候选全是已判死（余额/鉴权）的渠道。
      // 继续轮询只会白等 retryWaitMs 直到总预算耗尽（日志还会一直刷 [冷却中]），直接收摊给明确错误。
      if (roundTries === 0) {
        log.warn('剩余候选渠道均已判死（余额不足 / 鉴权失败），不再轮询', requestId);
        break;
      }
      if (sawNonRetryable) break;
      if (!retryLoop) break;
      if (waitedMs >= retryMaxWaitMs) {
        log.warn(`重试等待已达上限 ${retryMaxWaitMs}ms，放弃`, requestId);
        break;
      }
      if (waitedMs >= maxTotalWaitMs) {
        log.warn(`累计等待已达总预算 ${maxTotalWaitMs}ms，返回失败而不是继续挂住客户端`, requestId);
        budgetExhausted = true;
        break;
      }
      if (Date.now() >= deadlineAt) {
        // F3：绝对 deadline 判定（不再依赖"只统计重试等待"的 waitedMs）
        log.warn(`请求已用满总预算 ${maxTotalWaitMs}ms（累计等待 ${waitedMs}ms），停止继续轮询`, requestId);
        budgetExhausted = true;
        break;
      }
      if (res.destroyed || res.writableEnded) break; // 客户端已断开
      log.info(
        `全部渠道预算用尽，${retryWaitMs}ms 后从最高优先级重新开始（第 ${round + 1} 轮，累计等待 ${waitedMs}ms${Number.isFinite(retryMaxWaitMs) ? ` / 上限 ${retryMaxWaitMs}ms` : ' / 无上限'}）`,
        requestId,
      );
      const waitClock = Date.now();
      // F3：整池轮询的等待同样受限（wake 到点即返回，不会越过 deadline）
      const done = await waitBeforeRetry(Math.min(retryWaitMs, remainingBudgetMs()), clientGone.signal);
      waitedMs += Date.now() - waitClock;
      if (!done || res.destroyed || res.writableEnded) break;
      if (Date.now() >= deadlineAt) { budgetExhausted = true; break; }
    }
  } finally {
    res.off('close', onClientClose);
  }

  if (res.destroyed || res.writableEnded) {
    log.debug('客户端已断开，停止重试', requestId);
    finish({ ok: false, clientAbort: true, kind: 'client_abort', error: '客户端已断开', attempts, tries: totalTries, rounds: round, waitedMs, ...withTimings(lastTiming) });
    return;
  }

  const retryNote = round > 1 ? `（已循环重试 ${round - 1} 轮，累计等待 ${Math.round(waitedMs / 1000)}s）` : '';
  // 本轮一个渠道都没打到（候选全判死）时 attempts 是空的：退回上一轮的明细，
  // 否则客户端拿到的 503 里看不到任何失败原因。
  const shownAttempts = attempts.length ? attempts : lastAttempts;
  const detail = shownAttempts.map((a) => `${a.channel}(${a.tries}次): ${a.error}`).join(' | ');

  // F3：总预算到点 —— 给出**明确**的失败原因，而不是混在"全部渠道均失败"里。
  // 语义上这是"网关按配置主动收手"，与"渠道全挂"是两回事，排查时一眼可分。
  if (budgetExhausted) {
    const msg = `请求总时长已超过 maxTotalWaitMs=${maxTotalWaitMs}ms（网关主动收手）。共尝试 ${totalTries} 次${retryNote}。${truncate(detail, 400)}`;
    log.error(`总预算用尽，返回失败：${msg}`, requestId);
    finish({
      ok: false, kind: 'total_deadline',
      fingerprint: errorFingerprint('total_deadline', `budget ${maxTotalWaitMs}ms`),
      error: msg, attempts: shownAttempts, tries: totalTries, rounds: round, waitedMs,
      ...withTimings(lastTiming),
    });
    return sendJson(res, 504, errBody(msg, 'upstream_unavailable', 'total_deadline'));
  }
  log.error(`全部候选渠道均失败${retryNote}，共尝试 ${totalTries} 次 -> ${truncate(detail, 400)}`, requestId);
  const status = lastErr?.status && lastErr.status >= 400 && lastErr.status < 500 && lastErr.kind === 'bad_request'
    ? lastErr.status
    : 503;
  finish({
    ok: false,
    kind: lastErr?.kind || 'all_channels_failed',
    fingerprint: errorFingerprint(lastErr?.kind, lastErr?.message || 'all channels failed'),
    error: `所有候选渠道均无法完成请求（共尝试 ${totalTries} 次${retryNote}）。${truncate(detail, 800)}`,
    attempts: shownAttempts, tries: totalTries, rounds: round, waitedMs,
    ...withTimings(lastTiming),
  });
  return sendJson(
    res,
    status,
    errBody(
      `所有候选渠道均无法完成请求（共尝试 ${totalTries} 次${retryNote}）。${truncate(detail, 800)}`,
      'upstream_unavailable',
      'all_channels_failed',
    ),
  );
}

/**
 * 叠加 key 渠道的**并行竞速**：把同一个请求同时发给渠道里所有 key，
 * 第一个返回 2xx 的 key 胜出，下游沿用它的响应；其余 key 的请求立刻中止
 * （不再消耗上游生成，也不再占连接）。
 *
 * 全部失败时：
 *   - 只要有一个 key 回了 HTTP 响应，就挑一个"最适合继续降级"的（优先非 401/402/403）
 *     交给下游按既有口径分类抛出 —— 避免单个 key 的鉴权/余额问题把整条渠道判死；
 *   - 全部是网络/中止错误时，原样抛出第一个错误，交给调用方既有的分类逻辑
 *     （客户端断开 / 首字节超时 / 网络错误）。
 *
 * 这里**不维护 key 级健康状态**：每个请求都重发给全部 key，谁先成功用谁。
 */
async function raceUpstream({ plans, payload, signal, channel, sendFetch, requestId }) {
  // 叠 Key 专属指标（TASK 11）：race 一次性把所有 Key 都发出去，故 attempts = key 数
  stackedKeyStats.stackedKeyRequests += 1;
  stackedKeyStats.stackedKeyAttempts += plans.length;
  const controllers = plans.map(() => new AbortController());
  // cancelBody：丢弃 loser 的响应体，让连接尽快回收（cancel() 在部分实现里返回 promise）
  const cancelBody = (up) => {
    try {
      const p = up?.body?.cancel?.();
      if (p && typeof p.catch === 'function') p.catch(() => {});
    } catch { /* 忽略 */ }
  };
  const settled = []; // { index, upstream } | { index, error }
  let remaining = plans.length;
  let winner = null;
  let wake = null;
  const outcome = new Promise((resolve) => { wake = resolve; });
  const maybeWake = () => { if (winner || remaining === 0) wake(); };

  plans.forEach((plan, index) => {
    const keySignal = AbortSignal.any([signal, controllers[index].signal]);
    let req;
    try {
      req = sendFetch(plan.url, {
        method: 'POST',
        headers: plan.headers,
        body: JSON.stringify(payload),
        signal: keySignal,
        ...(channel.proxy ? { proxy: channel.proxy } : {}),
      });
    } catch (err) {
      req = Promise.reject(err);
    }
    Promise.resolve(req).then(
      (upstream) => {
        // 已经有胜出者：后到的响应直接丢弃（中止时会走 reject 分支，这里是"恰好同时返回"）
        if (winner) { cancelBody(upstream); return; }
        if (upstream.ok) {
          winner = { index, upstream };
          maybeWake();
          return;
        }
        settled.push({ index, upstream });
        remaining -= 1;
        maybeWake();
      },
      (error) => {
        if (winner) return; // 胜出后 loser 因 abort 产生的拒绝：忽略
        settled.push({ index, error });
        remaining -= 1;
        maybeWake();
      },
    );
  });

  await outcome;

  if (winner) {
    controllers.forEach((c, i) => { if (i !== winner.index) c.abort(); });
    for (const s of settled) cancelBody(s.upstream);
    stackedKeyStats.stackedKey429s += settled.filter((s) => s.upstream?.status === 429).length;
    if (plans.length > 1) stackedKeyStats.stackedKeySuccessAfterFailover += 1;
    log.info(
      `叠加 key 竞速：${plans.length} 个 key 同时请求，key#${winner.index + 1} 最先成功，其余已取消`,
      requestId,
    );
    return { upstream: winner.upstream, index: winner.index };
  }

  // 全部失败：优先挑非鉴权/非余额的响应交给下游分类，避免一个坏 key 把整条渠道判死
  const responses = settled.filter((s) => s.upstream);
  if (responses.length) {
    const keySpecific = (s) => [401, 402, 403].includes(s.upstream.status);
    const pick = responses.find((s) => !keySpecific(s)) || responses[0];
    controllers.forEach((c, i) => { if (i !== pick.index) c.abort(); });
    for (const s of settled) { if (s !== pick) cancelBody(s.upstream); }
    stackedKeyStats.stackedKey429s += responses.filter((s) => s.upstream.status === 429).length;
    stackedKeyStats.stackedKeyAllFailed += 1;
    log.warn(
      `叠加 key 竞速：${plans.length} 个 key 全部失败，按 key#${pick.index + 1}（HTTP ${pick.upstream.status}）的错误继续降级`,
      requestId,
    );
    return { upstream: pick.upstream, index: pick.index };
  }
  stackedKeyStats.stackedKeyAllFailed += 1;
  throw settled[0].error;
}

// ============================================================================
// 叠 Key 内部调度（Stacked-Key Scheduling）—— 只作用于"一条已被选中的渠道内部"
// ============================================================================
// NOTE（两层边界，TASK 0）：
//   Layer 1 = Channel Routing：routing.strategy / 两段式 preferred+fallback / 渠道熔断 /
//             sticky / sessionAffinity / 多渠道轮询 —— 全部由 lib/channels.mjs 负责，本文件不改其语义。
//   Layer 2 = Stacked-Key Scheduling：本段代码。当路由层**已经选中某一条 Channel**、
//             且该 Channel 配置了多个 apiKeys 时，才在这条渠道内部决定"这次用哪个 Key"。
//   ★ 本调度器只在一条渠道内部轮转 API Key，**绝不参与渠道选择**，也不改变任何渠道级状态机。
//
// 叠 Key 专属可观测指标（TASK 11）：与渠道级成功率**完全隔离**——
//   一次 Agent 请求（K1 429 / K2 429 / K3 成功）在渠道级只记"成功 1 次"，
//   在 Key 级记 attempts=3 / 429=2 / successAfterFailover=1，绝不污染现有路由健康度。
const stackedKeyStats = {
  stackedKeyRequests: 0,            // 进入叠 Key 内部调度的 Agent 请求数
  stackedKeyAttempts: 0,            // 实际发出的 Key 请求总数
  stackedKey429s: 0,                // 收到的 429 次数
  stackedKeyFastFailovers: 0,       // 因 429/可切 Key 错误而"立即换 Key"的次数
  stackedKeyAllFailed: 0,           // 整池 Key 都失败、交回渠道级错误处理的次数
  stackedKeySuccessAfterFailover: 0, // 换过至少一次 Key 之后才成功的次数
  // 整圈 Key 全部失败后又回到 key#1 重扫的圈数（只在"本请求没有别的候选渠道可换"时才会发生，
  // 见 rotate429Upstream 的 allowLapRetry）。
  stackedKeyLapRetries: 0,
};

/** 叠 Key 专属指标快照（供 /api/metrics 透传，不改渠道级口径） */
export function getStackedKeyStats() {
  const s = stackedKeyStats;
  return {
    ...s,
    avgAttemptsPerRequest: s.stackedKeyRequests
      ? Number((s.stackedKeyAttempts / s.stackedKeyRequests).toFixed(3))
      : null,
  };
}

/**
 * 叠 Key 内部：单个 Key 的响应该怎么处理（TASK 7）。
 * 只在 Key 池内部生效，**绝不改渠道级错误分类**——渠道级分类仍由 classifyFailure 负责。
 *   'accept'                  = 2xx，就地采用
 *   'next-key'                = 该 Key 被限流 / 鉴权 / 余额 / 上游 5xx，立即试下一个 Key
 *   'return-to-channel-layer' = 与具体 Key 无关的错误（业务参数等），整池重试无意义，交回渠道级
 */
export function classifyStackedKeyResult(status) {
  if (status >= 200 && status < 300) return 'accept';
  if (status === 429) return 'next-key';                  // 核心 fast-fail 信号
  if (status === 401 || status === 402 || status === 403) return 'next-key'; // 一个坏 Key 不该把整条多 Key 渠道判死
  if (status >= 500) return 'next-key';                   // 上游 5xx：换 Key 可能命中健康 Key
  if (status === 408 || status === 409 || status === 522 || status === 524) return 'next-key';
  // 其余 4xx（400/404/413/422…）多半与具体 Key 无关，不白扫一整圈
  return 'return-to-channel-layer';
}

/** 构造一个"中止类"错误，让上层既有的 client_abort / 超时 / 总预算分类逻辑原样生效 */
function abortLikeError() {
  const e = new Error('aborted');
  e.name = 'AbortError';
  return e;
}

/** 丢弃响应体，让连接尽快回收（与 raceUpstream 内的同名逻辑一致） */
function cancelUpstreamBody(up) {
  try {
    const p = up?.body?.cancel?.();
    if (p && typeof p.catch === 'function') p.catch(() => {});
  } catch { /* 忽略 */ }
}

/**
 * 叠 Key rotate-429 执行器（TASK 3 / 4 / 5 / 6）：
 *   环形游标逐个消费 Key —— K1 → K2 → K3 → …；
 *   429（以及 401/402/403/5xx 等"可切 Key 错误"）**零额外等待**立即换下一个；
 *   正常慢响应**耐心等**（不因"慢"启动下一个 Key，只受既有首字节/空闲/总预算超时约束）；
 *   成功后结束本次扫描，游标已在"下一个 Key"（由 takeNextRequestKey 一次完成推进）。
 * 一次请求最多扫描当前 Key 池**一整圈**，绝不无限循环；整池失败才交回渠道级错误处理。
 */
async function rotate429Upstream({ plans, payload, signal, channel, sendFetch, requestId, takeKey, allowLapRetry = false }) {
  const keyCount = plans.length;
  stackedKeyStats.stackedKeyRequests += 1;
  let attempts = 0;
  let rateLimited = 0;
  let lastIndex = -1;
  let lastErr = null;
  let laps = 0;

  /** 收尾记账：把本次扫描的 Key 尝试如实计入叠 Key 指标（不改渠道级计数） */
  const settle = () => {
    stackedKeyStats.stackedKeyAttempts += attempts;
    stackedKeyStats.stackedKey429s += rateLimited;
  };

  // 圈循环：allowLapRetry=false 时**只跑一圈**，行为与改造前逐位一致。
  // allowLapRetry=true（本请求没有别的候选渠道可换）时，整圈 Key 都失败就回 key#1 再扫一圈，
  // 直到某个 Key 成功、遇到"不可切 Key 的错误"、或 signal 被中止。终止由三道既有闸门保证：
  // 客户端断开 / 首字节超时（firstByteAbort）/ 请求级总预算（budgetAbort），所以不是真的"无限循环"。
  for (;;) {
    laps += 1;
    if (laps > 1) {
      // 环形游标跑完整圈会停在这一圈的**起点**，必须显式重置才是真的"回到第一个 Key"
      channel.resetKeyCursor?.();
      stackedKeyStats.stackedKeyLapRetries += 1;
      log.warn(
        `叠 Key 轮转：整圈 ${keyCount} 个 key 都没成功，且本请求没有别的候选渠道，回到 key#1 重扫第 ${laps} 圈`,
        requestId,
      );
    }
    let lastUpstream = null;   // 本圈最后一个 HTTP 响应（整圈都是网络错误时为 null）
    let sawSwitchable = false; // 本圈是否见过"可切 Key"的上游失败（决定值不值得再扫一圈）

    for (let i = 0; i < keyCount; i += 1) {
      // 客户端断开 / 首字节超时 / 总预算到点：立即终止，不再切 Key（TASK 7）
      if (signal.aborted) { settle(); throw abortLikeError(); }

      const slot = takeKey();
      if (!slot) break;
      const plan = plans[slot.index];
      attempts += 1;
      lastIndex = slot.index;

      let upstream;
      try {
        upstream = await sendFetch(plan.url, {
          method: 'POST',
          headers: plan.headers,
          body: JSON.stringify(payload),
          signal,
          ...(channel.proxy ? { proxy: channel.proxy } : {}),
        });
      } catch (err) {
        if (signal.aborted) { settle(); throw err; } // 中止类错误：交给上层既有分类
        lastErr = err;                                // 纯网络错误：这个 Key 不可用，立即换下一个
        stackedKeyStats.stackedKeyFastFailovers += 1;
        continue;
      }

      // 成功：就地采用（慢响应也是"等"，这里不会因为慢而主动切 Key）
      if (upstream.ok) {
        settle();
        if (attempts > 1) stackedKeyStats.stackedKeySuccessAfterFailover += 1;
        log.info(
          `叠 Key 轮转：共尝试 ${attempts} 次，key#${slot.index + 1} 成功` +
            `${laps > 1 ? `（第 ${laps} 圈回到 key#1 后重扫成功）` : `/${keyCount} 个 key`}`,
          requestId,
        );
        return { upstream, index: slot.index, attempts };
      }

      if (upstream.status === 429) rateLimited += 1;
      const action = classifyStackedKeyResult(upstream.status);
      const switchable = action === 'next-key';
      const isLastOfLap = i === keyCount - 1;
      // 不可切 Key 的错误（如普通 400）：整池重扫也无意义，立刻交回渠道级；
      // 可切 Key 的错误但本圈已扫到最后一个 Key 且不允许重扫：同样交回渠道级。
      if (!switchable || (isLastOfLap && !allowLapRetry)) {
        settle();
        // 计数口径与改造前逐位一致：可切 Key 的错误、或已扫完整圈，都记一次"叠 Key 全失败"
        if (switchable || isLastOfLap) stackedKeyStats.stackedKeyAllFailed += 1;
        log.warn(
          `叠 Key 轮转：共尝试 ${attempts} 次 / ${keyCount} 个 key${laps > 1 ? `（第 ${laps} 圈）` : ''}，` +
            `最终 HTTP ${upstream.status}，交回渠道级处理`,
          requestId,
        );
        return { upstream, index: slot.index, attempts };
      }
      lastUpstream = upstream;
      sawSwitchable = true;
      cancelUpstreamBody(upstream);                 // 429 等：不等待、不读体，立刻取下一个 Key
      stackedKeyStats.stackedKeyFastFailovers += 1;
    }

    // 整圈跑完没有任何 Key 成功：只有"没有别的候选渠道"且本圈确实见过可切 Key 的失败时才再扫一圈
    // （全网络错误时不重扫：交给渠道级重试去按 retryPerAttemptMs 节流，避免对着挂掉的上游热循环）
    if (allowLapRetry && sawSwitchable && !signal.aborted) continue;

    settle();
    stackedKeyStats.stackedKeyAllFailed += 1;
    if (lastUpstream) {
      log.warn(
        `叠 Key 轮转：共尝试 ${attempts} 次 / ${keyCount} 个 key（${laps} 圈），最终 HTTP ${lastUpstream.status}，交回渠道级处理`,
        requestId,
      );
      return { upstream: lastUpstream, index: lastIndex, attempts };
    }
    // 一圈内没有任何可用的 HTTP 响应（全是网络错误）：交回最后的网络错误分类
    if (lastErr) throw lastErr;
    throw new UpstreamError(`渠道 ${channel.name} 未配置可用的 apiKey`, { status: 401, kind: 'auth', retryable: false });
  }
}

/**
 * 并发闸门包装：拿到"全局 + 单渠道 + 单代理"三级许可后才真正转发；
 * 无论成功、失败还是被中止，退出时都会释放许可。
 * 排队超时视为可重试的上游失败，交给路由层换渠道。
 */
async function attemptChannel(args) {
  const { channel, limiter, requestId, res, agentId, timing } = args;
  let release = null;
  // F8（代码审查 2026-09-20）：半开窗口的并发上限必须**在这里强制**，不能只靠排序偏好。
  // 旧实现在转发层没有任何准入检查：唯一渠道处于半开窗口时，多个并发请求会一起打过去试探
  // （报告复现 F8_half_open_limit：配置 halfOpenMaxInFlight=1，上游观察到的峰值并发=3）。
  // 排序只决定"先试谁"，限流必须落在"真正要发上游请求"这一刻。
  const halfSlot = channel.tryAcquireHalfOpen?.();
  if (halfSlot === false) {
    throw new UpstreamError(
      `渠道 ${channel.name} 处于半开试探窗口且名额已满（halfOpenMaxInFlight=${channel.halfOpenMaxInFlight}），换下一家`,
      { status: 503, kind: 'overloaded', retryable: false },
    );
  }
  try {
    if (limiter) {
      // queueMs（P2 §2.5）：进入限流排队 -> 拿到许可。
      // 排队失败（超时）也要记：那段等待是真实发生过的，正是排查过载要看的东西。
      const queuedAt = Date.now();
      try {
        // F3：排队等待不得超过总预算剩余（否则预算会被排队悄悄吃掉）
        const budgetLeft = Number.isFinite(args.deadlineAt) ? Math.max(0, args.deadlineAt - Date.now()) : Infinity;
        release = await limiter.acquire(channel.name, agentId || null, budgetLeft);
      } catch (err) {
        if (timing) timing.queueMs = Date.now() - queuedAt;
        if (isOverloadError(err)) {
          log.warn(
            `并发闸门排队超时，放弃该渠道${err.message.includes('agent') ? '（子代理配额）' : ''}: ${err.message}`,
            requestId,
          );
          throw new UpstreamError(err.message, { status: 503, kind: 'overloaded', retryable: true });
        }
        throw err;
      }
      if (timing) timing.queueMs = Date.now() - queuedAt;
      // 排队期间客户端可能已经断开：不必再浪费一次上游配额。
      // 这个检查必须落在下面这层 try 里：acquire 成功之后再抛错，若绕过 finally
      // 槽位就永远不会归还——每泄露一次就永久少一个并发许可，攒满 maxConcurrent
      // 之后所有请求都在排队里超时，网关彻底卡死（上游明明是好的）。
      if (res?.destroyed || res?.writableEnded) {
        throw new UpstreamError('客户端在排队期间断开连接', { status: 499, kind: 'client_abort', retryable: false });
      }
    }
    return await attemptChannelInner(args);
  } finally {
    release?.();
    if (halfSlot === true) channel.releaseHalfOpen?.();
  }
}

async function attemptChannelInner({ channel, body, model, stream, res, requestId, clientProtocol = 'openai', agentId = null, routing = null, timing = null, deadlineAt = Infinity, onlyCandidate = false }) {
  // 叠加 key 渠道：同一个 baseUrl 下为每个 key 生成一份请求计划（彼此只差鉴权头）。
  // buildRequest 只从 channel.apiKey 取鉴权，所以这里用 Object.create 派生一个"只覆盖 apiKey
  // 的视图"，复用适配器原样构造请求——不动任何适配器代码，也不影响单 key 渠道。
  const reqKeys = channel.requestKeys || [];
  if (!reqKeys.length) {
    throw new UpstreamError(`渠道 ${channel.name} 未配置可用的 apiKey`, { status: 401, kind: 'auth', retryable: false });
  }
  const plans = reqKeys.map((key, index) => {
    const view = reqKeys.length === 1 ? channel : Object.assign(Object.create(channel), { apiKey: key });
    const { url, headers, payload } = view.adapter.buildRequest({ channel: view, model, body, stream });
    return { key, index, url, headers, payload };
  });
  const { url, headers, payload: basePayload } = plans[0];
  // 叠 Key 内部调度分层（TASK 9）：单 key 走旧路径；多 key 再按 stackedKeyStrategy 分流，
  // 三种逻辑互不揉搓。strategy 只决定"这条渠道内部怎么用 key"，与渠道路由完全无关。
  const multiKey = plans.length > 1;
  const keyStrategy = multiKey ? (channel.stackedKeyStrategy || 'race') : null;
  const rotateMode = keyStrategy === 'rotate-429';
  // 胜出的 key 序号 / 策略 / 尝试数：多 key 渠道才回传响应头（单 key 时为 null，保持旧行为零变化）
  const keyInfo = multiKey ? { count: plans.length, index: null, strategy: keyStrategy, attempts: 0 } : null;
  // WorkBuddy 国际版风控脱敏（11128）：出站 body 定型处执行（幂等可重入，见 lib/workbuddy.mjs）。
  // 只在目标渠道声明需要时改写——其余渠道零开销原样透传。
  let payload = basePayload;
  if (channel.workbuddySanitize) {
    const { sanitizeWorkbuddyBody } = await import('./workbuddy.mjs');
    payload = sanitizeWorkbuddyBody(payload);
  }
  // 流式用量采集（§4.1）：OpenAI 协议渠道出站强制补 stream_options.include_usage=true。
  // 不补的话多数上游不会在流末回 usage 帧，流式请求就永远统计不到 token。
  // Anthropic 协议渠道不吃这个字段——它的 usage 本来就在 message_start / message_delta 里，
  // 由下面的原始字节旁路扫描直接读到（见本文件 createUsageScan）。
  if (stream && channel.protocol === 'openai') {
    payload = { ...payload, stream_options: { ...(payload?.stream_options || {}), include_usage: true } };
  }
  // 叠加 key 渠道：所有 key 发同一份 payload（只有鉴权头不同，由各自的 plan.headers 承载）
  if (plans.length > 1) for (const p of plans) p.payload = payload;

  // 正文泄漏守卫：只作用于请求带 tools 的调用（文本协议 agent 不发 tools，不受影响）。
  // 响应若把工具调用写成正文（HTTP 200 但 agent 无法解析），视为该渠道失败换下一个。
  const guardActive = channel.guardToolCallText !== false
    && Array.isArray(body?.tools) && body.tools.length > 0;

  // 思考过程守卫：把各家五花八门的思考字段归一到 reasoning_content，
  // 并把"混进正文的思维链"拆回 reasoning_content（正文只留正式回答）。
  // 与工具守卫不同，它对所有请求都生效——思考过程泄漏不限于带 tools 的调用。
  const reasoningGuardActive = channel.guardReasoning !== false
    && routing?.reasoningGuard !== false;
  // 流式方向是否做思考守卫（拆分 + 空响应检测）。routing.reasoningGuardStream=false 时
  // 完全跳过流式的思考守卫（换更低的 TTFT，代价是纯思考流的空响应检测只剩工具守卫兜底）。
  const reasoningStreamGuard = reasoningGuardActive && routing?.reasoningGuardStream !== false;
  // 流式"正文思维链拆分"是否生效。Anthropic 客户端方向显式不做（见下方 A3 说明）：
  //   拆分会把 content_block_delta 的 delta.type 改成 thinking_delta 并塞 delta.thinking，
  //   但配对的 content_block_start 仍声明 {type:'text'}、也没有 thinking 块结构，
  //   客户端（Claude Code / Anthropic SDK）会收到类型不匹配的非法帧。
  //   因此该方向只做 reasoning 字段归一（由 streamToAnthropic 产出合法 thinking 块），不拆正文。
  const reasoningSplitActive = reasoningStreamGuard && clientProtocol !== 'anthropic';

  const clientAbort = new AbortController();
  const headerAbort = new AbortController();
  const idleAbort = new AbortController();
  // 首字节看门狗（P2 §2.1）：从"请求发出"起算，直到收到响应体的**第一个数据块**为止。
  // 与 headerTimer（只管到响应头）和 idleAbort（只管响应头之后的静默）**并发**计时，
  // 所以最坏等待是 max(三者) 而不是它们的和——两段计时绝不叠加。
  const firstByteAbort = new AbortController();
  // F3：请求级总预算的绝对 deadline（来自路由层）。到点即中止本次上游请求，
  // 覆盖"一直有数据到来、既不触发 idle 也不触发首字节看门狗"的长流——
  // 旧实现里 headerTimer 拿到响应头就被清掉，此后没有任何计时器能约束总时长。
  const budgetAbort = new AbortController();
  const hasBudget = Number.isFinite(deadlineAt);
  const budgetRemainingMs = () => (hasBudget ? Math.max(0, deadlineAt - Date.now()) : Infinity);
  const budgetTimer = hasBudget ? setTimeout(() => budgetAbort.abort(), Math.max(0, deadlineAt - Date.now())) : null;
  // idleAbort 覆盖"响应头到手之后"的整段 body 读取（首字节 + 空闲），因此可安全并入
  const signal = AbortSignal.any([clientAbort.signal, headerAbort.signal, idleAbort.signal, firstByteAbort.signal, budgetAbort.signal]);

  const headerTimer = setTimeout(() => headerAbort.abort(), Math.min(channel.timeoutMs, budgetRemainingMs()));
  // 首字节超时：routing.firstByteTimeoutMs，默认取渠道 timeoutMs（180000）以保持既有行为
  //（默认值 > streamIdleTimeoutMs，所以不配置时先到点的仍是空闲看门狗，错误文案不变）。
  // 只由"第一个数据块到手"解除，因此它也覆盖"上游只回响应头、一个字节都不吐"——
  // 以前这种情况只能靠 streamIdleTimeoutMs 收场，非流式更是完全不受保护。
  const firstByteTimeoutMs = Math.max(1, Number(routing?.firstByteTimeoutMs) || Number(channel.timeoutMs) || 180000);
  const firstByteTimer = setTimeout(() => firstByteAbort.abort(), firstByteTimeoutMs);
  const clearFirstByte = () => clearTimeout(firstByteTimer);

  // 首字节 / 空闲看门狗：**拿到响应头后立刻武装**，之后每读到一个数据块就重置。
  // 之前它只在流式 for-await 内部、收到第一个数据块之后才武装，于是"上游只回响应头、
  // 一个字节都不吐"会把请求永久挂住（非流式路径更是从来没有计时器）。
  const idleTimeoutMs = Math.max(1, Number(channel.routing?.streamIdleTimeoutMs) || 120000);
  let idleTimer = null;
  const armIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => idleAbort.abort(), idleTimeoutMs);
  };
  const clearIdle = () => clearTimeout(idleTimer);
  // 两段计时不叠加：收到第一个数据块时立刻解除首字节看门狗，交给空闲看门狗接力。
  // F3：body 计时器清理时一并清掉总预算计时器（它只约束"本次上游请求"，随尝试结束而结束）。
  const clearBodyTimers = () => { clearIdle(); clearFirstByte(); clearTimeout(budgetTimer); };

  /**
   * F3：总预算到点的统一错误。retryable=false —— 预算用尽是"网关按配置收手"，
   * 不是渠道故障，继续切渠道/重试只会继续超预算（路由层也会立刻停止轮询）。
   */
  const totalDeadlineError = (responseStarted = false) => new UpstreamError(
    `请求总时长预算已用尽（maxTotalWaitMs=${maxTotalWaitMs}ms）`,
    { status: 504, kind: 'total_deadline', retryable: false, responseStarted },
  );

  // waitDrain 自身超时（P2 §2.2）：默认复用 streamIdleTimeoutMs；
  // routing.writeDrainTimeoutMs 只在需要把"客户端不读"与"上游静默"两个场景分开时覆盖它
  //（测试要单独验证自超时分支），不配置时行为与"复用 streamIdleTimeoutMs"完全一致。
  const drainTimeoutMs = Math.max(1, Number(routing?.writeDrainTimeoutMs) || idleTimeoutMs);

  // 逐块重置空闲计时器的上游 body 包装（非流式 / 错误体读取路径用）。
  // readAllText 是通用工具、不感知计时器，由这层包装把"上游静默"变成可中止的读；
  // 全程保留 clientAbort 监听，所以客户端断开也能立刻终止上游请求。
  const watchedBody = async function* () {
    for await (const chunk of bodyStream) {
      clearFirstByte(); // 首字节到手：首字节看门狗功成身退
      armIdle();
      yield chunk;
    }
  };

  // body 读取期间被中止：区分"客户端断开"与"上游静默/首字节超时"，统一抛可换家的 UpstreamError。
  // readAllText 会吞掉迭代错误，所以读完必须显式检查，绝不能把半截 body 当成功。
  // 判定顺序 = 具体到宽泛：client_abort > 首字节超时 > 静默超时。
  const throwIfBodyAborted = () => {
    if (clientAbort.signal.aborted) {
      throw new UpstreamError('客户端断开连接', { status: 499, kind: 'client_abort', retryable: false });
    }
    // F3：总预算到点优先归类（它比"首字节/静默超时"更能说明真实原因）
    if (budgetAbort.signal.aborted) throw totalDeadlineError();
    if (firstByteAbort.signal.aborted) {
      throw new UpstreamError(`上游首字节超时（>${firstByteTimeoutMs}ms 未收到响应数据）`, { status: 504, kind: 'timeout', retryable: true });
    }
    if (idleAbort.signal.aborted) {
      throw new UpstreamError(`上游响应体读取超时（>${idleTimeoutMs}ms 无数据）`, { status: 504, kind: 'timeout', retryable: true });
    }
  };

  // 写回受阻后恢复：先把"为什么被中止"定性掉，再决定抛哪一类错误。
  // 响应头此刻必定已发出（下面两处 waitDrain 都在 sendHead 之后），所以 responseStarted=true。
  const throwIfStreamAborted = () => {
    if (clientAbort.signal.aborted) {
      throw new UpstreamError('客户端在流式传输中断开', {
        status: 499, kind: 'client_abort', retryable: false, responseStarted: true,
      });
    }
    if (budgetAbort.signal.aborted) throw totalDeadlineError(true); // F3
    if (firstByteAbort.signal.aborted) {
      throw new UpstreamError(`上游首字节超时（>${firstByteTimeoutMs}ms 未收到响应数据）`, {
        status: 504, kind: 'timeout', retryable: true, responseStarted: true,
      });
    }
    if (idleAbort.signal.aborted) {
      throw new UpstreamError(`上游流式静默超时（>${idleTimeoutMs}ms 无数据）`, {
        status: 504, kind: 'timeout', retryable: true, responseStarted: true,
      });
    }
  };

  // 背压等待：客户端既不读也不断开时靠自身超时收场并归还许可（P2 §2.2）。
  const waitDrainForWrite = async () => {
    try {
      await waitDrain(res, { signal: clientAbort.signal, timeoutMs: drainTimeoutMs });
    } catch (err) {
      log.warn(err.message, requestId);
      throw err;
    }
    throwIfStreamAborted();
  };

  const onClose = () => clientAbort.abort();
  res.on('close', onClose);

  let upstream;
  // 上游响应体（看门狗可达的 Node Readable）：拿到响应头后立刻包好，流式与非流式共用
  let bodyStream = null;
  // 三段耗时（P2 §2.5）：ttfbMs 的起点 = 真正发出上游请求的那一刻
  const upstreamSentAt = Date.now();
  try {
    // 渠道配置了出站代理（WorkBuddy 国际版默认走本机代理）时经 CONNECT 隧道转发
    const sendFetch = channel.proxy
      ? (await import('./outbound-proxy.mjs')).gwFetch
      : fetch;
    if (plans.length === 1) {
      upstream = await sendFetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal,
        ...(channel.proxy ? { proxy: channel.proxy } : {}),
      });
    } else if (rotateMode) {
      // 叠加 key（rotate-429）：环形游标逐个消费 key；429/可切 Key 错误零等待换下一个（TASK 3/4）
      const rolled = await rotate429Upstream({
        plans, payload, signal, channel, sendFetch, requestId,
        takeKey: () => channel.takeNextRequestKey(),
        // 只有"本请求没有别的候选渠道"时才允许整圈失败后回 key#1 重扫（详见函数注释）
        allowLapRetry: onlyCandidate,
      });
      upstream = rolled.upstream;
      keyInfo.index = rolled.index;
      keyInfo.attempts = rolled.attempts;
    } else {
      // 叠加 key（race）：并行竞速，第一个成功的 key 胜出（其余请求立刻取消）
      const raced = await raceUpstream({ plans, payload, signal, channel, sendFetch, requestId });
      upstream = raced.upstream;
      keyInfo.index = raced.index;
      keyInfo.attempts = plans.length;
    }
  } catch (err) {
    clearTimeout(headerTimer);
    clearFirstByte();
    res.off('close', onClose);
    const aborted = err.name === 'AbortError' || err.name === 'TimeoutError';
    if (aborted && clientAbort.signal.aborted) {
      throw new UpstreamError('客户端断开连接', { status: 499, kind: 'client_abort', retryable: false });
    }
    // F3：总预算到点（可能发生在"还没等到响应头"的这段时间里）
    if (aborted && budgetAbort.signal.aborted) throw totalDeadlineError();
    // 响应头都没等到：连 TTFB 都没有，所以只报超时，不写 ttfbMs（不造假）
    if (aborted && firstByteAbort.signal.aborted && !headerAbort.signal.aborted) {
      throw new UpstreamError(`上游首字节超时（>${firstByteTimeoutMs}ms 未收到响应数据）`, { status: 504, kind: 'timeout', retryable: true });
    }
    throw new UpstreamError(
      aborted ? `上游响应超时（>${channel.timeoutMs}ms）` : `网络错误: ${err.message} (${err.cause?.code || err.code || 'unknown'})`,
      { status: 504, kind: aborted ? 'timeout' : 'network' },
    );
  } finally {
    clearTimeout(headerTimer);
  }

  // 响应头已经到手：立刻武装"等首字节"的空闲看门狗（流式与非流式共用同一套）。
  // headerTimer 到这里已经被清掉，若不再武装，上游只要回个响应头就能把请求永久挂住。
  // 注意这里**不**重新计时首字节看门狗：它从请求发出起就一直开着（两段计时不叠加），
  // 直到第一个数据块到手才由 clearFirstByte() 解除。
  armIdle();
  // 上游响应体统一包成"看门狗可达"的读流（P2 §2.3：出站代理路径的 abort 传不到 body）
  bodyStream = watchdogBody(upstream.body, signal);
  // Token 用量旁路扫描（§4.1）：装在**上游原始字节流**上，非流式与流式共用同一条采集路径。
  // 字节透明（原样 yield），不影响协议转换 / 透传 / 看门狗计时。
  const usageScan = createUsageScan();
  bodyStream = tapUsage(bodyStream, usageScan);
  // TTFB（P2 §2.5）：发出请求 -> 收到响应头
  const ttfbAt = Date.now();
  if (timing) timing.ttfbMs = ttfbAt - upstreamSentAt;

  if (!upstream.ok) {
    let text = '';
    try {
      // 错误体读取同样受客户端断开 + 空闲超时约束（res.off 必须在读完之后）
      text = await readAllText(watchedBody());
      // bodyMs（P2 §2.5）：响应头 -> 读完（含中止那一刻，是真实经过的时间）
      if (timing) timing.bodyMs = Date.now() - ttfbAt;
      throwIfBodyAborted();
    } finally {
      clearBodyTimers();
      res.off('close', onClose);
    }
    const info = classifyFailure(upstream.status, text);
    const parsed = safeJsonParse(text, null);
    const msg = parsed?.error?.message || parsed?.message || text || `HTTP ${upstream.status}`;
    throw new UpstreamError(`HTTP ${upstream.status} ${truncate(msg, 200)}`, {
      status: upstream.status,
      kind: info.kind,
      retryable: info.retryable,
      modelIssue: info.modelIssue,
      detail: text,
    });
  }

  // ---- 成功：非流式 ----
  if (!stream) {
    // 客户端断开监听必须保留到 body 读完：以前它在读 body 之前就被摘掉，
    // 于是"上游只给响应头不吐数据"时既没有计时器、也收不到断开信号，请求永久挂住。
    try {
      // 上游只认流式（forceStream）时出站已强制 stream=true，这里把 SSE 聚合回标准 JSON
      let json;
      if (channel.forceStream) {
        const { aggregateStreamToCompletion } = await import('./workbuddy.mjs');
        json = await aggregateStreamToCompletion(parseSSE(Readable.from(watchedBody())));
        if (!json) throw new UpstreamError('上游流式响应为空', { status: 502, kind: 'empty_response' });
      } else {
        const text = await readAllText(watchedBody(), 16 * 1024 * 1024);
        throwIfBodyAborted();
        json = safeJsonParse(text, null);
        if (!json) throw new UpstreamError('上游返回非 JSON 内容', { status: 502, kind: 'bad_response' });
      }
      // bodyMs（P2 §2.5）：响应头 -> 响应体读完
      if (timing) timing.bodyMs = Date.now() - ttfbAt;
      const converted = convertResponse(json, channel.protocol, clientProtocol, model);
      if (guardActive) {
        const leak = findToolCallTextLeak(converted);
        // 响应还没发出，抛错后路由层自动换下一个渠道
        if (leak) throw toolCallLeakError(channel, leak);
      }
      if (reasoningGuardActive) {
        // 思考过程混进正文：把思维链拆回 reasoning_content，正文只留正式回答。
        // 这是"就地修复"而不是失败换家——上游确实答了，只是把过程也写进了正文。
        const fix = applyReasoningGuard(converted, { splitInline: true });
        if (fix.changed) {
          log.info(
            `思考字段归一/拆分：${fix.notes.join('、')}${fix.split ? `（正文拆出思维链，命中"${fix.marker}"）` : ''}`,
            requestId,
          );
        }
      }
      // 空调用检查：对"所有"响应生效（不限于带 tools 的请求）。
      // HTTP 200 但正文/工具调用/思考全空 = agent 收不到任何东西，按渠道失败换下一家。
      // 阈值 routing.emptyMinChars（默认 1）：只拦"有效字符数不足"的伪空响应
      // （纯空白 / 纯标点 / 只有 markdown 围栏壳），保守起见默认不误伤正常短回答。
      const empty = findUnusableResponse(converted, { minChars: routing?.emptyMinChars });
      if (empty) throw unusableResponseError(channel, empty);
      setGatewayHeaders(res, channel, model, agentId, keyInfo);
      sendJson(res, 200, converted);
      // 成功交付（非流式）：把上游真实回报的 usage 带回路由层记账（§4.1）
      return { usage: usageScan.result() };
    } catch (err) {
      // 迭代过程中直接抛出的中止错误（AbortError）在这里归一成可换家的 UpstreamError
      if (!(err instanceof UpstreamError)) {
        throwIfBodyAborted();
      }
      throw err;
    } finally {
      clearBodyTimers();
      res.off('close', onClose);
    }
  }

  // ---- 成功：流式 ----
  // 守卫开启时先缓冲流头部做正文泄漏检测，判定干净后才发响应头开始下发——
  // 响应头一旦发出就无法再整体换渠道，检测必须前置。
  const HEAD_BYTE_LIMIT = 16 * 1024; // 缓冲上限：正文迟迟不出现（如长思考）时最多缓冲 16KB
  const CONTENT_MIN_CHARS = 64;      // 正文累计这么多字符仍无标记即放行（标记最多被拆到相邻几个 chunk）

  let headersSent = false;
  const sendHead = () => {
    if (headersSent) return;
    setGatewayHeaders(res, channel, model, agentId, keyInfo);
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    headersSent = true;
  };

  let bytes = 0;
  const needConvert = channel.protocol !== clientProtocol;
  const source = !needConvert
    ? bodyStream
    : channel.protocol === 'anthropic'
      ? anthropicAdapter.streamToOpenAI(parseSSE(bodyStream), model, {
          onMidStreamError: (e) => log.warn(`上游流内错误: ${e.message}`, requestId),
        })
      : openaiAdapter.streamToAnthropic(parseSSE(bodyStream), model);

  // 头部缓冲：任一守卫开启时都要先攒着，整体解码才能正确处理跨 chunk 的多字节字符。
  // 思考守卫同样需要完整正文才能判断"思维链是否混进正文"，所以也要求缓冲。
  let head = (guardActive || reasoningStreamGuard) ? { items: [], byteLen: 0 } : null;
  // 思考拆分需要看完整段正文：只有窗口里已经出现收尾信号时才尝试
  let reasoningSawFinish = false;
  let reasoningSawStop = false;
  const headText = () => (Buffer.isBuffer(head.items[0]) ? Buffer.concat(head.items).toString('utf8') : head.items.join(''));
  const flushHead = async () => {
    for (const item of head.items) {
      // 客户端已断开时不能再写：写不进内核缓冲（write 返回 false）会让 waitDrain 白等
      if (res.destroyed || res.writableEnded) break;
      // 背压：客户端消费不过来时暂停读取上游，避免内存堆积；
      // 客户端既不读也不断开时由 waitDrain 自身超时收场（P2 §2.2）
      if (res.write(item) === false) await waitDrainForWrite();
    }
    head = null;
  };
  const judgeHead = async () => {
    const raw = headText();
    const content = extractStreamContentText(raw);
    const marker = hitStreamLeak(content);
    if (marker) throw toolCallLeakError(channel, { marker, text: content });
    // 零帧 / 非 SSE / 收尾后仍然空的响应都算"不可用"——agent 拿到它什么也做不了（见 findUnusableStreamHead）
    const unusable = findUnusableStreamHead(raw, { minChars: routing?.emptyMinChars });
    if (unusable) throw unusableResponseError(channel, unusable);
    // 思考过程混进正文：在响应头发出前把思维链整体挪到 reasoning_content。
    // 只有窗口内拿到"完整正文"时才可能安全拆分——因此要求已经看到收尾信号；
    // 否则原样放行（宁可让上游的过程文字过去，也不能切坏正式回答）。
    // Anthropic 客户端方向不拆（reasoningSplitActive=false），避免产出非法帧（见上方注释）。
    if (reasoningSplitActive && (reasoningSawFinish || reasoningSawStop)) {
      const fixed = rewriteStreamHeadReasoning(raw);
      if (fixed) {
        log.info('思考过程混进正文：已拆分为 reasoning_content，正文只保留正式回答', requestId);
        head.items = [Buffer.from(fixed, 'utf8')];
      }
    }
    sendHead();
    await flushHead();
  };

  try {
    if (!head) sendHead(); // 守卫未开启：立即放行响应头，行为与无守卫时一致
    // F1：逐块追踪终止信号。HTTP 响应体读完只说明"传输结束"，不等于"模型说完了"——
    // 没有终止信号的 EOF 以前会被当成功返回（markSuccess + ok:true + 更新粘性路由）。
    const terminal = createTerminalTracer();
    for await (const item of source) {
      clearFirstByte(); // 第一个数据块到手：首字节看门狗解除，改由空闲看门狗计时
      armIdle();
      if (res.destroyed || res.writableEnded) break;
      terminal.push(item);
      bytes += Buffer.isBuffer(item) ? item.length : Buffer.byteLength(item);
      if (head) {
        head.items.push(item);
        head.byteLen += Buffer.isBuffer(item) ? item.length : Buffer.byteLength(item);
        const headRaw = headText();
        const content = extractStreamContentText(headRaw);
        const marker = hitStreamLeak(content);
        if (marker) throw toolCallLeakError(channel, { marker, text: content });
        if (/"(?:finish_reason)"\s*:\s*"(?!null)/.test(headRaw)) reasoningSawFinish = true;
        if (/"type"\s*:\s*"(?:message_stop|content_block_stop)"/.test(headRaw)) reasoningSawStop = true;
        // A4（TTFT）：上游已经用 reasoning_content / thinking 明确承载思考时，
        // 正文里不可能有"混入的思维链"可拆，也不会有空响应风险——
        // 继续把头部攒到 16KB 或收尾只会白白推迟首字下发（长思考模型尤其明显）。
        // 所以看到"已分离的思考"就立即判定放行，不再等正文阈值。
        const sawSeparatedReasoning = /"reasoning_content"\s*:\s*"[^"]/.test(headRaw)
          || /"type"\s*:\s*"thinking/.test(headRaw);
        if (
          content.length >= CONTENT_MIN_CHARS
          || head.byteLen >= HEAD_BYTE_LIMIT
          || reasoningSawFinish
          || reasoningSawStop
          || sawSeparatedReasoning
        ) {
          await judgeHead();
          continue;
        }
        continue;
      }
      // 客户端已断开时不能再写，否则 waitDrain 会等一个永远不来的 drain
      if (res.destroyed || res.writableEnded) break;
      // 背压：客户端消费不过来时暂停读取上游，避免内存堆积；
      // 客户端既不读也不断开时由 waitDrain 自身超时收场（P2 §2.2）
      if (res.write(item) === false) await waitDrainForWrite();
    }
    // 客户端中途断开（可能发生在写回或背压等待期间）：既不能当渠道成功
    // （会污染成功率、把被中断的流钉成 lastGood 影响粘性路由），也不能当渠道失败
    // ——抛 client_abort 交给路由层按"客户端断开"记账。
    if (clientAbort.signal.aborted || res.destroyed) {
      throw new UpstreamError('客户端在流式传输中断开', {
        status: 499, kind: 'client_abort', retryable: false, responseStarted: headersSent,
      });
    }
    if (head) await judgeHead(); // 短响应在窗口内就结束：整体判定一次再放行
    // F1：流读完了，但上游从没给出终止信号 -> 截断。不能当成功返回：
    // 既不能 markSuccess/更新粘性路由，也不能把半截回答当完整结果交给 agent。
    if (!terminal.saw) {
      const msg = '上游流在没有终止信号的情况下结束（HTTP body 已结束但无 finish_reason / stop_reason / [DONE]）';
      if (!headersSent) {
        throw new UpstreamError(`${msg}（响应头未发出，可换家）`, {
          status: 502, kind: 'stream_truncated', retryable: false, responseStarted: false,
        });
      }
      await emitStreamError(res, clientProtocol, '上游流被截断，回答不完整');
      if (!res.writableEnded) res.end();
      throw new UpstreamError(msg, {
        status: 502, kind: 'stream_truncated', retryable: false, responseStarted: true,
      });
    }
    // 兼容上游：给了有效终止信号（finish_reason / stop_reason）却没发**终止帧**
    // （[DONE] / message_stop）。上游行为不统一，但客户端不该为此一直等——
    // 由转发层补一个标准终止帧，保持"客户端总能正常收尾"。
    if (terminal.saw && !terminal.sawEndFrame) {
      const endFrame = clientProtocol === 'anthropic'
        ? 'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        : 'data: [DONE]\n\n';
      bytes += Buffer.byteLength(endFrame);
      if (res.write(endFrame) === false) await waitDrainForWrite();
    }
    log.debug(`流式下发 ${bytes} 字节`, requestId);
    // bodyMs（P2 §2.5）：响应头 -> 流读完
    if (timing) timing.bodyMs = Date.now() - ttfbAt;
    if (!res.writableEnded) res.end();
    // 成功交付（流式）：流末 usage 已随旁路扫描汇聚，带回路由层记账（§4.1）
    return { usage: usageScan.result() };
  } catch (err) {
    // 头部尚未发出 / 客户端已断开：原样交给路由层（可整体让给下一个渠道，或按 client_abort 记账）
    if (err?.kind === 'tool_call_text' || err?.kind === 'empty_response' || err?.kind === 'client_abort' || err?.kind === 'stream_truncated') throw err;
    if (clientAbort.signal.aborted) {
      log.debug('客户端在流式传输中断开', requestId);
      throw new UpstreamError('客户端在流式传输中断开', {
        status: 499, kind: 'client_abort', retryable: false, responseStarted: headersSent,
      });
    }
    // F1：适配器标记的"上游流内报错 / 截断"（anthropic→openai、openai→anthropic 两个方向）。
    // 未提交响应 -> 整体换家；已提交 -> 按下游协议报流内错误 + 记失败，绝不拼接别家答案。
    if (err?.code === 'STREAM_TRUNCATED') {
      clearBodyTimers();
      if (!headersSent) {
        throw new UpstreamError(`上游流未正常结束（响应头未发出，可换家）: ${err.message}`, {
          status: 502, kind: 'stream_truncated', retryable: false, responseStarted: false,
        });
      }
      await emitStreamError(res, clientProtocol, err.message);
      if (!res.writableEnded) res.end();
      throw new UpstreamError(`上游流未正常结束: ${err.message}`, {
        status: 502, kind: 'stream_truncated', retryable: false, responseStarted: true,
      });
    }
    clearBodyTimers();
    // F3：总预算到点 —— 排在首字节/静默超时之前（真正原因更可能是"整条请求超时"，
    // 而不是某个看门狗）。已提交响应时补一个流内错误帧，让客户端知道回答被截断。
    if (budgetAbort.signal.aborted) {
      if (headersSent) {
        await emitStreamError(res, clientProtocol, `请求总时长预算已用尽（${maxTotalWaitMs}ms），回答可能不完整`);
        if (!res.writableEnded) res.end();
      }
      throw totalDeadlineError(headersSent);
    }
    // 首字节 / 空闲超时：走和断流相同的失败换家逻辑，绝不静默当成功。
    // 首字节超时排在前面：它比"静默超时"更具体（正文一个字节都没开始）。
    if (firstByteAbort.signal.aborted) {
      const fbMsg = `上游首字节超时（>${firstByteTimeoutMs}ms 未收到响应数据）`;
      if (!headersSent) {
        throw new UpstreamError(`${fbMsg}（响应头未发出，可换家）`, {
          status: 504, kind: 'timeout', retryable: true, responseStarted: false,
        });
      }
      // 响应头已发出、正文一个字节没来：这段等待是真实的 body 段耗时（可能只是部分）
      if (timing && timing.bodyMs === undefined) timing.bodyMs = Date.now() - ttfbAt;
      if (!res.writableEnded) res.end();
      throw new UpstreamError(fbMsg, { status: 504, kind: 'timeout', retryable: true, responseStarted: true });
    }
    if (idleAbort.signal.aborted) {
      const msg = `上游流式静默超时（>${idleTimeoutMs}ms 无数据）`;
      if (!headersSent) {
        throw new UpstreamError(`${msg}（响应头未发出，可换家）`, {
          status: 504, kind: 'timeout', retryable: true, responseStarted: false,
        });
      }
      if (!res.writableEnded) res.end();
      throw new UpstreamError(msg, { status: 504, kind: 'timeout', retryable: true, responseStarted: true });
    }
    // D2：响应头还没发出（守卫仍在缓冲头部，或上游只发了一两帧就静默断流）时，
    // 客户端一个字节都没拿到，可以整体让给下一个渠道换家，而不是把半截流丢给它。
    // 注意：这里绝不能 res.end()——一旦终结响应，路由层就无法再切换渠道。
    if (!headersSent) {
      throw new UpstreamError(`流式传输中断（响应头未发出，可换家）: ${err.message}`, {
        status: 502, kind: 'stream_break', responseStarted: false,
      });
    }
    // 流中途断开：已经下发过数据，无法再切渠道，标记失败让后续请求切换渠道
    if (!res.writableEnded) res.end();
    throw new UpstreamError(`流式传输中断: ${err.message}`, { status: 502, kind: 'stream_break', responseStarted: true });
  } finally {
    clearBodyTimers();
    res.off('close', onClose);
  }
}

// 上游响应 -> 客户端期望的协议格式（同协议时 openai 侧仅做字段兜底）
function convertResponse(json, upProtocol, clientProtocol, model) {
  if (upProtocol === clientProtocol) {
    return upProtocol === 'anthropic' ? json : openaiAdapter.toOpenAIResponse(json, model);
  }
  if (upProtocol === 'anthropic') return anthropicAdapter.toOpenAIResponse(json, model);
  return openaiAdapter.toAnthropicResponse(json, model);
}

function toolCallLeakError(channel, leak) {
  return new UpstreamError(
    `渠道把工具调用写成了正文（疑似上游未应用函数调用，命中 "${leak.marker}"）: ${truncate(leak.text, 120)}`,
    { status: 502, kind: 'tool_call_text', retryable: false },
  );
}

/**
 * 流式头部改写：把混在正文 delta 里的思维链整体挪到 reasoning_content delta。
 *
 * 只在窗口内已经看收尾信号时调用（此时窗口里的正文就是完整正文），
 * 因此可以安全地"先整体拆分、再重放"：
 *   - 拆不动（没有明确的最终答案分界）-> 返回 null，调用方原样下发
 *   - 拆得动 -> 把窗口内所有 content 增量合并成新序列：
 *       思维链部分 -> 若干 reasoning_content 增量
 *       正式回答   -> 若干 content 增量
 *     并保留非正文帧（usage / finish_reason / role 等）的顺序与内容。
 *
 * 兼容两种客户端协议：OpenAI（choices[].delta.content）与 Anthropic
 * （content_block_delta.text_delta —— 该路径下正文通常在 text 字段里）。
 */
function rewriteStreamHeadReasoning(raw) {
  const full = extractStreamContentText(raw);
  const { reasoning, content, split } = splitInlineReasoning(full);
  if (!split || !content) return null;

  const lines = String(raw).split('\n');
  const out = [];
  let injectedReasoning = false;
  let wroteContent = false;

  for (const line of lines) {
    if (!line.startsWith('data: ')) { out.push(line); continue; }
    const payload = line.slice(6);
    if (payload === '[DONE]') { out.push(line); continue; }
    let obj; try { obj = JSON.parse(payload); } catch { out.push(line); continue; }

    // OpenAI 形状
    const choice = obj?.choices?.[0];
    if (choice?.delta && typeof choice.delta.content === 'string') {
      const hadText = choice.delta.content.length > 0;
      delete choice.delta.content;
      // 思维链只作为一整段注入一次，避免被切成碎片后无法阅读
      if (!injectedReasoning && reasoning) {
        choice.delta.reasoning_content = reasoning;
        injectedReasoning = true;
      }
      if (hadText && !wroteContent) {
        choice.delta.content = content;
        wroteContent = true;
      }
      out.push(`data: ${JSON.stringify(obj)}`);
      continue;
    }

    // Anthropic 流（我方做协议转换时正文在 text_delta 里）
    const delta = obj?.delta;
    if (obj?.type === 'content_block_delta' && delta?.type === 'text_delta' && typeof delta.text === 'string') {
      const hadText = delta.text.length > 0;
      delete delta.text;
      if (!injectedReasoning && reasoning) {
        delta.type = 'thinking_delta';
        delta.thinking = reasoning;
        injectedReasoning = true;
      } else if (hadText && !wroteContent) {
        delta.text = content;
        wroteContent = true;
      } else {
        delta.type = 'text_delta';
        delta.text = '';
      }
      out.push(`data: ${JSON.stringify(obj)}`);
      continue;
    }

    out.push(line);
  }

  // 思维链还没机会注入（窗口里没有任何 content 帧）-> 不冒险改写
  if (reasoning && !injectedReasoning) return null;
  // 正文没落到任何一帧 -> 说明结构超出预期，放弃改写
  if (!wroteContent) return null;
  return out.join('\n');
}

/**
 * 有效字符数：去掉空白、引号、标点、markdown 装饰符号之后剩下的字符数。
 * 用于识别"伪空"响应——HTTP 200 但正文只有空白 / 标点 / 单个 "." / 代码围栏壳（```），
 * 对 agent 而言与完全空响应一样不可用，但现在会被当成功返回。
 */
const PSEUDO_EMPTY_STRIP_RE = /[\s`'"“”‘’。，、；：！？…·.,;:!?*_~#>+\-=|]/g;
function effectiveChars(text) {
  if (typeof text !== 'string') return 0;
  return [...text.replace(PSEUDO_EMPTY_STRIP_RE, '')].length;
}

/**
 * GPT 系模型 / 部分中转会返回 HTTP 200 但内容完全不可用（正文为空、既没有 tool_calls
 * 也没有任何 token）。agent 收到这种响应会"卡住不动"——对它来说等于网关没反应。
 * 这类空响应按渠道失败处理，直接换下一家。
 *
 * 覆盖两种协议形态（客户端拿到的是哪种，取决于客户端协议与上游协议的组合）：
 *   - OpenAI：choices[].message（content / reasoning_content / tool_calls）
 *   - Anthropic：content[]（text / thinking / tool_use）——以前这里直接 return null，
 *     所以 Anthropic 形态的"200 空响应"从来不被检查，会被当成功返回给客户端。
 * 另外也拦"200 却回了 JSON 错误体"（部分中转会把错误挂在 200 上）。
 *
 * @param {{minChars?:number}} opts 有效字符数阈值（routing.emptyMinChars，默认 1）：
 *   默认 1 只拦"去掉空白/标点/围栏后一个字符都不剩"的伪空，不会误伤正常短回答。
 */
export function findUnusableResponse(json, { minChars = 1 } = {}) {
  if (!json || typeof json !== 'object') return null;
  const min = Math.max(1, Number(minChars) || 1);

  // 200 却回了错误体：客户端拿不到任何可用内容
  if (json.error && !Array.isArray(json.choices) && !Array.isArray(json.content)) {
    return { reason: 'error_body', finish: null };
  }

  // ---- Anthropic 形态（原生 content[]，或 OpenAI->Anthropic 转换后的空 text 块）----
  if (Array.isArray(json.content) && !Array.isArray(json.choices)) {
    if (json.content.some((b) => b?.type === 'tool_use')) return null;
    const textBlocks = json.content.filter((b) => b?.type === 'text' && typeof b.text === 'string');
    const thinkBlocks = json.content.filter((b) => b?.type === 'thinking' && typeof b.thinking === 'string');
    const text = textBlocks.map((b) => b.text).join('');
    const thinking = thinkBlocks.map((b) => b.thinking).join('');
    if (effectiveChars(text) >= min || effectiveChars(thinking) >= min) return null;
    // stop_reason=max_tokens 且确实产出了 token，说明是被截断而非空响应，交由客户端处理
    const out = json.usage?.output_tokens ?? 0;
    if (json.stop_reason === 'max_tokens' && out > 0) return null;
    const reason = (text || thinking).trim() ? 'pseudo_empty' : 'empty_completion';
    return { reason, finish: json.stop_reason ?? null };
  }

  const choice = Array.isArray(json.choices) ? json.choices[0] : null;
  // choices 为空数组同样是"200 但没有任何可用内容"（以前会 return null 直接当成功）
  if (!choice) return Array.isArray(json.choices) ? { reason: 'empty_completion', finish: null } : null;
  const msg = choice.message || {};
  const hasTool = Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0;
  if (hasTool) return null;
  const text = typeof msg.content === 'string'
    ? msg.content
    : Array.isArray(msg.content) ? msg.content.map((c) => c?.text ?? '').join('') : '';
  const reasoning = typeof msg.reasoning_content === 'string' ? msg.reasoning_content : '';
  if (effectiveChars(text) >= min || effectiveChars(reasoning) >= min) return null;
  // finish_reason=length 且确实产出了 token，说明是被截断而非空响应，交由客户端处理
  const out = json.usage?.completion_tokens ?? 0;
  if (choice.finish_reason === 'length' && out > 0) return null;
  const reason = (text || reasoning).trim() ? 'pseudo_empty' : 'empty_completion';
  return { reason, finish: choice.finish_reason ?? null };
}

/** 流式头部判定：整段没有任何正文/工具调用/思考的 200 流 -> 不可用响应 */
export function findUnusableStreamHead(raw, { minChars = 1 } = {}) {
  const text = String(raw || '');
  const min = Math.max(1, Number(minChars) || 1);

  // 真正的 SSE 数据帧 = data: 后面跟 JSON（[DONE] 与空行不算）
  const jsonFrames = [...text.matchAll(/(?:^|\n)\s*data:\s*(.+)/g)]
    .map((m) => m[1].trim())
    .filter((d) => !/^\[DONE\]$/i.test(d) && (d.startsWith('{') || d.startsWith('[')));

  // 一个数据帧都没有：
  //   - 200 + 零个帧（空 body / 只有 [DONE]）——以前永远匹配不上，会被当"空成功"回给客户端；
  //   - 200 但正文根本不是 SSE（中转把 JSON 错误体挂在 200 上）。
  // 两种客户端都拿不到任何东西，一律判不可用换下一家。
  if (!jsonFrames.length) {
    const trimmed = text.trim();
    if (/^[[{]/.test(trimmed)) return { reason: 'non_sse_body', finish: null };
    return { reason: 'empty_stream', finish: null };
  }

  // 有数据帧时仍需看到收尾信号才判定（避免把"还在思考、正文还没开始"的合法流腰斩）。
  // Anthropic 方向用 message_stop / stop_reason 收尾，没有 finish_reason。
  const finished =
    /"finish_reason"\s*:\s*"(stop|length|tool_calls|end_turn|max_tokens)"/.test(text)
    || /"type"\s*:\s*"message_stop"/.test(text)
    || /"stop_reason"\s*:\s*"(end_turn|max_tokens|stop_sequence|tool_use)"/.test(text);
  if (!finished) return null;

  if (/"tool_calls"\s*:/.test(text) || /"type"\s*:\s*"tool_use"/.test(text)) return null;
  const content = extractStreamContentText(text);
  if (effectiveChars(content) >= min) return null;
  if (/"reasoning_content"\s*:\s*"[^"]/.test(text) || /"type"\s*:\s*"thinking/.test(text)) return null;
  return { reason: content.trim() ? 'pseudo_empty_stream' : 'empty_stream' };
}

function unusableResponseError(channel, info) {
  return new UpstreamError(
    `上游返回了 HTTP 200 但没有任何可用内容（${info.reason}${info.finish ? `, finish_reason=${info.finish}` : ''}）：agent 会因此收不到回复`,
    { status: 502, kind: 'empty_response', retryable: false },
  );
}

/**
 * F1（代码审查 2026-09-20）：响应已经提交给客户端、而上游流内报错或被截断时，
 * 按**下游协议**下发一个合法的流内错误帧。
 * 关键：**不伪造 finish_reason / [DONE]**——伪造就等于把半截回答包装成完整结果，
 * agent 会把中断内容当最终答案，路由层还会把该渠道记成功。
 */
async function emitStreamError(res, clientProtocol, message) {
  if (!res || res.destroyed || res.writableEnded) return;
  const frame = clientProtocol === 'anthropic'
    ? `event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'api_error', message } })}\n\n`
    : `data: ${JSON.stringify({ error: { message, type: 'upstream_error', code: 'stream_truncated' } })}\n\n`;
  try { res.write(frame); } catch { /* 客户端已经走了，忽略 */ }
}

/**
 * F1：终止信号追踪器 —— 判断一个上游流有没有给出"模型说完了"的协议信号。
 * 只认三种：finish_reason / stop_reason 是**字符串值**（`null` 不算）、`[DONE]`、`message_stop`。
 * 用 Buffer 针查找，避免每个数据块都做 utf8 解码（大流下省 CPU）。
 */
function createTerminalTracer() {
  const REASON_KEYS = [Buffer.from('"finish_reason"'), Buffer.from('"stop_reason"')];
  const PLAIN = [Buffer.from('[DONE]'), Buffer.from('"message_stop"')];
  let seen = false;
  let sawEndFrame = false;
  // key 之后跳过空白/冒号，若紧接着是双引号 -> 是字符串值（"stop"/"tool_calls"/...，不是 null）
  const keyHasStringValue = (buf, key) => {
    let i = buf.indexOf(key);
    while (i !== -1) {
      let j = i + key.length;
      while (j < buf.length && (buf[j] === 0x20 || buf[j] === 0x3a)) j += 1; // ' ' 或 ':'
      if (buf[j] === 0x22) return true; // '"'
      i = buf.indexOf(key, i + 1);
    }
    return false;
  };
  return {
    push(item) {
      if (seen) return true;
      const buf = Buffer.isBuffer(item) ? item : Buffer.from(String(item ?? ''), 'utf8');
      if (!buf.length) return seen;
      for (const p of PLAIN) {
        if (buf.indexOf(p) !== -1) {
          seen = true;
          sawEndFrame = true; // 命中 [DONE] 或 "message_stop"
          return seen;
        }
      }
      for (const k of REASON_KEYS) {
        if (keyHasStringValue(buf, k)) { seen = true; return seen; }
      }
      return seen;
    },
    get saw() { return seen; },
    // 上游是否发过**终止帧**（[DONE] / message_stop）。只有 finish_reason/stop_reason 时为 false：
    // 这类兼容上游客户端不会收到终止帧，转发层需要补一个（否则客户端可能一直等）
    get sawEndFrame() { return sawEndFrame; },
  };
}

/**
 * 思考过程守卫（非流式）：作用于已转成客户端协议的响应对象。
 *  - 各家的思考字段（reasoning / thinking / reasoning_details…）统一归并到 reasoning_content
 *  - 正文里若混入了思维链自述，拆成 reasoning_content + 干净正文
 * 返回 { changed, split, marker, notes }，供调用方打日志。
 */
function applyReasoningGuard(json, { splitInline = true } = {}) {
  const notes = [];
  let split = false;
  let marker = null;

  // OpenAI 形状：choices[].message
  if (Array.isArray(json?.choices)) {
    for (const ch of json.choices) {
      const msg = ch?.message;
      if (!msg || typeof msg !== 'object') continue;
      const res1 = cleanMessageReasoning(msg, { splitInline });
      if (res1.changed) {
        notes.push(msg.reasoning_content ? '思考字段已归一' : '思考字段已清理');
        if (res1.split) { split = true; marker = res1.marker; }
      }
    }
  }

  // Anthropic 形状：content 数组里的 text / thinking 块
  if (Array.isArray(json?.content)) {
    const textBlocks = json.content.filter((b) => b?.type === 'text' && typeof b.text === 'string');

    // 非标准思考块（如 reasoning / analysis）也没有对应的 Anthropic 类型，统一并入 thinking
    const stray = json.content.filter((b) => b && typeof b === 'object'
      && typeof b.reasoning === 'string' && b.reasoning);
    for (const b of stray) { b.type = 'thinking'; b.thinking = b.reasoning; delete b.reasoning; notes.push('思考块已归一'); }

    const joined = textBlocks.map((b) => b.text).join('');
    if (splitInline && joined) {
      const { reasoning, content: clean, split: did, marker: mk } = splitInlineReasoning(joined);
      if (did) {
        // 只有单块时才安全地就地改写；多块时保留首块承载干净正文，其余并入思考
        const first = textBlocks[0];
        first.text = clean;
        for (const b of textBlocks.slice(1)) b.text = '';
        // Anthropic 要求 thinking 块排在正文块之前，所以插到最前而不是追加到末尾
        if (reasoning) json.content.unshift({ type: 'thinking', thinking: reasoning });
        split = true;
        marker = mk;
        notes.push('正文拆出思维链');
      }
    }
  }

  return { changed: notes.length > 0, split, marker, notes };
}

function setGatewayHeaders(res, channel, model, agentId = null, keyInfo = null) {
  try {
    res.setHeader?.('x-gateway-channel', channel.name);
    res.setHeader?.('x-gateway-upstream-model', model);
    res.setHeader?.('x-gateway-protocol', channel.protocol);
    if (channel.tier) res.setHeader?.('x-gateway-tier', channel.tier);
    if (channel.effort) res.setHeader?.('x-gateway-effort', channel.effort);
    // 叠加 key 渠道：只回传序号 / 总数 / 策略 / 尝试数，
    // 绝不回传 Key 本身、Key 前缀或 Authorization（TASK 10）
    if (keyInfo && Number.isFinite(keyInfo.index)) {
      res.setHeader?.('x-gateway-key', `${keyInfo.index + 1}/${keyInfo.count}`);
      if (keyInfo.strategy) res.setHeader?.('x-gateway-key-strategy', keyInfo.strategy);
      if (Number.isFinite(keyInfo.attempts) && keyInfo.attempts > 0) {
        res.setHeader?.('x-gateway-key-attempts', String(keyInfo.attempts));
      }
    }
    // 子代理可观测性：客户端能直接从响应头看出"我是谁、被路由到了哪家"
    if (agentId) res.setHeader?.('x-gateway-agent', shortLabel(agentId));
  } catch {
    /* headers 已发送 */
  }
}

/**
 * 上游 HTTP 错误分类。统一委托给 util.classifyUpstreamFailure，
 * 让路由、探活、任务日志共享同一套错误口径（尤其是余额不足 / 鉴权失败）。
 */
export function classifyFailure(status, text) {
  return classifyUpstreamFailure(status, text);
}

export function sendJson(res, status, payload) {
  if (res.writableEnded || res.destroyed) return;
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

export function corsHeaders(res) {
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  res.setHeader('access-control-allow-headers', '*');
  res.setHeader('access-control-max-age', '86400');
}
