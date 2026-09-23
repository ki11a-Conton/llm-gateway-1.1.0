// 通用工具：URL 拼接、退避、SSE 解析、ID 生成

export function normalizeBaseUrl(raw, protocol = 'openai') {
  let u = String(raw || '').trim().replace(/\/+$/, '');
  if (!u) throw new Error('baseUrl 不能为空');
  if (!/^https?:\/\//i.test(u)) u = 'https://' + u;
  // 只补版本号，不假设路径；openai 约定 /v1，anthropic 官方也是 /v1
  if (!/\/v\d+(\w+)?$/.test(u)) u += '/v1';
  return u;
}

export function joinUrl(base, path) {
  return base.replace(/\/+$/, '') + (path.startsWith('/') ? path : '/' + path);
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms)));

export function genId(prefix) {
  const rand =
    globalThis.crypto?.randomUUID?.().replace(/-/g, '').slice(0, 16) ??
    Math.random().toString(16).slice(2, 18);
  return `${prefix}${rand}`;
}

export function safeJsonParse(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

// 从上游字节流中解析 SSE 事件，产出 { event, data }
export async function* parseSSE(byteStream) {
  const decoder = new TextDecoder();
  let buffer = '';
  for await (const chunk of byteStream) {
    buffer += typeof chunk === 'string' ? chunk : decoder.decode(chunk, { stream: true });
    let bound;
    // SSE 规范允许 CR、LF、CRLF 三种行结束符，用空行分隔事件。
    // 这里必须三种都找：只找 '\n\n' 时，合法的 CRLF 流（'\r\n\r\n'）永远切不开，
    // 整条流会被一路攒到 EOF 当成"一个"事件解析 —— 数据全部丢失，客户端拿到空回答。
    while ((bound = findFrameBoundary(buffer)) !== null) {
      const frame = buffer.slice(0, bound.index);
      buffer = buffer.slice(bound.index + bound.length);
      const evt = parseFrame(frame);
      if (evt) yield evt;
    }
  }
  const tail = parseFrame(buffer);
  if (tail) yield tail;
}

/**
 * 找出缓冲区里**最早**出现的事件分隔符。
 * 必须先比出最早的下标：'\r\n\r\n' 里含 '\r' 与 '\n'，若先匹配到后面的分隔符会把帧切歪。
 */
function findFrameBoundary(buffer) {
  let best = null;
  for (const sep of ['\r\n\r\n', '\n\n', '\r\r']) {
    const index = buffer.indexOf(sep);
    if (index !== -1 && (best === null || index < best.index)) best = { index, length: sep.length };
  }
  return best;
}

function parseFrame(frame) {
  if (!frame || !frame.trim()) return null;
  let event = 'message';
  const dataLines = [];
  // SSE 规范的行结束符是 CR、LF 或 CRLF 三者之一，拆行时三种都要认；
  // 只写 /\r?\n/ 会让"裸 CR"分隔的流解析不出任何 data 行（整帧被当成一行 event）。
  for (const line of frame.split(/\r\n|\r|\n/)) {
    if (line.startsWith(':')) continue;
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  }
  if (!dataLines.length) return null;
  return { event, data: dataLines.join('\n') };
}

// 把任意异步可迭代的 Uint8Array 转成文本块（用于非 SSE 的错误体读取）
export async function readAllText(stream, limit = 64 * 1024) {
  if (!stream) return '';
  const decoder = new TextDecoder();
  let out = '';
  try {
    for await (const chunk of stream) {
      out += decoder.decode(chunk, { stream: true });
      if (out.length > limit) break;
    }
  } catch {
    /* 上游断开，忽略 */
  }
  return out;
}

export function truncate(s, n = 300) {
  const str = String(s ?? '');
  return str.length > n ? str.slice(0, n) + '…' : str;
}

// 从环境变量字符串里替换 ${VAR} 占位符
export function expandEnv(str, env = process.env) {
  if (typeof str !== 'string') return str;
  return str.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (m, name) => env[name] ?? m);
}

export function deepExpandEnv(value, env = process.env) {
  if (typeof value === 'string') return expandEnv(value, env);
  if (Array.isArray(value)) return value.map((v) => deepExpandEnv(v, env));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = deepExpandEnv(v, env);
    return out;
  }
  return value;
}

// ---------- prompt token 粗略估算（上下文感知路由用） ----------
// 足够接近即可：CJK 约 1 token/字，其余约 4 字符/token；用途是判断「渠道窗口装不装得下」，
// 低估只会偶尔多一次上游 502（重试循环兜底），高估则会误伤可用渠道，所以宁可少算。

// ---------- 上游错误分类（路由 / 探活 / 日志共用一套口径） ----------

/** 余额不足 / 额度耗尽：重试意义为零，必须冷却该渠道并立刻换下一家 */
const BALANCE_RE = /insufficient|balance|quota|credits?|欠费|余额|额度|not enough|exceeded your current quota|billing|payment required|arrears/i;
/**
 * 鉴权失败：key 错/被禁，同样应该换下一家而不是把请求挂住。
 *
 * 注意**绝不要**用裸的 `invalid.*token`：OpenAI 兼容接口的 400 普遍带
 * `"type":"invalid_request_error"`，而 `max_tokens` / `max_completion_tokens` 里就含 "token"，
 * 两者一凑就把「参数非法」误判成「鉴权失败」——后果是每个被撞到的渠道冷却 10 分钟、
 * 且外层轮询会把整池反复捶到 maxTotalWaitMs 才回 503（而不是立刻回正确的 400）。
 * 所以这里只认真正指向凭据的措辞（api key / access token / credential / 密钥…）。
 */
const AUTH_RE = /invalid[\s_-]?token\b|(?:invalid|incorrect|wrong|missing|expired|bad|not\s+valid|无效|过期)[^.\n]{0,40}?(?:api[\s_-]?key|access[\s_-]?token|auth[\s_-]?token|bearer[\s_-]?token|credential|密钥)|(?:api[\s_-]?key|access[\s_-]?token|credential|密钥)[^.\n]{0,40}?(?:invalid|expired|incorrect|missing|not\s+valid|无效|过期)|unauthor|authentication|forbidden|未授权/i;

/**
 * 限流措辞。与 BALANCE_RE 同时命中时以限流为准：
 * 很多厂商的限流文案里也带 quota 字样（如 "rpm exhausted"、"inference exceeds tpm/rpm limit"），
 * 若先按"余额不足"处理会白冷却 900s 且跳过限流专用的不熔断逻辑。
 */
const RATE_LIMIT_RE = /rate[\s_-]?limit|too\s+many\s+requests|\brpm\b|\btpm\b|requests?\s+per\s+(?:minute|second)|tokens?\s+per\s+minute|限流|频率|请求过于频繁|并发.{0,6}(?:上限|超限)/i;

/**
 * 没有诊断价值的通用错误类型/代码：OpenAI 兼容接口几乎每个 400 都带
 * `invalid_request_error`，判定"是不是模型问题"前必须先剔掉，否则人人命中。
 */
const NOISE_RE = /invalid_request_error|invalid_request|invalid_value|integer_below_min_value|string_too_(?:short|long)/gi;

/**
 * 明确指向"模型/部署不存在或不支持"的措辞。
 *
 * 这里刻意保持克制：一旦判成 modelIssue，lib/proxy.mjs 会把该模型写进渠道的
 * `unsupported` 集合，而那个集合原先**没有任何移除路径** —— 等于该渠道对这个模型永久失效。
 * 早先的实现是 `MODEL_RE(/model|模型|deployment/) && /(…|invalid|unsupported|…)/`，
 * 因为通用错误类型里就有 "invalid"，于是
 * "max_tokens is too large: 33792. This model supports at most 8192 completion tokens"
 * 这种普通参数错误也会被永久拉黑（已实测复现）。
 */
const MODEL_ISSUE_RE = new RegExp(
  [
    String.raw`(?:model|模型|deployment)[^.\n]{0,48}?(?:not\s*found|not\s*exist|does\s*not\s*exist|doesn'?t\s*exist|unknown|unsupported|not\s*supported|unavailable|is\s*not\s*available|不存在|不可用|不支持|无效)`,
    String.raw`(?:unknown|unsupported|unrecognized|no\s+such|不存在|不支持)[^.\n]{0,48}?(?:model|模型)`,
    String.raw`no\s+provider\s+supported`,
    String.raw`model_not_found`,
  ].join('|'),
  'i',
);

/** 判定一段（已小写的）错误正文是否在说"模型维度的问题" */
function modelIssueText(lowerText) {
  return MODEL_ISSUE_RE.test(lowerText.replace(NOISE_RE, ' '));
}

/**
 * 分类一次上游失败。返回 { kind, retryable, modelIssue, cooldownHint }。
 * 统一口径的价值：路由层（换家）、探活层（该给多长冷却）、任务日志（错误指纹）
 * 对同一个上游错误得到同一个结论，不会出现"这里当限流、那里当余额不足"的分裂。
 */
export function classifyUpstreamFailure(status, text) {
  const t = String(text || '').toLowerCase();
  // 429 是明确的限流信号：只有正文里没有限流措辞时，才把"余额/额度"字样当成真的欠费。
  const rateLimited = status === 429 && RATE_LIMIT_RE.test(t);
  if (!rateLimited && BALANCE_RE.test(t)) {
    return { kind: 'insufficient_balance', retryable: false, modelIssue: false, cooldownHint: 'balance' };
  }
  if (!rateLimited && AUTH_RE.test(t)) {
    return { kind: 'auth', retryable: false, modelIssue: false, cooldownHint: 'auth' };
  }
  if (status === 402) return { kind: 'insufficient_balance', retryable: false, modelIssue: false, cooldownHint: 'balance' };
  if (status === 401 || status === 403) return { kind: 'auth', retryable: false, modelIssue: false, cooldownHint: 'auth' };
  if (status === 404) {
    // 404 也可能是"路径写错/网关地址错"，所以同样要求正文明确提到模型，
    // 宁可少拉黑（代价只是多试几次）也不要误拉黑（原先无法恢复）。
    const looksLikeModel = modelIssueText(t);
    return { kind: 'model_not_found', retryable: !looksLikeModel, modelIssue: looksLikeModel, cooldownHint: null };
  }
  if (status === 429) return { kind: 'rate_limit', retryable: true, modelIssue: false, cooldownHint: 'rate_limit' };
  if (status >= 500) return { kind: 'server_error', retryable: true, modelIssue: false, cooldownHint: null };
  if (status === 400) {
    const looksLikeModel = modelIssueText(t);
    return {
      kind: looksLikeModel ? 'model_unsupported' : 'bad_request',
      retryable: !looksLikeModel,
      modelIssue: looksLikeModel,
      cooldownHint: null,
    };
  }
  if (status === 408 || status === 409 || status === 522 || status === 524) {
    return { kind: `http_${status}`, retryable: true, modelIssue: false, cooldownHint: null };
  }
  // 其余 4xx（422/413…）多为请求本身的问题，重试同渠道没意义
  if (status >= 400 && status < 500) return { kind: `http_${status}`, retryable: false, modelIssue: false, cooldownHint: null };
  return { kind: `http_${status}`, retryable: true, modelIssue: false, cooldownHint: null };
}

/** 探活场景的轻量分类（只需要 kind） */
export function classifyProbeFailure(status, text) {
  return classifyUpstreamFailure(status, text);
}

// ---------- 思考强度（reasoning effort） ----------

/**
 * 模型名是否属于"会思考"的模型族。用于决定要不要强制最高强度思考。
 * 命中的都是已知支持 reasoning 的系列；未命中的一律不加思考参数，
 * 避免把不支持该参数的渠道直接打成 400。
 */
const THINKING_MODEL_RE = /(^|[/_:.-])(o[1-9]\b|o[1-9]-)|reason|think|r1\b|qwq|deepseek-(v\d|r\d)|ds-?v\d|gpt-5|gpt-6|claude-(3|4|opus|sonnet|haiku|.*-4)|glm-(4|5|z)|minimax-m|kimi-k|gemini-(2\.5|3)|magistral|grok-[3-9]|qvq|marco-o/i;

export function modelSupportsThinking(model) {
  const m = String(model || '').toLowerCase();
  if (!m) return false;
  if (/\b(chat|instruct|flash-lite|embed|tts|whisper|dall-e|image)\b/.test(m) && !/reason|think/.test(m)) return false;
  return THINKING_MODEL_RE.test(m);
}

/** 默认目标档位：未配置 routing.maxEffort / channel.maxEffort 时用它 */
export const MAX_EFFORT = 'high';

/** 档位被上游拒绝时的降级目标（lib/proxy.mjs 同渠道降档重试一次，不打穿渠道链） */
export const EFFORT_FALLBACK = 'high';

/**
 * 上游 400 文本是否在抱怨 reasoning_effort 档位非法。
 * 例：sensenova -> "field ReasoningEffort invalid, should be one of: low, medium, high, xhigh, none"
 */
export function isEffortRejection(message) {
  const m = String(message || '');
  if (!/reasoning[\s_-]?effort/i.test(m)) return false;
  return /invalid|should be|not\s*support|unsupported|不支持|非法/i.test(m);
}

export function estimateTokens(text) {
  const str = String(text ?? '');
  if (!str) return 0;
  let cjk = 0;
  let other = 0;
  for (const ch of str) {
    if (/[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]/.test(ch)) cjk += 1;
    else other += 1;
  }
  return Math.ceil(cjk + other / 4);
}

// OpenAI 请求体 -> 估算的 prompt token 数（消息 + 消息头开销 + tool_calls + system）
export function estimatePromptTokens(body) {
  if (!body || !Array.isArray(body.messages)) return 0;
  const MSG_OVERHEAD = 4; // role / 分隔 / 结束符等
  let sum = 0;
  for (const m of body.messages) {
    if (!m || typeof m !== 'object') continue;
    sum += MSG_OVERHEAD;
    if (typeof m.name === 'string') sum += estimateTokens(m.name);
    const c = m.content;
    if (typeof c === 'string') {
      sum += estimateTokens(c);
    } else if (Array.isArray(c)) {
      for (const part of c) {
        if (!part || typeof part !== 'object') continue;
        if (part.type === 'image_url' || part.type === 'image') sum += 85; // 图片粗略折算
        else if (typeof part.text === 'string') sum += estimateTokens(part.text);
      }
    }
    if (Array.isArray(m.tool_calls)) {
      for (const tc of m.tool_calls) {
        sum += 8;
        if (typeof tc?.function?.name === 'string') sum += estimateTokens(tc.function.name);
        if (typeof tc?.function?.arguments === 'string') sum += estimateTokens(tc.function.arguments);
      }
    }
    if (typeof m.tool_call_id === 'string') sum += 3;
  }
  if (typeof body.system === 'string') sum += estimateTokens(body.system);
  if (typeof body.prompt === 'string') sum += estimateTokens(body.prompt);
  return sum;
}
