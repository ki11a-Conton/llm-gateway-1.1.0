// 子代理（sub-agent）身份识别：从请求头 / body 里推断"这个请求来自哪个子代理 / 会话"。
// 目的是让网关在多并发下能回答三个问题：
//   1. 哪个子代理发来的请求？
//   2. 它被路由到了哪个渠道 / 模型？
//   3. 它现在有多少请求在途？
// 识别结果会贯穿：日志前缀、响应头、/api/status、限流（每代理配额）、会话亲和路由。
//
// 识别优先级（越靠前越可信）：
//   1. 显式请求头：x-agent-id / x-subagent-id / x-session-id / x-conversation-id
//   2. body / metadata 里的会话标识：比 api-key 更具体 ——
//      同一个 agent 进程可能用同一个 key 跑多个子代理，只有 body 能区分它们
//   3. Authorization 里的 key 指纹（不同 agent 常用不同 key）
//   4. 兜底：按客户端 IP + User-Agent 归一
// 全部拿不到时返回 null（单用户本地场景，不强制区分）。

const HEADER_KEYS = [
  'x-agent-id',
  'x-subagent-id',
  'x-sub-agent-id',
  'x-session-id',
  'x-conversation-id',
  'x-thread-id',
  'x-request-agent',
  'anthropic-session-id',
];

// body 里可能带会话标识的字段（OpenAI / Anthropic / 各家 SDK 的常见写法）
const BODY_KEYS = [
  'session_id',
  'sessionId',
  'conversation_id',
  'conversationId',
  'thread_id',
  'threadId',
  'agent_id',
  'agentId',
  'subagent_id',
];

const MAX_ID_LEN = 48;

/** 归一化：只留可打印字符，限制长度，避免把超长/含控制字符的内容塞进日志与 map key */
function sanitize(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  const cleaned = s.replace(/[^\w.:@\-]/g, '').slice(0, MAX_ID_LEN);
  return cleaned || null;
}

/** 从 Authorization / x-api-key 推出一个短指纹，用于区分不同 agent 用的 key */
function keyFingerprint(req) {
  const auth = req.headers?.authorization || '';
  const raw = auth.toLowerCase().startsWith('bearer ')
    ? auth.slice(7).trim()
    : req.headers?.['x-api-key'] || req.headers?.['api-key'] || '';
  if (!raw) return null;
  // 只用后 8 位做标识，不泄露完整 key
  const tail = String(raw).replace(/[^\w]/g, '').slice(-8);
  return tail ? `key:${tail}` : null;
}

function fromHeaders(req) {
  for (const k of HEADER_KEYS) {
    const v = sanitize(req.headers?.[k]);
    if (v) return { id: v, source: `header:${k}` };
  }
  return null;
}

function fromBody(body) {
  if (!body || typeof body !== 'object') return null;
  for (const k of BODY_KEYS) {
    const v = sanitize(body[k]);
    if (v) return { id: v, source: `body:${k}` };
  }
  // 部分 SDK 把会话信息放在 metadata 里
  const meta = body.metadata;
  if (meta && typeof meta === 'object') {
    for (const k of BODY_KEYS) {
      const v = sanitize(meta[k]);
      if (v) return { id: v, source: `metadata:${k}` };
    }
  }
  // user 字段常被 SDK 用来带会话标识（如 "user_abc_session_123"）
  const u = sanitize(body.user);
  if (u && u.length >= 6) return { id: u, source: 'body:user' };
  return null;
}

function fromTransport(req) {
  const ua = sanitize(req.headers?.['user-agent']);
  const ip = req.socket?.remoteAddress || '';
  if (ua) return { id: `ua:${ua}`, source: 'user-agent' };
  if (ip) return { id: `ip:${ip.replace(/[^\w.:]/g, '_')}`, source: 'remote-ip' };
  return null;
}

/**
 * 识别请求所属的子代理。
 * @param {import('node:http').IncomingMessage} req
 * @param {object} body 已解析的请求体
 * @returns {{id:string, source:string, explicit:boolean}}
 */
export function identifyAgent(req, body) {
  const h = fromHeaders(req);
  if (h) return { ...h, explicit: true };

  // body / metadata 优先于 api-key：同一个 key 可能被多个子代理共用，
  // 只有 body 里的会话标识才能把它们区分开
  const b = fromBody(body);
  if (b) return { ...b, explicit: true };

  const k = keyFingerprint(req);
  if (k) return { id: k, source: 'api-key', explicit: true };

  const t = fromTransport(req);
  if (t) return { ...t, explicit: false };

  return { id: null, source: 'none', explicit: false };
}

/**
 * 生成给客户端/日志看的短标签，例如 "alpha"、"key:RKEY1234"、"ua:curl..8.0"。
 * 只是展示用，不参与路由决策。
 */
export function shortLabel(agentId) {
  if (!agentId) return 'anon';
  const s = String(agentId);
  if (s.length <= 14) return s;
  const sep = s.indexOf(':');
  if (sep > 0 && sep <= 4) {
    const kind = s.slice(0, sep);
    const rest = s.slice(sep + 1);
    return `${kind}:${rest.slice(0, 4)}..${rest.slice(-4)}`;
  }
  return `${s.slice(0, 6)}..${s.slice(-4)}`;
}
