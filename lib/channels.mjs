// 渠道注册表：配置加载 / 模型发现 / 健康状态机 / 选路

import { readFileSync, writeFileSync, existsSync, copyFileSync, watch } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { log } from './logger.mjs';
import { normalizeBaseUrl, sleep, deepExpandEnv, readAllText, truncate, classifyUpstreamFailure } from './util.mjs';
import { openaiAdapter } from './adapters/openai.mjs';
import { anthropicAdapter } from './adapters/anthropic.mjs';
import { resolvePresets, applyPreset, loadUserPresets } from './presets.mjs';
import { GatewayLimiter } from './concurrency.mjs';

const ADAPTERS = { openai: openaiAdapter, anthropic: anthropicAdapter };

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(HERE, '..');

const DEFAULT_ROUTING = {
  // priority | round-robin | least-loaded | weighted
  //  priority      = 严格优先级降级链（默认；冷却中的渠道垫到最后）
  //  round-robin   = 每个请求轮换起点，把同一段（tier）内的渠道摊开使用（等权）
  //  least-loaded  = 最少在途优先（多并发推荐；空闲时在途数全为 0，退化为按 priority 排序）
  //  weighted      = 优先级加权轮询：按 priority 分档给份额，高优先多拿、低优先也参与，
  //                  用平滑加权轮询输出交错序列（不会出现"前 8 个请求全是同一家"的突发）
  // 注意真轮询与粘性天然冲突：sticky / sessionAffinity 都会把请求钉死在同一家，
  // 代码里 round-robin 已跳过"模型级粘性"，会话亲和仍按配置生效（想彻底摊开就关掉它）。
  strategy: 'priority',
  sticky: true,         // 跨请求粘性：最近成功过的渠道优先（可用时排最前）
  maxAttempts: 8,

  // ---- 优先级加权轮询（strategy: "weighted"）----
  // 权重按优先级分档，默认映射（键是优先级【上界】，取第一个 priority <= 键的档）：
  //   priority <= 10 -> 8 份额 | 11-30 -> 4 | 31-60 -> 2 | > 60 -> 1
  // 用 routing.priorityWeights 覆盖，形如 { "10": 8, "30": 4, "60": 2, "100": 1 }
  //（null/未配置 = 用上面的默认分档；某个渠道权重算成 0 表示它拿不到份额，但仍留在候选里兜底）
  priorityWeights: null,

  // ---- 半开试探 ----
  // 冷却到期后进入"半开窗口"：窗口内该渠道最多放行 halfOpenMaxInFlight 个在途请求做试探，
  // 其余请求跳过它（仍留在候选列表里，不会整个剔除）。试探成功 -> 立刻恢复满份额；
  // 试探失败 -> 重新进入既有的指数退避冷却。
  // halfOpenMs = 0 表示关闭半开（冷却到期即恢复满份额，与历史行为一致）。
  halfOpenMs: 15000,
  halfOpenMaxInFlight: 1,

  // ---- 两段式选路（Tier）----
  // 第一段：preferred 池，按 priority 轮询，渠道内重试沿用渠道 retries；把额度充裕的
  //   "好供应商"用尽才进第二段。
  // 第二段：其余供应商。上一次在这个池子里成功过的渠道会"钉"在最前面反复使用；
  //   它一旦失败，本次请求里再重试 fallbackAttempts 次（默认 1 + 2 次重试，每次间隔 3s），
  //   全部失败才换下一家；没有粘性记录时每次请求随机洗牌，避免总撞同一家。
  tiered: true,
  // 说明：历史上这里还有一个 `tierMode: 'random' | 'priority'`，但全项目从未读取过它
  //（第二段的挑选方式一直由 fallbackShuffle 决定），会误导运维，已删除。
  preferredBaseUrls: [                      // preferred 池按上游域名识别（子串匹配 baseUrl）
    'sensenova.cn',
    'api.b.ai',
  ],
  fallbackAttempts: 3,                      // 剩余供应商每家最多尝试次数（含首次 = 1 + 2 次重试）
  fallbackRetryIntervalMs: 3000,            // 剩余供应商每次尝试之间的固定等待
  fallbackShuffle: true,                    // 剩余供应商随机化（false = 仍按 priority 排）

  // 思考过程守卫：各家思考字段（reasoning/thinking/reasoning_details…）统一归一到
  // reasoning_content；正文里混入的思维链拆回 reasoning_content，agent 只看到干净回答
  // （渠道可配 guardReasoning:false 关闭，见 lib/reasoning-guard.mjs）
  reasoningGuard: true,
  // 流式方向是否做思考守卫（思考拆分 + 空响应检测）。true=开；false=流式完全跳过思考守卫
  // （含空响应检测），只保留工具守卫，换取更低的首字延迟（TTFT）。
  // 说明：上游已用 reasoning_content / thinking 明确承载思考时，网关会立即放行，
  // 因此默认开启的 TTFT 代价很小；仅在极端追求首字延迟时才需要关掉。
  reasoningGuardStream: true,
  // "伪空"响应阈值：正文去掉空白 / 标点 / markdown 装饰后剩余的有效字符数低于该值即视为
  // 不可用（HTTP 200 但只有 "." / "```" 壳之类），按渠道失败换下一家。
  // 默认 1 = 只拦"一个有效字符都不剩"的响应，不会误伤正常短回答。
  emptyMinChars: 1,

  // ---- 故障渠道隔离 ----
  // 余额不足 / 鉴权失败这类"重试也没用"的错误：不占用重试预算，但要把这家冷却掉，
  // 避免每次请求都先撞一遍。冷却到期后自动半开试探一次。
  disableOnBalanceError: true,              // 余额不足（insufficient balance / quota）直接冷却该渠道
  balanceCooldownMs: 900000,                // 余额不足冷却时长（15 分钟；到期自动重试一次）
  disableOnAuthError: true,                 // 鉴权失败（401/403）直接冷却该渠道
  authCooldownMs: 600000,                   // 鉴权失败冷却时长（10 分钟）

  // ---- 可用性兜底 ----
  // 任何情况下都要给客户端一个 HTTP 响应：全池失败时最多等 maxTotalWaitMs 就返回结果，
  // 绝不无限挂着让 agent 等到超时（"网关不理 agent"）。
  // 0 = 不限制（仅当 retryLoop 且 retryMaxWaitMs 本身也是 0 时才会真正无限等）
  maxTotalWaitMs: 600000,
  timeoutMs: 180000,
  failThreshold: 2,
  cooldownMs: 30000,
  maxCooldownMs: 600000,
  probeIntervalMs: 60000,
  discoverIntervalMs: 600000,
  // 全局出站代理（如 "http://127.0.0.1:7897"）；渠道级 proxy 字段可覆盖，GW_PROXY 环境变量兜底
  proxy: '',
  retryOnStatus: [408, 409, 429, 499, 500, 502, 503, 504, 522, 524],
  // 请求级循环重试：全池失败且都是可重试错误（429/5xx/网络）时，等待后重新循环，直到成功或超过总等待上限
  retryLoop: false,
  retryWaitMs: 3000,  // 每次重试固定等待（同渠道重试与整池轮间一致）
  retryMaxWaitMs: 0,  // 累计等待上限；0/未配置 = 无上限（固定 retryWaitMs 一直重试，直到成功或客户端断开）
  // 正文泄漏守卫：请求带 tools 时，识别"模型把工具调用写成正文"的渠道响应并换渠道
  // （渠道可配 guardToolCallText:false 关闭，见 lib/toolcall-guard.mjs）
  guardToolCallText: true,

  // ---- 两层重试（第 1 层：渠道内）----
  // 同一渠道的模型默认最多尝试 attemptsPerChannel 次（含首次），每次失败后等 retryPerAttemptMs 再试；
  // 全废才换下一家渠道。渠道可用 retries 字段单独覆盖次数
  attemptsPerChannel: 2,
  // 渠道内每次失败后的等待毫秒数；未配置时回落到 retryWaitMs（与整池轮间一致）
  retryPerAttemptMs: null,

  // ---- 多并发 / 多子代理 ----
  // 会话亲和：同一个子代理（会话）在 affinityTtlMs 内优先复用上次成功的渠道，
  //   避免多轮对话在不同上游之间来回跳（上下文缓存失效、行为不一致）
  sessionAffinity: true,
  affinityTtlMs: 10 * 60 * 1000,
  // 每代理并发配额：单个子代理同时在途上限，防止一个大代理把渠道配额吃光、其它代理饿死。0 = 不限
  maxConcurrentPerAgent: 16,

  // ---- 高并发保护 ----
  // 全局同时在途的上游请求上限；超出部分排队等待。0 = 不限制（不推荐）
  maxConcurrent: 128,
  // 单个渠道同时在途上限，避免把某一家上游打爆。0 = 不限制
  maxConcurrentPerChannel: 32,
  // 请求在并发队列里的最长等待时间，超时返回 503（可重试，让客户端或其它渠道接管）
  queueTimeoutMs: 60000,
  // 同一渠道失败去重窗口：窗口内的重复失败只计一次熔断计数。
  // 高并发下一批请求会同时收到同一个上游故障，没有去重就会瞬间累积到阈值触发长熔断
  failDedupMs: 2000,

  // ---- 模型黑名单（unsupported）----
  // 上游明确回报"模型不存在 / 不支持"时，把该模型临时从该渠道摘掉，避免每次请求都白撞一遍。
  // 必须带 TTL 且有上界：判错一次不能让渠道对该模型**永久**失效（早先正是如此，只能重启进程），
  // 也不能让客户端用任意模型名把它撑爆（会同时泄漏内存和 /api/status 的响应体积）。
  unsupportedTtlMs: 30 * 60 * 1000,         // 黑名单有效期（到期自动重新尝试）
  unsupportedMax: 200,                      // 单渠道黑名单条目上限（超出丢弃最旧的）
};

/**
 * 判定渠道属于哪一段选路池：
 *   1. 渠道显式写 tier: "preferred" | "fallback" 以配置为准
 *   2. 否则按 routing.preferredBaseUrls 里的域名子串匹配 baseUrl（sensenova.cn / api.b.ai …）
 *   3. 都没命中 -> fallback（第二段随机池）
 */
export function resolveTier(cfg, routing) {
  const explicit = String(cfg.tier || '').toLowerCase();
  if (explicit === 'preferred' || explicit === 'fallback') return explicit;
  const url = String(cfg.baseUrl || '').toLowerCase();
  const list = Array.isArray(routing?.preferredBaseUrls) ? routing.preferredBaseUrls : [];
  for (const p of list) {
    const pat = String(p || '').trim().toLowerCase();
    if (pat && url.includes(pat)) return 'preferred';
  }
  return 'fallback';
}

export class Channel {
  constructor(raw, routing, presets) {
    const cfg = applyPreset({ ...raw }, presets);
    this.preset = raw.preset || null;
    this.name = cfg.name;
    this.protocol = (cfg.protocol || 'openai').toLowerCase();
    this.adapter = ADAPTERS[this.protocol];
    if (!this.adapter) throw new Error(`渠道 ${cfg.name}: 不支持的 protocol "${this.protocol}"`);
    if (!cfg.baseUrl) throw new Error(`渠道 ${cfg.name}: 缺少 baseUrl`);

    this.baseUrl = normalizeBaseUrl(cfg.baseUrl, this.protocol);
    // 自定义路径：某些中转 / Azure 的端点不规则
    this.chatPath = cfg.chatPath || null;
    this.modelsPath = cfg.modelsPath || null;
    this.apiKey = cfg.apiKey ?? '';
    this.headers = cfg.headers || {};
    // OpenCode Zen 免费档：伪装 opencode 客户端请求头（匿名 key=public）
    this.opencodeFree = cfg.opencodeFree === true;
    // WorkBuddy 国际版 / 其它"只认流式"上游：出站强制 stream=true，非流式客户端由网关聚合
    this.forceStream = cfg.forceStream === true;
    // WorkBuddy 国际版：出站 body 风控脱敏（11128 逐字黑名单）+ tool_choice 归一（见 lib/workbuddy.mjs）
    this.workbuddySanitize = cfg.workbuddySanitize === true;
    // 账号 refresh_token（设备码授权自动写入；有值才参与定时 token 续期）
    this.refreshToken = cfg.refreshToken || '';
    // 出站代理（如 http://127.0.0.1:7897）：渠道级 > routing 全局 > GW_PROXY 环境变量
    this.proxy = cfg.proxy || routing.proxy || process.env.GW_PROXY || null;
    this.priority = Number.isFinite(cfg.priority) ? cfg.priority : 100;
    // 未显式指定 enabled 时：填了 key 就启用，key 为空（或环境变量未注入）就停用
    const keyReady =
      typeof this.apiKey === 'string' &&
      this.apiKey.trim() !== '' &&
      !/^\$\{[^}]*\}$/.test(this.apiKey.trim());
    this.enabled = cfg.enabled !== undefined ? cfg.enabled !== false : keyReady;
    this.keyMissing = !keyReady;
    this.alias = cfg.alias || {};
    this.maxTokens = cfg.maxTokens;
    // 请求改写：上游对 max_tokens/max_completion_tokens 有下限要求时（如必须 >2），把客户端的极小值抬到该下限
    // 支持路由级全局默认（routing.minMaxTokens）+ 渠道级覆盖（cfg.minMaxTokens）
    this.minMaxTokens = cfg.minMaxTokens ?? routing.minMaxTokens;
    // 上下文感知路由：该渠道绑定模型的上下文窗口（token）。请求 prompt 估算超过它时，候选过滤跳过该渠道
    this.contextWindow = Number.isFinite(cfg.contextWindow) && cfg.contextWindow > 0 ? cfg.contextWindow : undefined;
    // 正文泄漏守卫：渠道级覆盖路由级默认（routing.guardToolCallText）
    this.guardToolCallText = cfg.guardToolCallText ?? routing.guardToolCallText ?? true;
    // 思考过程守卫：渠道级覆盖路由级默认（routing.reasoningGuard）。
    // 归一各家思考字段 + 拆出混进正文的思维链（见 lib/reasoning-guard.mjs）
    this.guardReasoning = cfg.guardReasoning ?? routing.reasoningGuard ?? true;
    // 每渠道重试预算：一次候选轮次内最多尝试次数（含首次；429/TPM 也计入），失败间隔 retryPerAttemptMs（回落 retryWaitMs）
    // 未配置时回落到 routing.attemptsPerChannel
    this.retries = Number.isFinite(cfg.retries) && cfg.retries >= 1 ? Math.floor(cfg.retries) : null;
    // 渠道级失败间隔覆盖（毫秒）。未配置时：优先池回落到 routing.retryPerAttemptMs，
    // 随机池回落到 routing.fallbackRetryIntervalMs（见 proxy.mjs）。
    this.retryPerAttemptMs = Number.isFinite(cfg.retryPerAttemptMs) && cfg.retryPerAttemptMs >= 0
      ? Math.floor(cfg.retryPerAttemptMs)
      : null;
    this.anthropicVersion = cfg.anthropicVersion;
    this.timeoutMs = cfg.timeoutMs ?? routing.timeoutMs;
    this.description = cfg.description || '';
    // 思考强度标签：low | medium | high（请求带 reasoning_effort 时按此过滤路由）
    this.effort = cfg.effort ? String(cfg.effort).toLowerCase() : null;
    // 该渠道上游是否支持思考（thinking）参数。true=强制加；false=永不加；null=自动（按模型名判断）
    this.supportsThinking = cfg.supportsThinking === undefined ? null : cfg.supportsThinking !== false;
    // 该渠道的目标思考档位（low|medium|high|xhigh）。null=跟随 routing.maxEffort。
    // 默认策略是全局拉最高档（routing.maxEffort=xhigh），这里只登记"例外名单"：
    // 把吃不下最高档的渠道显式 pin 低一档——如 wxctf 开 xhigh 会把 max_tokens 全烧在
    // 思考上、正文返回空（200 空响应抓不到 400 降级兜底），只能 pin 到 high。
    this.maxEffort = cfg.maxEffort ? String(cfg.maxEffort).trim().toLowerCase() : null;
    // 选路分层：preferred（第一段，按 priority 轮询）| fallback（第二段，每请求随机挑）
    // 不显式配置时按 preferredBaseUrls 域名自动归类
    this.tier = resolveTier(cfg, routing);

    // 单模型渠道：model 字段等价 models:[model]，语义"这家供应商只提供一个模型"，且不参与自动发现
    const single = typeof cfg.model === 'string' && cfg.model.trim() !== '';
    this.singleModel = single;
    this.whitelist = single
      ? [cfg.model.trim()]
      : Array.isArray(cfg.models) ? cfg.models.filter(Boolean) : [];
    // 白名单为空 => 自动发现；发现失败时乐观放行
    this.autoDiscover = this.whitelist.length === 0;

    this.routing = routing;

    // 运行时状态
    this.models = new Set(this.whitelist);
    // 模型黑名单：model -> 过期时间戳。
    // 用"带 TTL 的 Map"而不是 Set：判错一次就让渠道对该模型永久失效是不可接受的
    //（早先正是 Set 且全项目没有 delete/clear，任何一次误判都只能重启进程才能恢复）。
    this.unsupported = new Map();
    this.failures = 0;
    this.openUntil = 0;
    // 半开试探窗口截止时间（毫秒时间戳）：cool() 时按 routing.halfOpenMs 计算。
    // 冷却到期后到该时间点之间是"半开窗口"——最多放行 halfOpenMaxInFlight 个在途请求
    // 试探这家是否恢复；成功（markSuccess）或再次冷却都会把它清 0。
    this.probeUntil = 0;
    // F8：半开窗口内已占用的试探名额（由转发层 tryAcquireHalfOpen 维护）
    this.halfOpenInFlight = 0;
    this.lastOkAt = 0;
    this.lastFailAt = 0;
    // 最近一次"被计入熔断计数"的失败时间，用于高并发下的失败去重
    this.lastCountedFailAt = 0;
    this.lastError = null;
    this.latency = 0; // 指数移动平均
    this.total = 0;
    this.failed = 0;
    this.rr = 0;
    this.discovered = !this.autoDiscover;
    // WorkBuddy 国际版：账号剩余额度与最近刷新时间（定时刷新，面板展示）。
    // 必须能从配置里读回来：刷新流程会把余额写回 config.json，而写回又会触发 watch -> load()，
    // 若只存在内存里，下一次热重载就把它抹掉（面板上的余额徽标几乎永远看不到）。
    this.creditRemain = Number.isFinite(Number(cfg.creditRemain)) ? Number(cfg.creditRemain) : null;
    this.balanceUpdatedAt = Number(cfg.balanceUpdatedAt) || 0;
  }

  get healthy() {
    return this.enabled && this.openUntil <= Date.now();
  }

  get coolingDown() {
    return this.openUntil > Date.now();
  }

  get coolRemainMs() {
    return Math.max(0, this.openUntil - Date.now());
  }

  /**
   * 是否处于"半开窗口"：冷却已到期，但距冷却到期还没超过 routing.halfOpenMs。
   * 窗口内该渠道最多放行 halfOpenMaxInFlight 个在途请求做试探，其余请求跳过它（但不剔除）。
   */
  get halfOpen() {
    const now = Date.now();
    return this.probeUntil > 0 && this.openUntil > 0 && this.openUntil <= now && now < this.probeUntil;
  }

  /** 半开窗口内允许的在途试探数（默认 1） */
  get halfOpenMaxInFlight() {
    return Math.max(1, Math.floor(Number(this.routing?.halfOpenMaxInFlight)) || 1);
  }

  /**
   * F8（代码审查 2026-09-20）：半开窗口的**真实准入**（不只是排序偏好）。
   * @returns {null|boolean} null = 不在半开窗口，无需名额；true = 已占到试探名额；
   *                         false = 名额已满，调用方必须换下一家（或按预算等待）
   */
  tryAcquireHalfOpen() {
    if (!this.halfOpen) return null;
    if (this.halfOpenInFlight >= this.halfOpenMaxInFlight) return false;
    this.halfOpenInFlight += 1;
    return true;
  }

  /** 释放一个半开试探名额（幂等） */
  releaseHalfOpen() {
    if (this.halfOpenInFlight > 0) this.halfOpenInFlight -= 1;
  }

  /** 是否属于第一段（preferred）池 */
  get isPreferred() {
    return this.tier === 'preferred';
  }

  /** 逻辑模型名 -> 该渠道上的真实模型名 */
  resolveModel(model) {
    return this.alias[model] ?? model;
  }

  /**
   * 渠道默认模型：池子总入口（unifiedModel）路由时，每家用自己绑定的那个模型。
   *
   * 显式配置（model / models）时尊重运维写的顺序；**自动发现**时顺序不可信 ——
   * 很多聚合网关把 embedding / whisper / 图像模型排在 /models 最前面，于是池子总入口
   * 会把聊天流量发到一个非聊天模型上（实测会挑中 text-embedding-3-small），
   * 随后探活拿 404 还会顺手把它拉黑。这里按"非嵌入优先 + 名称字典序"稳定挑选。
   */
  defaultModel() {
    if (this.whitelist.length) return this.whitelist[0];
    if (this.models.size) {
      const list = [...this.models];
      const NON_CHAT_RE = /embed|whisper|tts|dall-?e|image|rerank|moderation|audio|video/i;
      const chat = list.filter((m) => !NON_CHAT_RE.test(m));
      return (chat.length ? chat : list).slice().sort()[0];
    }
    for (const key of Object.keys(this.alias)) return this.alias[key];
    return null;
  }

  /**
   * 该渠道能否提供某个逻辑模型：
   * - 已知模型列表非空（白名单或自动发现成功）=> 严格匹配，避免聚合渠道抢走专属模型
   * - 一无所知（自动发现失败）=> 乐观放行，交给上游自己报错
   */
  supports(model) {
    if (this.isUnsupported(model)) return false;
    if (this.alias[model]) return true;
    if (this.models.size > 0) {
      if (this.models.has(model)) return true;
      for (const p of this.whitelist) {
        if (p === '*') return true;
        if (p.endsWith('*') && model.startsWith(p.slice(0, -1))) return true;
      }
      return false;
    }
    return true;
  }

  markSuccess(latencyMs) {
    this.total += 1;
    this.failures = 0;
    this.openUntil = 0;
    // 半开试探成功：立刻清掉半开态，恢复满份额（probeUntil=0 -> 不再是半开渠道）
    this.probeUntil = 0;
    this.lastOkAt = Date.now();
    this.lastError = null;
    this.lastCountedFailAt = 0;
    this.latency = this.latency ? this.latency * 0.8 + latencyMs * 0.2 : latencyMs;
  }

  /**
   * 只登记"这家这次没成"用于面板统计，**不参与熔断判定**：不涨 failures、不冷却、不影响选路。
   * 用于 embeddings 这类与 chat 熔断无关的调用路径——它们的失败不该把渠道打成不可用，
   * 但也不该伪装成 `rate_limit`（否则面板 lastError 会把 500 显示成"被限流"，误导排查）。
   */
  noteFailure(kind, message) {
    this.total += 1;
    this.failed += 1;
    this.lastFailAt = Date.now();
    this.lastError = `[${kind}] ${message}`;
  }

  /**
   * 记录一次失败。
   * @param {string} kind
   * @param {string} message
   * @param {{inFlight?: number}} opts 失败发生时该渠道的在途请求数。
   *   并发感知去抖：只有当失败发生在"并发批"里（inFlight > 1，说明还有一批请求可能同时撞上同一个上游故障）
   *   才启用时间窗去抖；串行的单发失败每次都计数，保证低频场景能正常熔断。
   */
  markFailure(kind, message, { inFlight = 1 } = {}) {
    const now = Date.now();
    this.total += 1;
    this.failed += 1;
    this.lastFailAt = now;
    this.lastError = `[${kind}] ${message}`;
    // TPM/429 限流：渠道本身是好的，只是暂时被限流——不计入熔断计数、不触发冷却（不降级），
    // 请求内由重试预算兜底（预算用尽直接换下一个 provider），限流恢复后立刻恢复可用
    if (kind === 'rate_limit') return;

    // 余额不足 / 鉴权失败：重试这家没有任何意义（每次都是同样结果），但必须把它冷却掉，
    // 否则每个请求都要先撞一遍死渠道。冷却到期后自动半开试探，充值/换 key 后自愈。
    if (kind === 'insufficient_balance' && this.routing.disableOnBalanceError !== false) {
      const ms = Math.max(1000, Number(this.routing.balanceCooldownMs) || 900000);
      this.cool(ms);
      this.failures += 1;
      log.warn(`上游余额不足，冷却 ${Math.round(ms / 1000)}s 并路由到下一家: ${message}`, this.name);
      return;
    }
    if (kind === 'auth' && this.routing.disableOnAuthError !== false) {
      const ms = Math.max(1000, Number(this.routing.authCooldownMs) || 600000);
      this.cool(ms);
      this.failures += 1;
      log.warn(`上游鉴权失败，冷却 ${Math.round(ms / 1000)}s 并路由到下一家: ${message}`, this.name);
      return;
    }

    // 高并发下一批请求会同时收到同一个上游故障。若每次都累加 failures，
    // 2 次阈值瞬间被打穿，且指数退避会直接顶到 maxCooldownMs（雪崩放大）。
    const dedupMs = Number(this.routing.failDedupMs) || 0;
    const dedupApplies = dedupMs > 0 && inFlight > 1;
    if (dedupApplies && this.lastCountedFailAt && now - this.lastCountedFailAt < dedupMs) {
      return;
    }
    this.lastCountedFailAt = now;
    this.failures += 1;
    if (this.failures >= this.routing.failThreshold) {
      // 指数退避但限制放大倍数：即使 failures 被并发打到很大，冷却也不会失控
      const growth = Math.min(this.failures - this.routing.failThreshold, 3);
      const backoff = Math.min(
        this.routing.cooldownMs * 2 ** growth,
        this.routing.maxCooldownMs,
      );
      this.openUntil = now + backoff;
      // 冷却到期后再留一个半开窗口（否则到期瞬间恢复满份额，死渠道会立刻拿回全部流量）
      const halfMs = Number(this.routing?.halfOpenMs);
      this.probeUntil = halfMs > 0 ? this.openUntil + halfMs : 0;
      log.warn(
        `熔断 ${Math.round(backoff / 1000)}s（连续失败 ${this.failures} 次${inFlight > 1 ? `，并发在途 ${inFlight}` : ''}）: ${this.lastError}`,
        this.name,
      );
    }
  }

  /**
   * 该模型是否仍在本渠道的黑名单里（过期即自动失效并顺手清理）。
   * 用 TTL 而不是"永久集合"：模型是否存在/是否被支持会随上游调整而变，
   * 一次误判不该需要重启进程才能恢复。
   */
  isUnsupported(model) {
    const until = this.unsupported.get(model);
    if (until === undefined) return false;
    if (until <= Date.now()) {
      this.unsupported.delete(model);
      return false;
    }
    return true;
  }

  markUnsupported(model) {
    if (!model) return;
    // 下限只用来挡掉 0/负数/NaN 这类无意义配置，不要把运维配的值悄悄改大
    // （否则"配了 5 实际生效 10"这种偏差很难查）。
    const ttl = Math.max(1000, Number(this.routing.unsupportedTtlMs) || 30 * 60 * 1000);
    // 先删后插：刷新它在 Map 里的顺序，配合下面的上限实现 LRU 淘汰
    this.unsupported.delete(model);
    this.unsupported.set(model, Date.now() + ttl);
    const max = Math.max(1, Number(this.routing.unsupportedMax) || 200);
    while (this.unsupported.size > max) {
      this.unsupported.delete(this.unsupported.keys().next().value);
    }
    const human = ttl >= 60000 ? `${Math.round(ttl / 60000)} 分钟` : `${Math.round(ttl / 1000)} 秒`;
    log.warn(`渠道不支持模型 ${model}，已从该模型的候选列表移除（${human}后自动重试）`, this.name);
  }

  /** 清除某模型（不传则全部）的黑名单；探活/发现拿到新模型列表后调用，让渠道能自愈 */
  clearUnsupported(model) {
    if (model === undefined) this.unsupported.clear();
    else this.unsupported.delete(model);
  }

  /** 当前仍生效的黑名单模型名（顺带清理过期项，供 /api/status 使用） */
  unsupportedModels() {
    const now = Date.now();
    const out = [];
    for (const [m, until] of this.unsupported) {
      if (until <= now) this.unsupported.delete(m);
      else out.push(m);
    }
    return out;
  }

  /**
   * 进入冷却；同时按下 routing.halfOpenMs 记下半开窗口的截止时间。
   * 冷却到期后不是立刻恢复满份额，而是先在 halfOpenMs 内只放行 1 个在途请求试探（见 manager 选路）。
   */
  cool(ms) {
    this.openUntil = Math.max(this.openUntil, Date.now() + ms);
    const halfMs = Number(this.routing?.halfOpenMs);
    this.probeUntil = halfMs > 0 ? this.openUntil + halfMs : 0;
  }

  uncool() {
    this.failures = 0;
    this.openUntil = 0;
    this.probeUntil = 0;
    this.lastCountedFailAt = 0;
  }

  toJSON() {
    return {
      name: this.name,
      protocol: this.protocol,
      baseUrl: this.baseUrl,
      priority: this.priority,
      enabled: this.enabled,
      keyMissing: this.keyMissing,
      healthy: this.healthy,
      coolingDown: this.coolingDown,
      coolRemainMs: this.coolRemainMs,
      halfOpen: this.halfOpen,
      probeUntil: this.probeUntil || null,
      failures: this.failures,
      latency: Math.round(this.latency),
      total: this.total,
      failed: this.failed,
      successRate: this.total ? Number((((this.total - this.failed) / this.total) * 100).toFixed(1)) : null,
      lastOkAt: this.lastOkAt || null,
      lastError: this.lastError,
      discovered: this.discovered,
      autoDiscover: this.autoDiscover,
      singleModel: this.singleModel,
      effort: this.effort,
      tier: this.tier,
      supportsThinking: this.supportsThinking,
      maxEffort: this.maxEffort,
      modelCount: this.models.size,
      models: [...this.models].slice(0, 200),
      unsupported: this.unsupportedModels(),
      preset: this.preset,
      chatPath: this.chatPath,
      modelsPath: this.modelsPath,
      opencodeFree: this.opencodeFree,
      forceStream: this.forceStream,
      workbuddySanitize: this.workbuddySanitize,
      refreshToken: this.refreshToken ? `<len=${this.refreshToken.length}>` : '',
      proxy: this.proxy,
      creditRemain: this.creditRemain,
      balanceUpdatedAt: this.balanceUpdatedAt || null,
      description: this.description,
      contextWindow: this.contextWindow,
      minMaxTokens: this.minMaxTokens,
      guardToolCallText: this.guardToolCallText,
      retries: this.retries,
    };
  }
}

export class ChannelManager {
  constructor(configPath) {
    this.configPath = configPath;
    this.channels = [];
    this.routing = { ...DEFAULT_ROUTING };
    this.config = {};
    this.routes = {};
    // 跨请求粘性：逻辑模型名 -> 最近一次成功使用的渠道名
    this.lastGood = new Map();
    // 会话亲和表：agentId -> { channel, at }
    this.affinity = new Map();
    // round-robin 的轮换游标：每排序一次优先池就前进一格（见 #orderPool）
    this.turn = 0;
    // 请求级游标记忆：requestId -> 该请求首次选路时的游标值。
    // 一次请求在 retryLoop 里会多次调用 candidatesFor，不记住就会多跳（见 #nextTurn）
    this.turnSeen = new Map();
    // 平滑加权轮询（strategy: weighted）的每渠道累计权重，键 = 渠道名
    this.weightState = new Map();
    // least-loaded 在未开渠道级限流时退化为 priority：只警告一次，避免每请求刷屏
    this.leastLoadedDegradeWarned = false;
    this.watcher = null;
    this.probeTimer = null;
    this.discoverTimer = null;
    this.gcTimer = null;
    this.userPresets = loadUserPresets();
    this.presets = resolvePresets(this.userPresets);
    // 并发闸门：全局 + 单渠道 + 单代理 三级限流，路由前 acquire、请求结束 release
    this.limiter = new GatewayLimiter({
      maxConcurrent: DEFAULT_ROUTING.maxConcurrent,
      maxConcurrentPerChannel: DEFAULT_ROUTING.maxConcurrentPerChannel,
      maxConcurrentPerAgent: DEFAULT_ROUTING.maxConcurrentPerAgent,
      queueTimeoutMs: DEFAULT_ROUTING.queueTimeoutMs,
      log,
    });
  }

  load() {
    const raw = readFileSync(this.configPath, 'utf8');
    const parsed = deepExpandEnv(JSON.parse(raw));

    // ---- 第一步：只在局部变量里完成构造与校验，完全不碰运行时状态 ----
    // 否则一旦构造失败或重名校验抛错，就会留下"routing / routes 已换成新配置、
    // 渠道列表还是旧的"这种半应用状态：运维只看到一句"热重载失败"，实际行为却已经变了。
    // 记录"用户在 config 里显式写过哪些 routing 键"。
    // 分层选路的两段式默认值（2 次 × 3s）只应作用于"完全没配置"的渠道；
    // 如果运维显式写了 attemptsPerChannel / retryPerAttemptMs，那是明确意图，必须原样生效。
    const routingExplicit = new Set(Object.keys(parsed.routing || {}));
    const routing = { ...DEFAULT_ROUTING, ...(parsed.routing || {}) };
    const routes = parsed.routes || {};
    const list = Array.isArray(parsed.channels) ? parsed.channels : [];
    const prev = new Map(this.channels.map((c) => [c.name, c]));

    const channels = list.map((raw2) => {
      const ch = new Channel(raw2, routing, this.presets);
      const old = prev.get(ch.name);
      // 同名渠道保留运行时状态，避免热重载丢失健康信息。
      // 模型列表例外：自动发现渠道沿用旧发现结果；固定白名单/单模型渠道以新配置为准（改了配置要生效）
      if (old && old.baseUrl === ch.baseUrl && old.protocol === ch.protocol) {
        if (ch.autoDiscover) {
          ch.models = old.models;
          ch.discovered = old.discovered;
        }
        ch.unsupported = old.unsupported;
        ch.failures = old.failures;
        ch.openUntil = old.openUntil;
        ch.probeUntil = old.probeUntil;
        ch.lastOkAt = old.lastOkAt;
        ch.lastFailAt = old.lastFailAt;
        ch.lastError = old.lastError;
        ch.latency = old.latency;
        ch.total = old.total;
        ch.failed = old.failed;
        // 余额：ctor 已从 config.json 读回，但内存里的刷新值比文件里的更新，优先保留
        if (old.creditRemain !== null && old.creditRemain !== undefined) ch.creditRemain = old.creditRemain;
        if (old.balanceUpdatedAt) ch.balanceUpdatedAt = old.balanceUpdatedAt;
        // 顺手清掉已过期的黑名单条目，避免它们跨重载一直累积
        ch.unsupportedModels();
      }
      return ch;
    });

    const names = channels.map((c) => c.name);
    const dup = names.filter((n, i) => names.indexOf(n) !== i);
    if (dup.length) throw new Error(`渠道名重复: ${[...new Set(dup)].join(', ')}`);

    // ---- 第二步：校验全部通过，才开始真正替换运行时状态 ----
    this.config = parsed;
    this.routingExplicit = routingExplicit;
    this.routing = routing;
    this.routes = routes;
    this.channels = channels;
    // 配置变更 = 重新评估选路，粘性记忆作废（避免配置改了还认旧渠道）
    this.lastGood.clear();
    // 选路内存态一并作废：请求级轮询游标记忆 + 加权累计权重（渠道集合/权重档可能已变）
    this.turnSeen.clear();
    this.weightState.clear();
    this.leastLoadedDegradeWarned = false;

    // 并发参数支持热重载
    this.limiter.configure({
      maxConcurrent: routing.maxConcurrent,
      maxConcurrentPerChannel: routing.maxConcurrentPerChannel,
      maxConcurrentPerAgent: routing.maxConcurrentPerAgent,
      queueTimeoutMs: routing.queueTimeoutMs,
    });
    this.limiter.prune(names);

    const conc = routing.maxConcurrent > 0
      ? `全局 ${routing.maxConcurrent}${routing.maxConcurrentPerChannel > 0 ? ` / 单渠道 ${routing.maxConcurrentPerChannel}` : ''}`
      : '不限制';
    log.info(`已加载 ${this.channels.length} 个渠道，路由策略 ${routing.strategy}，并发上限 ${conc}`);
    return this;
  }

  get enabledChannels() {
    return this.channels.filter((c) => c.enabled);
  }

  /** 记录某逻辑模型最近一次成功使用的渠道（跨请求粘性；失败/冷却会让它自然让位） */
  rememberSuccess(model, channelName) {
    if (!model) return;
    // 先删后插：刷新 LRU 顺序，并给它一个上界。
    // 客户端可以用任意模型名把这个表撑大（自动发现渠道的 supports() 全放行），
    // affinity 早就有 2000 条上限，这张表同样需要。
    this.lastGood.delete(model);
    this.lastGood.set(model, channelName);
    if (this.lastGood.size > 500) {
      this.lastGood.delete(this.lastGood.keys().next().value);
    }
  }

  /**
   * 返回本次请求的候选渠道列表，分两段（tier）：
   *
   *   第 1 段 preferred：按 priority 轮询（`priority` / `round-robin` / `least-loaded` 三种策略
   *     都只在这一段内生效），渠道内重试沿用渠道自己的 `retries`。默认是 sensenova / api.b.ai
   *     这类"额度充裕的好供应商"，用在最前面。
   *   第 2 段 fallback：preferred 全废之后才轮到。**上次成功的渠道钉在最前反复复用**；
   *     它一旦失败，本次请求里再按 `fallbackAttempts` 预算重试（默认 3 次 = 1 + 2 次重试，
   *     每次间隔 3s），全部失败才换下一家；无粘性记录时随机洗牌。失败 / 余额不足 /
   *     鉴权不过的渠道被冷却后自然让位。
   *
   * @param {{effort?:string|null, promptTokens?:number, agentId?:string|null, requestId?:string|null}} opts
   *  effort：请求带 reasoning_effort 时，优先路由到 effort 匹配的渠道
   *  promptTokens：请求 prompt 估算（token）；渠道 contextWindow 装不下时被过滤；
   *    全部装不下则退回全池尽力而为（由上游 502 + 重试循环兜底）
   *  agentId：子代理 / 会话标识；开启 sessionAffinity 时优先复用该代理上次成功的渠道
   *  requestId：请求级标识（可选）。round-robin 的轮换游标对同一 requestId 只前进一次，
   *    避免一次请求在 retryLoop 里多次选路把游标多跳；不传时保持旧行为（每次调用前进一格）
   *  池子总入口：model === config.unifiedModel 时，所有绑定了自己模型的启用渠道都是候选
   */
  candidatesFor(model, { effort = null, promptTokens = 0, agentId = null, requestId = null } = {}) {
    const explicit = this.routes[model] || this.routes['*'];
    const unified = this.config?.unifiedModel && model === this.config.unifiedModel;
    let pool = unified
      ? this.enabledChannels.filter((c) => c.defaultModel() && !c.isUnsupported(c.defaultModel()))
      : this.enabledChannels.filter((c) => c.supports(model));

    // 上下文感知路由：请求装不下的渠道直接排除（不做候选）；全排除则退回全池尽力而为
    if (promptTokens > 0) {
      const fitting = pool.filter((c) => !c.contextWindow || promptTokens < c.contextWindow);
      if (fitting.length) pool = fitting;
    }

    // routes 显式路由：完全按配置顺序，不做分层（配置意图优先）
    if (Array.isArray(explicit) && explicit.length) {
      const rank = new Map(explicit.map((n, i) => [n, i]));
      pool = pool.filter((c) => rank.has(c.name));
      pool.sort((a, b) => rank.get(a.name) - rank.get(b.name));
      return pool;
    }

    if (effort) {
      const matched = pool.filter((c) => c.effort === effort);
      const untagged = pool.filter((c) => !c.effort);
      if (matched.length || untagged.length) pool = [...matched, ...untagged];
      // matched 与 untagged 均空（渠道全有标记但没有这一档）-> 保持全池尽力而为
    }

    // 分层关闭时保持旧行为：整池一起排序
    if (this.routing.tiered === false) return this.#orderPool(pool, model, agentId, requestId);

    const preferred = pool.filter((c) => c.isPreferred);
    const fallback = pool.filter((c) => !c.isPreferred);
    const orderedPreferred = this.#orderPool(preferred, model, agentId, requestId);
    const orderedFallback = this.#orderFallback(fallback, agentId, model);
    return [...orderedPreferred, ...orderedFallback];
  }

  /** 第一段（preferred）与未分层模式的排序：strategy + 粘性 + 会话亲和 */
  #orderPool(pool, model, agentId, requestId = null) {
    const strategy = this.routing.strategy;
    let ordered;
    if (strategy === 'round-robin') {
      // 真·轮询：先按 priority 排好，再用"按请求递增的游标"轮换起点，把本段渠道均匀用起来。
      // 不能复用 a.rr：那是 lib/proxy.mjs 按"尝试次数"自增的，retries 高的渠道单请求就能 +6，
      // 会越用越偏；而且它只在优先级相同时才起作用——实测在"各家 priority 互不相同"的优先池里
      // 完全轮不动（永远第一家），正是"流量总在几个渠道打转"的直接原因。
      const sortFn = (a, b) => a.priority - b.priority || a.rr - b.rr || a.latency - b.latency;
      const open = pool.filter((c) => !c.coolingDown).sort(sortFn);
      const [live, saturated] = this.#splitHalfOpen(open);
      const cooling = pool.filter((c) => c.coolingDown).sort((a, b) => a.openUntil - b.openUntil);
      // 轮换只在**关闭 sticky 时**生效：sticky 会把起点钉死在"上次成功的那一家"，
      // 两者语义互斥（实测 round-robin + sticky 仍然全部打在同一个渠道上）。
      // 这样默认行为完全不变，想要真正摊开就配 `sticky:false`（会话亲和也要一并关掉）。
      // 游标按 requestId 前进：同一次请求（retryLoop 里多次选路）只前进一格；
      // 不传 requestId 时每次调用前进一格 = 旧行为。
      if (live.length > 1 && !this.routing.sticky) {
        const offset = this.#nextTurn(requestId) % live.length;
        ordered = [...live.slice(offset), ...live.slice(0, offset), ...cooling, ...saturated];
      } else {
        ordered = [...live, ...cooling, ...saturated];
      }
    } else if (strategy === 'least-loaded') {
      // 最少在途优先：多子代理并发时把请求摊开，避免全部扑向同一家；
      // 在途数相同时按优先级 + 平均延迟收尾（空闲时等价于按优先级排序）
      const channelLimitOn = this.limiter
        ? this.limiter.channelLimitEnabled
        : Number(this.routing.maxConcurrentPerChannel) > 0;
      let open;
      if (!channelLimitOn) {
        // 渠道级限流没开时 inFlightOf() 恒为 0，这个排序排不出任何东西（等于保持原顺序）。
        // 显式退化为 priority 排序并警告一次，免得运维以为"最少在途"真的在生效。
        if (!this.leastLoadedDegradeWarned) {
          this.leastLoadedDegradeWarned = true;
          log.warn('未开启渠道级限流（routing.maxConcurrentPerChannel=0），least-loaded 退化为 priority 排序');
        }
        open = pool.filter((c) => !c.coolingDown)
          .sort((a, b) => a.priority - b.priority || a.latency - b.latency);
      } else {
        const sortFn = (a, b) =>
          this.inFlightOf(a.name) - this.inFlightOf(b.name) ||
          a.priority - b.priority ||
          a.latency - b.latency;
        open = pool.filter((c) => !c.coolingDown).sort(sortFn);
      }
      const [live, saturated] = this.#splitHalfOpen(open);
      const cooling = pool.filter((c) => c.coolingDown).sort((a, b) => a.openUntil - b.openUntil);
      ordered = [...live, ...cooling, ...saturated];
    } else if (strategy === 'weighted') {
      // 优先级加权轮询：健康渠道按权重走平滑加权轮询（高优先多拿、低优先也参与，且交错出现），
      // 冷却中的垫后、半开窗口内已放满试探请求的放最末（都不剔除，保留兜底与试探）。
      // 模型级粘性对 weighted 不生效（否则份额被钉死在同一家，权重分配形同虚设），会话亲和仍生效。
      ordered = this.#orderWeighted(pool);
    } else {
      // 严格优先级降级链：健康渠道按 priority 排，**冷却中的整段垫到最后**（README「熔断中的排最后」）。
      // 早先只在"同优先级内"比较 coolingDown，而优先池里各家 priority 互不相同，于是刚熔断的
      // 渠道下个请求仍然第一个被打——熔断形同虚设；日志里约 22% 的请求都要先白撞一个刚失败的
      // 渠道才轮到健康渠道。垫到最后仍保留半开试探：健康渠道全部失败后依然会轮到它。
      const open = pool.filter((c) => !c.coolingDown)
        .sort((a, b) => a.priority - b.priority || a.latency - b.latency);
      const [live, saturated] = this.#splitHalfOpen(open);
      // 冷却段内部保持原来的 priority/latency 顺序（与改动前逐位一致，不改成按 openUntil 排）
      const cooling = pool.filter((c) => c.coolingDown)
        .sort((a, b) => a.priority - b.priority || a.latency - b.latency);
      ordered = [...live, ...cooling, ...saturated];
    }

    // 跨请求粘性：严格优先级模式下仅同优先级内生效（不把低优先级渠道顶到高优先级前面）；
    // least-loaded / weighted 模式下不做模型级粘性（会破坏负载摊开 / 权重分配），改由会话亲和负责
    if (this.routing.sticky) {
      const stickyName = this.lastGood.get(model);
      if (stickyName) {
        const idx = ordered.findIndex((c) => c.name === stickyName);
        if (idx > 0) {
          const s = ordered[idx];
          if (!s.coolingDown && s.failures === 0) {
            const front = ordered[0];
            const stickyOk = strategy === 'round-robin'
              || (strategy === 'priority' && front && front.priority === s.priority);
            if (stickyOk) ordered = [s, ...ordered.slice(0, idx), ...ordered.slice(idx + 1)];
          }
        }
      }
    }
    // 会话亲和：该代理上次成功的渠道若还在候选里，提到最前（但仍放在熔断渠道之前）。
    // 与 sticky 同样的约束：不允许把低优先级渠道顶到高优先级前面——
    // 否则同一 API key 的下一次不同模型请求会被上次的成功渠道劫持（跳过更高级渠道）。
    if (this.routing.sessionAffinity && agentId) {
      const aff = this.affinity.get(agentId);
      if (aff) {
        const age = Date.now() - aff.at;
        if (age < (this.routing.affinityTtlMs ?? 600000)) {
          const idx = ordered.findIndex((c) => c.name === aff.channel && !c.coolingDown);
          if (idx > 0) {
            const hit = ordered[idx];
            const front = ordered[0];
            if (front && hit.priority <= front.priority) {
              ordered.splice(idx, 1);
              ordered.unshift(hit);
            }
          }
        } else {
          this.affinity.delete(agentId);
        }
      }
    }
    return ordered.slice(0, Math.max(1, this.routing.maxAttempts));
  }

  /**
   * 第二段（fallback）排序：**成功过的渠道钉在最前反复使用**（"一个 provider 调通了就一直用
   * 它，失败才换"）；其余每次请求随机洗牌，健康的排前、冷却中的垫后。
   * fallbackShuffle=false 时其余部分按 priority。
   */
  #orderFallback(pool, agentId, model) {
    const shuffle = this.routing.fallbackShuffle !== false;
    const weighted = this.routing.strategy === 'weighted';
    const leastLoaded = this.routing.strategy === 'least-loaded';
    let open;
    let saturated;
    if (weighted) {
      // 加权：第二段同样按优先级权重交错（高优先多拿、低优先也参与），不再随机洗牌。
      // 冷却中的渠道不参与累积，并清掉旧账面（与 #orderWeighted 同一约定）
      for (const c of pool) if (c.coolingDown) this.weightState.delete(c.name);
      [open, saturated] = this.#orderWeightedOpen(pool.filter((c) => !c.coolingDown));
    } else {
      let cmp;
      if (leastLoaded) {
        // 最少在途优先（多子代理摊开负载）；在途相同再随机，避免并发全挤同一家
        cmp = (a, b) => this.inFlightOf(a.name) - this.inFlightOf(b.name)
          || (shuffle ? Math.random() - 0.5 : (a.priority - b.priority || a.latency - b.latency));
      } else if (shuffle) {
        cmp = () => Math.random() - 0.5;
      } else {
        cmp = (a, b) => a.priority - b.priority || a.latency - b.latency;
      }
      [open, saturated] = this.#splitHalfOpen([...pool.filter((c) => !c.coolingDown)].sort(cmp));
    }
    const cooling = [...pool.filter((c) => c.coolingDown)].sort((a, b) => a.openUntil - b.openUntil);

    // 模型级粘性："上次成功的 provider 钉到最前反复复用，失败过/冷却中就自动让位"。
    // least-loaded / weighted 策略下不做钉选——与 #orderPool 同样的约定：负载摊开（或权重分配）
    // 优先于渠道粘性，否则多子代理并发会被一把钉死在同一家（见 test-multiagent 的负载均衡用例）。
    if (this.routing.sticky && model && !leastLoaded && !weighted) {
      const stickyName = this.lastGood.get(model);
      if (stickyName) {
        const idx = open.findIndex((c) => c.name === stickyName && c.failures === 0);
        if (idx > 0) open.unshift(...open.splice(idx, 1));
      }
    }
    // 会话亲和仍以"浮到本段最前"的方式生效，避免同一子代理在随机池里反复跳家
    if (this.routing.sessionAffinity && agentId) {
      const aff = this.affinity.get(agentId);
      if (aff && Date.now() - aff.at < (this.routing.affinityTtlMs ?? 600000)) {
        const idx = open.findIndex((c) => c.name === aff.channel);
        if (idx > 0) open.unshift(...open.splice(idx, 1));
      }
    }
    // 半开窗口内已放满试探请求的渠道放最末（仍留在候选列表里，不是剔除）
    return [...open, ...cooling, ...saturated];
  }

  /**
   * 轮询游标：同一 requestId 只在首次选路时前进一格。
   * 一次请求在 retryLoop 里会多次调用 candidatesFor()，若每次都自增游标就会多跳
   * （重试多的请求把轮询份额吃掉，"均匀摊开"就失效了）。
   * 不传 requestId 时保持旧行为：每次调用前进一格。
   */
  #nextTurn(requestId) {
    if (requestId === undefined || requestId === null || requestId === '') return this.turn++;
    const seen = this.turnSeen.get(requestId);
    if (seen !== undefined) return seen;
    const turn = this.turn++;
    this.turnSeen.set(requestId, turn);
    // 上界：只记最近 1000 个请求，避免客户端伪造 requestId 把这本账撑爆
    while (this.turnSeen.size > 1000) this.turnSeen.delete(this.turnSeen.keys().next().value);
    return turn;
  }

  /**
   * 半开窗口内"试探名额已放满"的渠道：冷却到期后的 halfOpenMs 内，该渠道最多放行
   * halfOpenMaxInFlight 个在途请求做试探，其余请求跳过它（但不从候选里剔除）。
   */
  #halfOpenSaturated(ch) {
    if (!ch.probeUntil || ch.coolingDown) return false;
    if (Date.now() >= ch.probeUntil) return false;
    const maxInFlight = Math.max(1, Math.floor(Number(this.routing.halfOpenMaxInFlight)) || 1);
    return this.inFlightOf(ch.name) >= maxInFlight;
  }

  /** 把一组渠道按"半开是否占满"拆成 [可正常选, 半开占满]，保持组内相对顺序 */
  #splitHalfOpen(list) {
    const live = [];
    const saturated = [];
    for (const c of list) (this.#halfOpenSaturated(c) ? saturated : live).push(c);
    return [live, saturated];
  }

  /** 优先级加权轮询的整池排序：健康渠道按权重交错 + 冷却中的垫后 + 半开占满的放最末 */
  #orderWeighted(pool) {
    const cooling = pool.filter((c) => c.coolingDown).sort((a, b) => a.openUntil - b.openUntil);
    // 冷却中的渠道不参与权重累积，并把它的旧账面清掉（否则冷却期间攒下的负权重
    // 会让它到期后一时半会儿抢不到份额，"恢复满权重"就打了折扣）
    for (const c of cooling) this.weightState.delete(c.name);
    const [open, saturated] = this.#orderWeightedOpen(pool.filter((c) => !c.coolingDown));
    return [...open, ...cooling, ...saturated];
  }

  /**
   * 平滑加权轮询（smooth weighted round-robin）：给一组【健康】渠道排出"交错"顺序。
   *
   * 每轮给每个候选 `currentWeight += weight`，挑最大的作为首选并让它 `-= totalWeight`。
   * 这样输出是交错的（p10,p10,p20,p10,p50,…），而不是"前 8 个请求全是同一家"的突发；
   * 首选渠道就是本轮该用的渠道，所以首选份额长期收敛到 weight/total。
   *
   * 冷却中的渠道由调用方先剔除（不参与累计，避免带着旧账面醒来抢流量）；
   * 半开窗口内已放满试探名额的渠道同理，且会清掉累计权重（恢复后立刻满份额回归）。
   * 权重全为 0（配置误设）时退化为 priority 排序，避免除零 / 谁都选不出来。
   *
   * @returns {[Array, Array]} [加权交错顺序, 半开占满渠道]
   */
  #orderWeightedOpen(list) {
    const live = [];
    const saturated = [];
    for (const c of list) {
      if (this.#halfOpenSaturated(c)) {
        this.weightState.delete(c.name);
        saturated.push(c);
      } else {
        live.push(c);
      }
    }
    if (!live.length) return [live, saturated];
    const weight = (c) => this.#weightFor(c.priority);
    const total = live.reduce((sum, c) => sum + weight(c), 0);
    if (!(total > 0)) {
      return [live.slice().sort((a, b) => a.priority - b.priority || a.latency - b.latency), saturated];
    }
    for (const c of live) this.weightState.set(c.name, (this.weightState.get(c.name) || 0) + weight(c));
    let winner = null;
    let best = -Infinity;
    for (const c of live) {
      const cw = this.weightState.get(c.name);
      if (cw > best) { best = cw; winner = c; }
    }
    this.weightState.set(winner.name, best - total);
    const rest = live.filter((c) => c !== winner)
      .sort((a, b) => (this.weightState.get(b.name) || 0) - (this.weightState.get(a.name) || 0)
        || a.priority - b.priority
        || a.latency - b.latency);
    return [[winner, ...rest], saturated];
  }

  /**
   * 优先级 -> 权重。默认分档：<=10 -> 8，11-30 -> 4，31-60 -> 2，其余 -> 1。
   * routing.priorityWeights 可覆盖：键是优先级【上界】，取第一个 priority <= 键 的档
   * （形如 { "10": 8, "30": 4, "60": 2, "100": 1 }）；超出所有上界时用最后一个档。
   */
  #weightFor(priority) {
    const p = Number.isFinite(priority) ? Number(priority) : 100;
    const custom = this.routing.priorityWeights;
    if (custom && typeof custom === 'object' && !Array.isArray(custom)) {
      const tiers = Object.entries(custom)
        .map(([k, v]) => [Number(k), Number(v)])
        .filter(([k, v]) => Number.isFinite(k) && Number.isFinite(v))
        .sort((a, b) => a[0] - b[0]);
      if (tiers.length) {
        for (const [bound, w] of tiers) {
          if (p <= bound) return Math.max(0, w);
        }
        return Math.max(0, tiers[tiers.length - 1][1]);
      }
    }
    if (p <= 10) return 8;
    if (p <= 30) return 4;
    if (p <= 60) return 2;
    return 1;
  }

  /** 某渠道当前在途请求数（供"最少在途"选路使用） */
  inFlightOf(name) {
    return this.limiter?.channelInFlight(name) ?? 0;
  }

  /** 记录某子代理成功使用的渠道，供后续请求做会话亲和 */
  rememberAffinity(agentId, channelName) {
    if (!this.routing.sessionAffinity || !agentId || !channelName) return;
    this.affinity.set(agentId, { channel: channelName, at: Date.now() });
  }

  /** 清理过期的亲和记录，避免子代理不断新增导致 map 无限增长 */
  pruneAffinity() {
    const ttl = this.routing.affinityTtlMs ?? 600000;
    const now = Date.now();
    for (const [id, v] of this.affinity) {
      if (now - v.at > ttl) this.affinity.delete(id);
    }
    if (this.affinity.size > 2000) {
      // 极端情况：只保留最近的 1000 条
      const entries = [...this.affinity.entries()].sort((a, b) => b[1].at - a[1].at).slice(0, 1000);
      this.affinity = new Map(entries);
    }
  }

  /** 聚合所有可用逻辑模型名 */
  availableModels() {
    const set = new Set();
    for (const c of this.enabledChannels) {
      for (const m of c.models) set.add(m);
      for (const a of Object.keys(c.alias)) set.add(a);
    }
    // 乐观渠道（无白名单）没有可枚举模型，至少保证不为空
    if (!set.size) for (const c of this.enabledChannels) if (c.autoDiscover) set.add('*');
    return [...set].sort();
  }

  /**
   * 拉取各渠道的模型列表
   * @param {boolean} force    true 时连已配白名单的渠道也一起拉
   * @param {string|null} channel 只拉指定渠道
   * @returns {Promise<Array<{name:string,ok:boolean,count:number,error?:string}>>}
   */
  async discoverAll({ force = false, quiet = false, channel = null } = {}) {
    // 单模型渠道绑定固定模型，永远不参与模型拉取（拉来的列表会污染绑定）
    let targets = this.enabledChannels.filter((c) => !c.singleModel && (c.autoDiscover || force));
    if (channel) targets = targets.filter((c) => c.name === channel);

    return Promise.all(
      targets.map(async (ch) => {
        const result = { name: ch.name, ok: false, count: 0 };
        try {
          const { url, headers } = ch.adapter.buildModelsRequest(ch);
          const ac = new AbortController();
          const timer = setTimeout(() => ac.abort(), 15000);
          const sendFetch = ch.proxy ? (await import('./outbound-proxy.mjs')).gwFetch : fetch;
          const res = await sendFetch(url, { headers, signal: ac.signal, ...(ch.proxy ? { proxy: ch.proxy } : {}) });
          clearTimeout(timer);
          if (!res.ok) {
            result.error = `HTTP ${res.status}`;
            if (!quiet) log.warn(`模型拉取失败 HTTP ${res.status}`, ch.name);
            return result;
          }
          const json = await res.json();
          const models = ch.adapter.parseModels(json);
          if (!models || !models.length) {
            result.error = '上游返回空列表';
            if (!quiet) log.warn('模型拉取返回空列表', ch.name);
            return result;
          }
          ch.models = new Set(models);
          ch.discovered = true;
          ch.lastOkAt = ch.lastOkAt || Date.now();
          result.ok = true;
          result.count = models.length;
          if (!quiet) log.ok(`拉取到 ${models.length} 个模型`, ch.name);
          return result;
        } catch (err) {
          result.error = err.name === 'AbortError' ? '超时(15s)' : err.message;
          if (!quiet) log.warn(`模型拉取失败: ${result.error}`, ch.name);
          return result;
        }
      }),
    );
  }

  /**
   * 把已拉取到的模型列表写回 config.json 的 channels[].models
   * 写回后该渠道变成固定白名单（不再自动发现）；想恢复自动发现把 models 改成 []
   * @returns {{changed:number, file:string}}
   */
  saveDiscoveredModels({ only = null } = {}) {
    const file = this.configPath;
    let raw;
    try {
      raw = JSON.parse(readFileSync(file, 'utf8'));
    } catch (err) {
      throw new Error(`读取配置失败: ${err.message}`);
    }
    if (!Array.isArray(raw.channels)) return { changed: 0, file };

    let changed = 0;
    for (const ch of this.channels) {
      if (!ch.discovered || !ch.models.size) continue;
      if (only && ch.name !== only) continue;
      const cfgCh = raw.channels.find((c) => c && c.name === ch.name);
      if (!cfgCh) continue;
      const list = [...ch.models].sort();
      if (JSON.stringify(cfgCh.models || []) !== JSON.stringify(list)) {
        cfgCh.models = list;
        changed += 1;
      }
    }
    if (changed) writeFileSync(file, JSON.stringify(raw, null, 2) + '\n', 'utf8');
    return { changed, file };
  }

  /**
   * 新增渠道：写回 config.json 并热重载。支持 preset 引用或手填 baseUrl
   * @returns 新渠道的 toJSON 快照
   */
  addChannel(input = {}) {
    const file = this.configPath;
    const raw = JSON.parse(readFileSync(file, 'utf8'));
    if (!Array.isArray(raw.channels)) raw.channels = [];

    const name = String(input.name || '').trim();
    if (!name) throw new Error('缺少渠道名 name');
    if (raw.channels.some((c) => c && c.name === name)) throw new Error(`渠道名 "${name}" 已存在`);

    const entry = { name };
    if (input.preset) {
      entry.preset = String(input.preset).trim();
      if (!this.presets[entry.preset]) throw new Error(`预设 "${entry.preset}" 不存在（--list-presets 查看）`);
    } else {
      if (!input.baseUrl) throw new Error('未选 preset 时必须填 baseUrl');
      entry.protocol = String(input.protocol || 'openai').toLowerCase();
      entry.baseUrl = String(input.baseUrl).trim();
    }
    const key = String(input.apiKey ?? '').trim();
    if (key) entry.apiKey = key;
    if (input.model && String(input.model).trim()) entry.model = String(input.model).trim();
    if (Array.isArray(input.models) && input.models.length) {
      entry.models = input.models.map((m) => String(m)).filter(Boolean);
    }
    if (input.headers && typeof input.headers === 'object') {
      const hdrs = Object.fromEntries(
        Object.entries(input.headers).filter(([, v]) => v !== undefined && v !== null && String(v) !== ''),
      );
      if (Object.keys(hdrs).length) entry.headers = hdrs;
    }
    if (String(input.refreshToken || '').trim()) entry.refreshToken = String(input.refreshToken).trim();
    if (String(input.maxEffort || '').trim()) entry.maxEffort = String(input.maxEffort).trim().toLowerCase();
    if (input.forceStream === true) entry.forceStream = true;
    if (input.workbuddySanitize === true) entry.workbuddySanitize = true;
    if (String(input.proxy || '').trim()) entry.proxy = String(input.proxy).trim();
    if (input.effort) entry.effort = String(input.effort).toLowerCase();
    if (Number.isFinite(Number(input.priority))) entry.priority = Number(input.priority);
    if (input.description) entry.description = String(input.description);

    // 预检：用真实构造器验证配置合法（协议、preset 展开等），避免把坏配置写进文件
    const probe = new Channel({ ...entry }, this.routing, this.presets);
    if (!probe.apiKey) entry.enabled = false; // 没 key 先停用，填上 key 后热重载自动启用

    raw.channels.push(entry);
    writeFileSync(file, JSON.stringify(raw, null, 2) + '\n', 'utf8');
    this.load();
    const added = this.channels.find((c) => c.name === name);
    log.ok(`新增渠道 ${name}${probe.apiKey ? '' : '（未填 key，已停用）'}`);
    return added ? added.toJSON() : null;
  }

  /** 删除渠道：写回 config.json 并热重载；顺带清理 routes 里对它的引用 */
  removeChannel(name) {
    const file = this.configPath;
    const raw = JSON.parse(readFileSync(file, 'utf8'));
    const before = (raw.channels || []).length;
    raw.channels = (raw.channels || []).filter((c) => c && c.name !== name);
    if (raw.channels.length === before) throw new Error(`渠道 "${name}" 不存在`);
    if (raw.routes && typeof raw.routes === 'object') {
      for (const k of Object.keys(raw.routes)) {
        if (Array.isArray(raw.routes[k])) raw.routes[k] = raw.routes[k].filter((n) => n !== name);
      }
    }
    writeFileSync(file, JSON.stringify(raw, null, 2) + '\n', 'utf8');
    this.load();
    log.ok(`已删除渠道 ${name}`);
  }

  /**
   * 窄写单个渠道的配置字段（不整行覆盖，避免冲掉并发热重载期间的其它字段）。
   * 用于运行时回写 apiKey（token 续期）/ creditRemain（余额刷新）等派生字段。
   * @returns {boolean} 是否有字段真正变化并落盘
   */
  updateChannelFields(name, fields) {
    const file = this.configPath;
    const raw = JSON.parse(readFileSync(file, 'utf8'));
    const ch = (raw.channels || []).find((c) => c && c.name === name);
    if (!ch) throw new Error(`渠道 "${name}" 不存在`);
    let changed = false;
    for (const [k, v] of Object.entries(fields)) {
      if (v === undefined) continue;
      if (JSON.stringify(ch[k]) !== JSON.stringify(v)) {
        ch[k] = v;
        changed = true;
      }
    }
    if (changed) writeFileSync(file, JSON.stringify(raw, null, 2) + '\n', 'utf8');
    return changed;
  }

  /** 是否存在 WorkBuddy 国际版渠道（决定余额定时器是否空转） */
  hasWorkbuddyChannels() {
    return this.channels.some((c) => c.preset === 'workbuddy-intl');
  }

  /**
   * WorkBuddy 国际版后台任务（每小时）：token 续期 + 余额刷新（无签到）。
   * 逐渠道独立失败：token 续期失败不阻塞余额，余额失败不阻塞其它渠道。
   */
  async workbuddyTick() {
    const wb = await import('./workbuddy.mjs');
    for (const ch of this.channels) {
      if (ch.preset !== 'workbuddy-intl' || !ch.enabled || !ch.apiKey) continue;
      const h = ch.headers || {};
      const proxy = ch.proxy;
      try {
        let apiKey = ch.apiKey;
        let newRefreshToken = ch.refreshToken;
        // 1) token 续期（有 refresh_token 才续；失败不阻塞余额）
        if (ch.refreshToken) {
          try {
            const r = await wb.refreshAccountToken({
              accessToken: ch.apiKey,
              refreshToken: ch.refreshToken,
              user_id: h['X-User-Id'] || h['x-user-id'] || '',
              enterprise_id: h['X-Enterprise-Id'] || h['x-enterprise-id'] || '',
              domain: h['X-Domain'] || h['x-domain'] || '',
              proxy,
            });
            apiKey = r.accessToken;
            if (r.refreshToken) newRefreshToken = r.refreshToken;
          } catch (err) {
            log.warn(`WorkBuddy token 续期失败（不影响余额）: ${err?.message || err}`, ch.name);
          }
        }
        // 2) 余额刷新（失败不阻塞）
        let remain = null;
        try {
          remain = await wb.queryBalance({
            accessToken: apiKey,
            user_id: h['X-User-Id'] || h['x-user-id'] || '',
            enterprise_id: h['X-Enterprise-Id'] || h['x-enterprise-id'] || '',
            domain: h['X-Domain'] || h['x-domain'] || '',
            proxy,
          });
        } catch (err) {
          log.warn(`WorkBuddy 余额刷新失败: ${err?.message || err}`, ch.name);
        }
        const fields = {};
        if (apiKey && apiKey !== ch.apiKey) {
          fields.apiKey = apiKey;
          ch.apiKey = apiKey;
        }
        if (newRefreshToken && newRefreshToken !== ch.refreshToken) {
          fields.refreshToken = newRefreshToken;
          ch.refreshToken = newRefreshToken;
        }
        if (typeof remain === 'number') {
          fields.creditRemain = remain;
          ch.creditRemain = remain;
          ch.balanceUpdatedAt = Date.now();
          fields.balanceUpdatedAt = ch.balanceUpdatedAt;
        }
        if (Object.keys(fields).length) {
          try { this.updateChannelFields(ch.name, fields); } catch (err) {
            log.warn(`WorkBuddy 字段回写失败（内存态已生效）: ${err?.message || err}`, ch.name);
          }
        }
        if (typeof remain === 'number') {
          log.info(`WorkBuddy 余额刷新: ${remain}`, ch.name);
        }
      } catch (err) {
        log.warn(`WorkBuddy 后台任务失败: ${err?.message || err}`, ch.name);
      }
    }
  }

  /**
   * 探活：解除已过冷却期的熔断，并提前恢复仍可用的渠道。
   *
   * 关键约束（对应"探活成功或失败后 agent 请求被置之不理"）：
   *   1. 探活自身永远不能抛异常（否则定时器回调里的异常会中断后续渠道的恢复）；
   *   2. 探活必须并发有界 + 有硬超时，绝不能把事件循环/连接池占死，
   *      导致同时在跑用户请求迟迟拿不到上游。
   * @param {{force?:boolean}} opts force=true 时忽略"最近刚探过"的节流，立即全量探一次
   */
  async probe({ force = false } = {}) {
    if (this.probing) return this.probing;      // 同一时间只允许一轮探活
    this.probing = this.#probeAll(force).finally(() => { this.probing = null; });
    return this.probing;
  }

  async #probeAll(force) {
    const now = Date.now();
    const targets = this.enabledChannels.filter(
      (c) => c.coolingDown || force || now - c.lastOkAt > 5 * 60 * 1000,
    );
    if (!targets.length) return { probed: 0, ok: 0, failed: 0 };

    const concurrency = Math.max(1, Number(this.routing.probeConcurrency) || 4);
    let cursor = 0;
    let okCount = 0;
    let failCount = 0;

    const worker = async () => {
      for (;;) {
        const i = cursor++;
        if (i >= targets.length) return;
        const ch = targets[i];
        try {
          // 熔断中的渠道必须用真实 chat 接口探活（models 接口 200 ≠ chat 可用：
          // TPM 429、chat 500、key 仅对 chat 失效等场景下，models 探活会误把死渠道提前解融，
          // 请求反复打向打不通的渠道 = "探活解除熔断后 agent 一直调用不到"）
          let ok;
          if (ch.coolingDown) {
            ok = await this.probeChannelChat(ch);
            if (ok === null) ok = await this.probeChannelModels(ch); // 无绑定模型 -> 退回 models 探活
          } else if (ch.forceStream) {
            // 只认流式且无标准 /models 的渠道（WorkBuddy 国际版等）：例行探活也用 chat（流式）
            ok = await this.probeChannelChat(ch);
            if (ok === null) ok = true; // 无绑定模型无法 chat 探活：不误判失败
          } else {
            ok = await this.probeChannelModels(ch);
          }
          if (ok) {
            if (ch.coolingDown) log.ok('探活成功，解除熔断', ch.name);
            ch.uncool();
            ch.lastOkAt = Date.now();
            okCount += 1;
          } else {
            ch.cool(Math.min(this.routing.cooldownMs, 60000));
            failCount += 1;
          }
        } catch (err) {
          // 探活异常只影响这一家渠道，绝不能冒泡打断整轮
          failCount += 1;
          log.warn(`探活异常: ${err?.message || err}`, ch.name);
          try { ch.cool(Math.min(this.routing.cooldownMs, 60000)); } catch { /* ignore */ }
        }
      }
    };

    await Promise.all(Array.from({ length: Math.min(concurrency, targets.length) }, worker));
    return { probed: targets.length, ok: okCount, failed: failCount };
  }

  /** chat 接口探活：用渠道绑定模型发一个极小请求，2xx 才算健康；404 且像模型不存在则标记剔除该模型 */
  async probeChannelChat(ch) {
    const chatModel = ch.defaultModel();
    if (!chatModel) return null; // 无绑定模型 -> 调用方回退到 models 探活
    const probeBody = {
      messages: [{ role: 'user', content: 'ping' }],
      max_tokens: Math.max(4, ch.minMaxTokens ?? 1),
      stream: false,
    };
    const { url, headers, payload } = ch.adapter.buildRequest({ channel: ch, model: chatModel, body: probeBody, stream: false });
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), Math.min(15000, Number(this.routing.probeTimeoutMs) || 15000));
    try {
      const sendFetch = ch.proxy ? (await import('./outbound-proxy.mjs')).gwFetch : fetch;
      const res = await sendFetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: ac.signal,
        ...(ch.proxy ? { proxy: ch.proxy } : {}),
      });
      if (res.ok) return true;
      const text = await readAllText(res.body, 8192);
      // 探活也走同一套错误分类：余额不足/鉴权失败要长冷却，不要 60s 后又被拉起来打
      const info = classifyUpstreamFailure(res.status, text);
      if (info.kind === 'insufficient_balance' || info.kind === 'auth') {
        ch.markFailure(info.kind, `探活 HTTP ${res.status} ${truncate(text, 160)}`);
        return false;
      }
      if (res.status === 404 && /model|模型|not\s*found|不存在|no\s*such/i.test(text)) {
        log.warn(`探活发现渠道不支持模型 ${chatModel}，已从候选剔除`, ch.name);
        ch.markUnsupported(chatModel);
      }
      return false;
    } catch {
      return false; // 超时 / 网络错误 -> 保持熔断
    } finally {
      clearTimeout(timer);
    }
  }

  /** models 接口探活（仅用于健康渠道的例行刷新，便宜） */
  async probeChannelModels(ch) {
    const { url, headers } = ch.adapter.buildModelsRequest(ch);
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 10000);
    try {
      const sendFetch = ch.proxy ? (await import('./outbound-proxy.mjs')).gwFetch : fetch;
      const res = await sendFetch(url, { headers, signal: ac.signal, ...(ch.proxy ? { proxy: ch.proxy } : {}) });
      return res.ok;
    } finally {
      clearTimeout(timer);
    }
  }

  startTimers() {
    this.stopTimers();
    if (this.routing.probeIntervalMs > 0) {
      this.probeTimer = setInterval(() => this.probe().catch(() => {}), this.routing.probeIntervalMs);
      this.probeTimer.unref?.();
    }
    if (this.routing.discoverIntervalMs > 0) {
      this.discoverTimer = setInterval(
        () => this.discoverAll({ quiet: true }).catch(() => {}),
        this.routing.discoverIntervalMs,
      );
      this.discoverTimer.unref?.();
    }
    // WorkBuddy 国际版：每小时刷新余额 + 续期 token（无签到）；首轮延迟 30s 让服务先就绪
    if (this.balanceTimer) clearInterval(this.balanceTimer);
    this.balanceTimer = setInterval(() => {
      if (this.hasWorkbuddyChannels()) this.workbuddyTick().catch(() => {});
    }, 60 * 60 * 1000);
    this.balanceTimer.unref?.();
    this.balanceBootTimer = setTimeout(() => {
      if (this.hasWorkbuddyChannels()) this.workbuddyTick().catch(() => {});
    }, 30 * 1000);
    this.balanceBootTimer.unref?.();
    // 子代理会话会不断新增，定期回收无活动的信号量与亲和记录，避免 map 无限增长
    this.gcTimer = setInterval(() => {
      this.pruneAffinity();
      this.limiter.pruneAgents(Math.max(this.routing.affinityTtlMs ?? 600000, 5 * 60 * 1000));
    }, 5 * 60 * 1000);
    this.gcTimer.unref?.();
  }

  stopTimers() {
    if (this.probeTimer) clearInterval(this.probeTimer);
    if (this.discoverTimer) clearInterval(this.discoverTimer);
    if (this.gcTimer) clearInterval(this.gcTimer);
    if (this.balanceTimer) clearInterval(this.balanceTimer);
    if (this.balanceBootTimer) clearTimeout(this.balanceBootTimer);
    this.probeTimer = this.discoverTimer = this.gcTimer = null;
    this.balanceTimer = this.balanceBootTimer = null;
  }

  watchConfig(onReload) {
    if (this.watcher) return;
    let pending = null;
    this.watcher = watch(this.configPath, () => {
      clearTimeout(pending);
      pending = setTimeout(async () => {
        try {
          this.load();
          log.ok('配置已热重载');
          await this.discoverAll({ quiet: true });
          onReload?.();
        } catch (err) {
          log.error(`热重载失败: ${err.message}`);
        }
      }, 300);
    });
  }

  /**
   * 模型池视图：按逻辑模型聚合所有候选渠道
   * 返回 [{ model, channels: [{name, priority, healthy, coolingDown, coolRemainMs, singleModel}] }]
   */
  pool() {
    const pool = {};
    for (const c of this.enabledChannels) {
      const names = new Set([...c.models, ...Object.keys(c.alias)]);
      for (const m of names) {
        pool[m] ??= [];
        pool[m].push({
          name: c.name,
          priority: c.priority,
          healthy: c.healthy,
          coolingDown: c.coolingDown,
          coolRemainMs: c.coolRemainMs,
          singleModel: c.singleModel,
          effort: c.effort,
          protocol: c.protocol,
          latency: Math.round(c.latency),
        });
      }
    }
    // 每个模型下按优先级 + 延迟排序，和 candidatesFor 一致
    for (const m of Object.keys(pool)) {
      pool[m].sort((a, b) => a.priority - b.priority || a.latency - b.latency);
    }
    return Object.entries(pool)
      .map(([model, channels]) => ({ model, channels }))
      .sort((a, b) => a.model.localeCompare(b.model));
  }

  snapshot() {
    const conc = this.limiter.stats();
    return {
      routing: this.routing,
      models: this.availableModels(),
      pool: this.pool(),
      channels: this.channels.map((c) => ({
        ...c.toJSON(),
        inFlight: conc.channels[c.name]?.active ?? 0,
      })),
      routes: this.routes,
      concurrency: conc,
      // 子代理概览：谁在用、各自多少在途、被亲和到了哪家
      agents: this.agentsView(),
    };
  }

  /**
   * 子代理视图：把"哪个子代理 → 哪个渠道 → 多少在途"聚合出来，
   * 供 /api/status 和面板展示。多子代理并发时这是回答"谁走了哪家"的关键。
   */
  agentsView() {
    const out = [];
    for (const [id, sem] of this.limiter.agents) {
      const aff = this.affinity.get(id);
      out.push({
        id,
        inFlight: sem.active,
        queued: sem.pending,
        limit: this.limiter.agentLimit || null,
        affinity: aff ? { channel: aff.channel, ageMs: Date.now() - aff.at } : null,
      });
    }
    out.sort((a, b) => b.inFlight - a.inFlight || a.id.localeCompare(b.id));
    return out;
  }
}

export function resolveConfigPath(custom) {
  // 显式指定的配置路径写错时必须**报错**，不能静默回落到别的文件：
  // 否则 `--config confg.json`（拼错一个字）会安静地跑起一份完全不相干的配置，
  // 表现是"改了配置却没生效"，排查起来极其费时。
  if (custom && !existsSync(custom)) {
    throw new Error(`指定的配置文件不存在: ${custom}`);
  }
  const candidates = [
    custom,
    process.env.GW_CONFIG,
    path.join(ROOT, 'config.json'),
  ].filter(Boolean);
  const chosen = candidates.find((p) => existsSync(p));
  if (chosen) return chosen;

  const example = path.join(ROOT, 'config.example.json');
  const target = path.join(ROOT, 'config.json');
  if (existsSync(example)) {
    copyFileSync(example, target);
    log.warn(`未找到 config.json，已从模板生成 ${target}`);
    return target;
  }
  return target;
}
