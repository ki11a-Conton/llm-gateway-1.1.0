// 并发闸门：带 FIFO 队列与超时的信号量，以及"全局 + 单渠道 + 单代理"三级限流器
// 零依赖、纯异步。作用是把同时在途的上游请求数控制在安全范围内：
//   - 避免把上游供应商打爆（触发对方限流，进而连锁熔断）
//   - 避免本地 socket / 文件描述符耗尽（EMFILE）
//   - 超额请求排队等待，而不是无限并发，从而在高负载下仍能稳定吞吐
//
// 三级限流的层级：
//   global  —— 全局在途上限（保护本机 + 所有上游）
//   channel —— 单渠道在途上限（保护某一家上游）
//   agent   —— 单代理（子代理 / 会话）在途上限（防止一个 agent 吃光配额饿死其它 agent）

/** 最简单的可超时信号量：limit 个槽位，FIFO 公平排队 */
export class Semaphore {
  constructor(limit, name = 'sem') {
    this.name = name;
    this.limit = Math.max(1, Number(limit) || 1);
    this.active = 0;
    this.queue = [];
    this.acquired = 0;
    this.queued = 0;
    this.timedOut = 0;
    this.maxQueue = 0;
    // 排队时长统计（P2 §2.4）：只统计"真正排过队并最终拿到许可"的那些申请。
    // 立即拿到许可（没排队）不计入——否则均值会被大量"0ms"稀释，看不出排队是不是变严重了。
    // 排队超时/被 drain 的等待者也不计入：它们没拿到许可，时长没有"排队代价"的含义。
    this.waitMsTotal = 0;
    this.waitMsMax = 0;
    this.waitedCount = 0;
  }

  get pending() {
    return this.queue.length;
  }

  get available() {
    return Math.max(0, this.limit - this.active);
  }

  /** 动态调整上限（只影响之后的新申请） */
  setLimit(limit) {
    const next = Math.max(1, Number(limit) || 1);
    if (next === this.limit) return false;
    const raised = next > this.limit;
    this.limit = next;
    // F7：调高上限要把等待者唤醒——槽位变多了，不能再让它们干等。
    // （调低上限不在这里处理：在途请求正常跑完，_handoff 会按新上限决定能否移交。）
    if (raised) this._pump();
    return true;
  }

  /** 把空出来的槽位按 FIFO 交给等待者（上限提高 / 有新空闲时调用） */
  _pump() {
    while (this.queue.length && this.active < this.limit) {
      const waiter = this.queue.shift();
      if (waiter.settled) continue;
      waiter.settled = true;
      if (waiter.timer) clearTimeout(waiter.timer);
      this.acquired += 1;
      this._settleWait(waiter);
      this.active += 1;
      waiter.resolve(this._releaseFn());
    }
  }

  /**
   * 申请一个槽位。返回 release 函数（幂等，必须调用）。
   * timeoutMs > 0 时，排队超时会以 code='CONCURRENCY_TIMEOUT' 的 Error 拒绝。
   */
  acquire(timeoutMs = 0) {
    // 有空闲槽且无人在排队 -> 直接占用（保证 FIFO 语义：不插队）
    if (this.active < this.limit && this.queue.length === 0) {
      this.active += 1;
      this.acquired += 1;
      return Promise.resolve(this._releaseFn());
    }

    this.queued += 1;
    const queuedAt = Date.now();
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, timer: null, settled: false, queuedAt };
      if (timeoutMs > 0) {
        // 注意：这里的定时器必须保持事件循环活跃（不 unref）——
        // 排队中的请求是"在途工作"，它应该让进程存活到超时被处理。
        waiter.timer = setTimeout(() => {
          if (waiter.settled) return;
          waiter.settled = true;
          const i = this.queue.indexOf(waiter);
          if (i >= 0) this.queue.splice(i, 1);
          this.timedOut += 1;
          const err = new Error(`并发排队超时（等待 >${timeoutMs}ms，${this.name} limit=${this.limit}）`);
          err.code = 'CONCURRENCY_TIMEOUT';
          err.kind = 'overloaded';
          reject(err);
        }, timeoutMs);
      }
      this.queue.push(waiter);
      if (this.queue.length > this.maxQueue) this.maxQueue = this.queue.length;
    });
  }

  /** 丢弃队列中所有等待者（用于关闭时优雅收尾） */
  drain(reason = '网关正在关闭，排队请求已取消') {
    const waiters = this.queue.splice(0, this.queue.length);
    for (const w of waiters) {
      if (w.settled) continue;
      w.settled = true;
      if (w.timer) clearTimeout(w.timer);
      const err = new Error(reason);
      err.code = 'CONCURRENCY_DRAINED';
      w.reject(err);
    }
    return waiters.length;
  }

  _releaseFn() {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this._handoff();
    };
  }

  /** 释放槽位：队首有等待者就把槽位直接移交（active 不变），否则 active-1 */
  _handoff() {
    // F7：上限被调低时，在途数可能仍高于新上限。此时释放**只应把 active 降下来**，
    // 不能把槽位移交给等待者——否则新上限形同虚设（"调低上限后仍继续放行新请求"）。
    if (this.active - 1 >= this.limit) {
      this.active = Math.max(0, this.active - 1);
      return;
    }
    for (;;) {
      const waiter = this.queue.shift();
      if (!waiter) {
        this.active = Math.max(0, this.active - 1);
        return;
      }
      if (waiter.settled) continue; // 已被超时清理，继续找下一个
      waiter.settled = true;
      if (waiter.timer) clearTimeout(waiter.timer);
      this.acquired += 1;
      this._settleWait(waiter);
      // active 保持不变：槽位从旧持有者移交给了这个等待者
      waiter.resolve(this._releaseFn());
      return;
    }
  }

  /**
   * 结算一次排队等待（P2 §2.4）：只在"真正拿到许可"这一刻调用。
   * 排队超时 / 被 drain 的等待者不调用——它们没拿到许可，时长也没有排队代价的含义。
   */
  _settleWait(waiter) {
    if (typeof waiter?.queuedAt !== 'number') return;
    const ms = Math.max(0, Date.now() - waiter.queuedAt);
    this.waitMsTotal += ms;
    if (ms > this.waitMsMax) this.waitMsMax = ms;
    this.waitedCount += 1;
  }

  stats() {
    return {
      limit: this.limit,
      active: this.active,
      pending: this.queue.length,
      available: this.available,
      acquired: this.acquired,
      queued: this.queued,
      timedOut: this.timedOut,
      maxQueue: this.maxQueue,
      // P2 §2.4：排队时长（只统计拿到许可的那些等待者）
      waitMsTotal: this.waitMsTotal,
      waitMsMax: this.waitMsMax,
      waitedCount: this.waitedCount,
    };
  }
}

/**
 * 三级限流器：全局上限 + 每渠道上限 + 每代理上限。
 * 0 / 未设置表示该级不限制。渠道级与代理级信号量惰性创建并复用。
 */
export class GatewayLimiter {
  constructor({
    maxConcurrent = 0,
    maxConcurrentPerChannel = 0,
    maxConcurrentPerAgent = 0,
    queueTimeoutMs = 0,
    log = null,
  } = {}) {
    this.globalLimit = Number(maxConcurrent) || 0;
    this.channelLimit = Number(maxConcurrentPerChannel) || 0;
    this.agentLimit = Number(maxConcurrentPerAgent) || 0;
    this.queueTimeoutMs = Math.max(0, Number(queueTimeoutMs) || 0);
    this.global = this.globalLimit > 0 ? new Semaphore(this.globalLimit, 'global') : null;
    this.channels = new Map();
    this.agents = new Map();
    this.log = log;
    this.overloads = 0;
    this.agentRejections = 0;
    // 代理信号量是动态创建的（子代理会不断新增），需要一个回收机制避免无限增长
    this.agentLastSeen = new Map();
  }

  get channelLimitEnabled() {
    return this.channelLimit > 0;
  }

  get agentLimitEnabled() {
    return this.agentLimit > 0;
  }

  /**
   * 热重载并发参数。只有上限真正变化时才重建信号量；
   * 重建会丢弃当前排队请求（它们会收到可重试的错误，由上层切渠道或客户端重试）。
   */
  configure({ maxConcurrent, maxConcurrentPerChannel, maxConcurrentPerAgent, queueTimeoutMs } = {}) {
    const nextGlobal = Number(maxConcurrent) || 0;
    const nextChannel = Number(maxConcurrentPerChannel) || 0;
    const nextAgent = Number(maxConcurrentPerAgent) || 0;
    const nextQueueTimeout = Math.max(0, Number(queueTimeoutMs) || 0);

    this.queueTimeoutMs = nextQueueTimeout;

    if (nextGlobal !== this.globalLimit) {
      const beforeGlobal = this.globalLimit;
      this.globalLimit = nextGlobal;
      if (nextGlobal <= 0) {
        const dropped = this.global ? this.global.drain('全局并发限制已关闭') : 0;
        if (dropped && this.log) this.log.warn(`全局并发限制已关闭，丢弃 ${dropped} 个排队请求`);
        this.global = null;
      } else if (this.global) {
        // F7（代码审查 2026-09-20）：**保留同一个信号量实例**。
        // 旧实现在上限变化时 new 一个新的，新实例 active=0 —— 于是在途请求从计数里消失：
        // 调低上限后仍会放行新请求（实际在途 3 条、上限已改成 1），指标还低报在途数。
        // setLimit 只改上限，active 与等待队列原地保留。
        this.global.setLimit(nextGlobal);
        if (this.log) this.log.warn(`全局并发上限 ${beforeGlobal} -> ${nextGlobal}（保留在途计数 ${this.global.active}）`);
      } else {
        this.global = new Semaphore(nextGlobal, 'global');
      }
    }

    if (nextChannel !== this.channelLimit) {
      const before = this.channelLimit;
      this.channelLimit = nextChannel;
      // F7：旧实现这里 drain + clear()，把所有渠道的在途计数一起抹掉。
      // 现在只改上限、保留实例（已删除渠道的残留由 prune() 清理）。
      for (const sem of this.channels.values()) {
        if (nextChannel > 0) sem.setLimit(nextChannel);
        else sem.drain('渠道并发上限已关闭');
      }
      if (this.log) this.log.warn(`渠道并发上限 ${before} -> ${nextChannel}（保留在途计数）`);
    }

    // 代理上限变化时不清空信号量：子代理是活着的会话，清空会打断正在跑的请求。
    // 只把新上限同步给已存在的信号量与之后创建的信号量。
    if (nextAgent !== this.agentLimit) {
      this.agentLimit = nextAgent;
      for (const sem of this.agents.values()) sem.setLimit(nextAgent);
    }
  }

  _channelSem(name) {
    if (!this.channelLimitEnabled) return null;
    let sem = this.channels.get(name);
    if (!sem) {
      sem = new Semaphore(this.channelLimit, `ch:${name}`);
      this.channels.set(name, sem);
    }
    return sem;
  }

  _agentSem(agentId) {
    if (!this.agentLimitEnabled || !agentId) return null;
    let sem = this.agents.get(agentId);
    if (!sem) {
      sem = new Semaphore(this.agentLimit, `agent:${agentId}`);
      this.agents.set(agentId, sem);
    }
    this.agentLastSeen.set(agentId, Date.now());
    return sem;
  }

  /**
   * 申请一次上游调用许可。返回 release 函数（幂等）。
   * 任一级排队超时都会拒绝，并回滚已获得的许可。
   * @param {string} channelName
   * @param {string|null} agentId 子代理 / 会话标识，null 时不走代理级限流
   */
  /**
   * @param {string} channelName
   * @param {string|null} agentId
   * @param {number} budgetMs F3：请求总预算的剩余毫秒数（Infinity = 不限）。
   *   排队等待不得超过它，否则"总预算"会被排队吃光却没人察觉。
   */
  async acquire(channelName, agentId = null, budgetMs = Infinity) {
    const cap = (ms) => (Number.isFinite(budgetMs) ? Math.max(0, Math.min(ms, budgetMs)) : ms);
    const agentSem = this._agentSem(agentId);
    const sem = this._channelSem(channelName);

    // F6（代码审查 2026-09-20）：**先拿"每代理自己的"名额，再抢全局/渠道名额**。
    // 旧顺序（global -> channel -> agent）有个坑：某个 agent 已超额时，它后续的排队请求
    // 已经先占住了 global（还可能占住 channel）名额——此时真正在跑的只有 1 个请求，
    // 但 global.active 已经是 2，别的 agent 哪怕用一个**空闲渠道**也进不来
    // （报告复现 F6_agent_starvation）。先拿 agent 名额后，积压请求在拿到自己名额前
    // 不占用任何共享容量，"等待请求不计为执行中的全局/渠道负载"。
    let relAgent = null;
    if (agentSem) {
      try {
        relAgent = await agentSem.acquire(cap(this.queueTimeoutMs));
      } catch (err) {
        if (err?.code === 'CONCURRENCY_TIMEOUT') {
          this.overloads += 1;
          this.agentRejections += 1;
        }
        throw err;
      }
    }

    // 全局排队超时是最严重的一类过载（连全局名额都排不到），必须和渠道/代理级一样计入 overloads。
    let relGlobal = null;
    try {
      relGlobal = this.global ? await this.global.acquire(cap(this.queueTimeoutMs)) : null;
    } catch (err) {
      relAgent?.();
      if (err?.code === 'CONCURRENCY_TIMEOUT') this.overloads += 1;
      throw err;
    }

    let relChannel = null;
    try {
      if (sem) relChannel = await sem.acquire(cap(this.queueTimeoutMs));
    } catch (err) {
      relGlobal?.();
      relAgent?.();
      if (err?.code === 'CONCURRENCY_TIMEOUT') this.overloads += 1;
      throw err;
    }

    return () => {
      relChannel?.();
      relGlobal?.();
      relAgent?.();
    };
  }

  /** 某渠道当前在途请求数（用于并发感知的失败去抖） */
  channelInFlight(name) {
    return this.channels.get(name)?.active ?? 0;
  }

  /** 某代理当前在途请求数 */
  agentInFlight(agentId) {
    return this.agents.get(agentId)?.active ?? 0;
  }

  /** 清理已不存在的渠道信号量，避免渠道删改后残留 */
  prune(validNames) {
    const keep = new Set(validNames);
    for (const name of this.channels.keys()) {
      if (!keep.has(name)) this.channels.delete(name);
    }
  }

  /**
   * 回收长时间无活动的代理信号量（子代理会话结束后不会主动注销）。
   * 只回收 active=0 且超过 ttlMs 没被访问过的，避免误删活跃代理。
   */
  pruneAgents(ttlMs = 30 * 60 * 1000) {
    const now = Date.now();
    let removed = 0;
    for (const [id, sem] of this.agents) {
      const seen = this.agentLastSeen.get(id) ?? 0;
      if (sem.active === 0 && now - seen > ttlMs) {
        this.agents.delete(id);
        this.agentLastSeen.delete(id);
        removed += 1;
      }
    }
    return removed;
  }

  /** 当前有在途请求的代理数（用于 /api/status 概览） */
  activeAgentCount() {
    let n = 0;
    for (const sem of this.agents.values()) if (sem.active > 0) n += 1;
    return n;
  }

  /** 关闭时丢弃所有排队请求 */
  drain(reason) {
    let n = this.global ? this.global.drain(reason) : 0;
    for (const sem of this.channels.values()) n += sem.drain(reason);
    for (const sem of this.agents.values()) n += sem.drain(reason);
    return n;
  }

  /**
   * 三级信号量的排队时长汇总（P2 §2.4）。
   * 一次 acquire 可能在多级各排一次队，所以这里把各级各自结算的等待时长相加：
   * 呈现的是"这个请求为了拿到许可一共等了多久"的保守上界，而不是重复计数。
   * 立即拿到许可的申请不计入（见 Semaphore#_settleWait）。
   */
  _waitTotals() {
    const sems = [this.global, ...this.channels.values(), ...this.agents.values()];
    let total = 0;
    let max = 0;
    let count = 0;
    for (const sem of sems) {
      if (!sem) continue;
      total += sem.waitMsTotal;
      if (sem.waitMsMax > max) max = sem.waitMsMax;
      count += sem.waitedCount;
    }
    return { total, max, count };
  }

  stats() {
    const channels = {};
    for (const [name, sem] of this.channels) channels[name] = sem.stats();
    const agents = {};
    for (const [id, sem] of this.agents) agents[id] = sem.stats();
    const wait = this._waitTotals();
    return {
      maxConcurrent: this.globalLimit,
      maxConcurrentPerChannel: this.channelLimit,
      maxConcurrentPerAgent: this.agentLimit,
      queueTimeoutMs: this.queueTimeoutMs,
      overloads: this.overloads,
      agentRejections: this.agentRejections,
      // P2 §2.4：排队时长（三级信号量汇总；面板 / P3 直接消费这三个字段）
      queueWaitMsTotal: wait.total,
      queueWaitMsMax: wait.max,
      waitedCount: wait.count,
      global: this.global ? this.global.stats() : null,
      channels,
      agents,
      agentCount: this.agents.size,
      activeAgentCount: this.activeAgentCount(),
    };
  }
}

/** 便于调用方统一识别"过载排队超时" */
export function isOverloadError(err) {
  return err?.code === 'CONCURRENCY_TIMEOUT';
}
