// 渠道内重试：同一个渠道的模型最多请求 N 次，每次失败后固定等待，全部失败才换下一家。
// 与外层"路由切换"配合，形成两层重试：
//   layer 1（本模块）：渠道内 attemptsPerChannel 次，间隔 retryPerAttemptMs
//   layer 2（proxy.mjs）：所有渠道都废了 -> 等 retryWaitMs 再从头轮询一遍（可选，受 retryMaxWaitMs 限制）

export const DEFAULT_CHANNEL_RETRY = {
  // 同一渠道模型的请求次数上限（1 次首发 + N-1 次重试）。渠道可用 retries 字段单独覆盖
  attemptsPerChannel: 6,
  // 渠道内每次失败后的等待毫秒数（未配置时回落到 routing.retryWaitMs）
  retryPerAttemptMs: 3000,
};

/**
 * 判断一次失败是否值得在"同一渠道内"重试。
 * - 客户端断开（client_abort）：不该重试
 * - 不可重试错误（鉴权 / 参数 / 模型不存在）：重试无意义，直接换家
 * - 其余（网络 / 超时 / 429 / 5xx / 排队过载）：可重试
 */
export function shouldRetryInChannel(err, attempt, maxAttempts) {
  if (!err) return false;
  if (err.kind === 'client_abort') return false;
  if (err.retryable === false) return false;
  return attempt < maxAttempts;
}

/**
 * 固定的失败等待（可用 abortSignal 在客户端断开时立刻结束等待）。
 * @returns {Promise<boolean>} true=等待完成，false=被中止
 */
export function waitBeforeRetry(ms, signal) {
  if (!(ms > 0)) return Promise.resolve(true);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener?.('abort', onAbort);
      resolve(true);
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      resolve(false);
    }
    if (signal?.aborted) {
      clearTimeout(timer);
      resolve(false);
      return;
    }
    signal?.addEventListener?.('abort', onAbort, { once: true });
  });
}
