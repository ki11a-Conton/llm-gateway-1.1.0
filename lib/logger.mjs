// 轻量日志：无依赖、带颜色与渠道标签
// 高并发关键点：所有输出走"缓冲 + 批量写"，而不是每条一次 console.log。
// console.log 是同步写，QPS 高时每次系统调用都会阻塞事件循环，直接成为吞吐瓶颈。

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

const C = {
  reset: '\x1b[0m', dim: '\x1b[2m', gray: '\x1b[90m', red: '\x1b[31m',
  green: '\x1b[32m', yellow: '\x1b[33m', blue: '\x1b[34m', cyan: '\x1b[36m',
};

const useColor = process.env.NO_COLOR ? false : process.stdout.isTTY !== false;
const paint = (c, s) => (useColor ? `${c}${s}${C.reset}` : s);

let current = LEVELS[parseLevelName(process.env.GW_LOG_LEVEL)] ?? LEVELS.info;

function parseLevelName(v) {
  const n = String(v ?? '').toLowerCase();
  return Object.hasOwn(LEVELS, n) ? n : undefined;
}

function ts() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

// ---- 批量写缓冲 ----
const FLUSH_DELAY_MS = 20;  // 聚合窗口：这段时间内的日志合并成一次写
const BATCH_MAX = 500;      // 单批达到这个行数就立即 flush
const BUFFER_MAX = 5000;    // 缓冲上限，超过则丢弃并提示（保护内存）

let buffer = [];
let flushTimer = null;
let dropped = 0;

function flush() {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  if (!buffer.length && !dropped) return;
  if (dropped) {
    buffer.push(paint(C.yellow, `[log] 日志过载，已丢弃 ${dropped} 行`));
    dropped = 0;
  }
  const text = `${buffer.join('\n')}\n`;
  buffer = [];
  process.stdout.write(text);
}

function enqueue(line) {
  if (buffer.length >= BUFFER_MAX) {
    dropped += 1;
    return;
  }
  buffer.push(line);
  if (buffer.length >= BATCH_MAX) {
    flush();
    return;
  }
  if (!flushTimer) {
    // 该定时器只是把日志刷出去，不应阻止进程退出
    flushTimer = setTimeout(flush, FLUSH_DELAY_MS);
    flushTimer.unref?.();
  }
}

function emit(color, level, tag, msg) {
  const head = `${paint(C.dim, ts())} ${paint(color, level.padEnd(5))}`;
  const t = tag ? `${paint(C.cyan, '[' + tag + ']')} ` : '';
  enqueue(`${head} ${t}${msg}`);
}

export const log = {
  setLevel(name) {
    const lv = LEVELS[parseLevelName(name)];
    if (lv) current = lv;
  },
  get level() {
    return current;
  },
  debug: (msg, tag) => current <= LEVELS.debug && emit(C.gray, 'DEBUG', tag, msg),
  info: (msg, tag) => current <= LEVELS.info && emit(C.blue, 'INFO', tag, msg),
  ok: (msg, tag) => current <= LEVELS.info && emit(C.green, 'OK', tag, msg),
  warn: (msg, tag) => current <= LEVELS.warn && emit(C.yellow, 'WARN', tag, msg),
  error: (msg, tag) => current <= LEVELS.error && emit(C.red, 'ERRO', tag, msg),
  // 原始输出（启动 banner 等）：先清空缓冲保证顺序，再直接写
  raw: (msg) => {
    flush();
    process.stdout.write(`${msg}\n`);
  },
  // 主动刷出缓冲（关闭前调用，避免最后几行日志丢失）
  flush,
};

process.on('exit', () => {
  try {
    flush();
  } catch {
    /* 退出阶段忽略写入错误 */
  }
});
