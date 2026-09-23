/**
 * P5 —— 测试端口工具（零依赖，Node 内置模块）。
 *
 * 背景：此前每个测试套件都硬编码固定端口（8793-8813 / 8877-8878 / 9101-9184 /
 * 9241-9242 / 9251-9252 / 9270-9281 / 9300-9310），并行跑测试时互相抢端口，
 * 制造了大量"假失败"。这里统一改成运行时动态分配。
 *
 * 提供的 API：
 *   freePort()                  —— 拿一个空闲端口（listen(0) 后关闭）
 *   freePorts(n)                —— 拿 n 个互不相同的空闲端口
 *   freePortBlock(size)         —— 拿一段**连续**的空闲端口（绑全部再一起放掉）
 *   mockUpstreamPorts()         —— mock-upstream.mjs 的端口方案（连续块 + 平移）
 *   materializeConfig(src, …)   —— 把 .test.json 里的端口改写成动态值后写进临时目录
 *   cleanupConfigs()            —— 立刻删掉上面生成的临时配置（进程退出时也会自动删）
 *
 * 关于 mock-upstream.mjs 的平移规则：
 *   该 mock 的逻辑端口是 9101..9143（9101=正常上游、9102=永远 500、……、9143=思考档位）。
 *   设 MOCK_PORT_BASE=B 时，逻辑端口 p 实际监听在 `B + (p - 9101)`，即 B..B+42 这段连续端口。
 *   不设该环境变量时行为与改动前**逐位一致**（仍监听 9101..9143），所以其它工具/子代理
 *   单独直接跑 mock-upstream.mjs 不会受影响。
 *
 * 并发边界（务必知晓）：动态端口是"先申请、再交给子进程 bind"，两步之间天然有 TOCTOU 窗口。
 *   串行跑（`test/run-all.mjs` 的默认方式）完全安全；但若**同时**跑多个套件，两个进程的连续段
 *   理论上可能重叠，双方逐端口抢绑，网关就可能连到对方进程的 mock（表现为拿到无关载荷、
 *   或某个 handler 永久缺失）。为此 `test/mock-upstream.mjs` 在任何端口 EADDRINUSE 时
 *   **直接以非零码退出**，把"沉默的错误结论"变成"套件直接红"。要并发跑请给每个套件
 *   独立的 MOCK_PORT_BASE 区间。
 */
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';

/** mock-upstream.mjs 的基准（逻辑）端口 */
export const MOCK_REF_PORT = 9101;
/** mock-upstream.mjs 用到的最大（逻辑）端口 */
export const MOCK_MAX_PORT = 9143;
/** 需要连续保留的端口个数：9101..9143 共 43 个 */
export const MOCK_PORT_SPAN = MOCK_MAX_PORT - MOCK_REF_PORT + 1;

/** 本进程已经发出去过的端口，避免同一个测试里两次分配撞在一起 */
const handedOut = new Set();
/** 生成过的临时配置，进程退出时兜底清理 */
const tempConfigs = new Set();

let cleanupDone = false;
function cleanupAll() {
  if (cleanupDone) return;
  cleanupDone = true;
  for (const p of tempConfigs) {
    try { fs.rmSync(p, { force: true }); } catch { /* 忽略 */ }
  }
  tempConfigs.clear();
}
process.on('exit', cleanupAll);

/** 立刻删掉所有临时配置（测试自己也可以在 finally 里主动调一次） */
export function cleanupConfigs() {
  for (const p of tempConfigs) {
    try { fs.rmSync(p, { force: true }); } catch { /* 忽略 */ }
  }
  tempConfigs.clear();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 在指定端口上起一个监听 socket；成功返回 server，失败返回 null */
function bind(port) {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.unref();
    const onErr = () => { try { srv.close(); } catch { /* 忽略 */ } resolve(null); };
    srv.once('error', onErr);
    srv.listen({ port, host: '127.0.0.1', exclusive: true }, () => {
      srv.removeListener('error', onErr);
      resolve(srv);
    });
  });
}

function close(srv) {
  return new Promise((resolve) => {
    try { srv.close(() => resolve()); } catch { resolve(); }
  });
}

function portOf(srv) {
  const a = srv.address();
  return a && typeof a === 'object' ? a.port : 0;
}

/**
 * 从 start 开始向上找一个可用端口（跳过本进程已经发出去的）。
 * 为什么要"向上扫"而不是反复 `listen(0)`：Windows 的临时端口分配器是**按升序**回收的，
 * 一旦 `freePortBlock()` 释放了一段连续端口，之后每次 `listen(0)` 都会精确落回那段里的
 * 下一个端口——而它们全在 handedOut 里。只靠重试次数（< 段长 43）会**必然**耗尽并抛错
 * （这是 P5 联调时被抓到的真实坑）。向上扫一步就离开保留段，行为确定且有界。
 */
async function firstFreeFrom(start, span = 8192) {
  for (let p = start; p < start + span; p += 1) {
    if (handedOut.has(p)) continue;
    const s = await bind(p);
    if (!s) continue;
    await close(s);
    return p;
  }
  return null;
}

/**
 * 取一个空闲端口：`listen(0)` 拿一个起点，再向上扫到第一个既空闲、又没有发给本进程过的端口。
 * 同一个测试里不会重复，也不受调用顺序（先 freePort 还是先 mockUpstreamPorts）影响。
 * 第 t 轮把起点往后跳一段：Windows 的分配器总把刚释放的端口按升序还回来，
 * 只在原地重复扫会一直撞进同一片拥挤/系统保留区域。
 */
export async function freePort(tries = 24) {
  for (let i = 0; i < tries; i += 1) {
    const srv = await bind(0);
    if (!srv) { await sleep(10); continue; }
    const seed = portOf(srv);
    await close(srv);
    if (!seed) continue;
    const origin = Math.min(seed + i * 512, 64000);
    const port = await firstFreeFrom(origin, 1024);
    if (port === null) { await sleep(10); continue; }
    handedOut.add(port);
    return port;
  }
  throw new Error('freePort(): 连续多次都没拿到可用端口');
}

/** 取 n 个互不相同的空闲端口（顺序不保证，调用方按语义命名） */
export async function freePorts(n, tries = 40) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    for (let t = 0; t < tries; t += 1) {
      const p = await freePort();
      if (!out.includes(p)) { out.push(p); break; }
    }
  }
  if (out.length !== n) throw new Error(`freePorts(${n}): 只拿到 ${out.length} 个端口`);
  return out;
}

/**
 * 取一段**连续**空闲端口 [base, base+size-1]。
 * 做法：先 `listen(0)` 拿一个起点，再从这个起点向上找第一段"都没发出去过 +
 * 每一个都真的能绑住"的连续端口，全绑成功后一起关闭并把起点交出去。
 * Windows 上可能有系统保留端口段/瞬时被占用的段，绑不上就往上挪一格继续找；
 * 每轮（tries）把起点再往后跳一段，避免在同一片坏区域里反复失败。
 */
export async function freePortBlock(size, tries = 24) {
  if (!Number.isInteger(size) || size < 1) throw new Error('freePortBlock: size 必须是正整数');
  for (let t = 0; t < tries; t += 1) {
    const seedSrv = await bind(0);
    if (!seedSrv) { await sleep(10); continue; }
    const seed = portOf(seedSrv);
    await close(seedSrv);
    if (!seed) continue;

    const origin = Math.min(seed + t * 512, 64000);
    for (let start = origin; start < origin + 2048; start += 1) {
      if (handedOut.has(start)) continue; // 保留段：不绑，直接跳过（向上扫）
      const socks = [];
      let ok = true;
      for (let i = 0; i < size; i += 1) {
        const p = start + i;
        if (handedOut.has(p)) { ok = false; break; }
        const s = await bind(p);
        if (!s) { ok = false; break; }
        socks.push(s);
      }
      await Promise.all(socks.map(close));
      if (!ok) continue;
      for (let i = 0; i < size; i += 1) handedOut.add(start + i);
      return start;
    }
  }
  throw new Error(`freePortBlock(${size}): 找不到连续空闲端口段`);
}

/**
 * mock-upstream.mjs 的端口方案。
 *
 *   const mp = await mockUpstreamPorts();
 *   const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
 *   mp.url(9101)              // => 'http://127.0.0.1:<动态端口>'
 *   mp.url(9102, '/v1/models')// 带路径
 *   mp.port(9143)             // => 数字端口
 */
export async function mockUpstreamPorts() {
  const base = await freePortBlock(MOCK_PORT_SPAN);
  const port = (logical) => base + (logical - MOCK_REF_PORT);
  return {
    base,
    /** 直接丢给 spawn 的 env（保留原有环境变量） */
    env: { ...process.env, MOCK_PORT_BASE: String(base) },
    port,
    url: (logical, p = '') => `http://127.0.0.1:${port(logical)}${p}`,
    /** 逻辑端口 -> 实际端口 的映射表（materializeConfig 也会用同一套规则） */
    map: Object.fromEntries(
      Array.from({ length: MOCK_PORT_SPAN }, (_, i) => [MOCK_REF_PORT + i, base + i]),
    ),
  };
}

/** 把字符串里的 127.0.0.1:<port> 按 portMap（显式映射，优先）/ mock 平移规则改写 */
function shiftUrls(value, mockBase, portMap) {
  return value.replace(/127\.0\.0\.1:(\d+)/g, (whole, digits) => {
    const p = Number(digits);
    if (portMap && Object.prototype.hasOwnProperty.call(portMap, p)) return `127.0.0.1:${portMap[p]}`;
    if (mockBase && p >= MOCK_REF_PORT && p <= MOCK_MAX_PORT) return `127.0.0.1:${mockBase + (p - MOCK_REF_PORT)}`;
    return whole;
  });
}

function deepShift(value, mockBase, portMap) {
  if (typeof value === 'string') return shiftUrls(value, mockBase, portMap);
  if (Array.isArray(value)) return value.map((v) => deepShift(v, mockBase, portMap));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = deepShift(v, mockBase, portMap);
    return out;
  }
  return value;
}

/**
 * 把静态的 *.test.json 复制一份到临时目录，顺便把里面的端口改成动态值：
 *   - `server.port` → `port`（不传就保持原样）
 *   - 所有 `127.0.0.1:91xx`（9101..9143）→ 按 mockBase 平移
 *   - 所有出现在 `portMap` 里的端口（形如 `{ 9170: <动态端口> }`）→ 换成映射值（优先于平移）
 * 返回临时配置文件路径。文件会在进程退出时自动删除（也可手动 cleanupConfigs()）。
 *
 * 注意：配置里的相对路径（如 taskLog.dir）由 server.mjs 按**仓库根目录**解析，
 * 与配置文件放在哪里无关，所以搬到临时目录不会改变语义。
 */
export function materializeConfig(srcPath, { port, mockBase, portMap } = {}) {
  const raw = JSON.parse(fs.readFileSync(srcPath, 'utf8'));
  const cfg = (mockBase || portMap) ? deepShift(raw, mockBase, portMap) : raw;
  if (port !== undefined && port !== null) {
    cfg.server = { ...(cfg.server || {}), port };
  }
  const dir = path.join(os.tmpdir(), 'llm-gw-test-cfg');
  fs.mkdirSync(dir, { recursive: true });
  const out = path.join(
    dir,
    `${path.basename(srcPath, '.json')}-${process.pid}-${Math.random().toString(36).slice(2, 8)}.json`,
  );
  fs.writeFileSync(out, `${JSON.stringify(cfg, null, 2)}\n`);
  tempConfigs.add(out);
  return out;
}
