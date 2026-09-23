/**
 * P6 验收（OPTIMIZATION-PLAN.md §P6）：出站代理的 abort 兑现 + CONNECT/TLS 覆盖
 *
 * 背景（P2 实测出来的真 bug）：lib/outbound-proxy.mjs 的 roundTrip 在"响应头解析完成"
 * 那一刻就摘掉了 abort 监听（roundTrip/onData），openTunnel 的 abort 监听也在返回时被移除。
 * 后果：走 channel.proxy 时看门狗**准时 abort 了，但 body 读取永不返回** —— 请求挂死 +
 * 并发许可泄漏（P2 实测 status=0 15003ms、released=false、inFlight=1）。
 * P2 已在 lib/proxy.mjs 侧用 watchdogBody() 绕过（请求不再挂死、许可已归还），但 **abort 传不到
 * socket**：被中断的代理请求，其上游 socket 要等对端关闭才回收。本套件验收的正是这个根因。
 *
 * 覆盖：
 *   [A] 明文 HTTP 经代理（absolute-form）：响应头之后 abort -> 读流立刻以 AbortError 收场，
 *       且"网关 -> 代理"那一跳的 socket 被回收（不是只看请求结束）；调用方取消读流同理。
 *   [B] openTunnel：已 abort 的 signal / CONNECT 握手挂起 / TCP 连接阶段挂起 三条路径。
 *   [C] CONNECT + TLS 隧道：自签证书起 https mock 上游，用 NODE_EXTRA_CA_CERTS 让网关照常校验；
 *       验证隧道下 abort 同样兑现 + socket 回收 + gzip 解压链不吞错误，另加一条网关端到端
 *      （server.mjs -> channel.proxy -> CONNECT -> TLS -> https 上游）验证首字节看门狗。
 *
 * 端口：P5 起全部**运行时动态分配**（原 9300-9310 唯一段）：
 *   P_PROXY 本机代理（absolute-form http 转发 + CONNECT 隧道；统计"客户端->代理"的 socket）
 *   P_HTTPS_UP https mock 上游（自签证书，只能经 CONNECT 隧道抵达）
 *   P_HTTP_UP 明文 mock 上游（正常 / 只回响应头不给 body / 半截 body / gzip 停机）
 *   P_GW 网关（TLS 端到端）
 *   P_HANG_PROXY 只接受 TCP、对 CONNECT 不作答的代理（测 openTunnel 握手期间的 abort）
 *   P_SILENT_UP 静默 TCP 上游（CONNECT 成功、但永不回应 TLS ClientHello -> 测 TLS 握手期间的 abort）
 *
 * 跑法：node test/test-outbound-proxy-abort.mjs
 *
 * 关于 TLS：要让网关照常校验自签证书只能用 NODE_EXTRA_CA_CERTS，而该变量是**进程启动时**
 * 读取的，运行期设置无效。所以本文件自举一次：外层进程把内置的自签证书写进临时目录，用同样的
 * 命令带 NODE_EXTRA_CA_CERTS 重跑自己；内层跑完全部断言；外层清理临时目录并以同样的退出码结束。
 * 证书是测试专用的 throwaway 自签证书（CN=localhost，SAN=IP:127.0.0.1,DNS:localhost，
 * 有效期至 2126 年），只在本机 mock 上游上使用。
 */
import { spawn, spawnSync } from 'node:child_process';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { freePort } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const SELF = fileURLToPath(import.meta.url);
const NODE = process.execPath;
const LIB_URL = pathToFileURL(path.join(ROOT, 'lib', 'outbound-proxy.mjs')).href;

// ---- 测试专用 throwaway 自签证书（CN=localhost / SAN=IP:127.0.0.1,DNS:localhost）----
const TEST_CERT_PEM = `-----BEGIN CERTIFICATE-----
MIIDJzCCAg+gAwIBAgIUMZDrBYzxRcCKmX+Efpg1OdGlvwAwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDkxNjEzNDY1NloYDzIxMjYw
ODIzMTM0NjU2WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQC5Gg80LFIB7cUZQQ7JmpD0lToQkKXGdGnyGgZvQpL/
ZGQ/HSh5qW+R9VMYKgGC79ErfsJuMDKcfdxGLg9itAwjTpskIuAWIaDn0wiHjlx6
vXLf5sSI4xcZgaNQhSH1Hh+q0NKFKOwafd03nFAUiapHz8d+QYunaMgrH7xXu/+R
Qls/BaJvqOaMLlVrAu5jl+RAFVYeZbsQSdgMZVRwIBqwkJV892a1A+puCLWcl3qw
TMkxTnYjVNAnCsZHqppstUcyIbuZFkYWRKBPf44nXKxlZU3mo3c49ZhdPJIdZDut
IbcgO8LssRm6Q3pxqv1kMynyMRrkZRwMSf6cjA+tz0odAgMBAAGjbzBtMB0GA1Ud
DgQWBBRjNM7pgF+zBQCdZEyeNia8S36LdTAfBgNVHSMEGDAWgBRjNM7pgF+zBQCd
ZEyeNia8S36LdTAPBgNVHRMBAf8EBTADAQH/MBoGA1UdEQQTMBGHBH8AAAGCCWxv
Y2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAtSROjCbFFX3FYmwHFF1E8fuqKtBi
W5V1SJkQHFliXDwqfRjW3Ao42n088gywtmfsfClnvFh5M7f14/bl8lF8M0SwyB3Q
c+Aa08qdYoaxGvIWi3VCv6Il9IBHQtfJ6dIeFU7602ea5KBYMz34rXg2kOq3zGtI
5Vqj001Q4iga1cifOpSiACBkzS6LuAGFhwY0qS3B2exUgbQCO7tnrHrYoIudKHTA
VGLFx8J2mM18kjoDLQm78z6nYh3fEVTWDUFwwexZtteKihsSGE97Qu4jGSTPcePQ
megVTuJ2QK8TBgHy9QXj3T2L22Ey5rC6lXWrkoOEfSjcD7eEWPFoXDwREA==
-----END CERTIFICATE-----
`;
const TEST_KEY_PEM = `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC5Gg80LFIB7cUZ
QQ7JmpD0lToQkKXGdGnyGgZvQpL/ZGQ/HSh5qW+R9VMYKgGC79ErfsJuMDKcfdxG
Lg9itAwjTpskIuAWIaDn0wiHjlx6vXLf5sSI4xcZgaNQhSH1Hh+q0NKFKOwafd03
nFAUiapHz8d+QYunaMgrH7xXu/+RQls/BaJvqOaMLlVrAu5jl+RAFVYeZbsQSdgM
ZVRwIBqwkJV892a1A+puCLWcl3qwTMkxTnYjVNAnCsZHqppstUcyIbuZFkYWRKBP
f44nXKxlZU3mo3c49ZhdPJIdZDutIbcgO8LssRm6Q3pxqv1kMynyMRrkZRwMSf6c
jA+tz0odAgMBAAECggEAKtBtA1mcB1+upwp+B+I1VKleop2+hnJ/XfUol1wMAcBA
ErwlFEz9ZkKbG3v92QIs/NVHVjLWRg2zoVT+kItKQnFoX5mkgOH65JxSvP4QBIDk
/QaU33+9ZFQwyQteQSLcWseN5iiCwhbqT/ZbLIDyLsWP5HN3QLvzDsx6pUVyFfAh
OxHx8ABe7FAAfizFjOEiYvchQ7bHhbb2Cdr1rzD6x5JziF1NzOmIIHb6F1nQUE6D
ux4CKzAP84u4UaaghbGGBpSwgjm4U1mcpbia+YJZLp1xxWPKRvSviT3NkupN6XSm
P79sTOeMiqOrDGzsHE5Uwn7gRNnB+hp8jxXcSJQUQQKBgQDjmco1GDAvqtGkBtZj
hPeaFqNZ8MIoN1D1QiFfiagsv8DcaHY8FiMQVJC+FdZOgOIODEem9qdMMk0zm57r
8JFlEFBc5sD5m+QD2K9FlZrSIJbOo9tmrqySfUIu4G6kbC7JkKel1w6ujS2dZhZD
RHjePrUX2u6N+M9wrfDnbg/h3QKBgQDQMrux8P6bgHtCWI4KiSC59VZV7GRE+SCK
/imAQt0jrEjjTqkyUV+9Iexqrf/hAsFlIulcPpz0WP4r7H7AyCS/1c02xmRndaVF
+sag81A6M4ldMkw/dYsTdmW9KrsaNgaPm0dst+ALLGgGOZJPvAoKGCsCNbjCM/rs
4h1V+4olQQKBgQDE0TwEN/uLbPtHDcadXuHC9SqjX0h3AIbY4Cv98wtkTxnXP9pm
0XnW6FoWlsmxL1DMdyALKSa5BJKwOzXtsCX7MGVeQQnFkGJYVSwUT3AHn7jpztau
8AQ51WnDIb9sHkVDdv0Ss2t11I4Km7pKx06CWdW+YEuEqBGyzigR6aKAXQKBgQCc
jyn5myG6yZjmwTS+03NaoxSzNDoKa9R+8LVAkAc/BhUhaUtuXSbDULk5V3LtP2cy
qFgXV7YrQKiRWxvN2DNaVmok4HcsHZmU0AmBirYvrgWDoYkYx0k373Z+E53zDeFN
KBVDudmuTUxEMhLGBr797EKbPM6cqv3sF/S7bZXqgQKBgBnlFICmYnOALirH4LHQ
aaZxiwqwrJZA0jnsEuxKBc1XvITeiHr4ZenhbkV/dIv9VoEETZTD2URxEUnCXIH9
5oJvfC9t6jpaR4UwHocofTORdAopstrYOwzEwjpVRojjKzhyLQtlyZ1T1A9maOBx
4H18w5+UiHSPJbZ4tatErNKW
-----END PRIVATE KEY-----
`;

// ---- 自举：CONNECT+TLS 需要进程启动时就带 NODE_EXTRA_CA_CERTS ----
function bootstrap() {
  if (process.env.P6_TLS_DIR) return; // 已经是内层
  const dir = mkdtempSync(path.join(os.tmpdir(), 'llm-gw-p6-'));
  writeFileSync(path.join(dir, 'cert.pem'), TEST_CERT_PEM, 'utf8');
  writeFileSync(path.join(dir, 'key.pem'), TEST_KEY_PEM, 'utf8');
  console.log(`[自举] 写入测试自签证书 -> ${dir}，带 NODE_EXTRA_CA_CERTS 重跑本套件\n`);
  const r = spawnSync(NODE, [SELF], {
    stdio: 'inherit',
    env: { ...process.env, P6_TLS_DIR: dir, NODE_EXTRA_CA_CERTS: path.join(dir, 'cert.pem') },
  });
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
  process.exit(typeof r.status === 'number' ? r.status : 1);
}
bootstrap();

const TLS_DIR = process.env.P6_TLS_DIR;
const { gwFetch } = await import(LIB_URL);

const P_PROXY = await freePort();
const P_HTTPS_UP = await freePort();
const P_HTTP_UP = await freePort();
const P_GW = await freePort();
const P_HANG_PROXY = await freePort();
const P_SILENT_UP = await freePort();
const PROXY_URL = `http://127.0.0.1:${P_PROXY}`;
const HTTP_BASE = `http://127.0.0.1:${P_HTTP_UP}`;
const HTTPS_BASE = `https://127.0.0.1:${P_HTTPS_UP}`;
const GW_BASE = `http://127.0.0.1:${P_GW}`;
const KEY = 'TESTKEY';

let pass = 0;
let fail = 0;
let skipped = 0;
const ok = (name, cond, extra = '') => {
  if (cond) { pass += 1; console.log(`  PASS ${name}${extra ? `  [${extra}]` : ''}`); }
  else { fail += 1; console.log(`  FAIL ${name}${extra ? `  [${extra}]` : ''}`); }
};
const skip = (name, why) => { skipped += 1; console.log(`  SKIP ${name}  [${why}]`); };

// 回归（DEP0123）：openTunnel 若把 IP 字面量塞进 tls.connect 的 servername，Node 会打弃用警告
// （RFC 6066 禁止对 IP 发 SNI，未来版本会把 IP 当非法值忽略、隧道静默失效）。
// 修法：IP 目标改放 host 字段（身份校验仍按 IP，但不发 SNI、不警告）。
// 计数机制（v24 源码确认）：tls 内部有模块级 ipServernameWarned 标志，**每进程最多 emit 一次**，
// 且 emitWarning 走 process.nextTick 异步派发 —— 所以只能从进程启动数到断言点，中途**不能重置**。
let dep0123Count = 0;
process.on('warning', (w) => {
  if (w?.name === 'DeprecationWarning' && /ServerName to an IP/.test(w?.message || '')) dep0123Count += 1;
});

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, ms = 1500, step = 50) {
  const deadline = Date.now() + ms;
  for (;;) {
    let v = false;
    try { v = await fn(); } catch { v = false; }
    if (v) return true;
    if (Date.now() > deadline) return false;
    await wait(step);
  }
}
/** 把 promise 收敛成 {value} / {err}（永不 reject），避免未处理拒绝 */
const settled = (p) => Promise.resolve(p).then((value) => ({ value }), (err) => ({ err }));
/** 在 ms 内等一个已收敛的 promise；超时返回 {timeout:true} —— 这就是"挂死"的可观测形式 */
async function within(p, ms) {
  let timer;
  const t = new Promise((resolve) => { timer = setTimeout(() => resolve({ timeout: true }), ms); });
  const out = await Promise.race([p, t]);
  clearTimeout(timer);
  return out;
}
const why = (o) => (o?.timeout ? 'TIME-OUT(挂死)' : (o?.err?.name || `resolved(${JSON.stringify(o?.value)?.slice(0, 40)})`));

// ---- 服务器与 socket 记账 ----
const servers = [];
function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => { servers.push(server); resolve(server); });
  });
}
/** 统计 server 上的"对端 -> 本 server"连接（http server 的 'connection' 在解析前就触发，
 *  对 CONNECT 隧道同样计数）：abort 是否真的传到 socket，看的就是这个集合 */
function trackSockets(server) {
  const live = new Set();
  server.on('connection', (s) => {
    live.add(s);
    s.on('error', () => { /* mock 侧忽略 ECONNRESET */ });
    s.on('close', () => live.delete(s));
  });
  return live;
}
const newSocketsSince = (live, mark) => [...live].filter((s) => !mark.has(s));

let proxyLive = new Set();
let hangLive = new Set();
let silentLive = new Set();
let proxyConnectCount = 0;

/** 本机代理：absolute-form http 转发 + CONNECT 隧道（Clash 混合端口的最小行为） */
function createProxyServer() {
  const server = http.createServer((req, res) => {
    let target = null;
    try { target = new URL(req.url); } catch { /* 非法 */ }
    if (!target || target.protocol !== 'http:') { try { res.writeHead(400); res.end(); } catch { /* ignore */ } return; }
    const up = http.request({
      hostname: target.hostname,
      port: target.port || 80,
      path: `${target.pathname}${target.search}`,
      method: req.method,
      headers: { ...req.headers, host: target.host },
    }, (upRes) => {
      if (res.writableEnded || res.destroyed) { upRes.destroy(); return; }
      res.writeHead(upRes.statusCode || 502, upRes.headers);
      res.flushHeaders();
      upRes.pipe(res);
      res.on('close', () => upRes.destroy()); // 客户端走了就别再抱着上游
    });
    up.on('error', () => { try { if (!res.headersSent) res.writeHead(502); res.end(); } catch { /* ignore */ } });
    req.on('error', () => up.destroy());
    req.pipe(up);
  });
  server.on('connect', (req, clientSocket, head) => {
    proxyConnectCount += 1;
    const [host, port] = String(req.url).split(':');
    const up = net.connect(Number(port), host, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head && head.length) up.write(head);
      up.pipe(clientSocket);
      clientSocket.pipe(up);
    });
    up.on('error', () => clientSocket.destroy());
    clientSocket.on('close', () => up.destroy());
  });
  server.on('clientError', (_err, socket) => { try { socket.destroy(); } catch { /* ignore */ } });
  return server;
}

/** mock 上游：http 与 https 共用 */
function upstreamHandler(req, res) {
  res.on('error', () => { /* 客户端断开后写入 EPIPE，忽略 */ });
  let pathname = '/';
  try { pathname = new URL(req.url, 'http://x').pathname; } catch { /* ignore */ }

  if (pathname === '/ok') {
    const body = 'p6-ok-body';
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'content-length': Buffer.byteLength(body) });
    res.end(body);
    return;
  }
  if (pathname === '/stall') {
    // 只回响应头 + 声明 content-length，之后永不写 body —— P2 §2.3 的同一条语义
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'content-length': 4096 });
    res.flushHeaders();
    return;
  }
  if (pathname === '/stall-gzip') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'content-encoding': 'gzip', 'content-length': 4096 });
    res.flushHeaders();
    return;
  }
  if (pathname === '/partial') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'content-length': 4096 });
    res.write('0123456789'); // 半截 body 后停住
    return;
  }
  if (pathname === '/v1/chat/completions') {
    let raw = '';
    req.on('data', (c) => { raw += c; });
    req.on('end', () => {
      let model = null;
      try { model = JSON.parse(raw || '{}').model; } catch { /* ignore */ }
      if (model === 'tls-stall-model') {
        res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'content-length': 8192 });
        res.flushHeaders(); // 只回响应头，正文永不来
        return;
      }
      const body = JSON.stringify({
        id: 'chatcmpl-p6', object: 'chat.completion', created: 1, model: model || 'tls-ok-model',
        choices: [{ index: 0, message: { role: 'assistant', content: '经 CONNECT+TLS 隧道的正常回答' }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 3, completion_tokens: 5, total_tokens: 8 },
      });
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body) });
      res.end(body);
    });
    return;
  }
  res.writeHead(404, { 'content-length': 0 });
  res.end();
}

async function startMocks() {
  const proxy = createProxyServer();
  proxyLive = trackSockets(proxy);
  await listen(proxy, P_PROXY);

  const httpUp = http.createServer(upstreamHandler);
  httpUp.on('clientError', (_e, s) => { try { s.destroy(); } catch { /* ignore */ } });
  await listen(httpUp, P_HTTP_UP);

  const key = readFileSync(path.join(TLS_DIR, 'key.pem'));
  const cert = readFileSync(path.join(TLS_DIR, 'cert.pem'));
  const httpsUp = https.createServer({ key, cert }, upstreamHandler);
  httpsUp.on('clientError', (_e, s) => { try { s.destroy(); } catch { /* ignore */ } });
  httpsUp.on('tlsClientError', () => { /* 客户端握手失败：忽略 */ });
  await listen(httpsUp, P_HTTPS_UP);

  // 9305：只接受 TCP，对 CONNECT 永远不作答（openTunnel 卡在握手）
  // resume() 是必须的：不读的话收不到对端 FIN，也就观察不到客户端有没有真的拆掉 socket。
  const hang = net.createServer((s) => {
    s.on('error', () => { /* ignore */ });
    s.on('end', () => s.destroy());
    s.resume();
  });
  hangLive = trackSockets(hang);
  await listen(hang, P_HANG_PROXY);

  // 9306：静默 TCP 上游 —— CONNECT 隧道能建立（TCP 连得上），但永不回应 TLS ClientHello，
  // 于是 openTunnel 卡在 TLS 握手那一段
  const silent = net.createServer((s) => {
    s.on('error', () => { /* ignore */ });
    s.on('end', () => s.destroy());
    s.resume();
  });
  silentLive = trackSockets(silent);
  await listen(silent, P_SILENT_UP);
}

// ---- 网关小工具 ----
async function callGw(pathname, { method = 'GET', body, timeoutMs = 8000 } = {}) {
  try {
    const r = await fetch(`${GW_BASE}${pathname}`, {
      method,
      headers: { authorization: `Bearer ${KEY}`, ...(body ? { 'content-type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await r.text();
    let json = null;
    try { json = JSON.parse(text); } catch { /* 非 JSON */ }
    return { status: r.status, text, json };
  } catch (err) {
    return { status: 0, text: `TEST-CLIENT-TIMEOUT(${err?.name})`, json: null };
  }
}

/** 跑一段 ESM 源码（子进程），收集 stdout/stderr。
 *  必须用异步 spawn：spawnSync 会把本进程的事件循环钉住，而本进程里的 mock 代理/上游
 *  正是子进程要连的对象 —— 用 spawnSync 会自己把自己锁死（子进程永远等不到 CONNECT 应答）。 */
function runNodeScript(src, env, timeoutMs = 15000) {
  return new Promise((resolve) => {
    const child = spawn(NODE, ['--input-type=module', '-e', src], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let done = false;
    const timer = setTimeout(() => { try { child.kill(); } catch { /* ignore */ } }, timeoutMs);
    const finish = (out) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      resolve(out);
    };
    child.stdout.on('data', (b) => { stdout += b.toString(); });
    child.stderr.on('data', (b) => { stderr += b.toString(); });
    child.on('error', (err) => finish({ code: -1, stdout, stderr: String(err) }));
    child.on('close', (code) => finish({ code, stdout, stderr }));
  });
}

/** 探一下"到黑洞地址的 TCP 连接会不会挂起"（环境相关：有的网络会立刻 EHOSTUNREACH） */
function probeBlackhole() {
  return new Promise((resolve) => {
    // 目的地在 RFC 5737 TEST-NET-1（黑洞地址），这里的端口号只影响"连不连得上"，不会本机占用
    const s = net.connect({ port: P_PROXY, host: '192.0.2.1' });
    let done = false;
    const finish = (v) => { if (!done) { done = true; resolve(v); } };
    s.on('connect', () => { finish(false); s.destroy(); });
    s.on('error', () => finish(false));
    setTimeout(() => { finish(true); s.destroy(); }, 500);
  });
}

async function main() {
  await startMocks();

  // ---------- [A] 明文 HTTP 经代理 ----------
  console.log('\n[A] 明文 HTTP 经代理（absolute-form）：abort 的兑现 + socket 回收');
  {
    const res = await gwFetch(`${HTTP_BASE}/ok`, { proxy: PROXY_URL });
    const text = await res.text();
    ok('[A0] 经代理的普通请求正常返回（基线）',
      res.status === 200 && text === 'p6-ok-body', `status=${res.status} body=${JSON.stringify(text)}`);
  }

  {
    // 关键用例：响应头已到手 -> 再 abort（P6 的原缺陷：这一刻之后 abort 再也不可达）
    const mark = new Set(proxyLive);
    const ac = new AbortController();
    const res = await gwFetch(`${HTTP_BASE}/stall`, { proxy: PROXY_URL, signal: ac.signal });
    ok('[A1] 上游只回响应头也能拿到响应对象（这正是 P6 的起点）', res.status === 200, `status=${res.status}`);

    const reading = settled(res.text()); // 先挂起 body 读取，再 abort（复现"读取永不返回"）
    const t0 = Date.now();
    ac.abort();
    const out = await within(reading, 1500);
    const ms = Date.now() - t0;
    ok('[A2] abort 后 text() 以 AbortError 收场（不是永不返回）',
      out.err?.name === 'AbortError', `${ms}ms ${why(out)}`);
    ok('[A3] ...且 1.5s 内结束（看门狗一响读流立刻收场）', !out.timeout && ms < 1500, `${ms}ms`);

    const freed = await waitFor(() => newSocketsSince(proxyLive, mark).length === 0, 1500);
    ok('[A4] abort 后"客户端->代理"那一跳的 socket 被回收（不是等对端关闭）',
      freed, `剩余 ${newSocketsSince(proxyLive, mark).length} 个`);
  }

  {
    // 流式读法（for-await / getReader）同样要收场
    const ac = new AbortController();
    const res = await gwFetch(`${HTTP_BASE}/stall`, { proxy: PROXY_URL, signal: ac.signal });
    const reader = res.body.getReader();
    const reading = settled(reader.read());
    ac.abort();
    const out = await within(reading, 1500);
    ok('[A5] abort 后 res.body 的 reader.read() 也以 AbortError 拒绝',
      out.err?.name === 'AbortError', why(out));
  }

  {
    // 调用方取消（客户端断开）这条路径：半截 body 读一块后 cancel -> socket 必须回收
    const mark = new Set(proxyLive);
    const res = await gwFetch(`${HTTP_BASE}/partial`, { proxy: PROXY_URL });
    const reader = res.body.getReader();
    const first = await within(settled(reader.read()), 1500);
    ok('[A6] 半截 body 能读到第一块（mock 已发 10 字节）',
      !first.timeout && first.value?.value?.length === 10, `len=${first.value?.value?.length}`);
    await reader.cancel();
    const freed = await waitFor(() => newSocketsSince(proxyLive, mark).length === 0, 1500);
    ok('[A7] 调用方取消读流后，代理侧 socket 同样被回收（销毁路径不摘 abort 也不漏 socket）',
      freed, `剩余 ${newSocketsSince(proxyLive, mark).length} 个`);
  }

  // ---------- [B] openTunnel：abort 在握手期间/之后都要可达 ----------
  console.log('\n[B] openTunnel：abort 的可达性（已 abort / CONNECT 挂起 / TCP 连接挂起）');
  {
    const ac = new AbortController();
    ac.abort();
    const t0 = Date.now();
    const out = await within(settled(gwFetch(`${HTTP_BASE}/ok`, { proxy: PROXY_URL, signal: ac.signal })), 1000);
    ok('[B1] 已经 abort 的 signal：立刻以 AbortError 收场（不发请求、不挂到连接超时）',
      out.err?.name === 'AbortError' && Date.now() - t0 < 400, `${Date.now() - t0}ms ${why(out)}`);
  }

  {
    // 代理只接受 TCP、对 CONNECT 不作答 -> openTunnel 卡在握手；abort 必须能打断它
    const mark = new Set(hangLive);
    const ac = new AbortController();
    const p = settled(gwFetch(`${HTTPS_BASE}/ok`, { proxy: `http://127.0.0.1:${P_HANG_PROXY}`, signal: ac.signal }));
    await wait(250); // 让它把 CONNECT 发出去并卡在等应答
    ok('[B2] CONNECT 已发出且卡住（前置：挂起代理侧确实有 1 条连接）',
      newSocketsSince(hangLive, mark).length === 1, `连接数=${newSocketsSince(hangLive, mark).length}`);
    ac.abort();
    const out = await within(p, 1500);
    ok('[B3] CONNECT 握手挂起时 abort -> AbortError（不是通用网络错误，更不是挂死）',
      out.err?.name === 'AbortError', why(out));
    const freed = await waitFor(() => newSocketsSince(hangLive, mark).length === 0, 1500);
    ok('[B4] ...同时挂起代理侧的 socket 被回收', freed, `剩余 ${newSocketsSince(hangLive, mark).length} 个`);
  }

  {
    // TCP 连接阶段（连 SYN 都没回）的 abort：靠 waitConnect 收场
    if (!(await probeBlackhole())) {
      skip('[B5] TCP 连接阶段挂起时 abort -> AbortError', '本机到 192.0.2.1:9300 的连接没有挂起（网络环境相关，跳过）');
    } else {
      const ac = new AbortController();
      const p = settled(gwFetch(`${HTTP_BASE}/ok`, { proxy: `http://192.0.2.1:${P_PROXY}`, signal: ac.signal }));
      await wait(300);
      const t0 = Date.now();
      ac.abort();
      const out = await within(p, 1500);
      ok('[B5] TCP 连接阶段挂起时 abort -> AbortError（waitConnect 能被 abort 打断）',
        out.err?.name === 'AbortError', `${Date.now() - t0}ms ${why(out)}`);
    }
  }

  {
    // TLS 握手阶段挂起：CONNECT 已成功、隧道已建立，但对端永不回应 ClientHello。
    // 这一段曾是第二个"abort 不可达"的窗口：底层 socket 被 destroy 时 TLSSocket 只发 'close'
    //（不发 'error'），once(tlsSock,'secureConnect') 会永远等下去。
    const mark = new Set(silentLive);
    const ac = new AbortController();
    const p = settled(gwFetch(`https://127.0.0.1:${P_SILENT_UP}/ok`, { proxy: PROXY_URL, signal: ac.signal }));
    await wait(300); // 让它走完 CONNECT、进入 TLS 握手
    ok('[B6] 前置：CONNECT 已建立且 TLS 握手正卡住（上游侧有 1 条连接）',
      newSocketsSince(silentLive, mark).length === 1, `连接数=${newSocketsSince(silentLive, mark).length}`);
    ac.abort();
    const out = await within(p, 1500);
    ok('[B7] TLS 握手卡住时 abort -> AbortError（不是挂死、也不是通用 TLS 错误）',
      out.err?.name === 'AbortError', why(out));
    const freed = await waitFor(() => newSocketsSince(silentLive, mark).length === 0, 1500);
    ok('[B8] ...整条链路（客户端->代理->静默上游）都被拆掉', freed,
      `剩余 ${newSocketsSince(silentLive, mark).length} 个`);
  }

  // ---------- [C] CONNECT + TLS 隧道 ----------
  console.log('\n[C] CONNECT + TLS 隧道（自签证书 + NODE_EXTRA_CA_CERTS，网关照常校验）');
  ok('[C0] 内层进程带上了 NODE_EXTRA_CA_CERTS（否则自签上游不可能通过校验）',
    !!process.env.NODE_EXTRA_CA_CERTS, process.env.NODE_EXTRA_CA_CERTS || '(unset)');

  {
    // 反向对照：去掉 NODE_EXTRA_CA_CERTS，同一路径必须因"证书不受信"失败。
    // 这是"我们真的在校验、没有偷偷关掉校验"的证据。
    const childSrc = `
const { gwFetch } = await import(${JSON.stringify(LIB_URL)});
try {
  const r = await gwFetch(${JSON.stringify(`${HTTPS_BASE}/ok`)}, { proxy: ${JSON.stringify(PROXY_URL)} });
  console.log('UNEXPECTED-OK ' + r.status);
} catch (err) {
  console.log('ERR ' + (err.code || err.name));
}`;
    const env = { ...process.env };
    delete env.NODE_EXTRA_CA_CERTS;
    const r = await runNodeScript(childSrc, env, 15000);
    const outText = `code=${r.code} ${(r.stdout || '').trim()} ${(r.stderr || '').trim().slice(0, 160)}`;
    ok('[C1] 对照：不带 NODE_EXTRA_CA_CERTS 时同一路径必须因证书不受信而失败（证明网关照常校验）',
      /ERR (DEPTH_ZERO_SELF_SIGNED_CERT|SELF_SIGNED_CERT_IN_CHAIN|UNABLE_TO_VERIFY_LEAF_SIGNATURE|CERT_HAS_EXPIRED|ERR_TLS_CERT_ALTNAME_INVALID)/.test(r.stdout || ''),
      outText);
  }

  {
    const before = proxyConnectCount;
    const res = await gwFetch(`${HTTPS_BASE}/ok`, { proxy: PROXY_URL });
    const text = await res.text();
    ok('[C2] CONNECT + TLS 隧道正常返回（自签证书校验通过 -> 隧道分支真的被走到）',
      res.status === 200 && text === 'p6-ok-body', `status=${res.status} body=${JSON.stringify(text)}`);
    ok('[C3] 走的确实是 CONNECT 隧道（代理侧收到 CONNECT）',
      proxyConnectCount > before, `CONNECT 次数 ${before} -> ${proxyConnectCount}`);
  }

  {
    // 隧道分支：响应头之后 abort（与 [A2] 同一场景，但目标在 TLS 隧道另一头）
    const mark = new Set(proxyLive);
    const ac = new AbortController();
    const res = await gwFetch(`${HTTPS_BASE}/stall`, { proxy: PROXY_URL, signal: ac.signal });
    ok('[C4] 隧道分支：上游只回响应头也能拿到响应对象（隧道已建立）', res.status === 200, `status=${res.status}`);
    const reading = settled(res.text());
    const t0 = Date.now();
    ac.abort();
    const out = await within(reading, 1500);
    const ms = Date.now() - t0;
    ok('[C5] 隧道分支：abort 后 text() 以 AbortError 收场（看门狗在 CONNECT+TLS 下同样生效）',
      out.err?.name === 'AbortError', `${ms}ms ${why(out)}`);
    const freed = await waitFor(() => newSocketsSince(proxyLive, mark).length === 0, 1500);
    ok('[C6] 隧道分支：abort 后代理侧 socket 被回收', freed, `剩余 ${newSocketsSince(proxyLive, mark).length} 个`);
  }

  {
    // content-encoding: gzip 时调用方读到的是解压流：abort 必须穿透解压链
    const ac = new AbortController();
    const res = await gwFetch(`${HTTPS_BASE}/stall-gzip`, { proxy: PROXY_URL, signal: ac.signal });
    const reading = settled(res.text());
    const t0 = Date.now();
    ac.abort();
    const out = await within(reading, 1500);
    ok('[C7] 隧道分支 + content-encoding: gzip：abort 后 text() 同样以 AbortError 收场（解压链不吞错误/不挂死）',
      out.err?.name === 'AbortError', `${Date.now() - t0}ms ${why(out)}`);
  }

  {
    // 本套件进程内所有会走 CONNECT+TLS 的路径（[B6] 握手挂起 + 上面 C2/C4/C6/C7 共 5 次）目标都是
    // IP 字面量 127.0.0.1。DEP0123 每进程至多发一次，所以"从进程启动数到这一步仍为 0"等价于
    // "一次都没把 IP 塞进 servername" —— 这就是防回归的锁。
    ok('[C7b] CONNECT+TLS 全程未触发 DEP0123（servername 没被塞 IP 字面量 —— RFC 6066 + 未来 Node 兼容）',
      dep0123Count === 0, `DEP0123 次数=${dep0123Count}`);
  }

  // ---------- [C8] 端到端：网关 -> channel.proxy -> CONNECT -> TLS ----------
  console.log('\n[C8] 端到端：server.mjs 经 channel.proxy(CONNECT+TLS) 打到 https 上游');
  {
    const cfgDir = path.join(TLS_DIR, 'gw');
    mkdirSync(cfgDir, { recursive: true });
    const cfgPath = path.join(cfgDir, 'p6-tls.tmp.json');
    writeFileSync(cfgPath, JSON.stringify({
      server: { host: '127.0.0.1', port: P_GW, apiKey: KEY },
      routing: {
        strategy: 'round-robin', sticky: false, sessionAffinity: false, tiered: false,
        maxAttempts: 1, attemptsPerChannel: 1, retryLoop: false,
        timeoutMs: 20000,
        firstByteTimeoutMs: 800,       // 首字节看门狗
        streamIdleTimeoutMs: 60000,    // 空闲看门狗故意放大：证明是首字节那一下干的
        failThreshold: 100, cooldownMs: 1000, maxCooldownMs: 5000,
        probeIntervalMs: 0, discoverIntervalMs: 0, maxTotalWaitMs: 5000,
        maxConcurrent: 4, maxConcurrentPerChannel: 2, maxConcurrentPerAgent: 0,
      },
      taskLog: { enabled: false },
      channels: [
        { name: 'tls-ok-1', protocol: 'openai', baseUrl: HTTPS_BASE, apiKey: 'k', proxy: PROXY_URL, models: ['tls-ok-model'], priority: 1, tier: 'preferred' },
        { name: 'tls-stall-1', protocol: 'openai', baseUrl: HTTPS_BASE, apiKey: 'k', proxy: PROXY_URL, models: ['tls-stall-model'], priority: 1, tier: 'preferred' },
      ],
    }, null, 2), 'utf8');

    let gwLog = '';
    const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', cfgPath, '--no-discover', '--log-level', 'info'], {
      cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env },
    });
    gw.stdout.on('data', (b) => { gwLog += b.toString(); });
    gw.stderr.on('data', (b) => { gwLog += b.toString(); });

    try {
      let ready = false;
      for (let i = 0; i < 60 && !ready; i += 1) {
        try { const r = await fetch(`${GW_BASE}/health`); ready = r.ok; } catch { /* 还没起来 */ }
        if (!ready) await wait(100);
      }
      ok('[C9] 走 CONNECT+TLS 上游的网关启动就绪', ready);
      if (!ready) {
        console.log(gwLog.slice(-1500));
      } else {
        const rOk = await callGw('/v1/chat/completions', {
          method: 'POST', body: { model: 'tls-ok-model', messages: [{ role: 'user', content: '你好' }] },
        });
        ok('[C10] 端到端：经代理 CONNECT+TLS 的正常请求 200（网关出站真的穿过了隧道）',
          rOk.status === 200 && /隧道的正常回答/.test(rOk.text), `status=${rOk.status} ${rOk.text.slice(0, 140)}`);

        const mark = new Set(proxyLive);
        const t0 = Date.now();
        const rStall = await callGw('/v1/chat/completions', {
          method: 'POST', body: { model: 'tls-stall-model', messages: [{ role: 'user', content: '你好' }] },
        });
        const ms = Date.now() - t0;
        ok('[C11] 端到端：https 上游只回响应头 -> 首字节超时内 503（隧道下看门狗同样生效，不挂死）',
          rStall.status === 503 && ms < 2500, `status=${rStall.status} ${ms}ms`);
        ok('[C12] 端到端：错误文案是"首字节超时"', /首字节超时/.test(rStall.text), rStall.text.slice(0, 160));

        const freed = await waitFor(() => newSocketsSince(proxyLive, mark).length === 0, 2000);
        ok('[C13] 端到端：请求失败后"网关->代理"的 socket 被回收（abort 真的传到了 socket）',
          freed, `剩余 ${newSocketsSince(proxyLive, mark).length} 个`);

        const m = await callGw('/api/metrics');
        const active = m.json?.concurrency?.global?.active ?? -1;
        ok('[C14] 端到端：并发许可已归还（global.active = 0，没有 P2 实测的那种泄漏）',
          active === 0, `active=${active}`);

        const evidence = gwLog.split('\n').filter((l) => /首字节超时|CONNECT|TLS|certificate/.test(l));
        if (evidence.length) {
          console.log('\n网关日志证据：');
          for (const line of evidence.slice(0, 10)) console.log(`  | ${line.trim()}`);
        }
      }
    } finally {
      try { gw.kill(); } catch { /* ignore */ }
      await wait(300);
    }
  }

  return fail ? 1 : 0;
}

let code = 1;
try {
  code = await main();
} catch (err) {
  console.log(`\n测试异常: ${err?.stack || err}`);
  code = 1;
} finally {
  for (const s of servers) { try { s.close(); } catch { /* ignore */ } }
  console.log(`\n结果: ${pass} 通过, ${fail} 失败${skipped ? `, ${skipped} 跳过` : ''}`);
}
process.exit(code);
