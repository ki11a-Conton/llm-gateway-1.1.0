// 零依赖出站 HTTP 代理：HTTP CONNECT 隧道（https 目标）+ 绝对形式代理请求（http 目标）。
// 网关访问 www.workbuddy.ai 等需走本机代理（如 http://127.0.0.1:7897，Clash 混合端口）时使用；
// 返回与 Web fetch Response 兼容的对象（.ok/.status/.headers/.text()/.json()/.body），
// 供 proxy.mjs / channels.mjs / server.mjs 与全局 fetch 无差别替换。零 npm 依赖。
//
// 用法：
//   import { gwFetch } from './outbound-proxy.mjs';
//   const res = await gwFetch(url, { proxy: 'http://127.0.0.1:7897', method: 'POST', headers, body, signal });
// proxy 为空时直接回落全局 fetch（零开销直连路径）。

import net from 'node:net';
import tls from 'node:tls';
import zlib from 'node:zlib';
import { PassThrough } from 'node:stream';

const MAX_REDIRECTS = 5;
const MAX_HEAD_BYTES = 64 * 1024;

function abortError() {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

/** 大小写不敏感的响应头视图（够用的最小实现） */
class RespHeaders {
  constructor(map) {
    this.map = map; // key 全小写
  }
  get(name) {
    return this.map.get(String(name).toLowerCase()) ?? null;
  }
  has(name) {
    return this.map.has(String(name).toLowerCase());
  }
  entries() {
    return this.map.entries();
  }
}

/** 解析响应头文本：状态行 + 头字段（键小写），返回 {status, statusText, headers: Map} */
function parseResponseHead(headText) {
  const lines = headText.split('\r\n');
  const statusLine = lines[0] || '';
  const m = /^HTTP\/\d(?:\.\d)?\s+(\d{3})\s*(.*)$/.exec(statusLine);
  if (!m) throw new Error(`无法解析 HTTP 状态行: ${statusLine}`);
  const status = Number(m[1]);
  const statusText = m[2] || '';
  const headers = new Map();
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    const ci = line.indexOf(':');
    if (ci === -1) continue;
    headers.set(line.slice(0, ci).trim().toLowerCase(), line.slice(ci + 1).trim());
  }
  return { status, statusText, headers };
}

/** 从 socket 读一块直到 \r\n\r\n 的头文本（CONNECT 应答 / HTTP 应答头） */
function readHeadText(socket, maxBytes = MAX_HEAD_BYTES) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    const onData = (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      const idx = buf.indexOf('\r\n\r\n');
      if (idx !== -1) {
        cleanup();
        resolve(buf.subarray(0, idx).toString('latin1'));
      } else if (buf.length > maxBytes) {
        cleanup();
        reject(new Error('代理响应头过大'));
      }
    };
    const onError = (err) => {
      cleanup();
      reject(err);
    };
    const onClose = () => {
      cleanup();
      reject(new Error('代理连接提前关闭（读响应头时）'));
    };
    const cleanup = () => {
      socket.off('data', onData);
      socket.off('error', onError);
      socket.off('close', onClose);
    };
    socket.on('data', onData);
    socket.on('error', onError);
    socket.on('close', onClose);
  });
}

/** 把 src 接到 dst 上，并把"错误"与"销毁"两个方向都打通（pipe() 只管数据方向）。
 *
 * P6：pipe() 不转发错误 —— 当 src（roundTrip 的响应体流）因 abort / 上游出错被 destroy(err) 时，
 * dst 解压流会永远等不到 end，调用方（text() / json() / for-await）就此挂死；
 * 反向地，调用方取消 dst（客户端断开）时 src 也要被拆掉，否则底层 socket 只能等对端关闭。 */
function linkPipeline(src, dst) {
  // 兜底：出错那一刻可能没有活跃消费者（响应已被放弃），未监听的 'error' 会直接掀掉进程。
  // 真正的消费者（streamToString / for-await / makeWebBody）自己的监听不受影响。
  dst.on('error', () => {});
  src.once('error', (err) => { if (!dst.destroyed) dst.destroy(err); });
  dst.once('close', () => { if (!src.destroyed) src.destroy(); });
  src.pipe(dst);
  return dst;
}

/** 把 content-encoding 解压 transform 链接到响应体流上（与 fetch 语义一致：body 已是解压后的） */
function decompressBody(nodeStream, headers) {
  const enc = (headers.get('content-encoding') || '').toLowerCase();
  if (!enc) return nodeStream;
  // 支持 "gzip, br" 这种按序编码
  const steps = enc.split(',').map((s) => s.trim()).filter(Boolean).reverse();
  let out = nodeStream;
  for (const step of steps) {
    if (step === 'gzip') out = linkPipeline(out, zlib.createGunzip());
    else if (step === 'deflate') out = linkPipeline(out, zlib.createInflate());
    else if (step === 'br') out = linkPipeline(out, zlib.createBrotliDecompress());
  }
  return out;
}

function streamToString(nodeStream) {
  return new Promise((resolve, reject) => {
    const all = [];
    const cleanup = () => {
      nodeStream.off('readable', onReadable);
      nodeStream.off('end', onEnd);
      nodeStream.off('close', onEnd);
      nodeStream.off('error', onError);
    };
    const drain = () => {
      let c;
      while ((c = nodeStream.read()) !== null) all.push(c);
    };
    const onReadable = () => drain();
    const onEnd = () => {
      cleanup();
      resolve(Buffer.concat(all).toString('utf8'));
    };
    const onError = (err) => {
      cleanup();
      reject(err);
    };
    nodeStream.on('readable', onReadable);
    nodeStream.on('end', onEnd);
    nodeStream.on('close', onEnd);
    nodeStream.on('error', onError);
    drain(); // 先捞已缓冲的数据（'data' 事件不会补发已入队内容）
  });
}

/**
 * 把 node 流包装成 WHATWG ReadableStream（供 Readable.fromWeb / readAllText 消费）。
 * 用 'data' 事件 + resume() 驱动：立即把已缓冲数据以 data 事件吐出并实时增量转发，
 * end 由 'end' 事件统一触发 controller.close() —— 拉模式会在"end 早已触发"时永久挂起。
 * 不用 Readable.toWeb：Node 24 下"数据先入队再消费"会永久挂起。
 */
function makeWebBody(nodeStream) {
  const queue = [];
  let controller = null;
  let ended = false;
  let streamError = null;
  let closed = false;

  const cleanup = () => {
    nodeStream.off('data', onData);
    nodeStream.off('end', onEnd);
    nodeStream.off('error', onError);
  };
  const drain = () => {
    if (!controller || closed) return;
    while (queue.length) controller.enqueue(queue.shift());
    if (streamError) {
      closed = true;
      cleanup();
      controller.error(streamError);
    } else if (ended) {
      closed = true;
      cleanup();
      controller.close();
    }
  };
  const onData = (c) => {
    queue.push(c);
    drain();
  };
  const onEnd = () => {
    ended = true;
    drain();
  };
  const onError = (e) => {
    streamError = e;
    drain();
  };

  nodeStream.on('data', onData);
  nodeStream.on('end', onEnd);
  nodeStream.on('error', onError);
  nodeStream.resume(); // 立即流动：先把已缓冲数据以 data 事件吐出，再实时增量

  return new ReadableStream({
    start(c) {
      controller = c;
    },
    pull() {
      drain();
    },
    cancel() {
      closed = true;
      cleanup();
      nodeStream.destroy(); // 下游取消（客户端断连）：停止缓冲
    },
  });
}

function makeResponse({ status, statusText, headers, bodyStream }) {
  const decompressed = decompressBody(bodyStream, headers);
  let webBody = null;
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    url: '',
    headers: new RespHeaders(headers),
    // 惰性：ReadableStream 构造后会立刻拉一次 pull 把缓冲数据挪进 web 队列，
    // 若在构造时就包好，走 text()/json() 的调用方会读到空。只在真正消费 .body 时才包装。
    get body() {
      if (!webBody) webBody = makeWebBody(decompressed);
      return webBody;
    },
    text: () => streamToString(decompressed),
    async json() {
      const t = await this.text();
      return JSON.parse(t);
    },
  };
}

/** 等待 TCP 连接建立。connect / error / close / abort 四条路径都必须让等待收场：
 *  否则 abort 会被内核的连接超时（对拒绝响应的对端可长达数十秒）拖住，看门狗形同虚设。 */
function waitConnect(socket, signal) {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      socket.off('connect', onConnect);
      socket.off('error', onError);
      socket.off('close', onClose);
      signal?.removeEventListener('abort', onAbort);
    };
    const onConnect = () => { cleanup(); resolve(); };
    const onError = (err) => { cleanup(); reject(signal?.aborted ? abortError() : err); };
    const onClose = () => {
      cleanup();
      reject(signal?.aborted ? abortError() : new Error('代理连接提前关闭（等待 TCP 连接时）'));
    };
    const onAbort = () => {
      socket.destroy();
      cleanup();
      reject(abortError());
    };
    socket.on('connect', onConnect);
    socket.on('error', onError);
    socket.on('close', onClose);
    if (signal?.aborted) { onAbort(); return; }
    signal?.addEventListener('abort', onAbort, { once: true });
    if (!socket.connecting) { cleanup(); resolve(); } // 同步连上（罕见）：别等一个不会再来的事件
  });
}

/**
 * 等待 TLS 握手完成。与 waitConnect 同理：secureConnect / error / close / abort 四条路径都必须让等待收场。
 * 不能用 once(tlsSock,'secureConnect')：底层 socket 在握手期间被 destroy 时实测**只发 'close'**
 *（既不 'secureConnect' 也不 'error'），abort 会被永远挂住（本机实测 120s 仍不返回）。
 * 所以这里显式兜住 'close'。 */
function waitSecureConnect(tlsSock, signal) {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      tlsSock.off('secureConnect', onSecure);
      tlsSock.off('error', onError);
      tlsSock.off('close', onClose);
      signal?.removeEventListener('abort', onAbort);
    };
    const onSecure = () => { cleanup(); resolve(); };
    const onError = (err) => { cleanup(); reject(signal?.aborted ? abortError() : err); };
    const onClose = () => {
      cleanup();
      reject(signal?.aborted ? abortError() : new Error('TLS 握手期间连接被关闭'));
    };
    const onAbort = () => { tlsSock.destroy(); cleanup(); reject(abortError()); };
    tlsSock.on('secureConnect', onSecure);
    tlsSock.on('error', onError);
    tlsSock.on('close', onClose);
    if (signal?.aborted) { onAbort(); return; }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * 把 abort 绑到 socket 上：abort -> socket.destroy()。
 * 摘监听的条件是 **socket 自己关闭**，而不是"某个函数返回了"。
 * P6 修复点：openTunnel 原先在返回时用 finally 摘掉 abort 监听，于是隧道建立之后
 *（也就是 proxy.mjs 的首字节/空闲看门狗真正开火的时刻）abort 再也传不到 socket，
 * 被中断的请求只能等对端关闭才回收上游连接。 */
function armAbort(socket, signal) {
  if (!signal) return;
  const onAbort = () => socket.destroy();
  signal.addEventListener('abort', onAbort, { once: true });
  socket.once('close', () => signal.removeEventListener('abort', onAbort));
}

/** 建立到代理的 TCP 连接；https 目标先发 CONNECT 握手（200 后返回可用的隧道 socket） */
async function openTunnel(proxyUrl, targetHost, targetPort, useTls, signal) {
  if (signal?.aborted) throw abortError();
  const socket = net.connect(Number(proxyUrl.port || 80), proxyUrl.hostname);
  let tlsSock = null;
  try {
    await waitConnect(socket, signal);
    armAbort(socket, signal);
    // addEventListener 对"已经 abort 的 signal"不会再触发，这里补一次显式检查，堵住竞态窗口
    if (signal?.aborted) {
      socket.destroy();
      throw abortError();
    }
    if (useTls) {
      socket.write(
        `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\nHost: ${targetHost}:${targetPort}\r\n\r\n`,
      );
      const head = await readHeadText(socket);
      const { status, headers, statusText } = parseResponseHead(head);
      if (status !== 200) {
        socket.destroy();
        throw new Error(
          `代理 CONNECT 失败 HTTP ${status} ${statusText}（代理 ${proxyUrl.hostname}:${proxyUrl.port}）` +
            (headers.get('proxy-authenticate') ? '，代理要求认证（407）' : ''),
        );
      }
      // 隧道就绪：在已连通的 socket 上叠 TLS。
      // 主机名目标必须传 servername（SNI + 身份校验都靠它，不传则上游可能拿不到虚拟主机/证书不匹配）；
      // IP 字面量目标则**不能**传 servername —— RFC 6066 禁止对 IP 发 SNI，Node 会打 DEP0123 弃用警告
      // （"Setting the TLS ServerName to an IP address ... will be ignored in a future version"，
      //  未来版本把 IP 当非法值忽略后隧道会静默失效）。IP 改放 host 字段：
      // 实测 checkServerIdentity 拿到的仍是该 IP（身份校验不降级），但不发 SNI、不触发警告。
      const tlsOpts = { socket, ALPNProtocols: ['http/1.1'] };
      if (net.isIP(targetHost) === 0) tlsOpts.servername = targetHost;
      else tlsOpts.host = targetHost;
      tlsSock = tls.connect(tlsOpts);
      await waitSecureConnect(tlsSock, signal);
    }
    if (signal?.aborted) {
      (tlsSock || socket).destroy();
      throw abortError();
    }
    return tlsSock || socket;
  } catch (err) {
    socket.destroy();
    tlsSock?.destroy();
    // abort 打断握手时底层报的是"连接提前关闭"这类错误：对调用方而言它必须是 AbortError，
    // 否则 proxy.mjs 会把客户端主动断开误分类成 504 网络错误。
    if (signal?.aborted && err?.name !== 'AbortError') throw abortError();
    throw err;
  }
}

/** 在 socket 上发一个 HTTP 请求并读响应；body 以 PassThrough 流形式返回。
 * 结束判定按 HTTP 语义：content-length 计数 / chunked 解码 / 两者皆无才等 socket 关闭
 * （keep-alive 代理不会因为 connection: close 立刻关流，必须按帧判定）。 */
function roundTrip(socket, requestHead, bodyBuf, signal) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let headParsed = false;
    let finished = false;
    let settled = false;

    const body = new PassThrough();
    // abort / 上游出错时 body 会被 destroy(err)。调用方可能已经放弃这个响应（没有活跃 consumer），
    // 此时没有 'error' 监听会让 Node 把未捕获异常直接抛到进程上 —— 兜一个空监听；
    // 真正的消费者（streamToString / for-await / makeWebBody）自己的监听不受影响。
    body.on('error', () => {});
    const cleanup = () => {
      socket.off('data', onData);
      socket.off('end', onEnd);
      socket.off('error', onError);
      socket.off('close', onClose);
      signal?.removeEventListener('abort', onAbort);
    };
    const fail = (err) => {
      socket.destroy();
      if (settled) {
        finished = true; // 读流已按错误收场：后续 socket 事件不得再往里 push
        body.destroy(err);
        return;
      }
      settled = true;
      cleanup();
      reject(err);
    };
    const finishBody = () => {
      if (finished) return;
      finished = true;
      cleanup();
      body.push(null);
    };
    // 读流收场（end / 出错 / 被调用方销毁）之后才摘 abort 监听，并回收底层 socket。
    // P6 修复点：原先在"响应头解析完成"那一刻就摘掉了 abort 监听，于是头之后 abort 再也
    // 传不到读流与 socket —— 上游只回头不给数据时，看门狗准时 abort 了，body 读取却永不返回
    //（请求挂死 + 并发许可泄漏）。调用方取消（makeWebBody.cancel）同理必须拆掉 socket。
    const onBodyClose = () => {
      cleanup();
      if (!socket.destroyed) socket.destroy();
    };
    body.once('close', onBodyClose);

    // ---- 响应体按帧解码状态 ----
    let bodyMode = 'close';     // 'length' | 'chunked' | 'close'
    let remain = 0;             // length 模式剩余字节
    let chunkBuf = Buffer.alloc(0);
    let chunkStage = 'size';    // size | data | crlf
    let chunkRemain = 0;

    const runChunkDecoder = () => {
      while (true) {
        if (chunkStage === 'size') {
          const idx = chunkBuf.indexOf('\r\n');
          if (idx === -1) return;
          const size = parseInt(chunkBuf.subarray(0, idx).toString('latin1').trim(), 16);
          chunkBuf = chunkBuf.subarray(idx + 2);
          if (!Number.isFinite(size) || size < 0) {
            fail(new Error('响应 chunked 帧非法'));
            return;
          }
          if (size === 0) {
            finishBody();
            return;
          }
          chunkRemain = size;
          chunkStage = 'data';
        } else if (chunkStage === 'data') {
          if (chunkBuf.length < chunkRemain) return;
          body.push(chunkBuf.subarray(0, chunkRemain));
          chunkBuf = chunkBuf.subarray(chunkRemain);
          chunkStage = 'crlf';
        } else {
          if (chunkBuf.length < 2) return;
          if (chunkBuf[0] !== 13 || chunkBuf[1] !== 10) {
            fail(new Error('响应 chunked 尾部 CRLF 缺失'));
            return;
          }
          chunkBuf = chunkBuf.subarray(2);
          chunkStage = 'size';
        }
      }
    };

    const eatBodyBytes = (data) => {
      if (finished) return;
      if (bodyMode === 'length') {
        const take = Math.min(data.length, remain);
        if (take > 0) {
          body.push(data.subarray(0, take));
          remain -= take;
        }
        if (remain <= 0) finishBody();
      } else if (bodyMode === 'chunked') {
        chunkBuf = Buffer.concat([chunkBuf, data]);
        runChunkDecoder();
      } else {
        body.push(data);
      }
    };

    const onData = (chunk) => {
      if (!headParsed) {
        buf = Buffer.concat([buf, chunk]);
        const idx = buf.indexOf('\r\n\r\n');
        if (idx === -1) {
          if (buf.length > MAX_HEAD_BYTES) fail(new Error('响应头过大'));
          return;
        }
        const headText = buf.subarray(0, idx).toString('latin1');
        const rest = buf.subarray(idx + 4);
        let parsed;
        try {
          parsed = parseResponseHead(headText);
        } catch (err) {
          fail(err);
          return;
        }
        headParsed = true;
        settled = true;
        // 注意：这里**不摘** abort 监听（P6）——摘监听的条件是读流收场，见 onBodyClose/finishBody。
        if (parsed.headers.has('content-length')) {
          bodyMode = 'length';
          remain = Number(parsed.headers.get('content-length')) || 0;
        } else if ((parsed.headers.get('transfer-encoding') || '').toLowerCase().includes('chunked')) {
          bodyMode = 'chunked';
        }
        resolve(makeResponse({ ...parsed, bodyStream: body }));
        if (rest.length) eatBodyBytes(rest);
      } else {
        eatBodyBytes(chunk);
      }
    };
    const onEnd = () => {
      if (headParsed && !finished) {
        finishBody(); // 连接提前关闭：按已收数据收尾（close 模式的主路径）
      } else if (!headParsed) {
        fail(new Error('代理连接提前关闭（读响应头时）'));
      }
    };
    const onClose = () => onEnd();
    const onError = (err) => {
      if (headParsed && !finished) {
        finished = true;
        cleanup();
        body.destroy(err);
      } else if (!headParsed) {
        fail(err);
      }
    };
    const onAbort = () => {
      if (!headParsed) {
        fail(abortError());
        return;
      }
      cleanup();
      socket.destroy();
      finished = true;
      body.destroy(abortError());
    };

    socket.on('data', onData);
    socket.on('end', onEnd);
    socket.on('error', onError);
    socket.on('close', onClose);
    signal?.addEventListener('abort', onAbort, { once: true });
    // addEventListener 对"已经 abort 的 signal"不会再触发（openTunnel 返回 -> 这里注册之间的
    // 一个微任务窗口就足以漏掉一次 abort），所以显式补一刀：立刻收场，连请求都不发。
    if (signal?.aborted) {
      onAbort();
      return;
    }
    socket.write(requestHead);
    if (bodyBuf) socket.write(bodyBuf);
  });
}

/** 单次往返（含代理握手），返回 Response 兼容对象 */
async function doRoundTrip(proxyUrl, target, method, headers, bodyBuf, signal) {
  const useTls = target.protocol === 'https:';
  const targetPort = target.port ? Number(target.port) : useTls ? 443 : 80;
  const socket = await openTunnel(proxyUrl, target.hostname, targetPort, useTls, signal);

  const outHeaders = { ...headers };
  if (!outHeaders['accept-encoding']) outHeaders['accept-encoding'] = 'gzip, deflate, br';
  // http 目标走代理：绝对形式请求行；https 目标已 CONNECT，走相对形式
  const reqTarget = useTls ? (target.pathname || '/') + target.search : target.toString();
  const headerLines = [`${method} ${reqTarget} HTTP/1.1`, `Host: ${target.host}`];
  for (const [k, v] of Object.entries(outHeaders)) {
    if (v === undefined || v === null) continue;
    headerLines.push(`${k}: ${v}`);
  }
  if (bodyBuf) headerLines.push(`content-length: ${bodyBuf.length}`);
  headerLines.push('connection: close');
  const requestHead = headerLines.join('\r\n') + '\r\n\r\n';

  try {
    return await roundTrip(socket, requestHead, bodyBuf, signal);
  } catch (err) {
    socket.destroy();
    throw err;
  }
}

/**
 * 类 Web fetch 的出站请求，支持可选 HTTP 代理（CONNECT 隧道）。
 * @param {string} url 目标 URL（http/https）
 * @param {object} opts { proxy?, method?, headers?, body?(string|Buffer), signal?, redirect? }
 * @returns 与 Web Response 兼容的对象
 */
export async function gwFetch(url, { proxy, method = 'GET', headers = {}, body, signal, redirect = 'follow' } = {}) {
  if (!proxy) return fetch(url, { method, headers, body, signal, redirect });
  if (typeof proxy !== 'string' || !/^https?:\/\//i.test(proxy)) {
    throw new Error(`不支持的代理配置: ${proxy}（仅支持 http://host:port）`);
  }
  const proxyUrl = new URL(proxy);
  let bodyBuf = body == null ? null : Buffer.isBuffer(body) ? body : Buffer.from(String(body));
  let current = url;
  let curMethod = method;
  let curHeaders = { ...headers };

  for (let hop = 0; hop <= MAX_REDIRECTS; hop += 1) {
    const target = new URL(current);
    const res = await doRoundTrip(proxyUrl, target, curMethod, curHeaders, bodyBuf, signal);
    const loc = res.headers.get('location');
    if (redirect === 'follow' && res.status >= 300 && res.status < 400 && loc) {
      // 303 → GET；301/302 的 POST → GET（fetch 语义）；307/308 保持原方法
      if (res.status === 303 || ((res.status === 301 || res.status === 302) && curMethod === 'POST')) {
        curMethod = 'GET';
        bodyBuf = null;
        delete curHeaders['content-type'];
      }
      await res.text().catch(() => {}); // 放掉重定向响应体，释放 socket
      current = new URL(loc, target).toString();
      continue;
    }
    return res;
  }
  throw new Error(`重定向次数超过 ${MAX_REDIRECTS} 次`);
}

/** 按代理 URL 缓存绑定好的 fetch 函数 */
const proxyFetchCache = new Map();
export function createProxyFetch(proxy) {
  let fn = proxyFetchCache.get(proxy);
  if (!fn) {
    fn = (u, o) => gwFetch(u, { ...o, proxy });
    proxyFetchCache.set(proxy, fn);
  }
  return fn;
}

// 内部导出（仅供测试）：CONNECT 握手与应答头解析
export { openTunnel, parseResponseHead };

export default { gwFetch, createProxyFetch };
