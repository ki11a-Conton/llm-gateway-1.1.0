// 管理面安全回归（W6 Bug 1）：
//  - 跨站 Origin 请求 /api/* 一律被拒（既不带 Authorization 也读不到响应体，且不回任何 CORS 头）
//  - 非法 Host 头一律 403（防 DNS rebinding：攻击者域名解析到 127.0.0.1 也进不来）
//  - 同源 / 无 Origin 的面板请求照常 200 且功能可用（面板不能被安全修复改坏）
//  - /api/status 不再回传完整 apiKey；/v1 的 401 也不再回显 key（否则 /v1 的宽松 CORS 会把 key 送出去）
//  - 跨站 POST /api/channels 被拒且 config 未被改写；同源 POST /api/channels 仍可用
// 注意：为了精确控制 Host / Origin 头，这里用裸 http.request（fetch 不允许改 Host）。
import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import { readFileSync, writeFileSync, rmSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const CFG = path.join(HERE, 'admin-security.test.json');
// P5：网关端口动态分配；配置由 materializeConfig 复制改写（不动 admin-security.test.json 本身）。
const PORT = await freePort();
const MP = await mockUpstreamPorts();
// 下面"同源新增渠道"用的上游地址：本文件不需要 mock 在跑，但端口同样取动态值（逻辑 9101=正常上游）。
const UPSTREAM_URL = MP.url(9101);
const EVIL = 'https://evil.example';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

/** 裸 http 请求：可自定义 Host / Origin */
function raw({ method = 'GET', path: p, headers = {}, body = null, host = null } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1',
      port: PORT,
      method,
      path: p,
      setHost: false,
      headers: { host: host ?? `127.0.0.1:${PORT}`, ...headers },
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode,
        headers: res.headers,
        text: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    if (body != null) req.write(body);
    req.end();
  });
}
const toJson = (t) => { try { return JSON.parse(t); } catch { return null; } };
function postJson(p, payload, extraHeaders = {}) {
  const body = JSON.stringify(payload);
  return raw({
    method: 'POST',
    path: p,
    headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), ...extraHeaders },
    body,
  });
}
async function waitReady(tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch { /* retry */ }
    await wait(200);
  }
  throw new Error('gateway not ready');
}

const RUN_CFG = materializeConfig(CFG, { port: PORT, mockBase: MP.base });
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });

try {
  await waitReady();
  await wait(300);

  // ---- 1) 跨站 Origin 读 /api/status：被拒，读不到密钥，也没有 CORS 头 ----
  console.log('— 跨站 Origin（模拟任意网页 fetch 本机网关）—');
  {
    const r = await raw({ path: '/api/status', headers: { origin: EVIL } }); // 故意不带 Authorization
    ok('跨站 Origin 请求 /api/status 被拒（非 200）', r.status !== 200, `status=${r.status}`);
    ok('跨站响应没有 access-control-allow-origin',
      !r.headers['access-control-allow-origin'], String(r.headers['access-control-allow-origin']));
    ok('跨站响应没有 access-control-allow-headers',
      !r.headers['access-control-allow-headers'], String(r.headers['access-control-allow-headers']));
    ok('跨站响应体里没有 apiKey / 真实密钥',
      toJson(r.text)?.server?.apiKey === undefined && !r.text.includes('TESTKEY'), r.text.slice(0, 200));
  }

  // ---- 2) 非法 Host 头（DNS rebinding）→ 403 ----
  console.log('— Host 头校验（DNS rebinding）—');
  {
    const r = await raw({ path: '/api/status', host: 'evil.example' });
    ok('非法 Host 请求 /api/status → 403', r.status === 403, `status=${r.status} ${r.text.slice(0, 120)}`);
    const r2 = await raw({ path: '/', host: `evil.example:${PORT}` });
    ok('非法 Host 打开面板 → 403', r2.status === 403, `status=${r2.status}`);
    const r3 = await raw({ path: '/api/status', host: `localhost:${PORT}` });
    ok('Host: localhost:<port> 放行', r3.status === 200, `status=${r3.status}`);
    const r4 = await raw({ path: '/api/status', headers: { origin: `http://localhost:${PORT}` } });
    ok('Origin: http://localhost:<port> 放行', r4.status === 200, `status=${r4.status}`);
  }

  // ---- 3) 同源 / 无 Origin 的面板请求：200 且功能可用 ----
  console.log('— 面板同源回归 —');
  {
    const st = await raw({ path: '/api/status' });
    const data = toJson(st.text);
    ok('无 Origin 的本机 /api/status 200', st.status === 200, `status=${st.status}`);
    ok('/api/status 不回传完整 apiKey',
      data?.server?.apiKey === undefined && !st.text.includes('TESTKEY'), st.text.slice(0, 200));
    ok('/api/status 给出掩码提示（面板仍能显示配了哪个 key）',
      typeof data?.server?.apiKeyMasked === 'string' && data.server.apiKeyMasked.length > 0 && data.server.apiKeyMasked !== 'TESTKEY',
      JSON.stringify(data?.server));
    ok('/api/status 仍然带 channels/models/pool 视图（面板渲染所需）',
      Array.isArray(data?.channels) && Array.isArray(data?.models) && Array.isArray(data?.pool));
    ok('/api/status 的 server 段仍带 host/port/panel',
      data?.server?.host === '127.0.0.1' && data?.server?.port === PORT && data?.server?.panel === true,
      JSON.stringify(data?.server));

    const panel = await raw({ path: '/' });
    ok('面板首页 200 且是完整 HTML', panel.status === 200 && panel.text.includes('id="channels"') && panel.text.includes('id="apikey"'),
      `status=${panel.status} len=${panel.text.length}`);

    const tasks = await raw({ path: '/api/tasks?limit=5' });
    const tj = toJson(tasks.text);
    ok('/api/tasks 可用', tasks.status === 200 && !!tj?.stats && Array.isArray(tj?.recent), `status=${tasks.status}`);

    // 面板的 POST 请求一定带同源 Origin，不能被安全修复误伤
    const same = await raw({ path: '/api/status', headers: { origin: `http://127.0.0.1:${PORT}` } });
    ok('同源 Origin 的 GET 放行（面板轮询不被误伤）', same.status === 200, `status=${same.status}`);
  }

  // ---- 4) 跨站写配置被拒（config 未被改写），同源写配置仍可用 ----
  console.log('— 渠道写接口（跨站 vs 同源）—');
  {
    const before = readFileSync(RUN_CFG, 'utf8');
    const r = await postJson('/api/channels',
      { name: 'evil-chan', baseUrl: 'http://127.0.0.1:9', apiKey: 'k', model: 'm' },
      { origin: EVIL });
    ok('跨站 POST /api/channels 被拒', r.status === 403, `status=${r.status}`);
    ok('跨站响应没有 CORS 头', !r.headers['access-control-allow-origin']);
    ok('配置未被跨站请求改写', readFileSync(RUN_CFG, 'utf8') === before);
    const st = toJson((await raw({ path: '/api/status' })).text);
    ok('跨站渠道没有进内存', !(st?.channels || []).some((c) => c.name === 'evil-chan'));

    // 同源（面板"添加供应商"）必须照常工作
    const okPost = await postJson('/api/channels',
      { name: 'panel-chan', baseUrl: UPSTREAM_URL, apiKey: 'k', model: 'panel-model' },
      { origin: `http://127.0.0.1:${PORT}` });
    const pj = toJson(okPost.text);
    ok('同源 POST /api/channels 成功', okPost.status === 200 && pj?.ok === true, `${okPost.status} ${okPost.text.slice(0, 160)}`);
    const after = JSON.parse(readFileSync(RUN_CFG, 'utf8'));
    ok('同源新增渠道已写回配置', after.channels.some((c) => c.name === 'panel-chan'));
    const st2 = toJson((await raw({ path: '/api/status' })).text);
    ok('同源新增渠道已进入 /api/status', (st2?.channels || []).some((c) => c.name === 'panel-chan'));

    // 同源 DELETE 也应照常工作
    const del = await raw({ method: 'DELETE', path: '/api/channels/panel-chan', headers: { origin: `http://127.0.0.1:${PORT}` } });
    ok('同源 DELETE /api/channels/<name> 成功', del.status === 200 && toJson(del.text)?.ok === true, `status=${del.status}`);
    const delCross = await raw({ method: 'DELETE', path: '/api/channels/panel-chan', headers: { origin: EVIL } });
    ok('跨站 DELETE /api/channels/<name> 被拒', delCross.status === 403, `status=${delCross.status}`);
  }

  // ---- 5) OPTIONS 预检：/v1 保持宽松，/api 跨站直接拒 ----
  console.log('— CORS 分区（/v1 宽松 / /api 关闭）—');
  {
    const v1 = await raw({
      method: 'OPTIONS', path: '/v1/models',
      headers: { origin: EVIL, 'access-control-request-method': 'GET' },
    });
    ok('/v1 预检仍返回宽松 CORS（模型客户端不受影响）',
      v1.status === 204 && v1.headers['access-control-allow-origin'] === '*',
      `status=${v1.status} acao=${v1.headers['access-control-allow-origin']}`);
    const api = await raw({
      method: 'OPTIONS', path: '/api/channels',
      headers: { origin: EVIL, 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type' },
    });
    ok('跨站 /api 预检被拒且无 CORS 头',
      api.status === 403 && !api.headers['access-control-allow-origin'],
      `status=${api.status} acao=${api.headers['access-control-allow-origin']}`);
  }

  // ---- 6) /v1 的 401 不回显 apiKey（否则宽松 CORS 下等于把 key 送出去）----
  console.log('— /v1 鉴权提示不泄密 —');
  {
    const r = await raw({ path: '/v1/models' }); // 不带 Authorization
    ok('/v1 无 key → 401', r.status === 401, `status=${r.status}`);
    ok('/v1 401 响应体不含真实 apiKey', !r.text.includes('TESTKEY'), r.text.slice(0, 200));
  }

  // ---- 7) 热重载：轮换 server.apiKey 后 /api/reload 立即生效（W6 Bug 4）----
  // /api/status 在回环地址上不看 key（trusted），所以用 /v1/models 验证"当前生效的 key"。
  console.log('— 热重载（apiKey 轮换）—');
  {
    const maskBefore = toJson((await raw({ path: '/api/status' })).text)?.server?.apiKeyMasked;
    const before = await raw({ path: '/v1/models', headers: { authorization: 'Bearer TESTKEY' } });
    ok('轮换前旧 key 可用', before.status === 200, `status=${before.status}`);

    const cfg = JSON.parse(readFileSync(RUN_CFG, 'utf8'));
    cfg.server.apiKey = 'ROTATEDKEY';
    writeFileSync(RUN_CFG, JSON.stringify(cfg, null, 2) + '\n', 'utf8');

    const reload = await raw({ method: 'POST', path: '/api/reload' });
    ok('POST /api/reload 成功', reload.status === 200 && toJson(reload.text)?.ok === true,
      `${reload.status} ${reload.text.slice(0, 120)}`);

    const old = await raw({ path: '/v1/models', headers: { authorization: 'Bearer TESTKEY' } });
    ok('轮换后旧 key 立即失效（401）', old.status === 401, `status=${old.status}`);
    const next = await raw({ path: '/v1/models', headers: { authorization: 'Bearer ROTATEDKEY' } });
    ok('轮换后新 key 立即生效（200）', next.status === 200, `status=${next.status}`);

    const st = toJson((await raw({ path: '/api/status' })).text);
    ok('/api/status 回传的掩码跟随新 key 且仍不含完整 key',
      st?.server?.apiKeyMasked && st.server.apiKeyMasked !== maskBefore
        && st.server.apiKeyMasked !== 'ROTATEDKEY' && st.server.apiKey === undefined,
      `${maskBefore} -> ${st?.server?.apiKeyMasked}`);
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  await wait(500);
  try { if (existsSync(RUN_CFG)) rmSync(RUN_CFG); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
