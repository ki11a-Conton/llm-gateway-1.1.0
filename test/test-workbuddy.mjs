// WorkBuddy 国际版接入回归（M8）：
//  - 单元：sanitizeWorkbuddyBody（11128 黑名单/developer→system/tool_choice 归一/幂等）、
//          aggregateStreamToCompletion、buildProvisionEntry、queryBalance、refreshAccountToken、
//          设备码 start→pending→authorized→fetchUserInfo、410 过期。
//  - 网关 e2e：/api/workbuddy/auth/start 返回 authUrl（"获取 access token 的网址"）；
//             poll 授权后自动落号渠道（一账号一渠道）；chat 非流式经 forceStream 聚合；
//             流式透传；sanitize 让 11128 mock 不拒；走本机代理（本进程内假 CONNECT 代理，动态端口）
//             的渠道能出站（mock 收到 X-Via 标记）。
// 测试拓扑：mock-upstream.mjs（逻辑端口 9141 WorkBuddy 全家桶 / 9142 风控 mock）+
// 本进程内假代理（absolute-form + CONNECT）+ 网关（WORKBUDDY_BASE_URL 指逻辑 9141）。
// P5：以上端口全部运行时动态分配——mock 用 MOCK_PORT_BASE 整体平移（见 test/lib/ports.mjs），
// 网关/假代理各自 freePort()，静态配置由 materializeConfig 复制改写，不动 workbuddy.test.json 本身。
import { spawn } from 'node:child_process';
import path from 'node:path';
import http from 'node:http';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import { existsSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const CFG = path.join(HERE, 'workbuddy.test.json');
const PORT = await freePort();
const PROXY_PORT = await freePort();
const MP = await mockUpstreamPorts();
const MOCK = MP.url(9141);

// 必须先于 workbuddy.mjs 加载：库读 WORKBUDDY_BASE_URL 决定出站基址
process.env.WORKBUDDY_BASE_URL = MOCK;
const wb = await import('../lib/workbuddy.mjs');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 假本机代理（Clash 混合端口行为）：absolute-form http 转发 + CONNECT 隧道 ----
const fakeProxy = http.createServer((req, res) => {
  try {
    const u = new URL(req.url);
    if (u.protocol !== 'http:') return res.destroy();
    const up = http.request({
      host: u.hostname, port: u.port || 80, method: req.method, path: u.pathname + u.search,
      headers: { ...req.headers, host: u.host, 'x-via-fakeproxy': '1' },
    }, (upres) => {
      const { 'transfer-encoding': _te, ...h } = upres.headers;
      res.writeHead(upres.statusCode, h);
      upres.pipe(res);
    });
    req.pipe(up);
    up.on('error', () => res.destroy());
  } catch { res.destroy(); }
});
fakeProxy.on('connect', (req, clientSocket, head) => {
  const [host, port] = req.url.split(':');
  const srv = net.connect(Number(port), host, () => {
    clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    if (head && head.length) srv.write(head);
    srv.pipe(clientSocket);
    clientSocket.pipe(srv);
  });
  srv.on('error', () => clientSocket.destroy());
});
await new Promise((r) => fakeProxy.listen(PROXY_PORT, '127.0.0.1', r));

// ---- 起 mock 与网关 ----
// 静态配置里的 9141/9142 由 materializeConfig 按 mock 实际基准整体平移，server.port 换成 PORT；
// 配置里的假代理端口 9145 不在 mock 平移范围（9101..9143）内，所以复制出来后单独换成动态 PROXY_PORT。
const RUN_CFG = materializeConfig(CFG, { port: PORT, mockBase: MP.base });
{
  const cfg = JSON.parse(readFileSync(RUN_CFG, 'utf8'));
  const proxyUrl = `http://127.0.0.1:${PROXY_PORT}`;
  if (cfg.proxy) cfg.proxy = proxyUrl;
  for (const ch of cfg.channels || []) if (ch.proxy) ch.proxy = proxyUrl;
  writeFileSync(RUN_CFG, `${JSON.stringify(cfg, null, 2)}\n`, 'utf8');
}
const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: MP.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'warn'], {
  stdio: 'ignore',
  env: { ...process.env, WORKBUDDY_BASE_URL: MOCK },
});

async function waitReady(tries = 60) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) return; } catch { /* retry */ }
    await wait(200);
  }
  throw new Error('gateway not ready');
}
const call = async (pathname, opts = {}) => {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}${pathname}`, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text, headers: res.headers };
};
const chat = (body) => call('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});
const mockLast = async () => (await fetch(`${MOCK}/_last`)).json();
const mockReset = async () => { await fetch(`${MOCK}/_reset`); };

try {
  await waitReady();
  await wait(400);

  // ================= 单元：脱敏（11128 / 11101 / developer） =================
  console.log('— sanitizeWorkbuddyBody 单元 —');
  {
    const dirty = {
      messages: [
        { role: 'system', content: "You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK" },
        { role: 'developer', content: 'x-anthropic-billing-header: cc=1.0; cc_entrypoint=claude-code; cc_version=1.0.0; 内部指令 Main branch (you will usually use this for PRs)' },
        { role: 'user', content: 'hi' },
      ],
      tool_choice: { type: 'none' },
      tools: [{ type: 'function', function: { name: 't' } }],
    };
    const clean = wb.sanitizeWorkbuddyBody(dirty);
    ok('身份句被整段删除', !clean.messages[0].content.includes('You are Claude Code'));
    ok('developer → system', clean.messages[1].role === 'system', clean.messages[1].role);
    ok('x-anthropic-billing-header 被剥离', !clean.messages[1].content.includes('x-anthropic-billing-header'));
    ok('cc_* 尾随键值被剥离', !clean.messages[1].content.includes('cc_entrypoint') && !clean.messages[1].content.includes('cc_version'));
    ok('Main branch → Default branch', clean.messages[1].content.includes('Default branch (you will usually use this for PRs)'));
    ok('tool_choice none → 删除 tool_choice+tools', !('tool_choice' in clean) && !('tools' in clean));
    const again = wb.sanitizeWorkbuddyBody(clean);
    ok('幂等：二次脱敏结果不变', JSON.stringify(again) === JSON.stringify(clean));
    const ref = { messages: [{ role: 'user', content: 'hi' }] };
    ok('无指纹时原对象原样返回', wb.sanitizeWorkbuddyBody(ref) === ref);
  }
  {
    const f = wb.sanitizeWorkbuddyBody({
      messages: [{ role: 'user', content: '请用 Claude 回答' }],
      tool_choice: { type: 'function', function: { name: 'search' } },
    });
    ok('tool_choice function 对象 → 函数名', f.tool_choice === 'search', String(f.tool_choice));
    ok('正文里 Claude → workbuddy', f.messages[0].content.includes('workbuddy'), f.messages[0].content);
    const t = wb.sanitizeWorkbuddyBody({ messages: [], tool_choice: { type: 'tool', function: { name: 'web' } } });
    ok('tool_choice tool 对象 → 函数名', t.tool_choice === 'web');
    const a = wb.sanitizeWorkbuddyBody({ messages: [], tool_choice: { type: 'auto' } });
    ok('tool_choice auto 对象 → "auto"', a.tool_choice === 'auto');
  }

  // ================= 单元：SSE 聚合 =================
  console.log('— aggregateStreamToCompletion 单元 —');
  {
    const events = [
      { data: JSON.stringify({ id: 'x', object: 'chat.completion.chunk', created: 1, model: 'm', choices: [{ index: 0, delta: { content: 'a' }, finish_reason: null }] }) },
      { data: JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 'call_1', function: { name: 'f', arguments: '{"a":' } }] } }] }) },
      { data: JSON.stringify({ choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '1}' } }] }, finish_reason: 'tool_calls' }], usage: { total_tokens: 3 } }) },
      { data: '[DONE]' },
    ];
    const out = await wb.aggregateStreamToCompletion(events);
    ok('内容逐帧拼接', out.choices[0].message.content === 'a');
    ok('tool_calls 按 index 合并', out.choices[0].message.tool_calls?.[0]?.function?.arguments === '{"a":1}'
      && out.choices[0].message.tool_calls[0].function.name === 'f' && out.choices[0].message.tool_calls[0].id === 'call_1');
    ok('finish_reason 取末帧', out.choices[0].finish_reason === 'tool_calls');
    ok('usage 透传', out.usage?.total_tokens === 3);
  }

  // ================= 单元：渠道落号条目 =================
  {
    const entry = wb.buildProvisionEntry({ username: 'Alice_Intl', accessToken: 'tok', refreshToken: 'rt', uid: 'uid-alice', enterpriseId: 'ent-1' });
    ok('渠道名 wb-intl-* 小写化', entry.name === 'wb-intl-alice_intl', JSON.stringify(entry.name));
    ok('默认模型 deepseek-v4.1-flash', entry.model === 'deepseek-v4.1-flash');
    ok('headers 带 X-User-Id / X-Enterprise-Id', entry.headers['X-User-Id'] === 'uid-alice' && entry.headers['X-Enterprise-Id'] === 'ent-1');
    ok('refreshToken 落条目', entry.refreshToken === 'rt');
  }

  // ================= 单元：余额 / 续期（打到 9141 mock） =================
  console.log('— queryBalance / refreshAccountToken（走 mock）—');
  {
    const remain = await wb.queryBalance({ accessToken: 'tok-b', user_id: 'uid-1', proxy: '' });
    ok('余额聚合 300+150（负值钳 0）', remain === 450, String(remain));
    const last = await mockLast();
    ok('余额请求带 Bearer / X-User-Id / X-Domain', last.bearer === true && last.userid === 'uid-1' && last.domain === 'www.workbuddy.ai');
    ok('余额请求 ProductCode=p_tcaca Status=[0,3]', last.productCode === 'p_tcaca' && Array.isArray(last.status));

    const r1 = await wb.refreshAccountToken({ accessToken: 'tok-old', refreshToken: 'rt-old', proxy: '' });
    ok('续期返回新 accessToken', r1.accessToken.startsWith('tok-new-'), r1.accessToken);
    ok('续期返回新 refreshToken', r1.refreshToken === 'rt-new');
    const r2 = await wb.refreshAccountToken({ refreshToken: 'rt-no-return', proxy: '' });
    ok('上游未回传 refreshToken 时沿用旧值', r2.refreshToken === 'rt-no-return', r2.refreshToken);
    const l2 = await mockLast();
    ok('续期头 X-Refresh-Token / X-Auth-Refresh-Source', l2.refreshToken === 'rt-no-return' && l2.source === 'workbuddy');
  }

  // ================= 单元：设备码状态机（直连库函数） =================
  console.log('— 设备码（直连库函数）—');
  {
    const s = await wb.startDeviceFlow({ proxy: '' });
    ok('start 返回 state + authUrl', !!s.state && s.authUrl.includes('/login?state='), JSON.stringify(s));
    const p1 = await wb.pollDeviceFlow(s.state, { proxy: '' });
    ok('未授权 → pending', p1.status === 'pending', JSON.stringify(p1));
    await fetch(`${MOCK}/_authorize?state=${s.state}`);
    const p2 = await wb.pollDeviceFlow(s.state, { proxy: '' });
    ok('授权后 → authorized（带 accessToken/refreshToken/domain）',
      p2.status === 'authorized' && p2.accessToken === `tok-device-${s.state}` && p2.refreshToken && p2.domain === 'www.workbuddy.ai', JSON.stringify(p2));
    const info = await wb.fetchUserInfo(p2.accessToken, { state: s.state, proxy: '' });
    ok('用户信息 username/uid/enterpriseId',
      info.username === 'alice' && info.uid === 'uid-alice' && info.enterpriseId === 'ent-1', JSON.stringify(info));
    const expired = await wb.pollDeviceFlow('bogus-state', { proxy: '' });
    ok('未知 state → expired（410）', expired.status === 'expired', JSON.stringify(expired));
  }

  // ================= 网关 e2e：设备码授权 → 自动落号 =================
  console.log('— 网关 /api/workbuddy/auth/* —');
  let deviceState = '';
  {
    const r = await call('/api/workbuddy/auth/start', { method: 'POST' });
    ok('auth/start 返回 ok + state + authUrl', r.status === 200 && r.json?.ok === true && r.json?.state && r.json?.authUrl, r.text.slice(0, 160));
    ok('authUrl 是指向上游登录页的网址', r.json?.authUrl.includes('/login?state='), String(r.json?.authUrl));
    deviceState = r.json.state;
    const p = await call(`/api/workbuddy/auth/poll?state=${deviceState}`);
    ok('未授权 poll → pending', p.status === 200 && p.json?.status === 'pending', p.text.slice(0, 120));
    await fetch(`${MOCK}/_authorize?state=${deviceState}`);
    const a = await call(`/api/workbuddy/auth/poll?state=${deviceState}`);
    ok('授权后 poll → authorized + 自动落号渠道', a.status === 200 && a.json?.status === 'authorized' && a.json?.channel?.name === 'wb-intl-alice',
      JSON.stringify(a.json).slice(0, 200));
    ok('落号渠道 preset=workbuddy-intl', a.json?.channel?.preset === 'workbuddy-intl');
    ok('落号渠道带 refreshToken', !!a.json?.channel?.refreshToken);
    const st = await call('/api/status');
    ok('新增渠道已热加载进 /api/status', (st.json?.channels || []).some((c) => c.name === 'wb-intl-alice'));
    const saved = JSON.parse(readFileSync(RUN_CFG, 'utf8'));
    ok('新渠道已写回 config.json', (saved.channels || []).some((c) => c.name === 'wb-intl-alice'));
    const pollNoState = await call('/api/workbuddy/auth/poll');
    ok('poll 缺 state → 400', pollNoState.status === 400);
  }

  // ================= 网关 e2e：批量导入 access token（一账号一渠道进池子） =================
  console.log('— 批量导入（面板主入口）—');
  {
    const r = await call('/api/workbuddy/batch', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ tokens: ['tok-batch-1', 'tok-batch-2', 'tok-batch-3'] }),
    });
    ok('批量接口 200 且 3 个全部成功', r.status === 200 && r.json?.added?.length === 3 && r.json.added.every((x) => x.ok),
      JSON.stringify(r.json).slice(0, 200));
    const st = await call('/api/status');
    const names = (st.json?.channels || []).map((c) => c.name);
    ok('自动命名 wb-intl-1/2/3', ['wb-intl-1', 'wb-intl-2', 'wb-intl-3'].every((n) => names.includes(n)), JSON.stringify(names));
    const wb1 = (st.json?.channels || []).find((c) => c.name === 'wb-intl-1');
    ok('批量渠道 preset=workbuddy-intl', wb1?.preset === 'workbuddy-intl', JSON.stringify(wb1));
    ok('批量渠道绑定默认模型 deepseek-v4.1-flash', String(wb1?.model || wb1?.models || '').includes('deepseek-v4.1-flash'), JSON.stringify(wb1));
    const models = ((await call('/v1/models')).json?.data || []).map((m) => m.id);
    ok('池子里可调用 deepseek-v4.1-flash（同模型多账号=池）', models.includes('deepseek-v4.1-flash'), JSON.stringify(models));
    ok('批量渠道已写回 config.json', (JSON.parse(readFileSync(RUN_CFG, 'utf8')).channels || []).some((c) => c.name === 'wb-intl-2'));

    const dup = await call('/api/workbuddy/batch', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ tokens: ['tok-dup-1', 'tok-dup-2'] }),
    });
    ok('再次批量命名自动避冲突', dup.json?.added?.[0]?.name === 'wb-intl-1-2' && dup.json.added.every((x) => x.ok),
      JSON.stringify(dup.json?.added));

    const empty = await call('/api/workbuddy/batch', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ tokens: [] }),
    });
    ok('空 tokens → 400', empty.status === 400);
  }

  // ================= 网关 e2e：聊天（直连渠道，forceStream 聚合） =================
  console.log('— 聊天（直连 / 流式 / sanitize / 代理）—');
  {
    await mockReset();
    const r = await chat({ model: 'deepseek-v4.1-flash', messages: [{ role: 'user', content: 'hi' }], stream: false });
    ok('非流式请求成功（forceStream 聚合回 JSON）', r.status === 200 && r.json?.choices?.[0]?.message?.content === 'workbuddy 国际版 你好',
      `${r.status} ${r.text.slice(0, 160)}`);
    ok('由 wb-direct 渠道完成', r.headers.get('x-gateway-channel') === 'wb-direct', String(r.headers.get('x-gateway-channel')));
    const last = await mockLast();
    ok('出站已被强制 stream=true', last.stream === true, JSON.stringify(last));
    ok('出站模型与请求一致', last.model === 'deepseek-v4.1-flash');
    ok('出站角色未被篡改', JSON.stringify(last.roles) === JSON.stringify(['user']));
  }
  {
    await mockReset();
    const r = await chat({
      model: 'deepseek-v4.1-flash',
      messages: [{ role: 'user', content: 'hi' }],
      tools: [{ type: 'function', function: { name: 'search' } }],
      tool_choice: { type: 'function', function: { name: 'search' } },
      stream: false,
    });
    ok('tool_choice 对象请求成功', r.status === 200, r.text.slice(0, 160));
    const last = await mockLast();
    ok('tool_choice 已归一为函数名字符串', last.toolChoice === 'search', String(last.toolChoice));
    ok('tools 仍在', last.hasTools === true);
  }
  {
    await mockReset();
    const r = await chat({ model: 'deepseek-v4.1-flash', messages: [{ role: 'user', content: 'hi' }], stream: true });
    ok('流式请求透传 SSE', r.status === 200 && r.text.includes('[DONE]') && r.text.includes('workbuddy'), `${r.status} ${r.text.slice(0, 100)}`);
    ok('流式由 wb-direct 完成', r.headers.get('x-gateway-channel') === 'wb-direct');
  }
  {
    const r = await chat({
      model: 'wb-sanitize-model',
      messages: [
        { role: 'system', content: "You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK" },
        { role: 'developer', content: 'x-anthropic-billing-header: cc=1.0; cc_entrypoint=claude-code; cc_version=1.0.0; 内部指令' },
        { role: 'user', content: 'hi' },
      ],
      stream: false,
    });
    ok('含黑名单指纹的请求被脱敏后成功（11128 mock 未拒）', r.status === 200, `${r.status} ${r.text.slice(0, 160)}`);
    ok('sanitize 渠道返回内容正确', r.json?.choices?.[0]?.message?.content === '脱敏后正常返回');
    const rejects = (await (await fetch(MP.url(9142, '/_rejects'))).json()).rejects;
    ok('风控 mock 一次都没拦截', rejects === 0, `rejects=${rejects}`);
    const sl = await (await fetch(MP.url(9142, '/_last'))).json();
    ok('出站 body 无残留指纹/developer', !/x-anthropic-billing-header|You are Claude Code|cc_entrypoint=|"role"\s*:\s*"developer"/i.test(sl.rawBody || ''));
    ok('出站 developer 已变 system', (sl.rawBody || '').includes('"role":"system"'));
  }
  {
    await mockReset();
    const r = await chat({ model: 'wb-proxy-model', messages: [{ role: 'user', content: 'hi' }], stream: false });
    ok('走本机代理的渠道请求成功', r.status === 200 && r.json?.choices?.[0]?.message?.content === 'workbuddy 国际版 你好', `${r.status} ${r.text.slice(0, 160)}`);
    ok('由 wb-proxy 渠道完成', r.headers.get('x-gateway-channel') === 'wb-proxy');
    const last = await mockLast();
    ok('请求确实经过假代理（收到 X-Via 标记）', last.via === '1', JSON.stringify(last));
  }
  {
    const st = await call('/api/status');
    const dir = (st.json?.channels || []).find((c) => c.name === 'wb-direct');
    ok('直连渠道健康且未冷却', dir?.healthy === true && dir?.coolingDown !== true, JSON.stringify(dir));
    const mod = (st.json?.channels || []).find((c) => c.name === 'wb-proxy');
    ok('代理渠道同样健康', mod?.healthy === true, JSON.stringify(mod));
  }

  console.log(`\n结果: ${pass} 通过 / ${fail} 失败`);
} finally {
  gw.kill();
  mock.kill();
  fakeProxy.close();
  if (existsSync(RUN_CFG)) rmSync(RUN_CFG, { force: true });
}
process.exit(fail ? 1 : 0);
