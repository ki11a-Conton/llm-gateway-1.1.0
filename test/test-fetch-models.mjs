// 测试「拉取上游模型列表」功能：POST /api/models/fetch + ChannelManager.fetchUpstreamModels
//
// 为什么要有这个功能：模型名手打太容易填错（上游的 id 经常带 -reasoner/-v3.1/-preview 之类
// 后缀，拼错一个字符就是一路 404）。所以面板上给「模型」字段配了个「拉取模型」按钮，
// 从上游 /models 拉真实 id 出来点选。
//
// 覆盖点（每条都对应一个真实会踩的坑）：
//   1. 已保存渠道：按 name 拉，用该渠道已存的 baseUrl/key/请求头
//   2. 未保存的表单值：添加渠道时配置还没落盘，直接传 preset/baseUrl/key 也能拉
//   3. 表单里填了新 key 优先用新 key（改完还没保存也能先试拉）
//   4. 自定义 modelsPath 生效（中转站端点不规则）
//   5. Anthropic 协议走 x-api-key + anthropic-version，不是 Bearer
//   6. 上游 401 / 空列表 / 非 JSON 形状 都要给出人能看懂的错误
//   7. 没有可用 key（空 / 未展开的 ${ENV} 占位）时错误里要带提示，别让人对着 401 猜
//   8. 错误分支一律 400 且**绝不写 config.json**
//   9. 成功路径也是纯只读：config.json 字节不变（这个接口不该有任何副作用）
//  10. 响应里绝不能带出 apiKey 明文
//  11. 跨站 POST 被 CSRF 拦截
//  12. 面板 HTML 里确实有这个按钮和这个接口路径
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 自带 mock 上游（不碰共享的 test/mock-upstream.mjs，避免端口/顺序耦合）----------
const seen = [];       // 记录每次请求 { path, headers }
let mockPort = 0;
function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}
const mock = http.createServer((req, res) => {
  const p = new URL(req.url, 'http://x').pathname;
  seen.push({ path: p, headers: { ...req.headers } });
  const auth = String(req.headers.authorization || '');
  const xkey = String(req.headers['x-api-key'] || '');
  // 带凭据才算已认证。两个坑都要在这里判掉，否则测不出真实行为：
  //   1) 无 key 渠道发的是 "Bearer "（尾空格），HTTP 传输中会被 trim 成 "Bearer" —— 不能当成有效 token
  //   2) 未展开的 ${ENV} 占位会被适配器原样当 token 发出来（既有行为），真实上游一定 401
  const token = auth.replace(/^Bearer\s*/i, '').trim();
  const usable = (v) => v.trim() !== '' && !/^\$\{[^}]*\}$/.test(v.trim());
  const authed = usable(token) || usable(xkey);
  if (p === '/_seen') return json(res, 200, seen);
  if (p === '/_reset') { seen.length = 0; return json(res, 200, { ok: true }); }
  if (p === '/v1/models') {
    if (!authed) return json(res, 401, { error: { message: 'invalid api key' } });
    // 故意乱序返回：网关要排序后才能得到稳定结果
    return json(res, 200, { object: 'list', data: [{ id: 'zeta-model' }, { id: 'alpha-model' }, { id: 'mid-model' }] });
  }
  if (p === '/v1/models-empty') return json(res, 200, { object: 'list', data: [] });
  if (p === '/v1/models-bare') return json(res, 200, ['bare-2', 'bare-1']);
  if (p === '/v1/models-401') return json(res, 401, { error: { message: 'invalid api key' } });
  return json(res, 404, { error: { message: 'not found' } });
});
await new Promise((r) => mock.listen(0, '127.0.0.1', r));
mockPort = mock.address().port;

const PORT = await freePort();
// 配置里的 127.0.0.1:9101 映射到本测试的 mock 端口；配置走临时副本
const CFG = materializeConfig(path.join(HERE, 'fetch-models.test.json'), {
  port: PORT,
  portMap: { 9101: mockPort },
});

async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', 'content-type': 'application/json', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let body; try { body = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, body, text };
}
const fetchModels = (body) => call('/api/models/fetch', { method: 'POST', body: JSON.stringify(body) });
const cfgBytes = () => readFileSync(CFG, 'utf8');
const mockSeen = async (p) => (await fetch(`http://127.0.0.1:${mockPort}/_seen`).then((r) => r.json())).filter((x) => x.path === p);

async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch { /* 还没起来 */ }
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}

// --no-discover：禁止启动期自动拉模型，这样 mock 上的 /v1/models 只会被本测试显式触发
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(300);

  const cfgBefore = cfgBytes();

  // ---- 1. 已保存渠道：按 name 拉 ----
  await fetch(`http://127.0.0.1:${mockPort}/_reset`);
  const r1 = await fetchModels({ name: 'fm-ok' });
  ok('已保存渠道拉取成功', r1.status === 200 && r1.body?.ok === true, r1.text.slice(0, 200));
  ok('★ 返回模型按 id 升序（上游乱序也不影响）',
    JSON.stringify(r1.body?.models) === JSON.stringify(['alpha-model', 'mid-model', 'zeta-model']),
    JSON.stringify(r1.body?.models));
  ok('count 与列表长度一致', r1.body?.count === 3, String(r1.body?.count));
  ok('返回了实际请求的 url（便于排查）', /\/v1\/models$/.test(r1.body?.url || ''), String(r1.body?.url));
  const s1 = await mockSeen('/v1/models');
  ok('★ 用的是该渠道已保存的 key（Bearer k-ok）',
    s1.length === 1 && s1[0].headers.authorization === 'Bearer k-ok', JSON.stringify(s1.map((x) => x.headers.authorization)));

  // ---- 2. 未保存的表单值（添加渠道场景）：直接传 baseUrl + key ----
  const r2 = await fetchModels({ baseUrl: `http://127.0.0.1:${mockPort}`, protocol: 'openai', apiKey: 'k-adhoc' });
  ok('★ 未保存的表单值也能拉（添加渠道时用）', r2.status === 200 && r2.body?.ok === true, r2.text.slice(0, 200));
  const s2 = await mockSeen('/v1/models');
  ok('★ 用的是表单里刚填的 key（Bearer k-adhoc）',
    s2.some((x) => x.headers.authorization === 'Bearer k-adhoc'), JSON.stringify(s2.map((x) => x.headers.authorization)));

  // ---- 3. 表单里填了新 key 时优先于已保存的 key ----
  await fetch(`http://127.0.0.1:${mockPort}/_reset`);
  const r3 = await fetchModels({ name: 'fm-ok', apiKey: 'k-replaced' });
  ok('带 name 但填了新 key：请求成功', r3.status === 200 && r3.body?.ok === true, r3.text.slice(0, 200));
  const s3 = await mockSeen('/v1/models');
  ok('★ 新 key 覆盖了已保存的旧 key',
    s3.length === 1 && s3[0].headers.authorization === 'Bearer k-replaced',
    JSON.stringify(s3.map((x) => x.headers.authorization)));

  // ---- 4. 自定义 modelsPath 生效 ----
  const r4 = await fetchModels({ name: 'fm-bare' });
  ok('★ 自定义 modelsPath 生效 + 裸数组也能解析',
    r4.body?.ok === true && JSON.stringify(r4.body?.models) === JSON.stringify(['bare-1', 'bare-2']),
    r4.text.slice(0, 200));
  const s4 = await mockSeen('/v1/models-bare');
  ok('确实打到了自定义路径', s4.length === 1, JSON.stringify(s4.map((x) => x.path)));

  // ---- 5. Anthropic 协议：x-api-key + anthropic-version ----
  await fetch(`http://127.0.0.1:${mockPort}/_reset`);
  const r5 = await fetchModels({ name: 'fm-anthropic' });
  ok('Anthropic 渠道拉取成功', r5.status === 200 && r5.body?.ok === true, r5.text.slice(0, 200));
  const s5 = await mockSeen('/v1/models');
  ok('★ Anthropic 用 x-api-key 而不是 Bearer',
    s5.length === 1 && s5[0].headers['x-api-key'] === 'k-anth' && !s5[0].headers.authorization,
    JSON.stringify(s5[0]?.headers));
  ok('★ Anthropic 带上了 anthropic-version 头', Boolean(s5[0]?.headers['anthropic-version']), JSON.stringify(s5[0]?.headers));

  // ---- 6. 上游 401：给出人能看懂的错误 ----
  const r6 = await fetchModels({ name: 'fm-401' });
  ok('上游 401 时 ok=false（不是抛异常）', r6.status === 200 && r6.body?.ok === false, r6.text.slice(0, 200));
  ok('★ 错误里带上了上游状态码 401', String(r6.body?.error || '').includes('401'), String(r6.body?.error));
  ok('错误里带上了上游返回的原因', String(r6.body?.error || '').includes('invalid api key'), String(r6.body?.error));

  // ---- 7. 空列表 ----
  const r7 = await fetchModels({ name: 'fm-empty' });
  ok('上游返回空列表时 ok=false', r7.status === 200 && r7.body?.ok === false, r7.text.slice(0, 200));
  ok('★ 空列表的错误文案明确', String(r7.body?.error || '').includes('空'), String(r7.body?.error));

  // ---- 8. 没有可用 key 时错误里要有提示 ----
  const r8 = await fetchModels({ name: 'fm-nokey' });
  ok('无 key 渠道：上游 401 -> ok=false', r8.status === 200 && r8.body?.ok === false, r8.text.slice(0, 200));
  ok('★ 无 key 时错误里提示了 API Key 缺失', String(r8.body?.error || '').includes('API Key'), String(r8.body?.error));
  const r8b = await fetchModels({ name: 'fm-envkey' });
  ok('★ 未展开的 ${ENV} 占位同样按"没有可用 key"处理',
    r8b.body?.ok === false && String(r8b.body?.error || '').includes('API Key'), r8b.text.slice(0, 200));
  ok('无 key / ${ENV} 占位都被上游判成无效凭据（mock 记到的就是占位原文）',
    String(r8b.body?.error || '').includes('401'), String(r8b.body?.error));

  // ---- 9. 错误分支：400 且不写配置 ----
  const e9a = await fetchModels({ name: 'no-such-channel' });
  const e9b = await fetchModels({ baseUrl: '', preset: '' });
  const e9c = await fetchModels({ preset: 'no-such-preset-xyz' });
  ok('渠道不存在 -> 400', e9a.status === 400, e9a.text.slice(0, 160));
  ok('自定义但没填 baseUrl -> 400', e9b.status === 400, e9b.text.slice(0, 160));
  ok('不存在的 preset -> 400', e9c.status === 400, e9c.text.slice(0, 160));
  ok('★ 每个 400 都带人能看懂的 error 文案',
    [e9a, e9b, e9c].every((r) => typeof r.body?.error === 'string' && r.body.error.length > 0),
    JSON.stringify([e9a.body?.error, e9b.body?.error, e9c.body?.error]));

  // ---- 10. 只读保证：config.json 字节不变 ----
  ok('★ 整个拉取流程没有改动 config.json（逐字节比对）', cfgBytes() === cfgBefore);

  // ---- 11. 响应里不能带出 apiKey 明文 ----
  const allText = [r1.text, r2.text, r3.text, r5.text, r6.text].join('\n');
  ok('★ 响应里没有 apiKey 明文（k-ok / k-anth / k-bad / k-replaced）',
    !/k-ok|k-anth|k-bad|k-replaced/.test(allText), allText.slice(0, 160));

  // ---- 12. 跨站 POST 被 CSRF 拦截 ----
  const csrf = await call('/api/models/fetch', {
    method: 'POST',
    headers: { origin: 'http://evil.example.com' },
    body: JSON.stringify({ name: 'fm-ok' }),
  });
  ok('★ 跨站 POST 被拒 403', csrf.status === 403, `status=${csrf.status} ${csrf.text.slice(0, 120)}`);
  ok('跨站请求没有改动 config.json', cfgBytes() === cfgBefore);

  // ---- 13. 面板 HTML 确实带了这个入口 ----
  const panel = readFileSync(path.join(ROOT, 'public', 'index.html'), 'utf8');
  ok('★ 面板有「拉取模型」按钮', panel.includes('id="f-fetch-models"'));
  ok('★ 面板调用了 /api/models/fetch', panel.includes('/api/models/fetch'));
  ok('面板有可点选的模型下拉', panel.includes('id="f-model-picker"'));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.close();
  // Windows/Node 竞态规避：kill 后立刻 exit 会让 undici 池里的死连接与 libuv 关闭流程竞态
  await wait(500);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);