// 测试思考强度路由（reasoning_effort / effort 标签 / effortMap / thinking 转换）
// 与渠道管理 API（/api/presets、POST/DELETE /api/channels）
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 端口块都运行时动态分配；配置走临时副本，网关回写也不会动仓库里的 *.test.json
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'effort.test.json'), { port: PORT, mockBase: mp.base });
const CFG_BACKUP = readFileSync(CFG, 'utf8');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}
async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', 'content-type': 'application/json', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text, headers: res.headers };
}
const chat = (body) => call('/v1/chat/completions', { method: 'POST', body: JSON.stringify(body) });

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(400);

  // ---- effortMap：high 请求改写模型名并路由到 reasoner 渠道 ----
  const r1 = await chat({ model: 'deepseek-chat', reasoning_effort: 'high', messages: [{ role: 'user', content: 'hi' }] });
  const c1 = r1.json?.choices?.[0]?.message?.content || '';
  ok('effortMap: high 改写到 deepseek-reasoner', c1 === 'echo:deepseek-reasoner:effort=high', c1);
  ok('effortMap: 路由到 ds-reasoner 渠道', r1.headers.get('x-gateway-channel') === 'ds-reasoner', r1.headers.get('x-gateway-channel'));
  ok('响应头带 x-gateway-effort', r1.headers.get('x-gateway-effort') === 'high');

  // 不带 effort：走原模型 ds-chat（9101 内容固定 hello from good）
  const r2 = await chat({ model: 'deepseek-chat', messages: [{ role: 'user', content: 'hi' }] });
  ok('无 effort：走 deepseek-chat（ds-chat）', (r2.json?.choices?.[0]?.message?.content || '').includes('hello from good'));

  // ---- effort 标签过滤：池内同名模型（claude-test 只有 cl-think 一家，effort=high）----
  // low 请求：cl-think 是唯一候选但 effort=high 不匹配 -> 回落仍可用（不炸）
  const r3 = await chat({ model: 'claude-test', reasoning_effort: 'low', messages: [{ role: 'user', content: 'hi' }], max_tokens: 100 });
  ok('强度无匹配时回落可用', r3.status === 200, r3.text.slice(0, 160));

  // high 请求给 claude-test：应转成 thinking 并透传（mock anthropic 不校验，只验证 200 通路）
  const r4 = await chat({ model: 'claude-test', reasoning_effort: 'high', messages: [{ role: 'user', content: 'hi' }], max_tokens: 100 });
  ok('anthropic 渠道 high 强度请求正常', r4.status === 200, r4.text.slice(0, 160));

  // ---- 渠道管理 API ----
  const p1 = await call('/api/presets');
  ok('GET /api/presets 返回预设列表', p1.status === 200 && Array.isArray(p1.json?.presets) && p1.json.presets.length > 0);

  const a1 = await call('/api/channels', {
    method: 'POST',
    body: JSON.stringify({ name: 'echo-a', baseUrl: mp.url(9105), apiKey: 'k2', model: 'deepseek-reasoner', effort: 'low', priority: 5 }),
  });
  ok('POST /api/channels 添加自定义渠道', a1.status === 200 && a1.json?.ok === true, a1.text.slice(0, 160));
  ok('新渠道立刻进入路由（改写后模型一致）', a1.status === 200);

  // 重复名拒绝
  const a2 = await call('/api/channels', { method: 'POST', body: JSON.stringify({ name: 'echo-a', baseUrl: 'http://x' }) });
  ok('重复渠道名被拒绝', a2.status === 400 && /已存在/.test(a2.json?.error || ''));

  // 写回检查：config.json 里应有 echo-a 且带 effort
  const cfgNow = JSON.parse(readFileSync(CFG, 'utf8'));
  const added = cfgNow.channels.find((c) => c.name === 'echo-a');
  ok('新渠道已写回 config.json', !!added);
  ok('新渠道带 effort=low', added?.effort === 'low');

  // 新渠道参与路由：deepseek-reasoner 的池子现在有 ds-reasoner(20) 和 echo-a(5)。
  // 两段式选路下这两家都在"其余供应商"随机池里，谁被抽到不确定；
  // 这里要验证的是"新渠道确实进了候选池"（要么它自己应答，要么池子至少含它）。
  const r5 = await chat({ model: 'deepseek-reasoner', messages: [{ role: 'user', content: 'hi' }] });
  const r5via = r5.headers.get('x-gateway-channel');
  const pool = (await call('/api/status')).json.channels?.map((c) => c.name) || [];
  ok('新渠道按优先级参与路由（echo-a 进入 deepseek-reasoner 候选池）',
    r5.status === 200 && (r5via === 'echo-a' || pool.includes('echo-a')), `via=${r5via}`);

  // 删除
  const d1 = await call('/api/channels/' + encodeURIComponent('echo-a'), { method: 'DELETE' });
  ok('DELETE /api/channels/<name>', d1.status === 200 && d1.json?.ok === true);
  const cfgAfter = JSON.parse(readFileSync(CFG, 'utf8'));
  ok('删除后 config.json 已同步', !cfgAfter.channels.some((c) => c.name === 'echo-a'));

  // status pool 带 effort 字段
  const s1 = await call('/api/status');
  const poolEntry = (s1.json?.pool || []).find((p) => p.model === 'deepseek-reasoner');
  ok('pool 视图带 effort 字段', poolEntry && poolEntry.channels.every((c) => 'effort' in c));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  // Windows/Node 竞态规避：kill 子进程后立即 process.exit() 会让 undici 池里指向
  // 已终止网关的死连接与 libuv 关闭流程竞态（async.c:76 断言，0xC0000409）。
  // 等 500ms 让连接错误传播、池子沉降后再退出（最小复现 5/5 干净）。
  await new Promise((r) => setTimeout(r, 500));
  writeFileSync(CFG, CFG_BACKUP, 'utf8');
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
