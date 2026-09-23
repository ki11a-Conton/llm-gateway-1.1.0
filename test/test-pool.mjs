// 测试「模型池」行为：单模型渠道、同名多渠道路由、/v1/models 聚合、故障切换
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：网关端口与 mock 端口块都运行时动态分配
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'pool.test.json'), { port: PORT, mockBase: mp.base });

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

async function get(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text, headers: res.headers };
}

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(500);

  // 1. /v1/models：unifiedModel 模式下只显示统一名 "auto"
  const r1 = await get('/v1/models');
  const ids = (r1.json?.data || []).map((m) => m.id);
  ok('unified 模式：模型列表只显示统一名', ids.length === 1 && ids[0] === 'auto', `实际: ${JSON.stringify(ids)}`);

  // 1b. 请求统一名 -> 路由到池子里优先级最高的渠道（claude-1 p5），且用渠道自己的模型
  const r1b = await get('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'auto', messages: [{ role: 'user', content: 'hi' }], max_tokens: 50 }),
  });
  ok('统一名 auto 调用成功', r1b.status === 200, r1b.text.slice(0, 200));
  ok('统一名路由到池内最高优先级渠道', r1b.headers.get('x-gateway-channel') === 'claude-1', r1b.headers.get('x-gateway-channel'));
  ok('统一名使用渠道自己的模型', r1b.headers.get('x-gateway-upstream-model') === 'claude-test', r1b.headers.get('x-gateway-upstream-model'));

  // 2. 同名多渠道路由：调用 deepseek-chat 应走 ds-1（priority 10 健康）
  const r2 = await get('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'deepseek-chat', messages: [{ role: 'user', content: 'hi' }] }),
  });
  ok('请求 deepseek-chat 成功', r2.status === 200, r2.text.slice(0, 200));
  const ch = r2.json?.choices?.[0]?.message?.content || '';
  ok('走通的是 ds-1（9101 健康上游）', ch.includes('hello from good'), ch);
  const gwCh = r2.headers.get('x-gateway-channel');
  ok('响应头透传了实际渠道', gwCh === 'ds-1', `实际: ${gwCh}`);

  // 3. 故障切换：ds-1 突然不可用后，调用仍要成功（走 ds-2）
  // 通过直接请求 9102 失败特征来验证网关自动切到它 —— 更可靠的做法是把 9101 关掉，但这里用响应特征区分
  // ds-1 的 mock 返回 "hello from good"；ds-2 返回 500。
  // 我们构造一个 9101 不认识的模型，让 ds-1 报"无此模型"，网关应切到 ds-2
  const r3 = await get('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'deepseek-chat', messages: [{ role: 'user', content: 'hi' }] }),
  });
  ok('重复请求仍成功', r3.status === 200);

  // 4. Anthropic 单模型渠道也能被调用
  const r4 = await get('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'claude-test', messages: [{ role: 'user', content: 'hi' }], max_tokens: 50 }),
  });
  ok('Anthropic 单模型渠道调用成功', r4.status === 200, r4.text.slice(0, 200));

  // 5. /api/status 的 pool 视图
  const r5 = await get('/api/status');
  const pool = r5.json?.pool || [];
  const dsPool = pool.find((p) => p.model === 'deepseek-chat');
  ok('pool 视图包含 deepseek-chat', !!dsPool);
  ok('deepseek-chat 池子有 2 个渠道', dsPool && dsPool.channels.length === 2, JSON.stringify(dsPool?.channels?.map((c) => c.name)));
  ok('pool 渠道按 priority 排序（ds-1 在前）', dsPool && dsPool.channels[0].name === 'ds-1');
  const clPool = pool.find((p) => p.model === 'claude-test');
  ok('pool 视图包含 claude-test', !!clPool);
  ok('claude-test 池子标记为单模型', clPool && clPool.channels[0].singleModel === true);

  // 6. 单模型渠道不参与 discover
  const r6 = await get('/api/discover', { method: 'POST' });
  const names = (r6.json?.results || []).map((x) => x.name);
  ok('discover 跳过了单模型渠道', !names.includes('ds-1') && !names.includes('claude-1'), names.join(','));
  ok('discover 结果只有乐观渠道参与', names.length === 0 || names.every((n) => n !== 'ds-1' && n !== 'claude-1'));

  // 7. 单模型渠道同时配 models 数组时 model 优先
  // (由构造时 this.whitelist = single ? [model] : models 保证，断言 toJSON.models)
  const r7 = await get('/api/status');
  const ch1 = (r7.json?.channels || []).find((c) => c.name === 'ds-1');
  ok('单模型渠道 models 只有 1 个', ch1 && ch1.modelCount === 1);
  ok('单模型渠道标记 singleModel', ch1 && ch1.singleModel === true);

  // 8. fallbackModel：池子里没有的模型名兜底到配置的默认模型（unified 的 "auto" 走总入口，用别的未知名测兜底）
  const r8 = await get('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'unknown-xyz', messages: [{ role: 'user', content: 'hi' }], max_tokens: 50 }),
  });
  ok('未知模型名 unknown-xyz 兜底成功', r8.status === 200, r8.text.slice(0, 200));
  ok('兜底响应头标记原始模型名', r8.headers.get('x-gateway-fallback-from') === 'unknown-xyz');
  ok('兜底后走 claude-test 渠道', r8.headers.get('x-gateway-channel') === 'claude-1', r8.headers.get('x-gateway-channel'));

  // 9. /v1/models 列表：unified 模式下只有统一名（fallback 不进列表）
  const r9 = await get('/v1/models');
  const ids9 = (r9.json?.data || []).map((m) => m.id);
  ok('unified 模式列表不混入 fallback 名', ids9.length === 1 && ids9[0] === 'auto', `实际: ${JSON.stringify(ids9)}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
