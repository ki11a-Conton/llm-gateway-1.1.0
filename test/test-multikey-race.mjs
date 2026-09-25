// 测试「一条渠道叠加多个 key，并行竞速」：
//   - 同一 baseUrl 下把请求同时发给所有 key，第一个 2xx 的 key 胜出，其余立刻取消
//   - 响应头 x-gateway-key 暴露胜出序号/总数
//   - 单 key 渠道行为零变化（不回 x-gateway-key）
//   - 全部 key 失败时，优先拿"非鉴权/非余额"的错误继续降级，别让一个坏 key 把整条渠道判死
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'multikey.test.json'), { port: PORT, mockBase: mp.base });

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

const chat = (model, extra = {}) => get('/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ model, messages: [{ role: 'user', content: 'hi' }], max_tokens: 50, ...extra }),
});

// mock 9144 自身的控制面：清空/读取每个 key 的命中与被取消计数
const mockStats = async () => (await fetch(mp.url(9144, '/_stats'))).json();
const mockReset = async () => { await fetch(mp.url(9144, '/_reset')); };

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(300);

  // ---- 1. 非流式竞速：3 个 key 同时发，慢/坏 key 都不该拖住，最先成功的 fast 胜出 ----
  await mockReset();
  const r1 = await chat('mk-model');
  ok('叠加 key：非流式请求成功', r1.status === 200, r1.text.slice(0, 200));
  const c1 = r1.json?.choices?.[0]?.message?.content || '';
  ok('胜出的是最先成功的 key（sk-fast-a）', c1 === 'fast-sk-fast-a', c1);
  ok('响应头 x-gateway-key 标出胜出序号/总数', r1.headers.get('x-gateway-key') === '2/3', r1.headers.get('x-gateway-key'));
  ok('响应头 channel 为叠加 key 渠道', r1.headers.get('x-gateway-channel') === 'mk-race', r1.headers.get('x-gateway-channel'));

  const s1 = await mockStats();
  ok('三个 key 都收到了请求（真正并行竞速）',
    (s1.hits['sk-slow-a'] || 0) >= 1 && (s1.hits['sk-fast-a'] || 0) >= 1 && (s1.hits['sk-auth-a'] || 0) >= 1,
    JSON.stringify(s1.hits));
  ok('落败的慢 key 请求被取消', (s1.aborted['sk-slow-a'] || 0) >= 1, JSON.stringify(s1.aborted));

  // ---- 2. 流式竞速：走另一条发送路径（sendHead），同样回传 x-gateway-key ----
  await mockReset();
  const r2 = await chat('mk-model', { stream: true });
  ok('叠加 key：流式请求成功', r2.status === 200, r2.text.slice(0, 200));
  ok('流式响应体来自胜出 key', r2.text.includes('fast-sk-fast-a'), r2.text.slice(0, 200));
  ok('流式响应同样回传 x-gateway-key', r2.headers.get('x-gateway-key') === '2/3', r2.headers.get('x-gateway-key'));

  // ---- 3. 单 key 渠道：行为零变化（不回 x-gateway-key）----
  const r3 = await chat('single-model');
  ok('单 key 渠道调用成功', r3.status === 200, r3.text.slice(0, 200));
  ok('单 key 渠道不回 x-gateway-key', r3.headers.get('x-gateway-key') === null, String(r3.headers.get('x-gateway-key')));
  ok('单 key 渠道仍标记 channel', r3.headers.get('x-gateway-channel') === 'mk-single', r3.headers.get('x-gateway-channel'));

  // ---- 4. 全部 key 失败（401 + 500 混合）：取非鉴权错误降级，仍能换到下一家 ----
  const r4 = await chat('fail-model');
  ok('混合失败时仍降级到下一渠道成功', r4.status === 200, r4.text.slice(0, 200));
  ok('降级走的是兜底渠道', r4.headers.get('x-gateway-channel') === 'mk-allfail-fb', r4.headers.get('x-gateway-channel'));

  // ---- 5. 全部 key 失败且无兜底：错误按"非鉴权"口径抛出（HTTP 500 而非 401）----
  const r5 = await chat('strict-model');
  ok('全部 key 失败时返回 503', r5.status === 503, `status=${r5.status} ${r5.text.slice(0, 200)}`);
  ok('采用了非鉴权的 500 错误继续降级（不是 401）',
    r5.text.includes('HTTP 500') && !r5.text.includes('HTTP 401'), r5.text.slice(0, 300));

  // ---- 6. 面板/状态视图能看到叠加的 key 数 ----
  const r6 = await get('/api/status');
  const chs = r6.json?.channels || [];
  const race = chs.find((c) => c.name === 'mk-race');
  const single = chs.find((c) => c.name === 'mk-single');
  ok('status 里叠加渠道 keyCount=3', race && race.keyCount === 3, JSON.stringify(race?.keyCount));
  ok('status 里单 key 渠道 keyCount=1', single && single.keyCount === 1, JSON.stringify(single?.keyCount));

  // ---- 7. 面板「添加供应商」多行 key：POST /api/channels 应落成 apiKeys 数组 ----
  const r7 = await get('/api/channels', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: 'mk-added',
      protocol: 'openai',
      baseUrl: `http://127.0.0.1:${mp.port(9144)}`,
      apiKey: 'sk-fast-x\nsk-slow-x\nsk-auth-x', // 面板文本域：每行一个 key
      model: 'mk-model',
      priority: 50,
    }),
  });
  ok('面板新增叠加 key 渠道成功', r7.status === 200 && r7.json?.ok === true, r7.text.slice(0, 200));
  ok('新增后 keyCount=3', r7.json?.channel?.keyCount === 3, JSON.stringify(r7.json?.channel?.keyCount));
  const r7b = await get('/api/status');
  const added = (r7b.json?.channels || []).find((c) => c.name === 'mk-added');
  ok('热重载后叠加渠道可见且 keyCount=3', added && added.keyCount === 3, JSON.stringify(added?.keyCount));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);