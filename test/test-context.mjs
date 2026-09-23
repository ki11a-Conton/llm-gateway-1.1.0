// 上下文感知路由测试：
//  ① 单元：prompt token 估算函数（CJK/ASCII/消息头/tool_calls/system）
//  ② 单元：candidatesFor 按渠道 contextWindow 过滤（装不下排除；全排除退回全池）
//  ③ 集成：大请求跳过小窗口渠道直达大窗口渠道；小请求仍走小窗口渠道
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../lib/channels.mjs';
import { estimatePromptTokens } from '../lib/util.mjs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：单元部分（ChannelManager）与集成部分（spawn 网关）共用同一份 materialize 后的配置，
// 网关端口与 mock 端口块都运行时动态分配
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'context.test.json'), { port: PORT, mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- ① 估算函数 ----
ok('估算：500 个 ASCII 字符 > 100 token', estimatePromptTokens({ messages: [{ role: 'user', content: 'x'.repeat(500) }] }) > 100);
ok('估算：hi 远小于 100', estimatePromptTokens({ messages: [{ role: 'user', content: 'hi' }] }) < 100);
ok('估算：中文 120 字 > 100', estimatePromptTokens({ messages: [{ role: 'user', content: '你好'.repeat(60) }] }) > 100);
ok('估算：tool_calls 计入', estimatePromptTokens({
  messages: [{ role: 'assistant', content: '', tool_calls: [{ function: { name: 'foo', arguments: JSON.stringify({ a: 'x'.repeat(200) }) } }] }],
}) > 50);
ok('估算：system 提示词计入', estimatePromptTokens({ system: 's'.repeat(400), messages: [{ role: 'user', content: 'hi' }] }) > 100);

// ---- ② candidatesFor 上下文过滤 ----
{
  const mgr = new ChannelManager(CFG);
  mgr.load();
  const names = (pt) => mgr.candidatesFor('auto', { promptTokens: pt }).map((c) => c.name);
  ok('过滤：小请求两个渠道都在', names(50).includes('small') && names(50).includes('big'));
  ok('过滤：5000 token 请求只剩 big（small 窗口 100 装不下）', JSON.stringify(names(5000)) === JSON.stringify(['big']), names(5000).join(','));
  ok('过滤：全部装不下时退回全池尽力而为', names(999999999).length === 2);
  ok('过滤：未传 promptTokens 不做过滤', names(undefined).length === 2);
}

// ---- ③ 集成：起假上游 + 网关 ----
const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  for (let i = 0; i < 50; i++) {
    try { const r = await fetch(`http://127.0.0.1:${PORT}/health`); if (r.ok) break; } catch {}
    await wait(200);
  }
  await wait(300);

  const call = async (content) => {
    const res = await fetch(`http://127.0.0.1:${PORT}/v1/chat/completions`, {
      method: 'POST',
      headers: { authorization: 'Bearer TESTKEY', 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'auto', messages: [{ role: 'user', content }], max_tokens: 10 }),
    });
    const text = await res.text();
    return { status: res.status, channel: res.headers.get('x-gateway-channel'), text };
  };
  const status = async () => {
    const res = await fetch(`http://127.0.0.1:${PORT}/api/status`, { headers: { authorization: 'Bearer TESTKEY' } });
    return res.json();
  };

  // 先小请求（建立粘性 small），再大请求（small 应被上下文过滤跳过）
  const tiny = await call('hi');
  ok('小请求：走 small', tiny.status === 200 && tiny.channel === 'small', `status=${tiny.status} ch=${tiny.channel} ${tiny.text.slice(0, 120)}`);

  const big = await call('A'.repeat(600)); // 估算约 150+ token > small 的 100
  ok('大请求：直达 big（小窗口 small 被跳过）', big.status === 200 && big.channel === 'big', `status=${big.status} ch=${big.channel} ${big.text.slice(0, 120)}`);
  const snap = await status();
  const smallCh = (snap.channels || []).find((c) => c.name === 'small');
  ok('大请求：small 一次都没被尝试（failed=0）', smallCh && smallCh.failed === 0, `failed=${smallCh?.failed}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  await wait(300);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);