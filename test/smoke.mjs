// 冒烟测试：启动假上游 + 网关，验证鉴权 / 模型聚合 / 故障切换 / 协议转换 / 熔断
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：端口运行时动态分配（网关一个，mock 一段连续端口块），避免并行跑测试互相抢端口
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'gateway.test.json'), { port: PORT, mockBase: mp.base });
const BASE = `http://127.0.0.1:${PORT}`;
const KEY = 'TESTKEY';

let pass = 0;
let fail = 0;
const ok = (name, cond, extra = '') => {
  if (cond) {
    pass += 1;
    console.log(`  \x1b[32mPASS\x1b[0m ${name}`);
  } else {
    fail += 1;
    console.log(`  \x1b[31mFAIL\x1b[0m ${name} ${extra}`);
  }
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kill = (p) => {
  if (!p || p.exitCode !== null) return;
  p.kill('SIGTERM');
  setTimeout(() => p.exitCode === null && p.kill('SIGKILL'), 1500).unref();
};

async function waitReady(url, timeout = 15000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    try {
      const r = await fetch(url);
      if (r.ok) return true;
    } catch {
      /* retry */
    }
    await sleep(200);
  }
  return false;
}

const post = (pathname, body, headers = {}) =>
  fetch(BASE + pathname, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}`, ...headers },
    body: JSON.stringify(body),
  });

const main = async () => {
  console.log('\n启动假上游与网关…\n');
  const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
  await sleep(600);
  const gw = spawn(
    NODE,
    [
      path.join(ROOT, 'server.mjs'),
      '--config', CFG,
      '--port', String(PORT),
      '--api-key', KEY,
      '--log-level', 'warn',
    ],
    { stdio: 'ignore' },
  );

  const cleanup = () => { kill(mock); kill(gw); };
  process.on('exit', cleanup);

  if (!(await waitReady(`${BASE}/health`))) {
    console.error('网关启动失败');
    cleanup();
    process.exit(1);
  }
  await sleep(400); // 等模型发现完成

  try {
    // 1. 鉴权
    const noAuth = await fetch(`${BASE}/v1/models`);
    ok('缺少 API Key 返回 401', noAuth.status === 401, `got ${noAuth.status}`);
    const wrongAuth = await fetch(`${BASE}/v1/models`, { headers: { authorization: 'Bearer nope' } });
    ok('错误的 API Key 返回 401', wrongAuth.status === 401, `got ${wrongAuth.status}`);

    // 2. 模型聚合（来自自动发现）
    const modelsRes = await fetch(`${BASE}/v1/models`, { headers: { authorization: `Bearer ${KEY}` } });
    const models = await modelsRes.json();
    const ids = (models.data || []).map((m) => m.id);
    ok('/v1/models 聚合上游模型', ids.includes('test-model-1') && ids.includes('claude-test'), JSON.stringify(ids));

    // 3. 非流式 + 故障切换（bad 渠道 500 -> 自动切 good）
    const r1 = await post('/v1/chat/completions', {
      model: 'test-model-1',
      messages: [{ role: 'user', content: 'hi' }],
    });
    const j1 = await r1.json();
    ok('非流式自动切换到可用渠道', r1.status === 200 && j1.choices?.[0]?.message?.content === 'hello from good',
      `status=${r1.status} body=${JSON.stringify(j1).slice(0, 200)}`);
    ok('响应头回传实际渠道名', r1.headers.get('x-gateway-channel') === 'good', r1.headers.get('x-gateway-channel'));

    // 4. 流式透传
    const r2 = await post('/v1/chat/completions', {
      model: 'test-model-1',
      messages: [{ role: 'user', content: 'hi' }],
      stream: true,
    });
    const text2 = await r2.text();
    ok('流式返回 text/event-stream', (r2.headers.get('content-type') || '').includes('text/event-stream'));
    ok('流式内容拼接正确', text2.includes('你好') && text2.includes('世界') && text2.includes('!'));
    ok('流式以 [DONE] 收尾', text2.trimEnd().endsWith('data: [DONE]'), text2.slice(-80));

    // 5. Anthropic 原生协议 -> OpenAI 非流式
    const r3 = await post('/v1/chat/completions', {
      model: 'claude-test',
      messages: [
        { role: 'system', content: '你是助手' },
        { role: 'user', content: 'hi' },
      ],
    });
    const j3 = await r3.json();
    ok('Anthropic 非流式响应转换', j3.choices?.[0]?.message?.content === 'hello from anthropic',
      JSON.stringify(j3).slice(0, 200));
    ok('Anthropic usage 映射', j3.usage?.prompt_tokens === 11 && j3.usage?.completion_tokens === 5,
      JSON.stringify(j3.usage));
    ok('Anthropic finish_reason 映射', j3.choices?.[0]?.finish_reason === 'stop');

    // 6. Anthropic 流式 -> OpenAI chunk 流
    const r4 = await post('/v1/chat/completions', {
      model: 'claude-test',
      messages: [{ role: 'user', content: 'hi' }],
      stream: true,
    });
    const text4 = await r4.text();
    const chunks = text4.split('\n\n').filter((l) => l.startsWith('data: ') && !l.includes('[DONE]'));
    const first = chunks.length ? JSON.parse(chunks[0].slice(6)) : null;
    ok('Anthropic 流式转为 OpenAI chunk', text4.includes('"object":"chat.completion.chunk"'), text4.slice(0, 160));
    ok('Anthropic 流式首帧带 role', first?.choices?.[0]?.delta?.role === 'assistant', JSON.stringify(first));
    const joined = chunks.map((c) => JSON.parse(c.slice(6)).choices?.[0]?.delta?.content || '').join('');
    ok('Anthropic 流式内容完整', joined === '来自Anthropic的流', JSON.stringify(joined));
    ok('Anthropic 流式 [DONE] 收尾', text4.trimEnd().endsWith('data: [DONE]'));

    // 7. 熔断：连续打几次，bad 渠道应被熔断
    for (let i = 0; i < 3; i += 1) {
      await post('/v1/chat/completions', { model: 'test-model-1', messages: [{ role: 'user', content: 'hi' }] });
    }
    const status = await (await fetch(`${BASE}/api/status`)).json();
    const bad = status.channels.find((c) => c.name === 'bad');
    const good = status.channels.find((c) => c.name === 'good');
    ok('失败渠道被计入熔断', bad.coolingDown === true && bad.failures >= 2, JSON.stringify({ f: bad.failures, cd: bad.coolingDown }));
    ok('成功渠道保持健康', good.healthy === true && good.failed === 0, JSON.stringify(good.successRate));

    // 8. 熔断后不再打到坏渠道（成功率应为 100%）
    const r5 = await post('/v1/chat/completions', { model: 'test-model-1', messages: [{ role: 'user', content: 'hi' }] });
    ok('熔断后仍可用', r5.status === 200 && r5.headers.get('x-gateway-channel') === 'good');

    // 9. 没有任何渠道支持的模型 -> 明确的 400
    const r6 = await post('/v1/chat/completions', { model: 'no-such-model-xyz', messages: [{ role: 'user', content: 'hi' }] });
    const j6 = await r6.json();
    ok('无渠道支持的模型返回 400', r6.status === 400 && j6.error?.code === 'model_not_found',
      `status=${r6.status} ${JSON.stringify(j6).slice(0, 160)}`);

    // 10. routes 显式路由与白名单取交集（anth 不支持 test-model-2，应落到 good）
    const r7 = await post('/v1/chat/completions', { model: 'test-model-2', messages: [{ role: 'user', content: 'hi' }] });
    ok('routes 与渠道白名单取交集', r7.status === 200 && r7.headers.get('x-gateway-channel') === 'good',
      `channel=${r7.headers.get('x-gateway-channel')}`);

    // 11-13. Anthropic 客户端（Claude Code 等）走 /v1/messages
    const r8 = await post('/v1/messages', {
      model: 'test-model-1', max_tokens: 100, messages: [{ role: 'user', content: 'hi' }],
    });
    const j8 = await r8.json();
    ok('/v1/messages 输出 Anthropic 结构', j8.type === 'message' && j8.content?.[0]?.text === 'hello from good',
      JSON.stringify(j8).slice(0, 220));
    ok('/v1/messages usage 反向映射', j8.usage?.input_tokens === 1 && j8.usage?.output_tokens === 2,
      JSON.stringify(j8.usage));

    const r9 = await post('/v1/messages', {
      model: 'test-model-1', max_tokens: 100, messages: [{ role: 'user', content: 'hi' }], stream: true,
    });
    const t9 = await r9.text();
    ok('/v1/messages 流式发出 message_start', t9.includes('event: message_start'));
    ok('/v1/messages 流式发出 text_delta', t9.includes('"type":"text_delta"'));
    ok('/v1/messages 流式以 message_stop 收尾', t9.includes('event: message_stop'));
    const deltas = [...t9.matchAll(/"type":"text_delta","text":"((?:[^"\\]|\\.)*)"/g)]
      .map((m) => JSON.parse('"' + m[1] + '"')).join('');
    ok('/v1/messages 流式文本完整', deltas === '你好世界!', JSON.stringify(deltas));

    const r10 = await post('/v1/messages', {
      model: 'claude-test', max_tokens: 100, messages: [{ role: 'user', content: 'hi' }],
    });
    const j10 = await r10.json();
    ok('/v1/messages 同协议原样返回', j10.content?.[0]?.text === 'hello from anthropic' && j10.id === 'msg_mock',
      JSON.stringify(j10).slice(0, 220));

    // 14. 面板可访问
    const panel = await fetch(BASE + '/');
    ok('状态面板可访问', panel.status === 200 && (await panel.text()).includes('本地模型聚合网关'));
  } catch (err) {
    fail += 1;
    console.log(`  \x1b[31mFAIL\x1b[0m 测试异常: ${err.stack || err.message}`);
  } finally {
    cleanup();
  }

  console.log(`\n结果: \x1b[32m${pass} 通过\x1b[0m, ${fail ? `\x1b[31m${fail} 失败\x1b[0m` : '0 失败'}\n`);
  process.exit(fail ? 1 : 0);
};

main();
