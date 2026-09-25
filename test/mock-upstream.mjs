// 测试用的假上游：9101=正常 OpenAI，9102=总是 500，9103=Anthropic 原生协议
import http from 'node:http';

// P5：端口整体可平移。设置 MOCK_PORT_BASE=B 后，下面写的逻辑端口 p 会实际监听在
// B + (p - 9101)，也就是 B..B+43 这一段连续端口（测试用 test/lib/ports.mjs 动态申请）。
// **不设置 MOCK_PORT_BASE 时行为与改动前逐位一致**（仍监听 9101..9144），
// 所以单独 `node test/mock-upstream.mjs` 或别的工具直接用它都不受影响。
const PORT_BASE = Number(process.env.MOCK_PORT_BASE || 0);
const PORT_REF = 9101;
const actualPort = (logical) => (PORT_BASE ? PORT_BASE + (logical - PORT_REF) : logical);

const claimedPorts = new Set();
function start(port, handler) {
  // 同一个 mock 进程里重复注册同一端口 = 代码写错了：后注册的 handler 永远收不到请求，
  // 测试会拿到前一个 handler 的响应，以非常莫名的方式失败。必须立刻炸出来。
  // （上次踩坑：新增的 9121/9122/9123 处理器与负载均衡用的 A/B/C 上游撞号，
  //   空响应测试实际收到的是 "from B"，误以为网关的空调用守卫失效。）
  const listenPort = actualPort(port);
  if (claimedPorts.has(listenPort)) throw new Error(`mock 端口重复注册: ${port}`);
  claimedPorts.add(listenPort);
  const server = http.createServer(handler);
  // 端口被占用 = 端口方案出错了。**必须立刻炸掉**，不能"降级为告警继续"：
  // P5 起测试用的是运行时动态端口（MOCK_PORT_BASE），两个并发进程的连续段一旦重叠，
  // 双方会逐端口抢绑：谁都不报错、各自只绑到一部分端口，于是**网关会连到对方进程的 mock**，
  // 测试拿到完全无关的载荷（实测偏移 17 个逻辑端口），变成极难排查的假失败/假成功。
  // 明确退出比"带着半个 mock 继续跑"安全得多——失败会直接表现为套件红，而不是错误结论。
  server.on('error', (err) => {
    console.error(`mock:${listenPort}（逻辑端口 ${port}）启动失败: ${err.message}`);
    if (err.code === 'EADDRINUSE') {
      console.error('mock: 端口冲突——多半是另一个测试进程拿了重叠的端口段；请单独/串行重跑该套件');
    }
    process.exit(1);
  });
  server.listen(listenPort, '127.0.0.1', () => console.log('mock:' + listenPort));
  return server;
}

const json = (res, code, obj) => {
  const b = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(b) });
  res.end(b);
};

// ---- 9101: 正常 OpenAI 兼容上游 ----
let lastHeaders = {};
start(9101, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_last-headers') return json(res, 200, lastHeaders);
  lastHeaders = { ...req.headers };
  if (url.pathname === '/v1/models') {
    return json(res, 200, { object: 'list', data: [{ id: 'test-model-1' }, { id: 'test-model-2' }] });
  }
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    if (body.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const id = 'chatcmpl-mock';
      for (const piece of ['你好', '世界', '!']) {
        res.write(
          `data: ${JSON.stringify({
            id,
            object: 'chat.completion.chunk',
            created: 1,
            model: body.model,
            choices: [{ index: 0, delta: { content: piece }, finish_reason: null }],
          })}\n\n`,
        );
      }
      res.write(
        `data: ${JSON.stringify({
          id,
          object: 'chat.completion.chunk',
          created: 1,
          model: body.model,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        })}\n\n`,
      );
      return res.end('data: [DONE]\n\n');
    }
    return json(res, 200, {
      id: 'chatcmpl-mock',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'hello from good' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9107: 对 chat 请求永远返回 404 "model not available"（测统一池剔除不支持渠道）----
start(9107, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'ghost-model' }] });
  json(res, 404, { error: { message: "The requested model 'ghost-model' is not available." } });
});

// ---- 9108: 永远 429 TPM 限流（测限流渠道短冷却）----
start(9108, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'flaky-429' }] });
  json(res, 429, { error: { message: 'inference tpm exhausted', type: 'rate_limit' } });
});

// ---- 9110: 第一次请求 500，之后都成功（测重试循环后成功）----
let once500Count = 0;
start(9110, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'ok-model' }] });
  if (url.pathname === '/_reset-500') { once500Count = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/chat/completions') {
    once500Count += 1;
    if (once500Count === 1) return json(res, 500, { error: { message: 'first try boom', type: 'server_error' } });
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-ok',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'ok from good' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9111: 第一次 chat 请求 500，之后成功（测同渠道重试一次）----
let once500v2 = 0;
start(9111, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'rbad-model' }] });
  if (url.pathname === '/_reset-500v2') { once500v2 = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/chat/completions') {
    once500v2 += 1;
    if (once500v2 === 1) return json(res, 500, { error: { message: 'first try boom v2', type: 'server_error' } });
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-rbad',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'rbad ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9112: 永远成功的 OpenAI 上游（测粘性等）----
start(9112, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'g1-model' }] });
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-g1',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'g1 ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9113: 永远成功的第二个上游（测粘性 vs 轮询）----
start(9113, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'g2-model' }] });
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-g2',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'g2 ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9121/9122/9123: 三个同构上游，带一点点延迟（测多子代理负载均衡 / 会话亲和）----
// 用不同端口模拟"三家不同供应商"，便于观察请求被摊到哪几家
for (const [port, tag] of [[9121, 'A'], [9122, 'B'], [9123, 'C']]) {
  start(port, async (req, res) => {
    const url = new URL(req.url, 'http://x');
    if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'm1' }] });
    if (url.pathname === '/v1/chat/completions') {
      let raw = '';
      for await (const c of req) raw += c;
      const body = JSON.parse(raw || '{}');
      await new Promise((r) => setTimeout(r, 40)); // 制造在途窗口，让 /api/status 抓得到
      return json(res, 200, {
        id: 'chatcmpl-' + tag,
        object: 'chat.completion',
        created: 1,
        model: body.model,
        choices: [{ index: 0, message: { role: 'assistant', content: `from ${tag}` }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      });
    }
    json(res, 404, { error: { message: 'nf' } });
  });
}

// ---- 9116: models 接口正常、chat 接口永远 500（测探活必须按 chat 可用性判断）----
start(9116, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'dead-model' }] });
  json(res, 500, { error: { message: 'chat boom', type: 'server_error' } });
});

// ---- 9118: 前 5 次 chat 请求 500，之后成功（测渠道重试预算内多次重试后成功）----
let flaky500v3 = 0;
start(9118, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'budget-model' }] });
  if (url.pathname === '/_reset-500v3') { flaky500v3 = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/chat/completions') {
    flaky500v3 += 1;
    if (flaky500v3 <= 5) return json(res, 500, { error: { message: 'flaky v3 boom', type: 'server_error' } });
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-budget',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'budget ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9119: 把工具调用写成正文的上游（测正文泄漏守卫；请求带不带 tools 决定守卫是否生效）----
start(9119, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') {
    return json(res, 200, { data: [{ id: 'guard-model' }, { id: 'off-model' }] });
  }
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    const garbage = '<｜tool_calls｜> <invoke name="pwsh"><parameter name="command">dir</parameter></invoke>';
    if (body.stream) {
      // 标记 '<｜tool_calls｜>' 被拆到相邻 chunk（模拟 token 切分），守卫必须拼接后才能识别
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const id = 'chatcmpl-leak';
      for (const piece of ['让我来', '<｜tool', '_calls｜> <invoke name="pwsh">', '<parameter name="command">dir</parameter></invoke>']) {
        res.write(
          `data: ${JSON.stringify({
            id,
            object: 'chat.completion.chunk',
            created: 1,
            model: body.model,
            choices: [{ index: 0, delta: { content: piece }, finish_reason: null }],
          })}\n\n`,
        );
      }
      res.write(
        `data: ${JSON.stringify({
          id,
          object: 'chat.completion.chunk',
          created: 1,
          model: body.model,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        })}\n\n`,
      );
      return res.end('data: [DONE]\n\n');
    }
    return json(res, 200, {
      id: 'chatcmpl-leak',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: garbage }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9102: 永远失败的上游 ----
start(9102, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'test-model-1' }] });
  json(res, 500, { error: { message: 'upstream boom', type: 'server_error' } });
});

// ---- 9106: 前 N 次请求 429，之后成功的 OpenAI 上游（测渠道内重试）----
// N 由 /_reset-flaky?fail=N 设定，默认 2
let flakyCount = 0;
let flakyFail = 2;
start(9106, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'flaky-model' }] });
  if (url.pathname === '/_reset-flaky') {
    flakyCount = 0;
    const n = url.searchParams.get('fail');
    flakyFail = n === null ? 2 : Number(n);
    return json(res, 200, { ok: true, fail: flakyFail });
  }
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    flakyCount += 1;
    if (flakyCount <= flakyFail) {
      return json(res, 429, { error: { message: 'inference tpm exhausted', type: 'rate_limit' } });
    }
    return json(res, 200, {
      id: 'chatcmpl-flaky',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'flaky ok after ' + flakyCount + ' tries' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9104: 永远 401 的上游（测不可重试错误）----
start(9104, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'auth-model' }] });
  json(res, 401, { error: { message: 'invalid api key', type: 'authentication_error' } });
});

// ---- 9105: 回显请求体的 OpenAI 上游（测 reasoning_effort 透传 / effortMap 改写）----
start(9105, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'deepseek-reasoner' }] });
  if (url.pathname === '/v1/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    return json(res, 200, {
      id: 'chatcmpl-echo',
      object: 'chat.completion',
      created: 1,
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'echo:' + body.model + ':effort=' + (body.reasoning_effort ?? 'none') }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      _seen_effort: body.reasoning_effort ?? null,
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9103: Anthropic 原生协议上游 ----
start(9103, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') {
    return json(res, 200, { data: [{ id: 'claude-test', display_name: 'Claude Test' }] });
  }
  if (url.pathname === '/v1/messages') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');

    if (body.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`event: message_start\ndata: ${JSON.stringify({
        type: 'message_start',
        message: { id: 'msg_mock', model: body.model, usage: { input_tokens: 11 } },
      })}\n\n`);
      res.write(`event: content_block_start\ndata: ${JSON.stringify({
        type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' },
      })}\n\n`);
      for (const piece of ['来自', 'Anthropic', '的流']) {
        res.write(`event: content_block_delta\ndata: ${JSON.stringify({
          type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: piece },
        })}\n\n`);
      }
      res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: 0 })}\n\n`);
      res.write(`event: message_delta\ndata: ${JSON.stringify({
        type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 7 },
      })}\n\n`);
      res.write(`event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`);
      return res.end();
    }

    return json(res, 200, {
      id: 'msg_mock',
      type: 'message',
      role: 'assistant',
      model: body.model,
      content: [{ type: 'text', text: 'hello from anthropic' }],
      stop_reason: 'end_turn',
      usage: { input_tokens: 11, output_tokens: 5 },
    });
  }
  json(res, 404, { error: { message: 'nf' } });
});

// ---- 9120: 余额不足（HTTP 400 + Insufficient balance）——测"立刻换家 + 长冷却" ----
let balanceHits = 0;
start(9120, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_hits') return json(res, 200, { hits: balanceHits });
  if (url.pathname === '/_reset') { balanceHits = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'balance-model' }] });
  balanceHits += 1;
  json(res, 400, { error: { message: 'Insufficient balance, please top up your account', type: 'insufficient_balance' } });
});

// ---- 9130: 鉴权失败（HTTP 401 invalid api key）——测"绝不挂住请求，直接换家" ----
let authHits = 0;
start(9130, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_hits') return json(res, 200, { hits: authHits });
  if (url.pathname === '/_reset') { authHits = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'auth-model' }] });
  authHits += 1;
  json(res, 401, { error: { message: 'Invalid API key provided', type: 'authentication_error' } });
});

// ---- 9131: HTTP 200 但内容完全为空（GPT 系常见"假成功"）——测空响应换家 ----
start(9131, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'empty-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(`data: ${JSON.stringify({
      id: 'chatcmpl-empty', object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    })}\n\n`);
    return res.end('data: [DONE]\n\n');
  }
  json(res, 200, {
    id: 'chatcmpl-empty', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: '' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 0, total_tokens: 1 },
  });
});

// ---- 9132: 回声上游——把收到的 model / reasoning_effort / thinking 原样回显（测思考强度注入）----
start(9132, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'think-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  const seen = [
    `model=${body.model}`,
    `effort=${body.reasoning_effort ?? 'none'}`,
    `budget=${body.thinking?.budget_tokens ?? 'none'}`,
    `enable=${body.enable_thinking ?? 'none'}`,
  ].join('|');
  json(res, 200, {
    id: 'chatcmpl-echo', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: seen }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
});

// ---- 9124: 优先池渠道用的正常上游（永远成功，供两段式选路测试）----
start(9124, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'tier-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  json(res, 200, {
    id: 'chatcmpl-pref', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: 'hello from preferred tier' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
  });
});

// ---- 9125: 把思维链直接写进 content（实测 channel 17 / vyei 的行为）----
const INLINE_COT_EN = `Sure! Let's break this down step by step.

**Step 1: Break down the multiplication using distributive property**  
We can think of 23 as 20 + 3, so:
17 x 23 = (17 x 20) + (17 x 3)

**Step 2: Multiply each part**
- 17 x 20 = 340
- 17 x 3 = 51

So the final answer is 391.`;

start(9125, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'leak-think' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    // 按小片切开，模拟思维链与答案跨多个 chunk 流出
    const pieces = INLINE_COT_EN.match(/[\s\S]{1,60}/g) || [];
    for (const piece of pieces) {
      res.write(`data: ${JSON.stringify({
        id: 'chatcmpl-cot', object: 'chat.completion.chunk', created: 1, model: body.model,
        choices: [{ index: 0, delta: { content: piece }, finish_reason: null }],
      })}\n\n`);
    }
    res.write(`data: ${JSON.stringify({
      id: 'chatcmpl-cot', object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    })}\n\n`);
    return res.end('data: [DONE]\n\n');
  }
  json(res, 200, {
    id: 'chatcmpl-cot', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: INLINE_COT_EN }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 40, total_tokens: 41 },
  });
});

// ---- 9126: 用非标准字段名（reasoning）承载思考，content 是正式回答 ----
start(9126, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'reason-field' }] });
  json(res, 200, {
    id: 'chatcmpl-rf', object: 'chat.completion', created: 1, model: 'reason-field',
    choices: [{
      index: 0,
      message: { role: 'assistant', content: '这是正式回答', reasoning: '让我想想这个问题……' },
      finish_reason: 'stop',
    }],
    usage: { prompt_tokens: 1, completion_tokens: 8, total_tokens: 9 },
  });
});

// ---- 9127: 只返回 reasoning_content，content 为空（agent 仍应拿得到东西，不算空调用）----
start(9127, (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'reason-only' }] });
  json(res, 200, {
    id: 'chatcmpl-ro', object: 'chat.completion', created: 1, model: 'reason-only',
    choices: [{
      index: 0,
      message: { role: 'assistant', content: null, reasoning_content: '我在思考这个问题的解法' },
      finish_reason: 'stop',
    }],
    usage: { prompt_tokens: 1, completion_tokens: 5, total_tokens: 6 },
  });
});

// ---- 9128: OpenAI 上游，用 reasoning_content 承载思考、content 是正式回答（测 OpenAI->Anthropic 桥接）----
// 非流式：message.reasoning_content + message.content
// 流式：先若干 reasoning_content 增量，再若干 content 增量，最后 finish_reason=stop + [DONE]
const REASON_TEXT = '让我一步步算：17×23 = 17×20 + 17×3 = 340 + 51 = 391。';
start(9128, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'reason-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const id = 'chatcmpl-reason';
    const chunk = (delta, finish = null) => `data: ${JSON.stringify({
      id, object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta, finish_reason: finish }],
    })}\n\n`;
    // 思考先流出（拆成几片，验证 thinking 块只开一次、增量连续）
    for (const piece of ['让我一步步算：', '17×23 = 17×20 + 17×3 = 340 + 51 = 391。']) {
      res.write(chunk({ reasoning_content: piece }));
    }
    // 再流正文
    for (const piece of ['最终答案：', '391']) {
      res.write(chunk({ content: piece }));
    }
    res.write(chunk({}, 'stop'));
    return res.end('data: [DONE]\n\n');
  }
  json(res, 200, {
    id: 'chatcmpl-reason', object: 'chat.completion', created: 1, model: body.model,
    choices: [{
      index: 0,
      message: { role: 'assistant', content: '最终答案：391', reasoning_content: REASON_TEXT },
      finish_reason: 'stop',
    }],
    usage: { prompt_tokens: 1, completion_tokens: 30, total_tokens: 31 },
  });
});

// ---- 9129: HTTP 200 但正文只有装饰符号（"伪空"）——测空调用守卫的 pseudo_empty 分支 ----
start(9129, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'pseudo-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    // 只有代码围栏壳，一个有效字符都没有
    res.write(`data: ${JSON.stringify({
      id: 'chatcmpl-pseudo', object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta: { content: '```\n\n```' }, finish_reason: null }],
    })}\n\n`);
    res.write(`data: ${JSON.stringify({
      id: 'chatcmpl-pseudo', object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    })}\n\n`);
    return res.end('data: [DONE]\n\n');
  }
  json(res, 200, {
    id: 'chatcmpl-pseudo', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: '.' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
});

// ---- 9133: 可在"余额不足"与"健康"之间切换的上游（测冷却渠道被探活救回 C1）----
// 默认 dead（400 insufficient balance）；POST /_revive 变健康，/_kill 变回死。
let reviveAlive = false;
start(9133, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'revive-model' }] });
  if (url.pathname === '/_revive') { reviveAlive = true; return json(res, 200, { alive: true }); }
  if (url.pathname === '/_kill') { reviveAlive = false; return json(res, 200, { alive: false }); }
  if (url.pathname === '/_state') return json(res, 200, { alive: reviveAlive });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  if (!reviveAlive) {
    return json(res, 400, { error: { message: 'Insufficient balance, please top up', type: 'insufficient_balance' } });
  }
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  json(res, 200, {
    id: 'chatcmpl-revive', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: 'revived ok' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
  });
});

// ---- 9134: 只发一帧 SSE 就断流（测 D2：响应头未发出时应允许整体换家）----
start(9134, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'break-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  // 一帧短正文（不足 64 字符、没有 finish_reason），随后静默断开：客户端一个完整响应都拿不到
  res.write(`data: ${JSON.stringify({
    id: 'chatcmpl-break', object: 'chat.completion.chunk', created: 1, model: 'break-model',
    choices: [{ index: 0, delta: { content: '半' }, finish_reason: null }],
  })}\n\n`, () => {
    setTimeout(() => res.destroy(), 30);
  });
});

// ---- 9135: 首字延迟（TTFT）测试上游：先发一小段（<64 字符）正文，停顿后继续 ----
// 用途：量测思考守卫"攒头部"对流式首字延迟的影响（A4）
start(9135, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'ttft-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  if (!body.stream) {
    return json(res, 200, {
      id: 'chatcmpl-ttft', object: 'chat.completion', created: 1, model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'ttft ok' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
    });
  }
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const chunk = (delta, finish = null) => `data: ${JSON.stringify({
    id: 'chatcmpl-ttft', object: 'chat.completion.chunk', created: 1, model: body.model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  })}\n\n`;
  // 第一小段（故意不足 64 字符，守卫会继续攒头部）
  res.write(chunk({ content: '开头几个字' }));
  await new Promise((r) => setTimeout(r, 400));
  res.write(chunk({ content: '——后面才是正文的剩余部分' }));
  res.write(chunk({}, 'stop'));
  res.end('data: [DONE]\n\n');
});

// ---- 9136: 先发一段"够长"的正文（>=64 字符，守卫会放行并已写回客户端）再断流 ----
// 用途：验证"响应头已发出后不允许换家"（D2 的另一半）
start(9136, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'latebreak-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  // 80 个字符 > CONTENT_MIN_CHARS(64) -> 网关会立即放行头部并写回客户端
  res.write(`data: ${JSON.stringify({
    id: 'chatcmpl-latebreak', object: 'chat.completion.chunk', created: 1, model: 'latebreak-model',
    choices: [{ index: 0, delta: { content: 'A'.repeat(80) }, finish_reason: null }],
  })}\n\n`, () => {
    setTimeout(() => res.destroy(), 40);
  });
});

// ---- 9137: 健康 + 命中计数的兜底上游（用于证明"已下发数据后没有换家"）----
let lateFallbackHits = 0;
start(9137, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_hits') return json(res, 200, { hits: lateFallbackHits });
  if (url.pathname === '/_reset') { lateFallbackHits = 0; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/models') return json(res, 200, { data: [{ id: 'latebreak-model' }] });
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  lateFallbackHits += 1;
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  json(res, 200, {
    id: 'chatcmpl-latefallback', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: 'from late fallback' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
  });
});

// ============================================================================
// 9144: 按 key 区分行为的上游（测"一条渠道叠加多个 key，并行竞速"）
// ============================================================================
// 叠加 key 渠道会把同一个请求同时发给渠道里所有 key（各自带 Authorization: Bearer <key>）。
// 本 mock 用 key 前缀模拟不同 key 的命运：
//   sk-fast* 立刻成功    sk-slow* 延迟 250ms 后成功    sk-auth* 401    sk-boom* 500
// /_stats 记录每个 key 的命中数（hits）与被取消数（aborted）：
//   loser 被网关 abort 时响应还没写完，res 'close' 触发且 writableEnded 仍为 false -> 记一次 aborted。
// 用例据此断言"三个 key 都收到了请求""慢 key 的请求确实被取消"。
const mkHits = Object.create(null);
const mkAborted = Object.create(null);
start(9144, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_stats') return json(res, 200, { hits: mkHits, aborted: mkAborted });
  if (url.pathname === '/_reset') {
    for (const k of Object.keys(mkHits)) delete mkHits[k];
    for (const k of Object.keys(mkAborted)) delete mkAborted[k];
    return json(res, 200, { ok: true });
  }
  if (url.pathname === '/v1/models') {
    return json(res, 200, { data: [{ id: 'mk-model' }, { id: 'fail-model' }, { id: 'strict-model' }] });
  }
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });

  const key = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  mkHits[key] = (mkHits[key] || 0) + 1;
  let recorded = false;
  const recordAbort = () => {
    if (recorded || res.writableEnded) return;
    recorded = true;
    mkAborted[key] = (mkAborted[key] || 0) + 1;
  };
  res.on('close', recordAbort);
  req.on('aborted', recordAbort);

  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');

  if (key.startsWith('sk-auth')) {
    return json(res, 401, { error: { message: 'invalid api key', type: 'authentication_error' } });
  }
  if (key.startsWith('sk-boom')) {
    return json(res, 500, { error: { message: 'upstream boom for key ' + key, type: 'server_error' } });
  }
  const slow = key.startsWith('sk-slow');
  if (slow) {
    await new Promise((r) => setTimeout(r, 250));
    if (res.writableEnded || res.destroyed || recorded) return; // 已被竞速对手淘汰
  }
  const tag = slow ? 'slow' : 'fast';
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const chunk = (delta, finish = null) => `data: ${JSON.stringify({
      id: 'chatcmpl-mk', object: 'chat.completion.chunk', created: 1, model: body.model,
      choices: [{ index: 0, delta, finish_reason: finish }],
    })}\n\n`;
    res.write(chunk({ content: `${tag}-${key}` }));
    res.write(chunk({}, 'stop'));
    return res.end('data: [DONE]\n\n');
  }
  return json(res, 200, {
    id: 'chatcmpl-mk', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: `${tag}-${key}` }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
});

// ============================================================================
// WorkBuddy 国际版 mock（9141+）
// ============================================================================
// 对齐 lib/workbuddy.mjs 的上游约定：统一信封 {code,msg,data}，code===0 成功；
// 聊天只认流式（stream!==true → 400 code=11101）；auth/state 签发 state+authUrl；
// auth/token 按 state 状态机 pending→authorized（410=过期）；login/account 返回用户信息；
// token/refresh 续期；get-user-resource 返回 CycleCapacityRemain；models 接口 401。
// 测试通过 env WORKBUDDY_BASE_URL=http://127.0.0.1:9141 把网关/库指到本 mock
// （P5 起端口可由 MOCK_PORT_BASE 整体平移，见文件开头 actualPort()）。
// 9142 = 风控 mock：出站 body 残留黑名单指纹（11128）或 developer role 时拒绝，
//       用于证明网关的 sanitizeWorkbuddyBody 真正生效。

// ---- 9141: WorkBuddy 全家桶（多 state 状态机，一账号一线程）----
const wbStates = new Map(); // state -> {status:'pending'|'authorized', accessToken, refreshToken, username, uid, enterpriseId}
let wbLast = {};
start(9141, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  if (p === '/_last') return json(res, 200, wbLast);
  if (p === '/_reset') { wbStates.clear(); wbLast = {}; return json(res, 200, { ok: true }); }
  if (p === '/_authorize') {
    const state = url.searchParams.get('state') || '';
    const rec = wbStates.get(state);
    if (!rec) return json(res, 404, { code: 404, msg: 'no such state' });
    rec.status = 'authorized';
    rec.accessToken = 'tok-device-' + state;
    rec.refreshToken = 'rt-device-' + state;
    rec.username = 'alice';
    rec.uid = 'uid-alice';
    rec.enterpriseId = 'ent-1';
    wbLast = { path: p, state };
    return json(res, 200, { ok: true });
  }
  if (p === '/v1/models') {
    return json(res, 200, { object: 'list', data: [{ id: 'deepseek-v4.1-flash' }] });
  }
  if (p === '/v2/plugin/auth/state') {
    const state = 'st-' + Math.random().toString(36).slice(2, 10);
    wbStates.set(state, { status: 'pending' });
    wbLast = { path: p, method: req.method, state };
    return json(res, 200, { code: 0, msg: 'ok', data: { state, authUrl: `http://127.0.0.1:${actualPort(9141)}/login?state=${state}` } });
  }
  if (p === '/v2/plugin/auth/token') {
    const state = url.searchParams.get('state') || '';
    const rec = wbStates.get(state);
    wbLast = { path: p, state, hasRec: !!rec };
    if (!rec) return json(res, 410, { code: 410, msg: '授权已过期' });
    if (rec.status === 'pending') return json(res, 200, { code: 1, msg: 'pending', data: {} });
    return json(res, 200, {
      code: 0, msg: 'ok',
      data: { accessToken: rec.accessToken, refreshToken: rec.refreshToken, domain: 'www.workbuddy.ai' },
    });
  }
  if (p === '/v2/plugin/login/account') {
    const rec = wbStates.get(url.searchParams.get('state') || '');
    wbLast = { path: p };
    return json(res, 200, {
      code: 0, msg: 'ok',
      data: { account: { username: rec?.username || 'wbuser', uid: rec?.uid || 'uid-1', enterpriseId: rec?.enterpriseId } },
    });
  }
  if (p === '/v2/plugin/accounts') {
    wbLast = { path: p };
    return json(res, 200, { code: 0, msg: 'ok', data: { accounts: [{ username: 'wbuser', uid: 'uid-1', enterpriseId: 'ent-0' }] } });
  }
  if (p === '/v2/plugin/auth/token/refresh') {
    const rt = req.headers['x-refresh-token'] || '';
    wbLast = { path: p, refreshToken: rt, source: req.headers['x-auth-refresh-source'] || null };
    const data = { accessToken: 'tok-new-' + Date.now() };
    if (rt !== 'rt-no-return') data.refreshToken = 'rt-new';
    return json(res, 200, { code: 0, msg: 'ok', data });
  }
  if (p === '/v2/billing/meter/get-user-resource') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    wbLast = {
      path: p, productCode: body.ProductCode, status: body.Status,
      userid: req.headers['x-user-id'] || null, domain: req.headers['x-domain'] || null,
      bearer: !!req.headers.authorization,
    };
    return json(res, 200, {
      code: 0, msg: 'ok',
      data: { Response: { Data: { Accounts: [
        { CycleCapacityRemain: 300 }, { CapacityRemain: 150 }, { CycleCapacityRemain: -5 },
      ] } } },
    });
  }
  if (p === '/console/enterprises/personal/models' || p === '/v1/models/list') {
    return json(res, 401, { code: 401, msg: 'unauthorized' });
  }
  if (p === '/v2/chat/completions') {
    let raw = '';
    for await (const c of req) raw += c;
    const body = JSON.parse(raw || '{}');
    wbLast = {
      path: p, stream: body.stream, model: body.model,
      toolChoice: body.tool_choice ?? null, hasTools: Array.isArray(body.tools),
      roles: (body.messages || []).map((m) => m.role),
      via: req.headers['x-via-fakeproxy'] || null,
    };
    if (body.stream !== true) {
      return json(res, 400, { code: 11101, msg: '非流式不支持', data: null });
    }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const chunk = (delta, finish = null) => `data: ${JSON.stringify({
      id: 'wbcmpl-1', object: 'chat.completion.chunk', created: 9, model: body.model,
      choices: [{ index: 0, delta, finish_reason: finish }],
    })}\n\n`;
    res.write(chunk({ content: 'workbuddy' }));
    res.write(chunk({ content: ' 国际版' }));
    res.write(chunk({ content: ' 你好' }));
    res.write(chunk({}, 'stop'));
    res.write(`data: ${JSON.stringify({
      id: 'wbcmpl-1', object: 'chat.completion.chunk', created: 9, model: body.model,
      choices: [], usage: { prompt_tokens: 1, completion_tokens: 3, total_tokens: 4 },
    })}\n\n`);
    return res.end('data: [DONE]\n\n');
  }
  json(res, 404, { code: 404, msg: 'nf' });
});

// ---- 9142: 风控 mock（11128 黑名单指纹残留即拒）----
let wbSanitizeRejects = 0;
let wbSanitizeLast = null;
start(9142, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_rejects') return json(res, 200, { rejects: wbSanitizeRejects });
  if (url.pathname === '/_last') return json(res, 200, wbSanitizeLast);
  if (url.pathname === '/v1/models') return json(res, 200, { object: 'list', data: [{ id: 'wb-sanitize-model' }] });
  if (url.pathname !== '/v2/chat/completions') return json(res, 404, { code: 404, msg: 'nf' });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  wbSanitizeLast = { rawBody: raw, stream: body.stream };
  if (/x-anthropic-billing-header|You are Claude Code|cc_entrypoint=|cc_version=|"role"\s*:\s*"developer"/i.test(raw)) {
    wbSanitizeRejects += 1;
    return json(res, 400, { code: 11128, msg: '风控拦截', data: null });
  }
  if (body.stream !== true) return json(res, 400, { code: 11101, msg: '非流式不支持', data: null });
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const chunk = (delta, finish = null) => `data: ${JSON.stringify({
    id: 'wbcmpl-s', object: 'chat.completion.chunk', created: 1, model: body.model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  })}\n\n`;
  res.write(chunk({ content: '脱敏后正常返回' }));
  res.write(chunk({}, 'stop'));
  return res.end('data: [DONE]\n\n');
});

// ---- 9143: 思考档位 mock ----
// 回显收到的 reasoning_effort（测注入 / per-channel 覆盖 / 客户端显式值尊重）；
// 模型名带 reject-xhigh 时，只在 effort=xhigh 上模拟上游 400（测同渠道降档重试）。
let effortSeen = [];
start(9143, async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/_seen') return json(res, 200, { seen: effortSeen });
  if (url.pathname === '/_reset') { effortSeen = []; return json(res, 200, { ok: true }); }
  if (url.pathname === '/v1/models') {
    return json(res, 200, { data: [{ id: 'think-xhigh' }, { id: 'think-pin' }, { id: 'think-reject' }, { id: 'think-plain' }] });
  }
  if (url.pathname !== '/v1/chat/completions') return json(res, 404, { error: { message: 'nf' } });
  let raw = '';
  for await (const c of req) raw += c;
  const body = JSON.parse(raw || '{}');
  const effort = body.reasoning_effort ?? null;
  effortSeen.push({ model: body.model, effort });
  if (String(body.model).includes('reject') && effort === 'xhigh') {
    return json(res, 400, { error: { message: 'field ReasoningEffort invalid, should be one of: low, medium, high, xhigh, none' } });
  }
  json(res, 200, {
    id: 'chatcmpl-effort', object: 'chat.completion', created: 1, model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: `effort=${effort ?? 'none'}` }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
});
