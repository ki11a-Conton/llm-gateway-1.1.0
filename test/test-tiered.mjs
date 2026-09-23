// 两段式选路 + 失败处理 + 任务日志 回归试
//
// 覆盖用户提出的四类问题：
//   1. 两段式路由：sensenova / api.b.ai 优先池轮询完 -> 其余供应商随机路由，每家 2 次 × 3s
//   2. 余额不足 / 鉴权失败：不反复撞同一家，立刻换下一家；且请求一定有响应（绝不挂住）
//   3. GPT 系"假成功"（HTTP 200 空响应）：视为该渠道失败，自动换家
//   4. 能思考的模型统一最高强度（reasoning_effort=high / thinking budget）
//   5. 任务日志：每个请求一条记录，错误可按指纹归类
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// P5：mock 上游端口整体平移 + 网关端口运行时分配（避免并行跑测试时抢固定端口）
const PORT = await freePort();
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'tiered.test.json'), { port: PORT, mockBase: mp.base });
const LOG_DIR = path.join(ROOT, 'logs-test');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 单元：分层判定 ----------
{
  const { ChannelManager } = await import('../lib/channels.mjs');
  // 单独用一份内存配置验证"按 baseUrl 域名自动识别优先池"（不受集成配置改写影响）
  const { writeFileSync } = await import('node:fs');
  const unitCfg = path.join(HERE, 'tiered-unit.tmp.json');
  writeFileSync(unitCfg, JSON.stringify({
    server: { host: '127.0.0.1', port: 1, apiKey: 'k' },
    routing: { tiered: true, preferredBaseUrls: ['sensenova.cn', 'api.b.ai'] },
    channels: [
      { name: 'sn-1', protocol: 'openai', baseUrl: 'https://token.sensenova.cn/v1', apiKey: 'k', model: 'm', priority: 10 },
      { name: 'sn-2', protocol: 'openai', baseUrl: 'https://token.sensenova.cn/v1', apiKey: 'k', model: 'm', priority: 20 },
      { name: 'bai-1', protocol: 'openai', baseUrl: 'https://api.b.ai/v1', apiKey: 'k', model: 'm', priority: 30 },
      { name: 'explicit-pref', protocol: 'openai', baseUrl: 'http://127.0.0.1:9999', apiKey: 'k', model: 'm', priority: 1, tier: 'preferred' },
      { name: 'other-1', protocol: 'openai', baseUrl: mp.url(9101), apiKey: 'k', model: 'm', priority: 5 },
      { name: 'other-2', protocol: 'openai', baseUrl: mp.url(9102), apiKey: 'k', model: 'm', priority: 6 },
      { name: 'other-3', protocol: 'openai', baseUrl: mp.url(9103), apiKey: 'k', model: 'm', priority: 7 },
    ],
  }), 'utf8');
  const mgr = new ChannelManager(unitCfg);
  mgr.load();
  const tierOf = (n) => mgr.channels.find((c) => c.name === n)?.tier;
  ok('单元：baseUrl 含 sensenova.cn 的渠道进优先池', tierOf('sn-1') === 'preferred', tierOf('sn-1'));
  ok('单元：baseUrl 含 api.b.ai 的渠道进优先池', tierOf('bai-1') === 'preferred', tierOf('bai-1'));
  ok('单元：显式 tier=preferred 覆盖域名判定', tierOf('explicit-pref') === 'preferred', tierOf('explicit-pref'));
  ok('单元：其它供应商默认进随机池', tierOf('other-1') === 'fallback', tierOf('other-1'));

  const cands = mgr.candidatesFor('m');
  const firstFallbackIdx = cands.findIndex((c) => c.tier === 'fallback');
  ok('单元：优先池整体排在随机池之前',
    firstFallbackIdx === -1 || cands.slice(0, firstFallbackIdx).every((c) => c.tier === 'preferred'),
    cands.map((c) => `${c.name}:${c.tier}`).join(','));
  ok('单元：优先池按 priority 排序（p1 在 p2 前）',
    cands.filter((c) => c.tier === 'preferred').map((c) => c.name).join(',') === 'explicit-pref,sn-1,sn-2,bai-1',
    cands.filter((c) => c.tier === 'preferred').map((c) => c.name).join(','));

  // 随机池每次顺序应当不同（40 次取样里不可能全都一样）
  const orders = new Set();
  for (let i = 0; i < 40; i += 1) {
    orders.add(mgr.candidatesFor('m').filter((c) => c.tier === 'fallback').map((c) => c.name).join('>'));
  }
  ok('单元：随机池每次顺序打乱', orders.size > 1, `不同顺序数=${orders.size}`);
  rmSync(unitCfg, { force: true });
}

// ---------- 单元：错误分类口径 ----------
{
  const { classifyUpstreamFailure, modelSupportsThinking } = await import('../lib/util.mjs');
  ok('单元：Insufficient balance -> insufficient_balance',
    classifyUpstreamFailure(400, 'Insufficient balance').kind === 'insufficient_balance');
  ok('单元：token quota is not enough -> insufficient_balance',
    classifyUpstreamFailure(403, 'token quota is not enough').kind === 'insufficient_balance');
  ok('单元：invalid api key -> auth', classifyUpstreamFailure(401, 'Invalid API key provided').kind === 'auth');
  ok('单元：额度不足不可重试（retryable=false）',
    classifyUpstreamFailure(400, 'Insufficient balance').retryable === false);
  ok('单元：鉴权失败不可重试（retryable=false）',
    classifyUpstreamFailure(401, 'Invalid API key').retryable === false);
  ok('单元：429 -> rate_limit 可重试',
    classifyUpstreamFailure(429, 'tpm exhausted').kind === 'rate_limit'
      && classifyUpstreamFailure(429, 'tpm exhausted').retryable === true);

  ok('单元：deepseek-v4-flash 判定为可思考', modelSupportsThinking('deepseek-v4-flash') === true);
  ok('单元：gpt-5.6-luna 判定为可思考', modelSupportsThinking('gpt-5.6-luna') === true);
  ok('单元：qwen3.8-flash 不强制思考（未在白名单）', modelSupportsThinking('qwen3.8-flash') === false);
  ok('单元：deepseek-chat 不强制思考', modelSupportsThinking('deepseek-chat') === false);
}

// ---------- 集成 ----------
if (existsSync(LOG_DIR)) rmSync(LOG_DIR, { recursive: true, force: true });

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(1000);
const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });

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

try {
  await waitReady();
  await wait(400);

  // ===== ① 余额不足：立刻换家，不反复撞同一家，且请求必须成功返回 =====
  await fetch(mp.url(9120, '/_reset'));
  await fetch(mp.url(9130, '/_reset'));
  const t0 = Date.now();
  const rBalance = await chat({ model: 'balance-model', messages: [{ role: 'user', content: 'hi' }] });
  const balanceMs = Date.now() - t0;
  ok('① 余额不足渠道换家后请求成功（不挂住）', rBalance.status === 200, `status=${rBalance.status} ${rBalance.text.slice(0, 160)}`);
  ok('① 最终由健康渠道 ok-1 完成', rBalance.headers.get('x-gateway-channel') === 'ok-1', String(rBalance.headers.get('x-gateway-channel')));
  const balHits = (await (await fetch(mp.url(9120, '/_hits'))).json()).hits;
  ok('① 余额不足渠道只被撞 1 次（不做渠道内重试）', balHits === 1, `hits=${balHits}`);
  ok('① 余额不足换家很快（<2s，没有烧满重试预算）', balanceMs < 2000, `${balanceMs}ms`);

  const snapB = (await call('/api/status')).json.channels || [];
  const balCh = snapB.find((c) => c.name === 'bal-1');
  ok('① 余额不足渠道被冷却（coolingDown=true）', balCh?.coolingDown === true, `coolingDown=${balCh?.coolingDown}`);
  ok('① 余额不足渠道 tier 正确（随机池）', balCh?.tier === 'fallback', String(balCh?.tier));

  // ===== ② 鉴权失败：绝不挂住，直接换家 =====
  const t1 = Date.now();
  const rAuth = await chat({ model: 'auth-model', messages: [{ role: 'user', content: 'hi' }] });
  const authMs = Date.now() - t1;
  ok('② 鉴权失败渠道换家后请求成功（不挂住）', rAuth.status === 200, `status=${rAuth.status} ${rAuth.text.slice(0, 160)}`);
  ok('② 最终由健康渠道 ok-1 完成', rAuth.headers.get('x-gateway-channel') === 'ok-1', String(rAuth.headers.get('x-gateway-channel')));
  const authHits = (await (await fetch(mp.url(9130, '/_hits'))).json()).hits;
  ok('② 鉴权失败渠道只被撞 1 次（不做渠道内重试）', authHits === 1, `hits=${authHits}`);
  ok('② 鉴权失败换家很快（<2s）', authMs < 2000, `${authMs}ms`);
  const authCh = ((await call('/api/status')).json.channels || []).find((c) => c.name === 'auth-1');
  ok('② 鉴权失败渠道被冷却', authCh?.coolingDown === true, `coolingDown=${authCh?.coolingDown}`);

  // ===== ③ 空响应（GPT 假成功）：HTTP 200 但无内容 -> 换家 =====
  const rEmpty = await chat({ model: 'empty-model', messages: [{ role: 'user', content: 'hi' }] });
  ok('③ 空响应渠道被跳过，最终成功', rEmpty.status === 200, `status=${rEmpty.status} ${rEmpty.text.slice(0, 160)}`);
  ok('③ 成功渠道是 ok-1（不是返回空内容的 empty-1）', rEmpty.headers.get('x-gateway-channel') === 'ok-1', String(rEmpty.headers.get('x-gateway-channel')));
  ok('③ 响应正文非空（agent 拿得到东西）', (rEmpty.json?.choices?.[0]?.message?.content || '').length > 0);
  const emptyCh = ((await call('/api/status')).json.channels || []).find((c) => c.name === 'empty-1');
  ok('③ 空响应被记为渠道失败', (emptyCh?.failed ?? 0) >= 1, `failed=${emptyCh?.failed}`);

  // 流式空响应同样换家
  const rEmptyS = await chat({ model: 'empty-model', messages: [{ role: 'user', content: 'hi' }], stream: true });
  ok('③ 流式空响应也换家成功', rEmptyS.status === 200 && rEmptyS.headers.get('x-gateway-channel') === 'ok-1',
    `status=${rEmptyS.status} ch=${rEmptyS.headers.get('x-gateway-channel')}`);

  // ===== ④ 思考强度：能思考的模型统一最高强度 =====
  const rThink = await chat({ model: 'think-model', messages: [{ role: 'user', content: 'hi' }] });
  const seen = rThink.json?.choices?.[0]?.message?.content || '';
  ok('④ 可思考模型被注入 reasoning_effort=high', seen.includes('effort=high'), seen);
  ok('④ 响应头标注 tier', !!rThink.headers.get('x-gateway-tier'), String(rThink.headers.get('x-gateway-tier')));

  // 客户端已经指定 high 时不重复注入、不报错
  const rThinkHigh = await chat({ model: 'think-model', reasoning_effort: 'high', messages: [{ role: 'user', content: 'hi' }] });
  ok('④ 客户端已指定 high 时正常透传', (rThinkHigh.json?.choices?.[0]?.message?.content || '').includes('effort=high'),
    String(rThinkHigh.json?.choices?.[0]?.message?.content));

  // 非思考模型不得被强行加参数（否则会被上游 400）
  const rPlain = await chat({ model: 'plain-model', messages: [{ role: 'user', content: 'hi' }] });
  const plainSeen = rPlain.json?.choices?.[0]?.message?.content || '';
  ok('④ 非思考模型不注入思考参数', plainSeen.includes('effort=none'), plainSeen);

  // ===== ⑤ 任务日志 =====
  await wait(600); // 等去抖落盘
  const tl = await call('/api/tasks?limit=50');
  ok('⑤ /api/tasks 可访问', tl.status === 200, String(tl.status));
  ok('⑤ 任务日志记录了任务', (tl.json?.stats?.tasks ?? 0) >= 1, `tasks=${tl.json?.stats?.tasks}`);
  // 前面的坏渠道都被换家救回来了（整体 200），"遇到的错误"必须仍按尝试级记录在案
  ok('⑤ 尝试级错误被统计（余额/鉴权/空响应 >= 3 次）', (tl.json?.stats?.failedAttempts ?? 0) >= 3,
    `failedAttempts=${tl.json?.stats?.failedAttempts}`);
  ok('⑤ 任务记录带渠道与尝试链', Array.isArray(tl.json?.recent) && tl.json.recent.some((r) => Array.isArray(r.attempts)));

  const errs = await call('/api/errors?limit=50');
  ok('⑤ /api/errors 可访问', errs.status === 200, String(errs.status));
  const fp = errs.json?.byFingerprint || {};
  ok('⑤ 余额不足被归类为 insufficient_balance', (fp.insufficient_balance ?? 0) >= 1, JSON.stringify(fp));
  ok('⑤ 鉴权失败被归类为 auth', (fp.auth ?? 0) >= 1, JSON.stringify(fp));
  ok('⑤ 空响应被归类为 empty_response', (fp.empty_response ?? 0) >= 1, JSON.stringify(fp));

  // 落盘检查
  const logFile = path.join(LOG_DIR, 'tasks.jsonl');
  ok('⑤ 任务日志已落盘 logs-test/tasks.jsonl', existsSync(logFile), logFile);
  if (existsSync(logFile)) {
    const lines = readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean);
    ok('⑤ 落盘内容是可解析的 JSONL', lines.length > 0 && lines.every((l) => { try { JSON.parse(l); return true; } catch { return false; } }),
      `lines=${lines.length}`);
    const rec = JSON.parse(lines[lines.length - 1]);
    ok('⑤ 每条记录含 requestId/model/elapsedMs', !!(rec.requestId && rec.model && typeof rec.elapsedMs === 'number'), JSON.stringify(Object.keys(rec)));
  }

  // ===== ⑥ 两段式：优先池优先被使用 =====
  await fetch(mp.url(9101, '/_reset-pref')); // 让优先池渠道可用
  const rTier = await chat({ model: 'tier-model', messages: [{ role: 'user', content: 'hi' }] });
  const via = rTier.headers.get('x-gateway-channel');
  ok('⑥ 两段式：优先池渠道先被使用', ['explicit-pref', 'sn-1', 'sn-2', 'bai-1'].includes(via), String(via));
  ok('⑥ 响应头标注 tier=preferred', rTier.headers.get('x-gateway-tier') === 'preferred', String(rTier.headers.get('x-gateway-tier')));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  mock.kill();
  await wait(400);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
