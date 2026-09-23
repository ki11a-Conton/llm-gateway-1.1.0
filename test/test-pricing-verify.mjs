// test/test-pricing-verify.mjs —— T4 独立对抗验收：金额正确性
//
// 说明（provenance）：初版由独立验收 agent 编写，但存在 SyntaxError（`const in` 保留字）
// 且含硬编码 `true` 假断言，其"10 通过 0 失败"报告与事实不符。
// Lead 保留其 A–H 对抗面设计，重写成真能跑、真会失败的套件。
//
// 对齐 docs/superpowers/specs/2026-09-18-pricing-design.md §3 / §3.1 / §4 / §4.1

import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import os from 'node:os';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync, existsSync, readdirSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';
import {
  Pricing, configurePricing, getPricing,
  DEFAULT_PRICES, DEFAULT_CURRENCY, DEFAULT_SYMBOL, UNPRICED_LIST_MAX,
} from '../lib/pricing.mjs';
import { UsageStore, localDayKey, USAGE_RANGES } from '../lib/usage.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

const MOCK_PORT = await freePort();
const GW_PORT = await freePort();
const GW = `http://127.0.0.1:${GW_PORT}`;
const API_KEY = 'VERIFYKEY';
const EVIL = '<img src=x onerror=alert(1)>';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

/** 手算金额（不复用任何实现公式），独立验证基准 */
function handCalc(inputTokens, outputTokens, inPrice, outPrice) {
  const inT = Number.isFinite(Number(inputTokens)) && Number(inputTokens) > 0 ? Number(inputTokens) : 0;
  const outT = Number.isFinite(Number(outputTokens)) && Number(outputTokens) > 0 ? Number(outputTokens) : 0;
  return Math.round((inT * inPrice + outT * outPrice) / 1e6 * 1e6) / 1e6;
}

function assertPortFree(port) {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.once('error', () => resolve(false));
    s.listen(port, '127.0.0.1', () => s.close(() => resolve(true)));
  });
}

async function getJson(p, headers = {}) {
  const r = await fetch(GW + p, { headers });
  let j = null;
  try { j = await r.json(); } catch { /* ignore */ }
  return { status: r.status, json: j };
}

function runPanelRender(src) {
  const nodes = new Map();
  let innerHtmlWrites = 0;
  class El {
    constructor(tag) {
      this.tagName = String(tag || '').toUpperCase();
      this.children = [];
      this._text = '';
      this._html = '';
      this.style = {};
      this.className = '';
      this.title = '';
      this.value = '';
      this.classList = { add() {}, remove() {}, toggle() {} };
    }
    set textContent(v) { this._text = v == null ? '' : String(v); this.children = []; }
    get textContent() { return this._text; }
    set innerHTML(v) { innerHtmlWrites += 1; this._html = String(v); }
    get innerHTML() { return this._html; }
    appendChild(n) { this.children.push(n); return n; }
    addEventListener() {}
    removeEventListener() {}
    getAttribute() { return null; }
    setAttribute() {}
    focus() {}
  }
  const textOf = (n) => {
    if (!n) return '';
    if (n.children && n.children.length) return n.children.map(textOf).join(' ');
    return n.textContent || '';
  };
  const getById = (id) => {
    if (!nodes.has(id)) nodes.set(id, new El('div'));
    return nodes.get(id);
  };
  const ctx = {
    document: {
      getElementById: getById,
      createElement: (tag) => new El(tag),
      createTextNode: (t) => { const n = new El('#text'); n.textContent = t; return n; },
      addEventListener() {},
    },
    localStorage: { getItem: () => null, setItem() {} },
    location: { origin: `http://127.0.0.1:${GW_PORT}` },
    fetch: async () => ({ ok: false, status: 404, statusText: 'Not Found', json: async () => null }),
    setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, confirm: () => false, console,
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: 'panel-extract.js' });
  return { ctx, getById, textOf, innerHtmlWrites: () => innerHtmlWrites };
}

const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-pricing-vfy-'));

try {
  // ============================================================
  // A. 金额数学：手算独立验证
  // ============================================================
  console.log('— A. 金额数学 —');
  {
    configurePricing({ models: { 'alpha': { input: 0.28, output: 0.42 } } });
    const p = getPricing();

    // (a) 纯输入
    const a1 = handCalc(1000, 0, 0.28, 0.42);
    ok('纯输入：1000 × 0.28 / 1e6', p.cost('alpha', 1000, 0) === a1 && a1 === 0.00028, `${p.cost('alpha', 1000, 0)} vs ${a1}`);

    // (b) 纯输出
    const a2 = handCalc(0, 2000, 0.28, 0.42);
    ok('纯输出：2000 × 0.42 / 1e6', p.cost('alpha', 0, 2000) === a2 && a2 === 0.00084, `${p.cost('alpha', 0, 2000)} vs ${a2}`);

    // (c) 两者都有
    const a3 = handCalc(1000, 2000, 0.28, 0.42);
    ok('混合：(1000×0.28 + 2000×0.42)/1e6', p.cost('alpha', 1000, 2000) === a3 && a3 === 0.00112, `${p.cost('alpha', 1000, 2000)} vs ${a3}`);

    // (d) 0 token
    ok('0 token 且已配价 → 0', p.cost('alpha', 0, 0) === 0, String(p.cost('alpha', 0, 0)));

    // (e) 超大 token（1e9 量级不丢精度）
    const a5 = handCalc(1e9, 2e9, 0.28, 0.42);
    ok('超大 token 1e9/2e9 不溢出', p.cost('alpha', 1e9, 2e9) === a5 && a5 === 1120, `${p.cost('alpha', 1e9, 2e9)} vs ${a5}`);

    // (f) 极小金额四舍五入到 6 位
    configurePricing({ models: { 'tiny': { input: 0.1, output: 0 } } });
    const pt = getPricing();
    ok('10 token × 0.1 / 1e6 = 0.000001（精确 6 位）', pt.cost('tiny', 10, 0) === 0.000001, String(pt.cost('tiny', 10, 0)));
    ok('1 token × 0.1 / 1e6 = 1e-7 < 半微 → 舍入为 0', pt.cost('tiny', 1, 0) === 0, String(pt.cost('tiny', 1, 0)));
  }

  // ============================================================
  // B. null vs 0 的区分 + §4.1 四种口径场景
  // ============================================================
  console.log('— B. null vs 0 + §4.1 四种场景 —');
  {
    configurePricing({ models: { 'priced': { input: 1, output: 2 }, 'free': { input: 0, output: 0 } } });
    const p = getPricing();

    // 未配模型 → null
    ok('未配价格模型 cost() → null（不是 0）', p.cost('unknown', 100, 100) === null, String(p.cost('unknown', 100, 100)));

    // 显式 0 价 → 0
    ok('显式 0 价 cost() → 0（不是 null）', p.cost('free', 100, 100) === 0, String(p.cost('free', 100, 100)));

    // §4.1 场景 1：无记录 → cost null, complete true
    const s1 = p.summarize({});
    ok('§4.1-1 无记录：cost=null, complete=true', s1.cost === null && s1.complete === true, JSON.stringify(s1));

    // §4.1 场景 2：全部配价 → cost 数字, complete true
    const s2 = p.summarize({ 'priced': { inputTokens: 1e6, outputTokens: 1e6 } });
    ok('§4.1-2 全部配价：cost=3, complete=true', s2.cost === 3 && s2.complete === true, JSON.stringify(s2));

    // §4.1 场景 3：部分未配 → cost 只含已配部分, complete false
    const s3 = p.summarize({ 'priced': { inputTokens: 1e6, outputTokens: 0 }, 'unknown': { inputTokens: 500, outputTokens: 500 } });
    ok('§4.1-3 部分未配：cost=1, complete=false, unpricedTokens=1000',
      s3.cost === 1 && s3.complete === false && s3.unpricedTokens === 1000, JSON.stringify(s3));

    // §4.1 场景 4：全部未配 → cost null, complete false
    const s4 = p.summarize({ 'unknown': { inputTokens: 100, outputTokens: 100 } });
    ok('§4.1-4 全部未配：cost=null, complete=false', s4.cost === null && s4.complete === false, JSON.stringify(s4));

    // 显式 0 价汇总
    const s5 = p.summarize({ 'free': { inputTokens: 1e6, outputTokens: 1e6 } });
    ok('显式 0 价汇总：cost=0, complete=true', s5.cost === 0 && s5.complete === true, JSON.stringify(s5));
  }

  // ============================================================
  // C. 通配匹配：最长前缀 + 正则元字符不误匹配
  // ============================================================
  console.log('— C. 通配匹配 —');
  {
    configurePricing({ models: { 'gpt-*': { input: 2, output: 8 }, 'gpt-4*': { input: 3, output: 9 }, 'gpt-4o': { input: 1, output: 1 } } });
    const p = getPricing();

    ok('gpt-4.9-x 命中 gpt-4*（最长前缀优先，input=3）', p.price('gpt-4.9-x')?.input === 3, JSON.stringify(p.price('gpt-4.9-x')));
    ok('gpt-9 命中 gpt-*（input=2）', p.price('gpt-9')?.input === 2, JSON.stringify(p.price('gpt-9')));
    ok('gpt-4o 精确匹配优先于通配（input=1）', p.price('gpt-4o')?.input === 1, JSON.stringify(p.price('gpt-4o')));

    // 内置精确条目不被用户通配覆盖
    configurePricing({ models: { 'gpt-4*': { input: 99, output: 99 } } });
    const p2 = getPricing();
    ok('内置 gpt-4.1 精确条目优先于用户 gpt-4* 通配',
      p2.price('gpt-4.1')?.input === DEFAULT_PRICES['gpt-4.1'].input, JSON.stringify(p2.price('gpt-4.1')));

    // 正则元字符
    ok('m+ 不被当通配符', p.price('m+') === null);
    ok('gpt.4 中 . 不当通配符', p.price('gpt.4') === null);
    ok('空字符串 → null', p.price('') === null);
  }

  // ============================================================
  // D. 脏配置：NaN/Infinity/字符串/负数忽略
  // ============================================================
  console.log('— D. 脏配置 —');
  {
    configurePricing({ models: {
      'badNaN': { input: NaN, output: 1 },
      'badInf': { input: Infinity, output: 1 },
      'badNeg': { input: -1, output: 1 },
      'badStr': { input: '0.5', output: 1 },
      'good': { input: 1, output: 1 },
    }});
    const p = getPricing();

    ok('NaN 被忽略 → null', p.price('badNaN') === null && p.cost('badNaN', 1e6, 0) === null);
    ok('Infinity 被忽略 → null', p.price('badInf') === null);
    ok('负数被忽略 → null', p.price('badNeg') === null);
    ok('字符串被忽略 → null', p.price('badStr') === null);
    ok('合法条目不受影响', p.price('good')?.input === 1);
    ok('脏价目模型的 cost 是 null（不是 NaN）', p.cost('badNaN', 1e6, 1e6) === null);

    const sum = p.summarize({ 'badNaN': { inputTokens: 1e6, outputTokens: 0 }, 'good': { inputTokens: 1e6, outputTokens: 0 } });
    ok('summarize 在脏配置下不产生 NaN', !Number.isNaN(sum.cost) && sum.cost === 1, JSON.stringify(sum));
  }

  // ============================================================
  // E. 热重载/改价：同一历史范围金额立即变化
  // ============================================================
  console.log('— E. 热重载改价 —');
  {
    const eDir = path.join(tmpDir, 'usage-e');
    mkdirSync(eDir, { recursive: true });

    configurePricing({ models: { 'hot': { input: 1, output: 1 } } });
    const store = new UsageStore({ dir: eDir, keepDays: 5, enabled: true });
    await store.record({ ts: Date.now(), model: 'hot', channel: 'c', input: 1e6, output: 1e6 });
    await wait(100);

    const before = await store.query('today');
    ok('改价前金额 = 2（1e6×1 + 1e6×1）/1e6', before.cost === 2, String(before.cost));

    configurePricing({ models: { 'hot': { input: 10, output: 10 } } });
    const after = await store.query('today');
    ok('改价后同一历史范围金额立即变化 → 20', after.cost === 20, String(after.cost));
    ok('前后金额不同（证明非缓存）', before.cost !== after.cost);
    ok('既有 token 字段不因改价而变化', before.inputTokens === after.inputTokens && after.inputTokens === 1e6);

    // JSONL 不含 cost
    const dayFile = path.join(eDir, `${localDayKey(Date.now())}.jsonl`);
    if (existsSync(dayFile)) {
      const raw = readFileSync(dayFile, 'utf8');
      ok('JSONL 里不落 cost（查询时算，不改历史文件）', !raw.includes('"cost"'), raw.slice(0, 200));
    } else {
      ok('JSONL 文件存在', false, 'file not found');
    }
  }

  // ============================================================
  // F. 端到端：真起网关 + mock 上游 + /api/usage
  // ============================================================
  console.log('— F. 端到端 —');
  {
    const mock = http.createServer((req, res) => {
      if (req.url?.includes('/models')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        return res.end(JSON.stringify({ object: 'list', data: [{ id: 'e2e-model', object: 'model' }] }));
      }
      const usage = { prompt_tokens: 1000, completion_tokens: 2000, total_tokens: 3000 };
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        id: 'm1', object: 'chat.completion', model: 'e2e-model',
        choices: [{ index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' }],
        usage,
      }));
    });
    await new Promise((r) => mock.listen(MOCK_PORT, '127.0.0.1', r));
    let gw = null;
    try {
      if (!(await assertPortFree(GW_PORT))) throw new Error(`端口 ${GW_PORT} 已被占用（gateway）`);

      const fDir = path.join(tmpDir, 'f-logs');
      mkdirSync(fDir, { recursive: true });
      const uDir = path.join(fDir, 'usage');
      mkdirSync(uDir, { recursive: true });
      const cfg = {
        server: { host: '127.0.0.1', port: GW_PORT, apiKey: API_KEY, panel: true },
        taskLog: { enabled: true, dir: fDir, file: 'tasks.jsonl', ringMax: 50, usageKeepDays: 30 },
        routing: {
          strategy: 'priority', tiered: false, attemptsPerChannel: 1, retryLoop: false,
          sessionAffinity: false, forceMaxEffort: false, maxConcurrent: 8, maxConcurrentPerChannel: 0,
          queueTimeoutMs: 5000, timeoutMs: 20000, streamIdleTimeoutMs: 20000,
          probeIntervalMs: 0, discoverIntervalMs: 0,
        },
        channels: [
          { name: 'e2e-ch', protocol: 'openai', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, apiKey: 'k', model: 'e2e-model', priority: 10 },
        ],
        pricing: { currency: 'CNY', symbol: '¥', models: { 'e2e-model': { input: 2, output: 4 } } },
      };
      const runCfg = path.join(tmpDir, 'f-config.json');
      writeFileSync(runCfg, JSON.stringify(cfg, null, 2), 'utf8');

      gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', runCfg, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });

      let ready = false;
      for (let i = 0; i < 80; i++) {
        try { const r = await fetch(`${GW}/health`); if (r.ok) { ready = true; break; } } catch {}
        await wait(150);
      }
      ok('网关切就绪', ready);

      if (ready) {
        const chatRes = await fetch(`${GW}/v1/chat/completions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}` },
          body: JSON.stringify({ model: 'e2e-model', messages: [{ role: 'user', content: 'hi' }] }),
        });
        ok('POST /v1/chat/completions 成功', chatRes.status === 200, `status=${chatRes.status}`);
        await wait(300);

        const u = await getJson('/api/usage', { authorization: `Bearer ${API_KEY}` });
        const today = u.json?.today;
        const m = today?.byModel?.['e2e-model'];

        const expectedCost = handCalc(1000, 2000, 2, 4);
        ok('端到端金额 = 手算(token × 单价) = 0.01', m?.cost === expectedCost && expectedCost === 0.01, JSON.stringify({ actual: m?.cost, expected: expectedCost }));
        ok('token 数正确（1000/2000）', m?.inputTokens === 1000 && m?.outputTokens === 2000, JSON.stringify(m));
        ok('既有字段形状不变', typeof today?.requests === 'number' && typeof today?.totalTokens === 'number');
        ok('顶层 pricing 元信息正确', u.json?.pricing?.currency === 'CNY' && u.json?.pricing?.symbol === '¥' && u.json?.pricing?.models >= 1, JSON.stringify(u.json?.pricing));
      }
    } finally {
      if (gw) { try { gw.kill(); } catch {} }
      mock.close();
      await wait(100);
    }
  }

  // ============================================================
  // G. 面板：vm DOM stub 真实执行渲染脚本
  // ============================================================
  console.log('— G. 面板渲染 —');
  {
    const html = readFileSync(path.join(ROOT, 'public', 'index.html'), 'utf8');
    const scriptSrc = (html.match(/<script>([\s\S]*?)<\/script>/) || [])[1] || '';
    ok('面板脚本可提取', scriptSrc.length > 0, `len=${scriptSrc.length}`);

    const r = runPanelRender(scriptSrc);

    // payload 故意用非 $ 符号，验证符号跟随
    r.ctx.renderUsage({
      today: {
        requests: 2, inputTokens: 1000, outputTokens: 2000, totalTokens: 3000,
        cost: 0.01, costComplete: false, unpricedTokens: 500, unpriced: ['unknown-m'],
        byModel: {
          'e2e-model': { requests: 1, inputTokens: 1000, outputTokens: 2000, totalTokens: 3000, cost: 0.01 },
          'unknown-m': { requests: 1, inputTokens: 500, outputTokens: 0, totalTokens: 500, cost: null },
          [EVIL]: { requests: 1, inputTokens: 10, outputTokens: 10, totalTokens: 20, cost: null },
        },
      },
      d1: { requests: 0, inputTokens: 0, outputTokens: 0, totalTokens: 0, cost: null, costComplete: true, unpricedTokens: 0, unpriced: [], byModel: {} },
      d7: { requests: 0, inputTokens: 0, outputTokens: 0, totalTokens: 0, cost: null, costComplete: true, unpricedTokens: 0, unpriced: [], byModel: {} },
      d30: { requests: 0, inputTokens: 0, outputTokens: 0, totalTokens: 0, cost: null, costComplete: true, unpricedTokens: 0, unpriced: [], byModel: {} },
      d90: { requests: 0, inputTokens: 0, outputTokens: 0, totalTokens: 0, cost: null, costComplete: true, unpricedTokens: 0, unpriced: [], byModel: {} },
      pricing: { currency: 'CNY', symbol: '¥', models: 2 },
    });

    const cardsText = r.textOf(r.getById('usage-cards'));
    const chartText = r.textOf(r.getById('usage-chart'));

    ok('卡片渲染出费用文本', cardsText.includes('费用'), cardsText.slice(0, 200));
    ok('币种符号取自 pricing.symbol（¥ 而非 $）', cardsText.includes('¥') && !cardsText.includes('$'), cardsText.slice(0, 300));
    ok('未配价显示「未配置价格」', cardsText.includes('未配置价格') || chartText.includes('未配置价格'), `${cardsText.slice(0,100)} | ${chartText.slice(0,100)}`);
    ok('无数据显示「未采集」', cardsText.includes('未采集'), cardsText.slice(0, 200));
    ok('innerHTML 写入次数为 0', r.innerHtmlWrites() === 0, `writes=${r.innerHtmlWrites()}`);
    ok('恶意模型名只作为文本出现', chartText.includes(EVIL), chartText.slice(0, 200));

    // renderUsage(null) → 未采集
    r.ctx.renderUsage(null);
    const nullText = r.textOf(r.getById('usage-cards'));
    ok('renderUsage(null) → 费用行显示「未采集」', nullText.includes('未采集'), nullText.slice(0, 200));
  }

  // ============================================================
  // H. 有界性：unpriced ≤ UNPRICED_LIST_MAX
  // ============================================================
  console.log('— H. 有界性 —');
  {
    configurePricing({ models: {} });
    const p = getPricing();
    const many = {};
    for (let i = 0; i < UNPRICED_LIST_MAX + 10; i++) {
      many[`model-${i}`] = { inputTokens: i * 100, outputTokens: 0 };
    }
    const s = p.summarize(many);
    ok(`unpriced 列表 ≤ ${UNPRICED_LIST_MAX}`, s.unpriced.length <= UNPRICED_LIST_MAX, `actual=${s.unpriced.length}`);
    ok('unpriced 按 token 降序（最贵的排最前）', s.unpriced[0] === `model-${UNPRICED_LIST_MAX + 9}`, JSON.stringify(s.unpriced.slice(0, 3)));
    ok('unpricedTokens 统计全部未配价 token（不受截断影响）',
      s.unpricedTokens === Array.from({ length: UNPRICED_LIST_MAX + 10 }, (_, i) => i * 100).reduce((a, b) => a + b, 0),
      String(s.unpricedTokens));
  }

} finally {
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
