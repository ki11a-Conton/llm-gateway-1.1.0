// Token 用量柱状图的多分辨率布局回归（真实浏览器，零依赖）
//
// 为什么要这条：`test-usage.mjs` 用 DOM stub 验证的是"渲染逻辑对不对"（文本、柱宽百分比、
// 无 innerHTML），但**看不到真实布局**——窄屏下 190px 的模型名列会不会把柱区挤没、
// 长模型名会不会把页面撑出横向滚动条，stub 答不了。这里用真 Chromium 量真实几何。
//
// 实现：直接驱动已安装的 Chromium（--remote-debugging-port）+ Node 22 自带 WebSocket 走 CDP，
// **不引入 Playwright 依赖**（设计文档 §6：不加新依赖）。找不到浏览器时**优雅跳过**（exit 0），
// 这样在没装浏览器的机器上 `npm test` 依然全绿。
//
// 可用性：GW_CHROME 环境变量指定可执行文件；否则自动找 ms-playwright 的 chromium-*/chrome-win64，
// 再退回系统 Chrome 常见路径。想跳过：GW_SKIP_BROWSER=1
import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, existsSync, readdirSync, statSync } from 'node:fs';
import { freePort } from './lib/ports.mjs';

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

// ---------- 找浏览器 ----------
function findBrowser() {
  if (process.env.GW_SKIP_BROWSER === '1') return null;
  if (process.env.GW_CHROME && existsSync(process.env.GW_CHROME)) return process.env.GW_CHROME;
  const rel = process.platform === 'win32' ? path.join('chrome-win64', 'chrome.exe') : path.join('chrome-linux', 'chrome');
  const dirs = [
    process.env.LOCALAPPDATA && path.join(process.env.LOCALAPPDATA, 'ms-playwright'),
    process.env.HOME && path.join(process.env.HOME, '.cache', 'ms-playwright'),
  ].filter(Boolean);
  const hits = [];
  for (const base of dirs) {
    if (!existsSync(base)) continue;
    for (const d of readdirSync(base)) {
      if (!/^chromium-\d+$/.test(d)) continue; // 只认完整版（headless_shell 也可，但完整版更稳）
      const p = path.join(base, d, rel);
      if (existsSync(p)) hits.push({ p, rev: Number(d.split('-')[1]) });
    }
  }
  hits.sort((a, b) => b.rev - a.rev);
  if (hits.length) return hits[0].p;
  const sys = [
    'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  ];
  return sys.find((p) => existsSync(p)) || null;
}

const browserPath = findBrowser();
if (!browserPath) {
  console.log('  跳过：未找到 Chromium/Chrome 可执行文件（可用 GW_CHROME=<path> 指定，或 GW_SKIP_BROWSER=1 显式跳过）');
  console.log('\n结果: 0 通过, 0 失败（跳过）');
  process.exit(0);
}

// ---------- 极简 CDP 客户端（Node 22 自带 WebSocket）----------
class Cdp {
  constructor(ws) {
    this.ws = ws;
    this.seq = 0;
    this.pending = new Map();
    ws.addEventListener('message', (ev) => {
      let m;
      try { m = JSON.parse(ev.data); } catch { return; }
      if (m.id && this.pending.has(m.id)) {
        const { resolve, reject } = this.pending.get(m.id);
        this.pending.delete(m.id);
        if (m.error) reject(new Error(`${m.error.message || 'cdp error'}`));
        else resolve(m.result);
      }
    });
  }
  send(method, params = {}, sessionId) {
    const id = ++this.seq;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  }
}

async function getJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let b = '';
      res.on('data', (c) => { b += c; });
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(e); } });
    }).on('error', reject);
  });
}

async function waitDevTools(port, tries = 80) {
  for (let i = 0; i < tries; i += 1) {
    try {
      const v = await getJson(`http://127.0.0.1:${port}/json/version`);
      if (v?.webSocketDebuggerUrl) return v.webSocketDebuggerUrl;
    } catch { /* not up */ }
    await wait(150);
  }
  throw new Error('Chromium DevTools 未就绪');
}

// ---------- 夹具：造用量数据 + 起网关 ----------
const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'gw-panel-layout-'));
const LOGS_DIR = path.join(tmpDir, 'logs');
const USAGE_DIR = path.join(LOGS_DIR, 'usage');
mkdirSync(USAGE_DIR, { recursive: true });

// 故意放一个很长的模型名：验证靠省略号截断而不是把布局撑破
const LONG_MODEL = 'deepseek-ai/DeepSeek-V4-Pro-0813-with-a-very-long-suffix';
const pad = (n) => String(n).padStart(2, '0');
const day = (ms) => { const d = new Date(ms); return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()); };
const d0 = new Date();
d0.setHours(0, 0, 0, 0);
const midnight = d0.getTime();
const DAY = 86400000;
const rec = (ts, model, input, output) =>
  JSON.stringify({ ts: new Date(ts).toISOString(), model, channel: 'c', input, output });
writeFileSync(path.join(USAGE_DIR, day(midnight + 10 * 3600 * 1000) + '.jsonl'),
  [
    rec(midnight + 10 * 3600 * 1000, 'deepseek-chat', 1284000, 312000),
    rec(midnight + 10 * 3600 * 1000, LONG_MODEL, 482000, 96500),
    rec(midnight + 10 * 3600 * 1000, 'glm-5.3:free', 12300, 4100),
  ].join('\n') + '\n', 'utf8');
writeFileSync(path.join(USAGE_DIR, day(midnight - 10 * DAY) + '.jsonl'),
  [rec(midnight - 10 * DAY, 'deepseek-chat', 3400000, 900000)].join('\n') + '\n', 'utf8');
writeFileSync(path.join(USAGE_DIR, day(midnight - 60 * DAY) + '.jsonl'),
  [rec(midnight - 60 * DAY, 'deepseek-chat', 5600000, 1200000)].join('\n') + '\n', 'utf8');

const GW_PORT = await freePort();
const DBG_PORT = await freePort();
const RUN_CFG = path.join(tmpDir, 'panel-layout.test.json');
writeFileSync(RUN_CFG, JSON.stringify({
  server: { host: '127.0.0.1', port: GW_PORT, apiKey: 'LAYOUTKEY', panel: true },
  taskLog: { enabled: true, dir: LOGS_DIR, file: 'tasks.jsonl', ringMax: 200, usageKeepDays: 120 },
  routing: { probeIntervalMs: 0, discoverIntervalMs: 0, retryLoop: false },
  channels: [{ name: 'demo', protocol: 'openai', baseUrl: 'http://127.0.0.1:9/v1', apiKey: 'k', model: 'demo-model', priority: 10 }],
}, null, 2) + '\n', 'utf8');

const gateway = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--no-discover', '--log-level', 'error'], { stdio: 'ignore' });
let chrome = null;
let ws = null;

/** 在浏览器里量真实几何 */
const MEASURE = `(() => {
  const host = document.getElementById('usage-chart');
  const chart = host && host.querySelector('.uchart');
  if (!chart) return JSON.stringify({ ready: false });
  const rows = [...chart.querySelectorAll('.urow')].filter((r) => !r.classList.contains('uhead'));
  const tracks = [...chart.querySelectorAll('.utrack')];
  const segs = [...chart.querySelectorAll('.utrack i')];
  const names = [...chart.querySelectorAll('.uname')];
  const w = (el) => el.getBoundingClientRect().width;
  return JSON.stringify({
    ready: true,
    rows: rows.length,
    tracks: tracks.length,
    segs: segs.length,
    maxSeg: segs.length ? Math.max(...segs.map(w)) : 0,
    minSeg: segs.length ? Math.min(...segs.map(w)) : 0,
    minTrack: tracks.length ? Math.min(...tracks.map(w)) : 0,
    // 长模型名是否靠省略号截断（scrollWidth > clientWidth 说明截断了而不是撑破布局）
    longNameTruncated: names.some((n) => n.scrollWidth - n.clientWidth > 1),
    overflow: document.documentElement.scrollWidth - window.innerWidth,
    viewport: window.innerWidth,
  });
})()`;

try {
  // 等网关就绪
  let ready = false;
  for (let i = 0; i < 60; i += 1) {
    try { const r = await fetch(`http://127.0.0.1:${GW_PORT}/health`); if (r.ok) { ready = true; break; } } catch { /* retry */ }
    await wait(200);
  }
  ok('网关就绪', ready);
  if (!ready) throw new Error('网关未就绪');

  chrome = spawn(browserPath, [
    '--headless=new', '--disable-gpu', '--no-sandbox', '--no-first-run', '--no-default-browser-check',
    '--hide-scrollbars', `--remote-debugging-port=${DBG_PORT}`,
    `--user-data-dir=${path.join(tmpDir, 'chrome-profile')}`,
    'about:blank',
  ], { stdio: 'ignore' });

  const wsUrl = await waitDevTools(DBG_PORT);
  ws = new WebSocket(wsUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', () => reject(new Error('WebSocket 连接失败')), { once: true });
  });
  const cdp = new Cdp(ws);
  ok('已连上 Chromium DevTools（CDP over 内置 WebSocket，无外部依赖）', true);

  const { targetId } = await cdp.send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await cdp.send('Target.attachToTarget', { targetId, flatten: true });
  await cdp.send('Page.enable', {}, sessionId);
  await cdp.send('Runtime.enable', {}, sessionId);

  // 逐分辨率量测：宽屏 / 常见笔记本 / 平板 / 窄窗
  const WIDTHS = [1440, 1180, 1024, 900, 760];
  for (const width of WIDTHS) {
    await cdp.send('Emulation.setDeviceMetricsOverride',
      { width, height: 1000, deviceScaleFactor: 1, mobile: false }, sessionId);
    await cdp.send('Page.navigate', { url: `http://127.0.0.1:${GW_PORT}/` }, sessionId);
    // 等面板把 /api/usage 拉回来并渲染出柱状图
    let m = null;
    for (let i = 0; i < 60; i += 1) {
      const r = await cdp.send('Runtime.evaluate', { expression: MEASURE, returnByValue: true }, sessionId);
      m = JSON.parse(r.result.value || '{}');
      if (m.ready && m.tracks > 0) break;
      await wait(200);
    }
    const label = `${width}px`;
    ok(`${label}：柱状图渲染出来（3 模型 × 4 列 = 12 条轨道 / 24 个柱段 = 12 格 × 输入+输出）`,
      m?.ready === true && m.rows === 3 && m.tracks === 12 && m.segs === 24, JSON.stringify(m));
    ok(`${label}：柱区没有被模型名列挤没（最窄轨道 ≥ 20px，实测 ${m?.minTrack?.toFixed?.(1)}px）`,
      (m?.minTrack ?? 0) >= 20, JSON.stringify({ minTrack: m?.minTrack }));
    // CSS 的 min-width:2px 保证再小的用量也看得见（曾用百分比下限，窄屏缩到 0.25px 等于没画）
    ok(`${label}：柱段真实可见（最小段 ≥ 1.9px，实测 ${m?.minSeg?.toFixed?.(2)}px）`,
      (m?.minSeg ?? 0) >= 1.9 && (m?.segs ?? 0) > 0, JSON.stringify({ minSeg: m?.minSeg, segs: m?.segs }));
    ok(`${label}：页面无横向溢出（scrollWidth ≤ 视口）`,
      (m?.overflow ?? 99) <= 1, JSON.stringify({ overflow: m?.overflow, viewport: m?.viewport }));
  }

  // 长模型名靠省略号截断而不是撑破布局
  const last = JSON.parse((await cdp.send('Runtime.evaluate', { expression: MEASURE, returnByValue: true }, sessionId)).result.value);
  ok('超长模型名用省略号截断（scrollWidth > clientWidth），没有撑破布局',
    last?.longNameTruncated === true, JSON.stringify({ longNameTruncated: last?.longNameTruncated }));

  // 柱长确实按数据等比：宽屏下最大柱（deepseek-chat 近 90 天 8.5M）应显著长于最小柱
  await cdp.send('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false }, sessionId);
  await cdp.send('Page.navigate', { url: `http://127.0.0.1:${GW_PORT}/` }, sessionId);
  let wide = null;
  for (let i = 0; i < 60; i += 1) {
    const r = await cdp.send('Runtime.evaluate', { expression: MEASURE, returnByValue: true }, sessionId);
    wide = JSON.parse(r.result.value || '{}');
    if (wide.ready && wide.segs > 0) break;
    await wait(200);
  }
  ok('宽屏下柱长拉开差距（最长段 ≥ 5× 最短段，说明按用量等比而不是等长）',
    (wide?.maxSeg ?? 0) >= (wide?.minSeg ?? 1) * 5, JSON.stringify({ maxSeg: wide?.maxSeg, minSeg: wide?.minSeg }));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  try { ws?.close(); } catch { /* ignore */ }
  try { chrome?.kill(); } catch { /* ignore */ }
  try { gateway?.kill(); } catch { /* ignore */ }
  await wait(300);
  try { rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
