/**
 * P5 —— 一把跑完全部测试套件（串行）。
 *
 *   node test/run-all.mjs                     跑全部套件（test-*.mjs + smoke.mjs）
 *   node test/run-all.mjs --only test-pool.mjs 只跑一个（文件名 / 去扩展名 / 子串都行）
 *   node test/run-all.mjs --list              只列出会被跑的套件
 *   npm test                                  等价于第一条
 *   npm run test:one -- test-tiered.mjs        等价于第二条
 *
 * 串行是刻意的：输出可读，且动态端口在极端情况下也不会互相撞车。
 * 每个套件实时透传输出，并抓取它自己打印的 `结果: N 通过, M 失败` 行做汇总；
 * 退出码 = 失败套件数（全绿时 0）。
 * 套件超时默认 5 分钟，可用环境变量 TEST_SUITE_TIMEOUT_MS 覆盖（防止某个套件挂死拖住整轮）。
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;

const argv = process.argv.slice(2);
const flagValue = (name) => {
  const i = argv.indexOf(name);
  return i >= 0 ? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : '') : null;
};
const only = flagValue('--only');
const listOnly = argv.includes('--list');
const SUITE_TIMEOUT_MS = Number(process.env.TEST_SUITE_TIMEOUT_MS || 300000);

/** 被当作"套件"的文件：全部 test-*.mjs + smoke.mjs（bench-*.mjs 是基准脚本，不在这里跑） */
function discover() {
  return fs.readdirSync(HERE)
    .filter((f) => f.endsWith('.mjs'))
    .filter((f) => /^test-.+\.mjs$/.test(f) || f === 'smoke.mjs')
    .sort();
}

function select(all, want) {
  if (want === null) return all;
  if (!want) return [];
  const bare = want.endsWith('.mjs') ? want.slice(0, -4) : want;
  return all.filter((f) => f === want || f === `${want}.mjs` || f === `${bare}.mjs` || f.includes(bare));
}

const all = discover();
const suites = select(all, only);

if (listOnly) {
  for (const f of suites) console.log(f);
  console.log(`共 ${suites.length} 个套件`);
  process.exit(0);
}
if (!suites.length) {
  console.error(only === null ? 'test/ 下没找到任何套件' : `--only ${only} 没匹配到套件（共 ${all.length} 个）`);
  process.exit(1);
}

const RESULT_RE = /结果:\s*(\d+)\s*通过\s*[，,/]?\s*(\d+)\s*失败/g;
/** 有的套件（smoke 等）给结果行加了 ANSI 颜色，解析前先剥掉 */
const stripAnsi = (s) => s.replace(/\u001b\[[0-9;]*m/g, '');

function runSuite(file) {
  return new Promise((resolve) => {
    const child = spawn(NODE, [path.join(HERE, file)], {
      cwd: ROOT,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    });
    let out = '';
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill(); }, SUITE_TIMEOUT_MS);

    child.stdout.on('data', (b) => { const s = b.toString(); out += s; process.stdout.write(s); });
    child.stderr.on('data', (b) => { const s = b.toString(); out += s; process.stderr.write(s); });
    child.on('error', (err) => {
      clearTimeout(timer);
      resolve({ code: -1, out: `${out}\n[spawn error] ${err.message}`, timedOut });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code: code ?? -1, out, timedOut });
    });
  });
}

console.log(`\n==== 测试运行器：共 ${suites.length} 个套件（串行）====`);

let suitePass = 0;
let suiteFail = 0;
let assertPass = 0;
let assertFail = 0;
const failures = [];

for (let i = 0; i < suites.length; i += 1) {
  const file = suites[i];
  console.log(`\n──────────────────────────────────────────────────────────────`);
  console.log(`▶ [${i + 1}/${suites.length}] ${file}`);
  console.log(`──────────────────────────────────────────────────────────────`);

  const started = Date.now();
  const { code, out, timedOut } = await runSuite(file);
  const secs = ((Date.now() - started) / 1000).toFixed(1);

  RESULT_RE.lastIndex = 0;
  const plain = stripAnsi(out);
  let m = null;
  let last = null;
  while ((m = RESULT_RE.exec(plain)) !== null) last = m;
  const p = last ? Number(last[1]) : 0;
  const f = last ? Number(last[2]) : 0;

  const failed = timedOut || code !== 0 || !last || f > 0;
  if (failed) {
    suiteFail += 1;
    failures.push(`${file}${timedOut ? '（超时被强杀）' : ''}`);
    console.log(`✗ ${file}  失败（退出码 ${code}${last ? `，结果 ${p}/${f}` : '，没打印结果行'}，${secs}s）`);
  } else {
    suitePass += 1;
    console.log(`✓ ${file}  ${p} 通过 / 0 失败（${secs}s）`);
  }
  assertPass += p;
  assertFail += f;
}

console.log(`\n==== 汇总 ====`);
console.log(`套件: ${suitePass} 通过 / ${suiteFail} 失败（共 ${suites.length}）`);
console.log(`断言: ${assertPass} 通过 / ${assertFail} 失败`);
if (failures.length) console.log(`失败套件: ${failures.join(', ')}`);

process.exit(Math.min(255, suiteFail));
