// 发布包守门：把设计规格 DoD #6「生成的发布包仍不含任何真实 key」从人工复验变成自动断言。
//
// 这个套件做三件事，缺一不可：
//   1. 规则层：打包器的排除表必须覆盖 .gitignore 的每一条（漂移检查）——有人往 .gitignore
//      加了敏感文件却没同步打包器，这里就红；
//   2. 字节层：真建一个 zip，自己解析中央目录 + inflate，逐文件与源树比对，
//      并用**独立实现的 CRC32** 复核（不复用打包器的 crc32，否则同错同销）；
//   3. 内容层：包内文本里出现的密钥特征必须全部落在「已知假 key」白名单里，
//      且模板文件 config.example.json 的 apiKey 只能是占位形态。
//
// 跑完不往 dist/ 写东西（用临时目录）。

import { inflateRawSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { collect, pack, isExcluded, EXCLUDE_FILES, EXCLUDE_DIRS } from '../tools/package.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

let pass = 0;
let fail = 0;
const ok = (name, cond, info = '') => {
  if (cond) { pass += 1; console.log(`  ✓ ${name}`); } else {
    fail += 1; console.log(`  ✗ ${name}${info ? `  <- ${info}` : ''}`);
  }
};

// ---- 独立实现的 CRC32（按位算，无查表，与打包器的查表版互为独立实现）----
function crc32Independent(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i += 1) {
    c ^= buf[i];
    for (let k = 0; k < 8; k += 1) c = (c >>> 1) ^ (0xedb88320 & -(c & 1));
  }
  return (c ^ 0xffffffff) >>> 0;
}

// ---- 最小 ZIP 读取器（只读中央目录 + inflate，用于端到端复核）----
function readZipCentral(buf) {
  let eocd = -1;
  const back = Math.min(buf.length, 65557);
  for (let i = buf.length - 22; i >= buf.length - back && i >= 0; i -= 1) {
    if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('找不到 EOCD 记录');
  const count = buf.readUInt16LE(eocd + 10);
  const cdSize = buf.readUInt32LE(eocd + 12);
  const cdOff = buf.readUInt32LE(eocd + 16);
  const entries = [];
  let p = cdOff;
  for (let i = 0; i < count; i += 1) {
    if (buf.readUInt32LE(p) !== 0x02014b50) throw new Error(`第 ${i} 个中央目录项签名不对 @${p}`);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    entries.push({
      name: buf.toString('utf8', p + 46, p + 46 + nameLen),
      method: buf.readUInt16LE(p + 10),
      crc: buf.readUInt32LE(p + 16),
      compSize: buf.readUInt32LE(p + 20),
      size: buf.readUInt32LE(p + 24),
      localOff: buf.readUInt32LE(p + 42),
    });
    p += 46 + nameLen + extraLen + commentLen;
  }
  return { entries, count, cdSize, cdOff, eocd };
}

function readZipEntry(buf, entry) {
  const p = entry.localOff;
  if (buf.readUInt32LE(p) !== 0x04034b50) throw new Error(`本地头签名不对: ${entry.name}`);
  const nameLen = buf.readUInt16LE(p + 26);
  const extraLen = buf.readUInt16LE(p + 28);
  const start = p + 30 + nameLen + extraLen;
  const raw = buf.subarray(start, start + entry.compSize);
  return entry.method === 0 ? Buffer.from(raw) : inflateRawSync(raw);
}

console.log('\n── A. 打包器排除规则 ──');

// A1. 敏感路径必须被排除（与 .gitignore「发布安全」段一一对应）
const MUST_EXCLUDE = [
  ['config.json', '真实配置（含密钥）'],
  ['providers.json', '自定义 provider 预设（可能含密钥）'],
  ['config.wbtest.json', '个人测试实例'],
  ['docs/workbuddy-intl-task.md', '内部任务书'],
  ['handover-token-usage-dashboard.md', '根目录旧副本（正式版在 docs/）'],
  ['secret.local.json', '*.local.json'],
  ['node_modules/pkg/index.js', 'node_modules/'],
  ['.DS_Store', '.DS_Store'],
  ['logs/tasks.jsonl', 'logs/'],
  ['app.log', '*.log'],
  ['logs-test/x.jsonl', 'logs-test/'],
  ['.workbuddy-ai/session.json', '.workbuddy-ai/'],
  ['dist/llm-gateway-9.9.9.zip', 'dist/'],
];
for (const [probe, why] of MUST_EXCLUDE) {
  ok(`排除 ${probe}（${why}）`, isExcluded(probe) === true);
}

// A2. 反例：正常源码不能被误排（排除规则写宽了会把源码一起排掉）
const MUST_KEEP = [
  'config.example.json', 'providers.example.json', 'package.json', 'README.md',
  'server.mjs', 'start.ps1', 'lib/proxy.mjs', 'lib/adapters/openai.mjs',
  'public/index.html', 'test/run-all.mjs', 'tools/package.mjs',
  'docs/handover-token-usage-dashboard.md', 'LLM-Gateway-Code-Review.md',
];
const wronglyDropped = MUST_KEEP.filter((p) => isExcluded(p));
ok('正常源码不会被误排（反例 13 条）', wronglyDropped.length === 0, JSON.stringify(wronglyDropped));

// A3. .gitignore 漂移检查：.gitignore 的每一条都必须有对应的打包排除规则。
// 故意不"自动推导"——新增一条 .gitignore 却不想改打包器时，必须来这里显式登记（写下探针路径），
// 强迫作者做一次"这条到底该不该进包"的判断。
const GITIGNORE_PROBE = {
  'config.json': 'config.json',
  'providers.json': 'providers.json',
  '*.local.json': 'x.local.json',
  'node_modules/': 'node_modules/a/b.js',
  '.DS_Store': '.DS_Store',
  '*.log': 'x.log',
  'logs/': 'logs/tasks.jsonl',
  'logs-test/': 'logs-test/tasks.jsonl',
  'config.wbtest.json': 'config.wbtest.json',
  '.workbuddy-ai/': '.workbuddy-ai/mem.md',
  '.trae-html-share-packages/': '.trae-html-share-packages/index.html',
  'dist/': 'dist/out.zip',
  'docs/workbuddy-intl-task.md': 'docs/workbuddy-intl-task.md',
};
const ignoreLines = readFileSync(path.join(ROOT, '.gitignore'), 'utf8')
  .split(/\r?\n/)
  .map((l) => l.trim())
  .filter((l) => l && !l.startsWith('#'));
const unmapped = ignoreLines.filter((l) => !(l in GITIGNORE_PROBE));
ok('.gitignore 每条都有打包侧登记（无漏登）', unmapped.length === 0,
  `漏登: ${JSON.stringify(unmapped)}`);
const probeFailed = ignoreLines
  .filter((l) => l in GITIGNORE_PROBE)
  .filter((l) => !isExcluded(GITIGNORE_PROBE[l]));
ok('.gitignore 的每条都被打包器真正排除（无漂移）', probeFailed.length === 0,
  `未生效: ${JSON.stringify(probeFailed.map((l) => `${l} -> ${GITIGNORE_PROBE[l]}`))}`);
const staleProbe = Object.keys(GITIGNORE_PROBE).filter((k) => !ignoreLines.includes(k));
ok('打包侧探针没有指向已从 .gitignore 删掉的规则', staleProbe.length === 0,
  JSON.stringify(staleProbe));

console.log('\n── B. 产出的 zip（字节层复核）──');

const tmp = mkdtempSync(path.join(tmpdir(), 'gw-release-'));
let zipBuf = null;
let entries = null;
try {
  const built = pack({ root: ROOT, outDir: tmp, quiet: true });
  zipBuf = readFileSync(built.zipPath);

  // B1. sha256 收据与实际字节一致
  const declared = readFileSync(built.sha256Path, 'utf8').trim().split(/\s+/)[0];
  const actual = createHash('sha256').update(zipBuf).digest('hex');
  ok('sha256 收据与实际 zip 字节一致', declared === actual, `${declared} vs ${actual}`);
  ok('sha256 文件名与 zip 同名', built.sha256Path === `${built.zipPath}.sha256`, built.sha256Path);

  // B2. 结构自洽
  const cd = readZipCentral(zipBuf);
  ok('EOCD 条目数与中央目录一致', cd.count === cd.entries.length, `${cd.count} vs ${cd.entries.length}`);
  ok('中央目录长度与 EOCD 记录一致', cd.cdSize === zipBuf.length - cd.cdOff - 22,
    `${cd.cdSize} vs ${zipBuf.length - cd.cdOff - 22}`);
  ok('EOCD 位于文件末尾（无尾随垃圾）', cd.eocd + 22 === zipBuf.length, `${cd.eocd + 22} vs ${zipBuf.length}`);

  // B3. 路径卫生：全在 llm-gateway/ 下、无绝对路径、无 ..
  const badPaths = cd.entries.filter((e) => !e.name.startsWith('llm-gateway/')
    || e.name.startsWith('/') || e.name.includes('..') || e.name.includes('\\'));
  ok('包内路径全部规整（llm-gateway/ 前缀、无绝对路径 / .. / 反斜杠）', badPaths.length === 0,
    JSON.stringify(badPaths.slice(0, 3).map((e) => e.name)));

  // B4. 条目集合 == 打包器声明的清单
  const declaredFiles = collect(ROOT).map((f) => f.name).sort();
  const zipNames = cd.entries.map((e) => e.name).sort();
  ok('zip 条目数 == 清单数', zipNames.length === declaredFiles.length,
    `${zipNames.length} vs ${declaredFiles.length}`);
  const missing = declaredFiles.filter((n) => !zipNames.includes(n));
  const extra = zipNames.filter((n) => !declaredFiles.includes(n));
  ok('zip 条目集合与清单完全一致', missing.length === 0 && extra.length === 0,
    `缺 ${JSON.stringify(missing.slice(0, 3))} 多 ${JSON.stringify(extra.slice(0, 3))}`);

  // B5. 逐条目 inflate + 独立 CRC + 与源文件逐字节比对
  entries = cd.entries.map((e) => ({ ...e, data: readZipEntry(zipBuf, e) }));
  let crcBad = 0;
  let sizeBad = 0;
  let contentBad = 0;
  for (const e of entries) {
    if (crc32Independent(e.data) !== e.crc) crcBad += 1;
    if (e.data.length !== e.size) sizeBad += 1;
    const src = readFileSync(path.join(ROOT, e.name.slice('llm-gateway/'.length)));
    if (!src.equals(e.data)) contentBad += 1;
  }
  ok('每个条目的 CRC32 都被独立实现复核通过', crcBad === 0, `不符 ${crcBad} 个`);
  ok('每个条目的解压长度与记录一致', sizeBad === 0, `不符 ${sizeBad} 个`);
  ok('每个条目的内容与源文件逐字节一致', contentBad === 0, `不符 ${contentBad} 个`);

  // B6. 解出来的内容里不含任何被排除的路径
  const leaked = zipNames.filter((n) => EXCLUDE_FILES.includes(n.slice('llm-gateway/'.length))
    || EXCLUDE_DIRS.some((d) => {
      const r = n.slice('llm-gateway/'.length);
      return r === d || r.startsWith(`${d}/`);
    }));
  ok('zip 里没有任何被排除的路径', leaked.length === 0, JSON.stringify(leaked.slice(0, 5)));

  // B7. 关键文件必须在（防止排除写宽把源码排掉）
  const required = [
    'README.md', 'package.json', 'config.example.json', 'providers.example.json',
    'server.mjs', 'start.ps1', 'public/index.html', 'tools/package.mjs',
    'lib/proxy.mjs', 'lib/channels.mjs', 'lib/concurrency.mjs', 'lib/usage.mjs',
    'lib/adapters/openai.mjs', 'lib/adapters/anthropic.mjs',
    'test/run-all.mjs', 'test/lib/ports.mjs', 'test/mock-upstream.mjs',
    '.gitignore',
  ].map((p) => `llm-gateway/${p}`);
  const absent = required.filter((n) => !zipNames.includes(n));
  ok('关键文件齐全（17 项）', absent.length === 0, JSON.stringify(absent));

  console.log('\n── C. 内容层：不含真实密钥 ──');

  // C1. 模板文件的 apiKey / apiKeys 只能是占位形态
  const example = JSON.parse(readFileSync(path.join(ROOT, 'config.example.json'), 'utf8'));
  const PLACEHOLDER = /^(\$\{[A-Z0-9_]+\}|PROXY_MANAGED|TESTKEY|)$/;
  const chans = Array.isArray(example.channels) ? example.channels : [];
  const badKeys = chans.filter((c) => c.apiKey != null && !PLACEHOLDER.test(String(c.apiKey)));
  ok(`config.example.json 的 apiKey 全是占位（${chans.length} 个渠道）`,
    chans.length > 0 && badKeys.length === 0, JSON.stringify(badKeys.map((c) => c.apiKey)));
  // 叠加 key（apiKeys 数组）同样只能是占位形态
  const badMulti = chans.flatMap((c) => (Array.isArray(c.apiKeys) ? c.apiKeys : [])
    .filter((k) => !PLACEHOLDER.test(String(k)))
    .map((k) => `${c.name}:${k}`));
  ok('config.example.json 的 apiKeys 全是占位',
    badMulti.length === 0, JSON.stringify(badMulti));

  // C2. 密钥特征扫描：包内文本文件的命中必须全在「已知假 key」白名单
  const KNOWN_FAKE = new Map([
    ['sk-LEAKTEST0123456789abcdef', 'test-usage.mjs / handover 文档：喂给脱敏逻辑的假 key'],
    ['sk-live-obsabcdef123456', 'test-metrics-observability.mjs：指标脱敏用例的假 key'],
    ['sk-live-abcdef123456', 'test-tasklog-rotate.mjs：任务日志脱敏用例的假 key'],
    ['sk-abcdef123456', 'test-tasklog-rotate.mjs：脱敏单元用例的假 key'],
  ]);
  const SECRET_RE = /(?:sk|ghp|gho|ghs|ghr|xox[bp]|AIza|AKIA)[-_][A-Za-z0-9_-]{12,}/g;
  const textExt = ['.mjs', '.json', '.md', '.html', '.ps1', '.txt', '.yml', '.yaml', '.gitignore'];
  const SELF = 'llm-gateway/test/test-release-package.mjs'; // 本文件：白名单字面量就在里面
  const hits = new Map();
  const outside = new Set(); // 除本文件外的命中（用于判断白名单是不是名存实亡）
  for (const e of entries) {
    if (!textExt.some((x) => e.name.endsWith(x)) && !e.name.endsWith('.gitignore')) continue;
    const text = e.data.toString('utf8');
    for (const m of text.match(SECRET_RE) || []) {
      if (!hits.has(m)) hits.set(m, []);
      hits.get(m).push(e.name);
      if (e.name !== SELF) outside.add(m);
    }
  }
  const unknown = [...hits.keys()].filter((k) => !KNOWN_FAKE.has(k));
  ok(`密钥特征命中全部是已知假 key（命中 ${hits.size} 种）`, unknown.length === 0,
    `未登记: ${JSON.stringify(unknown)}`);
  const staleFake = [...KNOWN_FAKE.keys()].filter((k) => !outside.has(k));
  ok('白名单里没有已不存在的假 key（防白名单名存实亡）', staleFake.length === 0,
    JSON.stringify(staleFake));

  // C3. 私钥块只允许出现在那个自签证书测试里（且是 throwaway）
  // 注意：标记串必须拼出来，否则本文件自己就会被扫到（自指）
  const PEM_MARK = `${'-----BEGIN'} PRIVATE ${'KEY-----'}`;
  const pemFiles = entries.filter((e) => e.data.includes(PEM_MARK))
    .map((e) => e.name);
  ok('含私钥块的文件只有 test-outbound-proxy-abort.mjs（test-only throwaway 自签证书）',
    pemFiles.length === 1 && pemFiles[0] === 'llm-gateway/test/test-outbound-proxy-abort.mjs',
    JSON.stringify(pemFiles));
  const pemSrc = readFileSync(path.join(ROOT, 'test/test-outbound-proxy-abort.mjs'), 'utf8');
  ok('该私钥确实标注为 throwaway 测试用', /throwaway/.test(pemSrc));
} finally {
  rmSync(tmp, { recursive: true, force: true });
}

ok('临时目录已清理（不留垃圾）', !existsSync(tmp), tmp);
console.log(`\n结果: ${pass} 通过 / ${fail} 失败`);
if (fail > 0) process.exit(1);
