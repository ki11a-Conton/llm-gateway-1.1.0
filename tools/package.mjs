#!/usr/bin/env node
// 发布包构建器：把「可公开的源码」打成一个 zip，零依赖（只用 Node 内置模块）。
//
// 为什么自己写 zip 而不是调 Compress-Archive / 7z：
//   1. 项目本身零运行时依赖，打包链也不该引入外部工具；
//   2. 排除规则要能被测试断言（见 test/test-release-package.mjs），
//      外部工具的排除参数没法在这里做漂移检查。
//
// 排除规则与 .gitignore 的「发布安全」段保持一致。
// **改这里就必须同步改 .gitignore**（或反之）——测试会做漂移检查。
//
// 用法：
//   node tools/package.mjs                  # 产出 dist/llm-gateway-<version>.zip + .sha256
//   node tools/package.mjs --out <dir>      # 换输出目录
//   node tools/package.mjs --list           # 只列出会打包哪些文件，不写盘
//   node tools/package.mjs --root <dir>     # 换项目根（默认本脚本上一级）
//   node tools/package.mjs --quiet          # 静默（供测试调用）

import { createHash } from 'node:crypto';
import { deflateRawSync } from 'node:zlib';
import {
  existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync,
} from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = path.resolve(HERE, '..');

// ---- 排除规则（与 .gitignore「发布安全」段一一对应）----

// 顶层出现的这些文件名永不进包
export const EXCLUDE_FILES = [
  'config.json',                        // 你的真实配置（含密钥）
  'providers.json',                     // 自定义 provider 预设（可能含密钥）
  'config.wbtest.json',                 // 个人测试实例（含本机代理端口等环境信息）
  'docs/workbuddy-intl-task.md',        // 内部任务书：第三方私有端点 + 逆向结论
  'handover-token-usage-dashboard.md',  // 根目录旧副本；正式版在 docs/ 下（见 README）
];

// 这些目录整棵不进包
export const EXCLUDE_DIRS = [
  '.git',
  '.workbuddy-ai',   // AI 工具的记忆/会话元数据
  '.trae-html-share-packages', // AI 工具生成的 HTML 分享包产物，不属于项目源码
  'node_modules',
  'logs',            // 任务日志：含上游错误文本
  'logs-test',
  'dist',            // 打包产物自身
  'tools/.tmp',      // 本脚本的临时目录
];

// 按扩展名/文件名尾缀排除
export const EXCLUDE_SUFFIX = ['.local.json', '.log'];
export const EXCLUDE_NAMES = ['.DS_Store', 'Thumbs.db'];

/** 判定某个相对路径（POSIX 分隔符）是否应被排除 */
export function isExcluded(rel) {
  if (EXCLUDE_FILES.includes(rel)) return true;
  if (EXCLUDE_NAMES.includes(path.posix.basename(rel))) return true;
  if (EXCLUDE_SUFFIX.some((s) => rel.endsWith(s))) return true;
  for (const dir of EXCLUDE_DIRS) {
    if (rel === dir || rel.startsWith(`${dir}/`)) return true;
  }
  return false;
}

// ---- 最小 ZIP 写入（deflate + CRC32，够用即可，不做 zip64）----

const CRC_TABLE = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c;
  }
  return t;
})();

function crc32(buf) {
  let c = -1;
  for (let i = 0; i < buf.length; i += 1) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
}

/** 把 Date 折成 MS-DOS 的 (time, date)；1980 年前一律按 1980-01-01 处理 */
function dosStamp(d) {
  const year = Math.max(1980, d.getFullYear());
  return {
    time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
    date: ((year - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  };
}

/**
 * 组装 zip 字节流。
 * @param {{name: string, data: Buffer, mtime: Date}[]} entries 已按 name 排序
 */
export function buildZip(entries) {
  const parts = [];
  const central = [];
  let offset = 0;

  for (const e of entries) {
    const nameBuf = Buffer.from(e.name, 'utf8');
    const raw = e.data;
    const comp = deflateRawSync(raw, { level: 9 });
    const crc = crc32(raw);
    const { time, date } = dosStamp(e.mtime);

    const lh = Buffer.alloc(30);              // 本地文件头
    lh.writeUInt32LE(0x04034b50, 0);
    lh.writeUInt16LE(20, 4);                  // version needed
    lh.writeUInt16LE(0x0800, 6);              // flags：bit11 = 文件名是 UTF-8
    lh.writeUInt16LE(8, 8);                   // method = deflate
    lh.writeUInt16LE(time, 10);
    lh.writeUInt16LE(date, 12);
    lh.writeUInt32LE(crc, 14);
    lh.writeUInt32LE(comp.length, 18);
    lh.writeUInt32LE(raw.length, 22);
    lh.writeUInt16LE(nameBuf.length, 26);
    lh.writeUInt16LE(0, 28);                  // extra len
    parts.push(lh, nameBuf, comp);

    const ch = Buffer.alloc(46);              // 中央目录项
    ch.writeUInt32LE(0x02014b50, 0);
    ch.writeUInt16LE(20, 4);                  // version made by
    ch.writeUInt16LE(20, 6);                  // version needed
    ch.writeUInt16LE(0x0800, 8);
    ch.writeUInt16LE(8, 10);
    ch.writeUInt16LE(time, 12);
    ch.writeUInt16LE(date, 14);
    ch.writeUInt32LE(crc, 16);
    ch.writeUInt32LE(comp.length, 20);
    ch.writeUInt32LE(raw.length, 24);
    ch.writeUInt16LE(nameBuf.length, 28);
    ch.writeUInt16LE(0, 30);                  // extra
    ch.writeUInt16LE(0, 32);                  // comment
    ch.writeUInt16LE(0, 34);                  // disk start
    ch.writeUInt16LE(0, 36);                  // internal attrs
    ch.writeUInt32LE(0, 38);                  // external attrs
    ch.writeUInt32LE(offset, 42);             // 本地头偏移
    central.push(ch, nameBuf);

    offset += lh.length + nameBuf.length + comp.length;
  }

  const cd = Buffer.concat(central);
  const eocd = Buffer.alloc(22);              // 中央目录结束记录
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8);
  eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(cd.length, 12);
  eocd.writeUInt32LE(offset, 16);
  eocd.writeUInt16LE(0, 20);
  return Buffer.concat([...parts, cd, eocd]);
}

// ---- 收集文件 ----

/**
 * 遍历项目根，返回 { name, abs, size, mtime }[]（name 形如 `llm-gateway/lib/proxy.mjs`）。
 * @param {string} root 项目根
 * @param {string} prefix zip 内的顶层目录名
 */
export function collect(root, prefix = 'llm-gateway') {
  const out = [];
  const walk = (dir, rel) => {
    for (const ent of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const childRel = rel ? `${rel}/${ent.name}` : ent.name;
      const abs = path.join(dir, ent.name);
      if (isExcluded(childRel)) continue;
      if (ent.isDirectory()) {
        walk(abs, childRel);
      } else if (ent.isFile()) {
        const st = statSync(abs);
        out.push({ name: `${prefix}/${childRel}`, rel: childRel, abs, size: st.size, mtime: st.mtime });
      }
    }
  };
  walk(root, '');
  // 按名字排序 -> 打包结果可复现（同样的输入产出同样的字节序列）
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

/** 读 package.json 的 version（拿不到就回退 dev） */
export function readVersion(root) {
  try {
    const pkg = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8'));
    return pkg.version || 'dev';
  } catch {
    return 'dev';
  }
}

/**
 * 构建发布包。
 * @returns {{ zipPath: string, sha256Path: string, sha256: string, entries: object[], bytes: number }}
 */
export function pack({ root = DEFAULT_ROOT, outDir = path.join(root, 'dist'), quiet = false } = {}) {
  const version = readVersion(root);
  const files = collect(root);
  if (!files.length) throw new Error(`没有可打包的文件（root=${root}）`);

  const entries = files.map((f) => ({ name: f.name, data: readFileSync(f.abs), mtime: f.mtime }));
  const zip = buildZip(entries);
  const sha256 = createHash('sha256').update(zip).digest('hex');

  mkdirSync(outDir, { recursive: true });
  const zipPath = path.join(outDir, `llm-gateway-${version}.zip`);
  const sha256Path = `${zipPath}.sha256`;
  writeFileSync(zipPath, zip);
  writeFileSync(sha256Path, `${sha256}  llm-gateway-${version}.zip\n`, 'utf8');

  if (!quiet) {
    const rawTotal = entries.reduce((s, e) => s + e.data.length, 0);
    const byDir = new Map();
    for (const f of files) {
      const top = f.rel.includes('/') ? `${f.rel.split('/')[0]}/` : '(根目录)';
      byDir.set(top, (byDir.get(top) || 0) + 1);
    }
    console.log(`发布包: ${zipPath}`);
    console.log(`  版本     ${version}`);
    console.log(`  文件数   ${files.length}`);
    console.log(`  原始大小 ${(rawTotal / 1024).toFixed(1)} KiB  ->  压缩后 ${(zip.length / 1024).toFixed(1)} KiB`);
    console.log(`  sha256   ${sha256}`);
    console.log('  目录分布');
    for (const [k, v] of [...byDir.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`    ${String(v).padStart(3)}  ${k}`);
    }
    console.log('  已排除');
    for (const f of [...EXCLUDE_FILES, ...EXCLUDE_DIRS]) {
      if (existsSync(path.join(root, f.split('/')[0]))) console.log(`    -     ${f}`);
    }
  }
  return { zipPath, sha256Path, sha256, entries: files, bytes: zip.length, version };
}

// ---- CLI ----

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const argv = process.argv.slice(2);
  const argOf = (flag) => {
    const i = argv.indexOf(flag);
    return i !== -1 && argv[i + 1] ? argv[i + 1] : null;
  };
  const root = argOf('--root') ? path.resolve(argOf('--root')) : DEFAULT_ROOT;

  if (argv.includes('--list')) {
    const files = collect(root);
    const rawTotal = files.reduce((s, f) => s + f.size, 0);
    console.log(`${files.length} 个文件，原始 ${(rawTotal / 1024).toFixed(1)} KiB`);
    for (const f of files) console.log(`  ${f.name}`);
  } else {
    const outDir = argOf('--out') ? path.resolve(argOf('--out')) : path.join(root, 'dist');
    pack({ root, outDir, quiet: argv.includes('--quiet') });
  }
}
