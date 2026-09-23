// B2 + B3 回归：任务日志的轮转 / 体积上限 / 落盘脱敏 / 时间窗
//
// B2：单档体积上限 + 保留 N 档（tasks.1.jsonl …），以及 recent() 的 since/until 时间窗。
// B3：落盘副本必须脱敏（上游错误体常回显 Authorization / api key 片段），
//     内存 ring 仍保留原文供排障。
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { existsSync, readFileSync, mkdtempSync, rmSync, statSync, readdirSync } from 'node:fs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const { TaskLog, redactSecrets } = await import('../lib/tasklog.mjs');

// ---------- B3 单元：脱敏规则 ----------
{
  const k1 = redactSecrets('upstream said: sk-abcdef123456 is invalid');
  ok('B3 单元：sk- 前缀 key 被掩码', !k1.includes('sk-abcdef123456') && k1.includes('***REDACTED***'), k1);

  const k2 = redactSecrets('Authorization: Bearer abcdefghijklmnop');
  ok('B3 单元：Bearer token 被掩码', !k2.includes('abcdefghijklmnop') && /Bearer\s+\*\*\*REDACTED\*\*\*/.test(k2), k2);

  const k3 = redactSecrets('{"error":{"message":"invalid apiKey=secretvalue123"}}');
  ok('B3 单元：apiKey 值被掩码', !k3.includes('secretvalue123'), k3);

  const k4 = redactSecrets('x-api-key: 0123456789abcdef');
  ok('B3 单元：x-api-key 头被掩码', !k4.includes('0123456789abcdef'), k4);

  const k5 = redactSecrets('access_token=abcdefghijkl');
  ok('B3 单元：access_token 被掩码', !k5.includes('abcdefghijkl'), k5);

  const normal = 'HTTP 500 upstream boom (server_error)';
  ok('B3 单元：普通错误文本不被误伤', redactSecrets(normal) === normal, redactSecrets(normal));
  ok('B3 单元：非字符串输入安全处理', typeof redactSecrets({ a: 'sk-abcdef123456' }) === 'string');
  ok('B3 单元：null 原样返回', redactSecrets(null) === null);
}

// ---------- B3 + B2 集成：落盘脱敏 + 轮转 ----------
{
  const dir = mkdtempSync(path.join(os.tmpdir(), 'gw-tasklog-'));
  const base = path.join(dir, 'tasks.jsonl');
  const archive = (i) => path.join(dir, `tasks.${i}.jsonl`);

  const tl = new TaskLog({ enabled: true, dir, file: 'tasks.jsonl', maxFileBytes: 600, keepFiles: 3 });

  const SECRET_ERR = 'HTTP 401 invalid api key sk-live-abcdef123456 (Authorization: Bearer sk-live-abcdef123456)';
  tl.write({
    requestId: 'r1', model: 'm', ok: false, kind: 'auth',
    error: SECRET_ERR,
    attempts: [{ channel: 'c1', error: SECRET_ERR, kind: 'auth' }],
    events: [{ level: 'error', message: SECRET_ERR }],
  });
  await tl.flushNow();

  ok('B3 集成：落盘文件已生成', existsSync(base), base);
  const disk1 = existsSync(base) ? readFileSync(base, 'utf8') : '';
  ok('B3 集成：落盘不含明文 sk- key', !disk1.includes('sk-live-abcdef123456'), disk1.slice(0, 200));
  ok('B3 集成：落盘含掩码标记', disk1.includes('***REDACTED***'), disk1.slice(0, 200));
  ok('B3 集成：落盘的 error 与 attempts/events 都脱敏',
    !/sk-live/.test(disk1), disk1.slice(0, 300));
  ok('B3 集成：内存 ring 仍保留原文（排障需要）',
    tl.recent(1)[0]?.error === SECRET_ERR, String(tl.recent(1)[0]?.error).slice(0, 120));

  // 持续写入触发轮转（每条 ~ 300 字节，上限 600）
  const filler = 'x'.repeat(240);
  for (let i = 0; i < 6; i += 1) {
    tl.write({ requestId: `r${i + 2}`, model: 'm', ok: true, note: filler });
    await tl.flushNow();
  }

  ok('B2 集成：产生第 1 档归档', existsSync(archive(1)), archive(1));
  ok('B2 集成：产生第 2 档归档', existsSync(archive(2)), archive(2));
  ok('B2 集成：超出 keepFiles 的归档被删除（不存在第 3 档）', !existsSync(archive(3)), archive(3));

  const cur = statSync(base).size;
  ok('B2 集成：当前档不超过体积上限', cur <= 600, `size=${cur}`);
  for (const i of [1, 2]) {
    const s = statSync(archive(i)).size;
    ok(`B2 集成：第 ${i} 档不超过体积上限`, s <= 600, `size=${s}`);
    const lines = readFileSync(archive(i), 'utf8').trim().split('\n').filter(Boolean);
    ok(`B2 集成：第 ${i} 档是可解析 JSONL`,
      lines.length > 0 && lines.every((l) => { try { JSON.parse(l); return true; } catch { return false; } }),
      `lines=${lines.length}`);
  }

  // 轮转目录里不应出现命名以外的残留
  const names = readdirSync(dir).sort();
  ok('B2 集成：归档命名符合 tasks.<n>.jsonl 约定',
    names.every((n) => /^tasks(\.\d+)?\.jsonl$/.test(n)), JSON.stringify(names));

  ok('B2 单元：snapshot 暴露轮转配置',
    tl.snapshot().maxFileBytes === 600 && tl.snapshot().keepFiles === 3,
    JSON.stringify({ maxFileBytes: tl.snapshot().maxFileBytes, keepFiles: tl.snapshot().keepFiles }));

  // ---------- B2：recent() 时间窗 ----------
  {
    const dir2 = mkdtempSync(path.join(os.tmpdir(), 'gw-tasklog-win-'));
    const tl2 = new TaskLog({ enabled: false, dir: dir2, file: 'tasks.jsonl' });
    const t0 = Date.UTC(2026, 0, 1, 0, 0, 0);
    const iso = (ms) => new Date(ms).toISOString();
    tl2.write({ ts: iso(t0), requestId: 'a', ok: true });
    tl2.write({ ts: iso(t0 + 1000), requestId: 'b', ok: false, fingerprint: 'auth' });
    tl2.write({ ts: iso(t0 + 2000), requestId: 'c', ok: true });

    ok('B2 单元：since 闭区间过滤（ISO 字符串）',
      tl2.recent(10, { since: iso(t0 + 1000) }).map((r) => r.requestId).join(',') === 'c,b',
      tl2.recent(10, { since: iso(t0 + 1000) }).map((r) => r.requestId).join(','));
    ok('B2 单元：until 闭区间过滤（毫秒时间戳）',
      tl2.recent(10, { until: t0 + 1000 }).map((r) => r.requestId).join(',') === 'b,a',
      tl2.recent(10, { until: t0 + 1000 }).map((r) => r.requestId).join(','));
    ok('B2 单元：since+until 同时生效',
      tl2.recent(10, { since: t0 + 1000, until: t0 + 1000 }).map((r) => r.requestId).join(',') === 'b');
    ok('B2 单元：时间窗与 failed 过滤叠加',
      tl2.recent(10, { since: t0, onlyFailed: true }).map((r) => r.requestId).join(',') === 'b');
    ok('B2 单元：非法时间串被忽略（不过滤）', tl2.recent(10, { since: 'not-a-date' }).length === 3);
    rmSync(dir2, { recursive: true, force: true });
  }

  rmSync(dir, { recursive: true, force: true });
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
