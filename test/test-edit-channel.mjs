// 测试「编辑已保存的渠道」功能：PATCH /api/channels/<name> + ChannelManager.updateChannel
//
// 覆盖点（每条都对应一个真实会踩的坑）：
//   1. 字段窄写：只改传进来的字段，其它字段（尤其 apiKey）原样保留
//   2. 密钥留空 = 保持不变（面板不回显明文，空值绝不能当成清空）
//   3. 密钥替换：单 key <-> 多 key（apiKeys + stackedKeyStrategy）形态互转
//   4. 清空字段：空串 = 删除该字段（回到"跟随全局"）
//   5. enabled 开关 + "补上 key 后自动重新启用"
//   6. ${ENV} 占位不能被写死成 enabled:false（否则环境变量注入后永远起不来）
//   7. 改名：同步改写 routes 引用，不留悬空项
//   8. 错误分支：渠道不存在 / 重名 / 非法 preset / 自定义缺 baseUrl 一律 400 且不落盘
//   9. 跨站 PATCH 被 CSRF 拦截且配置未被改写
//  10. 编辑后立即热重载（/api/status 反映新配置）
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
// 端口动态分配；配置走临时副本，网关回写不会动仓库里的 *.test.json
const PORT = await freePort();
const CFG = materializeConfig(path.join(HERE, 'edit-channel.test.json'), { port: PORT });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitReady(url, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok) return true; } catch { /* 还没起来 */ }
    await wait(250);
  }
  throw new Error('gateway not ready: ' + url);
}
async function call(pathname, opts = {}) {
  const headers = { authorization: 'Bearer TESTKEY', 'content-type': 'application/json', ...(opts.headers || {}) };
  const res = await fetch(`http://127.0.0.1:${PORT}` + pathname, { ...opts, headers });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, json, text };
}
const patch = (name, body) => call('/api/channels/' + encodeURIComponent(name), {
  method: 'PATCH',
  body: JSON.stringify(body),
});
const cfgNow = () => JSON.parse(readFileSync(CFG, 'utf8'));
const chan = (name) => cfgNow().channels.find((c) => c && c.name === name);

const gw = spawn(NODE, [path.join(ROOT, 'server.mjs'), '--config', CFG, '--no-discover', '--log-level', 'warn'], { stdio: 'ignore' });
try {
  await waitReady(`http://127.0.0.1:${PORT}/health`);
  await wait(300);

  // ---- 1. 字段窄写：只改传进来的字段 ----
  const e1 = await patch('edit-a', { description: '改过的备注', priority: 7, model: 'deepseek-reasoner' });
  ok('PATCH 基本字段成功', e1.status === 200 && e1.json?.ok === true, e1.text.slice(0, 160));
  let a = chan('edit-a');
  ok('description 已更新', a?.description === '改过的备注', JSON.stringify(a?.description));
  ok('priority 已更新', a?.priority === 7, String(a?.priority));
  ok('model 已更新', a?.model === 'deepseek-reasoner', String(a?.model));
  ok('★ 未传的 apiKey 原样保留', a?.apiKey === 'orig-key', String(a?.apiKey));

  // ---- 2. 密钥留空 = 保持不变（显式传空串/空数组也不能清空）----
  const e2 = await patch('edit-a', { apiKey: '', apiKeys: [] });
  ok('空密钥 PATCH 返回成功', e2.status === 200, e2.text.slice(0, 160));
  ok('★ 空密钥不会清掉原 apiKey', chan('edit-a')?.apiKey === 'orig-key', String(chan('edit-a')?.apiKey));

  // ---- 3. 密钥替换：单 key -> 多 key（apiKeys + 叠 Key 策略）----
  const e3 = await patch('edit-a', { apiKeys: ['n1', 'n2'], stackedKeyStrategy: 'rotate-429' });
  ok('换成多 key 成功', e3.status === 200, e3.text.slice(0, 160));
  a = chan('edit-a');
  ok('落成 apiKeys 数组（2 个）', Array.isArray(a?.apiKeys) && a.apiKeys.length === 2, JSON.stringify(a?.apiKeys));
  ok('单 key 字段 apiKey 已移除', a?.apiKey === undefined, String(a?.apiKey));
  ok('叠 Key 策略已落盘', a?.stackedKeyStrategy === 'rotate-429', String(a?.stackedKeyStrategy));
  // 多 key -> 单 key：apiKeys 归一为 apiKey，策略字段应被清掉
  const e3b = await patch('edit-a', { apiKeys: ['solo'] });
  a = chan('edit-a');
  ok('退回单 key 落成 apiKey', e3b.status === 200 && a?.apiKey === 'solo', JSON.stringify(a));
  ok('单 key 渠道不再保留 stackedKeyStrategy', a?.stackedKeyStrategy === undefined, String(a?.stackedKeyStrategy));

  // ---- 4. 清空字段：空串 = 删除该字段 ----
  await patch('edit-a', { effort: 'high' });
  ok('effort 可写入', chan('edit-a')?.effort === 'high', String(chan('edit-a')?.effort));
  await patch('edit-a', { effort: '' });
  ok('★ effort 空串 = 删除该字段', chan('edit-a')?.effort === undefined, String(chan('edit-a')?.effort));

  // ---- 5. enabled 开关 ----
  await patch('edit-a', { enabled: false });
  ok('enabled=false 落盘', chan('edit-a')?.enabled === false, String(chan('edit-a')?.enabled));
  let st = await call('/api/status');
  ok('停用后 /api/status 反映 enabled=false',
    st.json?.channels?.find((c) => c.name === 'edit-a')?.enabled === false);
  await patch('edit-a', { enabled: true });
  ok('enabled=true 落盘', chan('edit-a')?.enabled === true, String(chan('edit-a')?.enabled));

  // ---- 6. 改名 + routes 引用同步 ----
  const e6 = await patch('edit-a', { name: 'edit-a2' });
  ok('改名成功', e6.status === 200 && e6.json?.channel?.name === 'edit-a2', e6.text.slice(0, 160));
  ok('旧名已从 config 消失', chan('edit-a') === undefined);
  ok('新名已写入 config', chan('edit-a2') !== undefined);
  ok('★ routes 里的旧名同步改成新名',
    JSON.stringify(cfgNow().routes?.['deepseek-chat']) === JSON.stringify(['edit-a2']),
    JSON.stringify(cfgNow().routes));
  st = await call('/api/status');
  ok('改名后热重载生效（/api/status 可见新名）', !!st.json?.channels?.find((c) => c.name === 'edit-a2'));

  // ---- 7. 错误分支：一律 400 且不落盘 ----
  const before = JSON.stringify(cfgNow());
  const bad1 = await patch('no-such-channel', { description: 'x' });
  ok('不存在的渠道 -> 400', bad1.status === 400 && /不存在/.test(bad1.json?.error || ''), bad1.text.slice(0, 160));
  const bad2 = await patch('edit-a2', { name: 'edit-preset' });
  ok('改成已存在的名字 -> 400', bad2.status === 400 && /已存在/.test(bad2.json?.error || ''), bad2.text.slice(0, 160));
  const bad3 = await patch('edit-preset', { preset: 'no-such-preset-xyz' });
  ok('非法 preset -> 400', bad3.status === 400 && /不存在/.test(bad3.json?.error || ''), bad3.text.slice(0, 160));
  const bad4 = await patch('edit-preset', { preset: '' });
  ok('预设渠道改成自定义但不填 baseUrl -> 400', bad4.status === 400 && /baseUrl/.test(bad4.json?.error || ''), bad4.text.slice(0, 160));
  const bad5 = await patch('edit-a2', { priority: 'abc' });
  ok('非法 priority 被忽略（仍 200 且值不变）',
    bad5.status === 200 && chan('edit-a2')?.priority === 7, bad5.text.slice(0, 160));
  ok('★ 全部错误分支都没改写配置', JSON.stringify(cfgNow()) === before);

  // ---- 8. 预设渠道 -> 自定义渠道（带 baseUrl）----
  const e8 = await patch('edit-preset', { preset: '', protocol: 'openai', baseUrl: 'http://127.0.0.1:9101' });
  ok('预设渠道可改成自定义', e8.status === 200, e8.text.slice(0, 160));
  const p8 = chan('edit-preset');
  ok('preset 字段已移除', p8?.preset === undefined, String(p8?.preset));
  ok('baseUrl 已落盘', p8?.baseUrl === 'http://127.0.0.1:9101', String(p8?.baseUrl));
  ok('protocol 已落盘', p8?.protocol === 'openai', String(p8?.protocol));
  ok('★ 原有 apiKey 保留', p8?.apiKey === 'preset-key', String(p8?.apiKey));

  // ---- 9. 之前因缺 key 被显式停用：补上 key 后自动重新启用 ----
  ok('edit-disabled 初始为停用', chan('edit-disabled')?.enabled === false);
  await patch('edit-disabled', { apiKey: 'fresh-key' });
  const d9 = chan('edit-disabled');
  ok('补上 key 已落盘', d9?.apiKey === 'fresh-key', String(d9?.apiKey));
  ok('★ 补上 key 后自动重新启用（不再写死 enabled=false）', d9?.enabled !== false, String(d9?.enabled));

  // ---- 10. ${ENV} 占位不能被写死成 enabled:false ----
  await patch('edit-env', { description: '只改备注' });
  const v10 = chan('edit-env');
  ok('未展开的 ${ENV} 占位保持原样', v10?.apiKey === '${UNSET_ENV_VAR_FOR_TEST}', String(v10?.apiKey));
  ok('★ 占位 key 不写死 enabled:false（环境变量注入后仍能自动启用）', v10?.enabled === undefined, String(v10?.enabled));

  // ---- 11. 跨站 PATCH 被拒（CSRF）且配置未被改写 ----
  // 先做同源对照组（它会成功改写配置），再取快照做跨站断言，避免把对照组的写入误判成攻击生效
  const sameOrigin = await patch('edit-a2', { description: '同源可写' });
  ok('同源 PATCH 仍可用（对照组）', sameOrigin.status === 200, sameOrigin.text.slice(0, 120));
  const beforeCsrf = JSON.stringify(cfgNow());
  const csrfRes = await call('/api/channels/edit-a2', {
    method: 'PATCH',
    headers: { origin: 'http://evil.example.com' },
    body: JSON.stringify({ description: '攻击者写入' }),
  });
  ok('★ 跨站 PATCH 被拒 403', csrfRes.status === 403, `status=${csrfRes.status} ${csrfRes.text.slice(0, 120)}`);
  ok('★ 跨站 PATCH 未改写配置', JSON.stringify(cfgNow()) === beforeCsrf);
  ok('跨站写入的内容没有落盘', chan('edit-a2')?.description === '同源可写', String(chan('edit-a2')?.description));

  // ---- 12. 编辑后立即热重载：模型池反映新绑定 ----
  await patch('edit-a2', { model: 'brand-new-model' });
  st = await call('/api/status');
  const chA2 = st.json?.channels?.find((c) => c.name === 'edit-a2');
  ok('★ 改模型后热重载：/api/status 反映新模型',
    chA2?.singleModel === true && chA2?.models?.includes('brand-new-model'), JSON.stringify(chA2?.models));

  // ---- 13. model 与 models 的优先级（README：同时出现以 model 为准）----
  await patch('edit-a2', { model: 'win-model', models: ['lose-1', 'lose-2'] });
  const m13 = chan('edit-a2');
  ok('★ model 与 models 同时传时以 model 为准',
    m13?.model === 'win-model' && m13?.models === undefined,
    JSON.stringify({ model: m13?.model, models: m13?.models }));
  // 只传 models：落成白名单，单模型字段被清掉
  await patch('edit-a2', { models: ['wl-1', 'wl-2'] });
  const m13b = chan('edit-a2');
  ok('只传 models 时落成白名单、单模型字段被清掉',
    JSON.stringify(m13b?.models) === JSON.stringify(['wl-1', 'wl-2']) && m13b?.model === undefined,
    JSON.stringify({ model: m13b?.model, models: m13b?.models }));
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  gw.kill();
  // Windows/Node 竞态规避：kill 后立刻 process.exit() 会让 undici 池里指向已终止网关的
  // 死连接与 libuv 关闭流程竞态。等 500ms 让连接错误传播、池子沉降后再退出。
  await wait(500);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
