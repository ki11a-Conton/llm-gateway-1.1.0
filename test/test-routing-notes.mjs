// E2 回归：配置文件里的 `_note*` 注释键不能污染路由决策
//
// config.json / config.example.json 里用 `_noteXxx` 写中文说明（JSON 不支持注释）。
// 这些键会随 `{...DEFAULT_ROUTING, ...parsed.routing}` 进入 routing 对象，
// 也会被 `routingExplicit`（用 Object.keys 构造）收录——必须确认它们
// **不改变任何选路/重试/分层默认值**，否则运维写一行说明就会悄悄改行为。
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFileSync, rmSync } from 'node:fs';

const HERE = path.dirname(fileURLToPath(import.meta.url));

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const { ChannelManager } = await import('../lib/channels.mjs');

const baseRouting = {
  strategy: 'priority',
  tiered: true,
  preferredBaseUrls: ['sensenova.cn'],
  // 关闭随机洗牌，让候选顺序可确定地断言（默认 true 会打乱 fallback 池顺序）
  fallbackShuffle: false,
  probeIntervalMs: 0,
  discoverIntervalMs: 0,
};
const noteRouting = {
  ...baseRouting,
  _note: '这里是中文说明，JSON 不支持注释',
  _noteTiered: '两段式选路说明',
  _noteAttempts: '每渠道尝试次数说明',
};
const channels = [
  { name: 'sn-a', protocol: 'openai', baseUrl: 'https://token.sensenova.cn/v1', apiKey: 'k', model: 'm', priority: 10 },
  { name: 'sn-b', protocol: 'openai', baseUrl: 'https://token.sensenova.cn/v1', apiKey: 'k', model: 'm', priority: 20 },
  { name: 'fb-a', protocol: 'openai', baseUrl: 'http://127.0.0.1:9101', apiKey: 'k', model: 'm', priority: 30 },
  { name: 'fb-b', protocol: 'openai', baseUrl: 'http://127.0.0.1:9102', apiKey: 'k', model: 'm', priority: 40 },
];

const tmpBase = path.join(HERE, 'routing-notes-base.tmp.json');
const tmpNote = path.join(HERE, 'routing-notes-note.tmp.json');
const writeCfg = (file, routing) => writeFileSync(file, JSON.stringify({
  server: { host: '127.0.0.1', port: 1, apiKey: 'k' },
  routing,
  channels,
}), 'utf8');
writeCfg(tmpBase, baseRouting);
writeCfg(tmpNote, noteRouting);

const mBase = new ChannelManager(tmpBase).load();
const mNote = new ChannelManager(tmpNote).load();

// ① _note* 确实进了 routingExplicit（既有行为），但没有把真实调优键带进来
const noteKeys = [...mNote.routingExplicit].filter((k) => k.startsWith('_')).sort();
ok('E2 `_note*` 键进入 routingExplicit（无害）',
  noteKeys.join(',') === '_note,_noteAttempts,_noteTiered', JSON.stringify([...mNote.routingExplicit]));
ok('E2 routingExplicit 里除说明键外与不带说明的配置一致',
  JSON.stringify([...mNote.routingExplicit].filter((k) => !k.startsWith('_')).sort())
    === JSON.stringify([...mBase.routingExplicit].sort()),
  JSON.stringify([...mNote.routingExplicit]));
ok('E2 routingExplicit 不含 attemptsPerChannel（分层默认值仍生效）',
  mNote.routingExplicit.has('attemptsPerChannel') === false);
ok('E2 routingExplicit 不含 retryPerAttemptMs', mNote.routingExplicit.has('retryPerAttemptMs') === false);

// ② 分层默认值没被说明键改动
ok('E2 attemptsPerChannel 仍为默认 2', mNote.routing.attemptsPerChannel === 2, String(mNote.routing.attemptsPerChannel));
ok('E2 fallbackAttempts 仍为默认 3', mNote.routing.fallbackAttempts === 3, String(mNote.routing.fallbackAttempts));
ok('E2 tiered 仍为 true', mNote.routing.tiered === true);
ok('E2 forceMaxEffort 未被污染（默认强制最高强度）', mNote.routing.forceMaxEffort !== false);

// ③ 去掉下划线键后，routing 与"没有说明键"的配置逐字段一致
const stripNotes = (obj) => Object.fromEntries(Object.entries(obj).filter(([k]) => !k.startsWith('_')));
ok('E2 除说明键外 routing 完全一致',
  JSON.stringify(stripNotes(mNote.routing)) === JSON.stringify(stripNotes(mBase.routing)),
  `\n note=${JSON.stringify(stripNotes(mNote.routing))}\n base=${JSON.stringify(stripNotes(mBase.routing))}`);

// ④ 选路结果完全一致（分层、优先级、tier 判定都不受影响）
const namesOf = (mgr) => mgr.candidatesFor('m').map((c) => c.name).join(',');
ok('E2 候选顺序完全一致', namesOf(mNote) === namesOf(mBase), `note=${namesOf(mNote)} base=${namesOf(mBase)}`);
ok('E2 优先池仍排在随机池之前', namesOf(mNote) === 'sn-a,sn-b,fb-a,fb-b', namesOf(mNote));
ok('E2 tier 判定不受影响',
  mNote.channels.find((c) => c.name === 'sn-a')?.tier === 'preferred'
    && mNote.channels.find((c) => c.name === 'fb-a')?.tier === 'fallback');

// ⑤ fallback 池的排序函数也读不到说明键（sticky 关闭时按 priority）
const fbOrder = mNote.candidatesFor('m').filter((c) => c.tier === 'fallback').map((c) => c.name).join(',');
ok('E2 fallback 池仍按 priority 排（未被说明键打乱）', fbOrder === 'fb-a,fb-b', fbOrder);

rmSync(tmpBase, { force: true });
rmSync(tmpNote, { force: true });

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
