// channels.mjs 路由/黑名单加固回归测试
//  ① round-robin 在关闭 sticky 时必须真正轮换起点。
//     原先的比较器是 `priority - priority || rr - rr || …`，而优先池里各家 priority 互不相同，
//     于是 rr 永远轮不到 -> 40 次请求全打在同一家：这正是"流量总在几个渠道打转"的直接原因。
//  ② priority 策略下"冷却中的渠道"必须垫到最后（README「熔断中的排最后」）。
//     原先只在"同优先级内"比较 coolingDown，而各优先级只有一家，于是刚熔断的渠道下个请求
//     仍然第一个被打 —— 熔断形同虚设，日志里约 22% 的请求要先白撞一个失败渠道。
//  ③ unsupported 黑名单必须带 TTL、有上界、可清除（原先是个只能 .add() 的 Set，只能重启进程）。
//  ④ defaultModel() 不得依赖上游 /models 的返回顺序（否则池子总入口会把聊天流量发给嵌入模型）。
//  ⑤ 热重载失败不得留下"routing 已生效、渠道列表还是旧的"半应用状态。
//  ⑥ 显式指定不存在的配置路径必须报错，不能静默回落到别的文件。
//  ⑦ 余额状态必须能跨热重载存活。
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChannelManager, resolveConfigPath } from '../lib/channels.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CFG = path.join(HERE, 'routing-hardening.test.json');
const DUP_CFG = path.join(HERE, 'routing-hardening-dup.test.json');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const mgr = new ChannelManager(CFG);
mgr.load();

// ---------- ① 轮换 ----------
{
  const preferred = mgr.enabledChannels.filter((c) => c.tier === 'preferred').map((c) => `${c.name}(p${c.priority})`);
  ok('测试形状与真实配置一致：优先池多家且优先级互不相同', preferred.length === 4 && new Set(mgr.enabledChannels.filter((c) => c.tier === 'preferred').map((c) => c.priority)).size === 4, preferred.join(' '));

  const firsts = [];
  for (let i = 0; i < 8; i += 1) firsts.push(mgr.candidatesFor('shared-model')[0]?.name);
  ok('round-robin + sticky:false：连续 8 次请求覆盖全部 4 家优先渠道', new Set(firsts).size === 4, firsts.join(','));
  ok('轮换是确定性的循环（pf1→pf2→pf3→pf4）', firsts.slice(0, 4).join(',') === 'pf1,pf2,pf3,pf4', firsts.join(','));

  // 回归：开着 sticky 时必须保持原行为（既有 test-routing-policy 依赖这个语义）
  mgr.routing.sticky = true;
  const stickyFirsts = [];
  for (let i = 0; i < 8; i += 1) stickyFirsts.push(mgr.candidatesFor('shared-model')[0]?.name);
  ok('round-robin + sticky:true：保持原行为，不轮换（粘性优先）', new Set(stickyFirsts).size === 1 && stickyFirsts[0] === 'pf1', stickyFirsts.join(','));
  mgr.routing.sticky = false;
}

// ---------- ② 冷却垫底 ----------
{
  mgr.routing.strategy = 'priority';
  const pf1 = mgr.channels.find((c) => c.name === 'pf1');
  pf1.cool(60000);
  const order = mgr.candidatesFor('shared-model').map((c) => c.name);
  ok('priority：冷却中的渠道不再排在第一位', order[0] !== 'pf1', order.join('>'));
  ok('priority：冷却中的渠道被垫到所有健康优先渠道之后', order.indexOf('pf1') > order.indexOf('pf4'), order.join('>'));
  ok('priority：冷却渠道仍留在候选里（保留半开试探）', order.includes('pf1'), order.join('>'));
  ok('priority：健康渠道仍按优先级排序', order.slice(0, 3).join(',') === 'pf2,pf3,pf4', order.join('>'));
}

// ---------- ③ 黑名单 TTL / 上界 / 可清除 ----------
{
  const wild = mgr.channels.find((c) => c.name === 'wild');
  ok('自动发现渠道对未知模型乐观放行', wild.supports('some-model') === true);
  wild.markUnsupported('some-model');
  ok('拉黑后 supports() 立即返回 false', wild.supports('some-model') === false);
  ok('黑名单是可过期的 Map（不再是只能 .add 的 Set）', wild.unsupported instanceof Map && wild.isUnsupported('some-model') === true);

  wild.unsupported.set('some-model', Date.now() - 1); // 模拟 TTL 到期
  ok('过期后自动恢复可用（原先只能重启进程或改 baseUrl）', wild.supports('some-model') === true);
  ok('过期条目被顺手清理', wild.unsupported.has('some-model') === false);

  mgr.routing.unsupportedMax = 5;
  for (let i = 0; i < 30; i += 1) wild.markUnsupported(`m${i}`);
  ok('黑名单有上界（原先无界，客户端可用任意模型名把它撑爆）', wild.unsupported.size <= 5, `size=${wild.unsupported.size}`);
  ok('淘汰的是最旧条目、保留最新', wild.isUnsupported('m29') === true && wild.isUnsupported('m0') === false, JSON.stringify([...wild.unsupported.keys()]));
  ok('toJSON().unsupported 仍是数组（面板 / /api/status 兼容）', Array.isArray(wild.toJSON().unsupported));

  wild.clearUnsupported();
  ok('clearUnsupported() 可整体清除', wild.unsupported.size === 0);
}

// ---------- ④ defaultModel 确定性 ----------
{
  const wild = mgr.channels.find((c) => c.name === 'wild');
  wild.models = new Set(['text-embedding-3-small', 'whisper-1', 'gpt-4o-mini']);
  ok('自动发现渠道不选嵌入/语音模型当默认模型', wild.defaultModel() === 'gpt-4o-mini', String(wild.defaultModel()));
  wild.models = new Set(['zzz-chat', 'aaa-chat']);
  ok('自动发现渠道的默认模型稳定（字典序，不随上游顺序变）', wild.defaultModel() === 'aaa-chat', String(wild.defaultModel()));
  const pf2 = mgr.channels.find((c) => c.name === 'pf2');
  ok('显式配置 model 的渠道仍尊重运维写的那个模型', pf2.defaultModel() === 'shared-model', String(pf2.defaultModel()));
}

// ---------- ⑤ 热重载原子性 ----------
{
  const m2 = new ChannelManager(CFG);
  m2.load();
  const snapNames = m2.channels.map((c) => c.name).join(',');
  m2.configPath = DUP_CFG;
  let threw = false;
  try { m2.load(); } catch { threw = true; }
  ok('重名配置会让热重载抛错', threw === true);
  ok('失败后 strategy 未被半应用', m2.routing.strategy === 'round-robin', String(m2.routing.strategy));
  ok('失败后 maxConcurrent 未被半应用', m2.routing.maxConcurrent === 128, String(m2.routing.maxConcurrent));
  ok('失败后渠道列表保持原样', m2.channels.map((c) => c.name).join(',') === snapNames, m2.channels.map((c) => c.name).join(','));
  ok('失败后 config 对象未被替换', m2.config?.unifiedModel === 'auto', String(m2.config?.unifiedModel));
}

// ---------- ⑥ 配置路径 ----------
{
  let rpThrew = false;
  try { resolveConfigPath(path.join(HERE, 'definitely-missing-config.json')); } catch { rpThrew = true; }
  ok('显式指定不存在的配置路径必须报错（原先静默回落到 config.json）', rpThrew === true);
  ok('未显式指定时仍返回可用路径', typeof resolveConfigPath(undefined) === 'string');
  ok('存在且合法的路径原样返回', resolveConfigPath(CFG) === CFG);
}

// ---------- ⑦ 余额状态 ----------
{
  const m3 = new ChannelManager(CFG);
  m3.load();
  const ch = m3.channels.find((c) => c.name === 'pf1');
  ch.creditRemain = 123.45;
  ch.balanceUpdatedAt = 1700000000000;
  m3.load();
  const after = m3.channels.find((c) => c.name === 'pf1');
  ok('余额跨热重载存活（原先写回配置即被抹掉）', after.creditRemain === 123.45, String(after.creditRemain));
  ok('余额刷新时间跨热重载存活', after.balanceUpdatedAt === 1700000000000, String(after.balanceUpdatedAt));
  ok('死配置 tierMode 已从默认值移除', m3.routing.tierMode === undefined, String(m3.routing.tierMode));
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
