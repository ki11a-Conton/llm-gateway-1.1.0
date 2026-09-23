// P1 验收：优先级加权轮询（strategy: "weighted"）+ 半开试探 + least-loaded 退化 + 每请求一次游标
//
// 纯单元测试：只 import lib/channels.mjs，**不 spawn 网关/mock、不监听任何端口**。
// 覆盖 OPTIMIZATION-PLAN.md P1「验收」的 7 条断言（①-⑦），并补几条回归锁：
//   - weighted 下冷却渠道垫后但仍留在候选里（不能因为加权把"半开试探"弄丢）
//   - routing.priorityWeights 覆盖默认分档
//   - 第二段（fallback 池）同样按权重分配
//
// 权重默认分档：priority <=10 -> 8，11-30 -> 4，31-60 -> 2，其余 -> 1（合计 15）。
// 因此 4 家 p1/p20/p50/p100 渠道的期望份额是 8/15、4/15、2/15、1/15 = 53.3/26.7/13.3/6.7(%)。
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFileSync, rmSync } from 'node:fs';
import { ChannelManager } from '../lib/channels.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CFG = path.join(HERE, 'routing-weighted.tmp.json');

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (n, total) => `${((n / total) * 100).toFixed(1)}%`;

// 4 家渠道，优先级 1 / 20 / 50 / 100 -> 默认权重 8 / 4 / 2 / 1
const CH = (name, priority) => ({
  name, protocol: 'openai', baseUrl: 'https://weighted-test.invalid/v1',
  apiKey: 'k', model: 'w-model', priority,
});
const FOUR = [CH('w1', 1), CH('w20', 20), CH('w50', 50), CH('w100', 100)];
const EXPECT = { w1: 8 / 15, w20: 4 / 15, w50: 2 / 15, w100: 1 / 15 };

// 测试基线：不分层（整池一起排序，直接观察加权顺序）、关粘性与随机洗牌、不启定时器
const BASE = {
  tiered: false,
  sticky: false,
  sessionAffinity: false,
  fallbackShuffle: false,
  probeIntervalMs: 0,
  discoverIntervalMs: 0,
  maxAttempts: 8,
};

function mgrWith(routing, channels = FOUR) {
  writeFileSync(CFG, JSON.stringify({
    server: { host: '127.0.0.1', port: 1, apiKey: 'k' },
    routing: { ...BASE, ...routing },
    channels,
  }), 'utf8');
  return new ChannelManager(CFG).load();
}

/** 跑 N 次选路，统计首选渠道的出现次数 */
function firstCounts(mgr, model, n) {
  const counts = {};
  for (let i = 0; i < n; i += 1) {
    const name = mgr.candidatesFor(model)[0]?.name;
    counts[name] = (counts[name] || 0) + 1;
  }
  return counts;
}

try {
  // ---------- ① 权重分布 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted' });
    const N = 1500;
    const counts = firstCounts(mgr, 'w-model', N);
    ok('① 首选渠道全部落在池内（无空选路）',
      Object.keys(counts).length === 4 && !counts.undefined,
      JSON.stringify(counts));
    for (const [name, share] of Object.entries(EXPECT)) {
      const got = (counts[name] || 0) / N;
      ok(`① 权重份额 ${name} ≈ ${(share * 100).toFixed(1)}%（±4pp，1500 次）`,
        Math.abs(got - share) <= 0.04,
        `got=${pct(counts[name] || 0, N)} ${JSON.stringify(counts)}`);
    }
  }

  // ---------- ② 交错而非突发 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted' });
    const seq = [];
    for (let i = 0; i < 12; i += 1) seq.push(mgr.candidatesFor('w-model')[0]?.name);
    let maxRun = 1;
    let run = 1;
    for (let i = 1; i < seq.length; i += 1) {
      run = seq[i] === seq[i - 1] ? run + 1 : 1;
      if (run > maxRun) maxRun = run;
    }
    ok('② 前 12 次首选里同一家连续出现不超过 2 次（平滑加权，不是分桶轮询）',
      maxRun <= 2, `seq=${seq.join(',')} maxRun=${maxRun}`);
    ok('② 前 12 次至少覆盖 3 家（低优先渠道也真的参与）',
      new Set(seq).size >= 3, `seq=${seq.join(',')}`);
  }

  // ---------- ③ 半开只放 1 个 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted' });
    const ch = mgr.channels.find((c) => c.name === 'w1');
    ch.cool(40);                         // 冷却 40ms，同时记下半开窗口
    await wait(60);                      // 等冷却到期 -> 进入半开
    ok('③ 冷却到期进入半开窗口（halfOpen=true，probeUntil 跨过 openUntil）',
      ch.coolingDown === false && ch.halfOpen === true,
      `coolingDown=${ch.coolingDown} halfOpen=${ch.halfOpen} probeUntil-openUntil=${ch.probeUntil - ch.openUntil}`);

    // 用既有的 inFlightOf（不新造计数器）模拟"该渠道已有 5 个在途请求"
    mgr.inFlightOf = (name) => (name === 'w1' ? 5 : 0);
    let picked = 0;
    for (let i = 0; i < 5; i += 1) {
      if (mgr.candidatesFor('w-model')[0]?.name === 'w1') picked += 1;
    }
    ok('③ 半开 + 5 个在途：w1 被选中次数 <= 1（试探名额只放 halfOpenMaxInFlight=1 个）',
      picked <= 1, `picked=${picked}`);

    // 试探成功 -> 立刻清半开态、回到满份额
    ch.markSuccess(20);
    ok('③ markSuccess 立刻清掉半开态（probeUntil 归零）',
      ch.halfOpen === false && ch.probeUntil === 0,
      `halfOpen=${ch.halfOpen} probeUntil=${ch.probeUntil}`);
    delete mgr.inFlightOf;               // 恢复真实在途计数（限流未启用 -> 恒为 0）
    const M = 600;
    const counts = firstCounts(mgr, 'w-model', M);
    const share = (counts.w1 || 0) / M;
    ok('③ 试探成功后 w1 恢复满份额（≈53%，±8pp）',
      Math.abs(share - EXPECT.w1) <= 0.08,
      `share=${pct(counts.w1 || 0, M)} ${JSON.stringify(counts)}`);
  }

  // ---------- ④ 半开不剔除 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted' });
    const ch = mgr.channels.find((c) => c.name === 'w1');
    ch.cool(40);
    await wait(60);
    mgr.inFlightOf = (name) => (name === 'w1' ? 5 : 0);
    let listed = 0;
    let order = [];
    for (let i = 0; i < 5; i += 1) {
      order = mgr.candidatesFor('w-model').map((c) => c.name);
      if (order.includes('w1')) listed += 1;
    }
    ok('④ 半开 + 名额放满：w1 仍留在候选列表里（不剔除）', listed === 5, `listed=${listed} order=${order.join('>')}`);
    ok('④ 半开名额放满的 w1 被排到所有健康渠道之后（只是跳过，不是踢掉）',
      order.indexOf('w1') > order.indexOf('w100'), order.join('>'));

    // 同一条约束在默认策略（priority）下也成立：既有测试断言过"冷却渠道仍留在候选里"
    const mgr2 = mgrWith({ strategy: 'priority' });
    const ch2 = mgr2.channels.find((c) => c.name === 'w1');
    ch2.cool(40);
    await wait(60);
    mgr2.inFlightOf = (name) => (name === 'w1' ? 1 : 0);
    const order2 = mgr2.candidatesFor('w-model').map((c) => c.name);
    ok('④ priority 策略下半开放满同样不剔除、且不排首位',
      order2.includes('w1') && order2[0] !== 'w1', order2.join('>'));
  }

  // ---------- ⑤ least-loaded 退化 ----------
  {
    const inflight = { w1: 3, w20: 0, w50: 5, w100: 1 };
    const done = mgrWith({ strategy: 'least-loaded', maxConcurrentPerChannel: 0 });
    done.inFlightOf = (name) => inflight[name] ?? 0;
    const orderDone = done.candidatesFor('w-model').map((c) => c.name);
    ok('⑤ maxConcurrentPerChannel=0：least-loaded 退化为 priority 排序（忽略在途数）',
      orderDone.join(',') === 'w1,w20,w50,w100', orderDone.join(','));
    ok('⑤ 退化只警告一次（leastLoadedDegradeWarned 已置位）',
      done.leastLoadedDegradeWarned === true);
    delete done.inFlightOf;

    const live = mgrWith({ strategy: 'least-loaded', maxConcurrentPerChannel: 4 });
    live.inFlightOf = (name) => inflight[name] ?? 0;
    const orderLive = live.candidatesFor('w-model').map((c) => c.name);
    ok('⑤ 开了渠道级限流时不退化：按在途数排（在途 0 的 w20 在首位）',
      orderLive[0] === 'w20', orderLive.join(','));
    ok('⑤ 开了渠道级限流时无退化警告', live.leastLoadedDegradeWarned === false);
    delete live.inFlightOf;
  }

  // ---------- ⑥ 游标每请求一次 ----------
  {
    const mgr = mgrWith({ strategy: 'round-robin' });
    const first = (rid) => mgr.candidatesFor('w-model', rid ? { requestId: rid } : {})[0]?.name;
    const a1 = first('req-1');
    const a2 = first('req-1');
    const a3 = first('req-1');
    ok('⑥ 同一 requestId 连续 3 次选路，首选渠道相同（游标只前进一格）',
      a1 === a2 && a2 === a3, `${a1},${a2},${a3}`);
    const b1 = first('req-2');
    ok('⑥ 换一个 requestId -> 前进一档', b1 !== a1, `req-1=${a1} req-2=${b1}`);
    const noId = [first(), first(), first(), first()];
    ok('⑥ 不传 requestId 时保持旧行为（连续 4 次覆盖全部 4 家）',
      new Set(noId).size === 4, noId.join(','));
  }

  // ---------- ⑦ 默认不漂移 ----------
  {
    const explicit = mgrWith({ strategy: 'priority' });
    const none = mgrWith({});                       // 完全不配 strategy
    ok('⑦ 不配 strategy 时默认仍是 priority', none.routing.strategy === 'priority', String(none.routing.strategy));
    const seqExplicit = [];
    const seqNone = [];
    for (let i = 0; i < 6; i += 1) {
      seqExplicit.push(explicit.candidatesFor('w-model').map((c) => c.name).join('>'));
      seqNone.push(none.candidatesFor('w-model').map((c) => c.name).join('>'));
    }
    ok('⑦ 默认行为与显式 priority 逐位一致（6 次选路）',
      seqExplicit.join('|') === seqNone.join('|'), `default=${seqNone.join('|')} priority=${seqExplicit.join('|')}`);
    ok('⑦ 默认顺序仍是严格优先级', seqNone.every((s) => s === 'w1>w20>w50>w100'), seqNone.join('|'));
    ok('⑦ 新配置项都有默认值且不改变默认行为（priorityWeights=null / halfOpenMs=15000 / 试探 1 个）',
      none.routing.priorityWeights === null
      && none.routing.halfOpenMs === 15000
      && none.routing.halfOpenMaxInFlight === 1,
      JSON.stringify({ w: none.routing.priorityWeights, ms: none.routing.halfOpenMs, n: none.routing.halfOpenMaxInFlight }));
    ok('⑦ 默认策略下不产生任何加权累计状态', none.weightState.size === 0, `size=${none.weightState.size}`);
  }

  // ---------- 回归锁：加权不会弄丢"冷却垫后但仍可试探" ----------
  {
    const mgr = mgrWith({ strategy: 'weighted' });
    const ch = mgr.channels.find((c) => c.name === 'w1');
    ch.cool(60000);
    const order = mgr.candidatesFor('w-model').map((c) => c.name);
    ok('加权：冷却中的 w1 被垫到所有健康渠道之后',
      order.indexOf('w1') > order.indexOf('w100'), order.join('>'));
    ok('加权：冷却中的 w1 仍留在候选列表里（保留半开试探）',
      order.includes('w1'), order.join('>'));
  }

  // ---------- 回归锁：priorityWeights 覆盖默认分档 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted', priorityWeights: { 10: 3, 100: 1 } });
    // p1 -> 3；p20/p50/p100 都 <= 100 -> 1，合计 6 -> w1 期望 50%
    const N = 600;
    const counts = firstCounts(mgr, 'w-model', N);
    ok('加权：priorityWeights={10:3,100:1} 生效（w1 ≈ 50%）',
      Math.abs((counts.w1 || 0) / N - 0.5) <= 0.06,
      `${pct(counts.w1 || 0, N)} ${JSON.stringify(counts)}`);
    ok('加权：覆盖后其余三家各 ≈ 16.7%',
      ['w20', 'w50', 'w100'].every((n) => Math.abs((counts[n] || 0) / N - 1 / 6) <= 0.06),
      JSON.stringify(counts));
  }

  // ---------- 回归锁：第二段（fallback 池）同样按权重分配 ----------
  {
    const mgr = mgrWith({ strategy: 'weighted', tiered: true });   // 域名不匹配 preferredBaseUrls -> 全在 fallback 池
    ok('加权：四家渠道确实落在 fallback 池（前置检查）',
      mgr.channels.every((c) => c.tier === 'fallback'),
      mgr.channels.map((c) => `${c.name}:${c.tier}`).join(' '));
    const N = 800;
    const counts = firstCounts(mgr, 'w-model', N);
    ok('加权：第二段（fallback）同样按权重分配（w1 ≈ 53%）',
      Math.abs((counts.w1 || 0) / N - EXPECT.w1) <= 0.05,
      `${pct(counts.w1 || 0, N)} ${JSON.stringify(counts)}`);
  }
} catch (err) {
  console.error('TEST ERROR', err);
  fail += 1;
} finally {
  rmSync(CFG, { force: true });
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);
