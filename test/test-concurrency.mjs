// 并发闸门单元测试：验证 limit 生效、FIFO、超时、release 幂等、两级限流
import { Semaphore, GatewayLimiter, isOverloadError } from '../lib/concurrency.mjs';

let pass = 0;
let fail = 0;
const ok = (name, cond, extra = '') => {
  if (cond) { pass += 1; console.log(`  \x1b[32mPASS\x1b[0m ${name}`); }
  else { fail += 1; console.log(`  \x1b[31mFAIL\x1b[0m ${name} ${extra}`); }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const main = async () => {
  console.log('\n== Semaphore 基础 ==');

  // 1) 并发上限生效：只有前 limit 个立即获得，其余排队，释放时逐格移交
  {
    const sem = new Semaphore(3, 't1');
    const held = [];
    for (let i = 0; i < 3; i += 1) held.push(await sem.acquire(5000));
    ok('前 3 个立即获得', sem.active === 3, `active=${sem.active}`);

    let acquiredTotal = 0;
    const bucket = [];
    const pendingPs = Array.from({ length: 7 }, () =>
      sem.acquire(5000).then((r) => { acquiredTotal += 1; bucket.push(r); return r; }));
    await sleep(10);
    ok('后 7 个进入排队', sem.pending === 7, `pending=${sem.pending}`);
    ok('active 不超过 limit=3', sem.active === 3, `active=${sem.active}`);

    held.forEach((r) => r());
    await sleep(10);
    ok('释放 3 个后 3 个等待者接管槽位', acquiredTotal === 3, `total=${acquiredTotal}`);
    ok('仍有 4 个在排队', sem.pending === 4, `pending=${sem.pending}`);

    // 逐个释放已获得的，让队列继续推进直到排空
    let guard = 0;
    while (sem.pending > 0 && guard < 100) {
      guard += 1;
      const r = bucket.shift();
      if (r) r();
      await sleep(2);
    }
    ok('7 个等待者全部最终获得', acquiredTotal === 7, `total=${acquiredTotal}`);
    ok('队列已排空', sem.pending === 0, `pending=${sem.pending}`);

    bucket.forEach((r) => r());
    await sleep(5);
    ok('全部释放后 active=0', sem.active === 0, `active=${sem.active}`);
  }

  // 2) active 随持有者释放逐步回落
  {
    const sem = new Semaphore(2, 't2');
    const r1 = await sem.acquire();
    const r2 = await sem.acquire();
    ok('占满 2 个槽位', sem.active === 2);
    let acquired = false;
    const p3 = sem.acquire().then((r) => { acquired = true; return r; });
    await sleep(20);
    ok('第三个被排队（未立即获得）', !acquired && sem.pending === 1);
    r1();
    const r3 = await p3;
    ok('释放一个后队首立刻获得', acquired && sem.active === 2);
    r2(); r3();
    ok('最终 active=0', sem.active === 0);
  }

  // 3) FIFO 顺序：每个等待者拿到后立刻释放，链条依次推进
  {
    const sem = new Semaphore(1, 't3');
    const r0 = await sem.acquire();
    const order = [];
    const waiters = [1, 2, 3].map((i) =>
      sem.acquire().then((r) => { order.push(i); r(); }),
    );
    r0();
    await Promise.all(waiters);
    ok('FIFO 顺序 1,2,3', order.join(',') === '1,2,3', order.join(','));
    ok('全部释放后 active=0', sem.active === 0);
  }

  // 4) 排队超时
  {
    const sem = new Semaphore(1, 't4');
    const r = await sem.acquire();
    let err = null;
    try { await sem.acquire(80); } catch (e) { err = e; }
    ok('排队超时被拒绝', err !== null && err.code === 'CONCURRENCY_TIMEOUT', err?.message);
    ok('isOverloadError 识别超时', isOverloadError(err));
    ok('超时后队列已清空', sem.pending === 0, `pending=${sem.pending}`);
    r();
    ok('超时不影响后续正常申请', (await (async () => { const x = await sem.acquire(); x(); return true; })()));
  }

  // 5) release 幂等
  {
    const sem = new Semaphore(2, 't5');
    const r = await sem.acquire();
    r(); r(); r();
    ok('重复 release 不会让 active 变负', sem.active === 0, `active=${sem.active}`);
    const r2 = await sem.acquire();
    ok('重复 release 后仍可正常申请', sem.active === 1);
    r2();
  }

  // 6) 统计字段
  {
    const sem = new Semaphore(2, 't6');
    const a = await sem.acquire();
    const b = await sem.acquire();
    let timedOut = false;
    const p = sem.acquire(40).catch((e) => {
      timedOut = e.code === 'CONCURRENCY_TIMEOUT';
      return null;
    });
    const stMid = sem.stats();
    ok(
      'stats 报告正确 limit/active/pending',
      stMid.limit === 2 && stMid.active === 2 && stMid.pending === 1,
      JSON.stringify(stMid),
    );
    await p;
    const st = sem.stats();
    ok('排队超时被统计', st.timedOut === 1 && timedOut, JSON.stringify(st));
    a(); b();
    ok('释放后 active=0', sem.active === 0, `active=${sem.active}`);
  }

  console.log('\n== GatewayLimiter 两级限流 ==');

  // 7) 全局上限生效
  {
    const lim = new GatewayLimiter({ maxConcurrent: 2, maxConcurrentPerChannel: 0, queueTimeoutMs: 3000 });
    const r1 = await lim.acquire('a');
    const r2 = await lim.acquire('a');
    ok('全局 limit=2 时两个渠道名额被占满', lim.global.active === 2);
    let got = false;
    const p = lim.acquire('b').then((r) => { got = true; return r; });
    await sleep(20);
    ok('第三个请求被全局排队', !got);
    r1();
    const r3 = await p;
    ok('释放后第三个获得', got);
    r2(); r3();
    ok('全释放后 active=0', lim.global.active === 0);
  }

  // 8) 渠道级上限生效（全局不限）
  {
    const lim = new GatewayLimiter({ maxConcurrent: 0, maxConcurrentPerChannel: 2, queueTimeoutMs: 3000 });
    const r1 = await lim.acquire('ch1');
    const r2 = await lim.acquire('ch1');
    ok('渠道 ch1 占满 2 个', lim.channels.get('ch1').active === 2);
    let gotA = false; let gotB = false;
    const pA = lim.acquire('ch1').then((r) => { gotA = true; return r; });
    const pB = lim.acquire('ch2').then((r) => { gotB = true; return r; });
    await sleep(20);
    ok('同渠道第三个被排队', !gotA);
    ok('另一渠道不受影响', gotB);
    r1();
    const rA = await pA;
    ok('释放后同渠道第三个获得', gotA);
    r2(); rA(); (await pB)();
    ok('渠道统计包含 ch1/ch2', !!lim.stats().channels.ch1 && !!lim.stats().channels.ch2);
  }

  // 9) 渠道级超时回滚全局名额
  {
    const lim = new GatewayLimiter({ maxConcurrent: 5, maxConcurrentPerChannel: 1, queueTimeoutMs: 60 });
    const r1 = await lim.acquire('x');
    ok('持有渠道 x 的 1 个槽位', lim.global.active === 1);
    let err = null;
    try { await lim.acquire('x'); } catch (e) { err = e; }
    ok('同渠道第二个排队超时', isOverloadError(err), err?.message);
    ok('超时后全局名额已回滚（未泄漏）', lim.global.active === 1, `global.active=${lim.global.active}`);
    r1();
    ok('释放后全局 active=0', lim.global.active === 0);
    ok('overloads 计数 +1', lim.stats().overloads === 1);
  }

  // 10) 高并发压力：不超限、不泄漏
  {
    const lim = new GatewayLimiter({ maxConcurrent: 8, maxConcurrentPerChannel: 4, queueTimeoutMs: 10000 });
    let inFlight = 0;
    let peak = 0;
    const tasks = Array.from({ length: 200 }, async (_, i) => {
      const rel = await lim.acquire('p' + (i % 5));
      inFlight += 1;
      if (inFlight > peak) peak = inFlight;
      await sleep(1);
      inFlight -= 1;
      rel();
    });
    await Promise.all(tasks);
    ok('200 并发压力下峰值不超全局上限 8', peak <= 8, `peak=${peak}`);
    ok('全部完成后全局 active=0', lim.global.active === 0, `active=${lim.global.active}`);
    const leak = [...lim.channels.values()].some((s) => s.active !== 0);
    ok('全部完成后渠道无泄漏', !leak);
  }

  console.log(`\n结果: ${pass} 通过, ${fail} 失败\n`);
  if (fail) process.exit(1);
};

main();
