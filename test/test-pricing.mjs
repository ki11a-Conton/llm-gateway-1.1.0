// 价格计费（lib/pricing.mjs）单元验收
//
// 对齐 docs/superpowers/specs/2026-09-18-pricing-design.md：
//   §3   接口签名（price / cost / summarize / meta / configurePricing / getPricing）
//   §3.1 七条语义（未配价 null、显式 0 价 0、6 位小数、最长前缀通配、单例、空汇总、脏价目忽略）
//
// 纯单元：不启网关、不占端口、不写盘。
import {
  Pricing, configurePricing, getPricing,
  DEFAULT_PRICES, DEFAULT_CURRENCY, DEFAULT_SYMBOL, UNPRICED_LIST_MAX,
} from '../lib/pricing.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

/** §3 价目条：单价 = 每 100 万 token */
const P = (input, output) => ({ input, output });

console.log('— 价格核心：未配价 / 0 价 / 金额数学 —');

{
  const p = new Pricing({ models: { known: P(1, 2) } });

  // §3.1-1 未配价格 -> null（不是 0）
  ok('未配价格的模型 cost() 返回 null（不是 0）',
    p.cost('unknown-model', 100, 100) === null, String(p.cost('unknown-model', 100, 100)));
  ok('price() 未配 -> null', p.price('unknown-model') === null, JSON.stringify(p.price('unknown-model')));

  // §3.1-6 显式 0 价 = "已定价为免费" -> 0
  const free = new Pricing({ models: { 'free-model': P(0, 0) } });
  ok('显式配置 0 价 -> 返回 0（不是 null）',
    free.cost('free-model', 12345, 6789) === 0, String(free.cost('free-model', 12345, 6789)));
  ok('0 价模型的 price() 不是 null（说明"配到了"）',
    free.price('free-model') !== null, JSON.stringify(free.price('free-model')));

  // 金额公式：(in × 单价in + out × 单价out) / 1e6
  ok('金额 = (输入×单价in + 输出×单价out) / 1e6（手算 2）',
    p.cost('known', 1_000_000, 500_000) === 2, String(p.cost('known', 1_000_000, 500_000)));
  ok('纯输入金额正确（1e6 × 1 / 1e6 = 1）',
    p.cost('known', 1_000_000, 0) === 1, String(p.cost('known', 1_000_000, 0)));
  ok('纯输出金额正确（1e6 × 2 / 1e6 = 2）',
    p.cost('known', 0, 1_000_000) === 2, String(p.cost('known', 0, 1_000_000)));
  ok('0 token 且已配价 -> 0（有价就是 0 元，不是"未知"）',
    p.cost('known', 0, 0) === 0, String(p.cost('known', 0, 0)));

  // §3.1-2 四舍五入到 6 位小数
  ok('极小金额保留到 6 位小数（1 token × 1 = 0.000001）',
    p.cost('known', 1, 0) === 0.000001, String(p.cost('known', 1, 0)));
  const tiny = new Pricing({ models: { tiny: P(0.4, 0) } });
  ok('小于 0.5 微元的金额舍入为 0（不产生 4e-7 脏值）',
    tiny.cost('tiny', 1, 0) === 0, String(tiny.cost('tiny', 1, 0)));
  ok('大 token 量不丢精度（1e9 输入 + 1e9 输出 = 3000）',
    p.cost('known', 1e9, 1e9) === 3000, String(p.cost('known', 1e9, 1e9)));
  ok('浮点脏值被抹平（0.1 单价 × 10 token = 精确 0.000001，不是 9.99999e-7 类脏值）',
    new Pricing({ models: { x: P(0.1, 0.2) } }).cost('x', 10, 0) === 0.000001,
    String(new Pricing({ models: { x: P(0.1, 0.2) } }).cost('x', 10, 0)));
  ok('非法 token 数按 0 计（NaN / 负数不产生 NaN 金额）',
    p.cost('known', NaN, -5) === 0, String(p.cost('known', NaN, -5)));
}

console.log('— 价格核心：匹配与通配 —');

{
  const w = new Pricing({
    models: {
      'gpt-4o': P(1, 1),
      'gpt-*': P(2, 2),
      'gpt-4*': P(3, 3),
    },
  });
  ok('通配 gpt-* 命中（gpt-9 -> 2）', w.price('gpt-9')?.input === 2, JSON.stringify(w.price('gpt-9')));
  ok('精确匹配优先于通配（gpt-4o -> 1 而不是 2/3）',
    w.price('gpt-4o')?.input === 1, JSON.stringify(w.price('gpt-4o')));
  ok('多个通配命中时取最长前缀（gpt-4.1-nano -> gpt-4* = 3 而不是 gpt-* = 2）',
    w.price('gpt-4.1-nano')?.input === 3, JSON.stringify(w.price('gpt-4.1-nano')));
  ok('通配命中内置表的精确条目时，精确条目仍优先（gpt-4.1 -> 内置 2 而不是 gpt-4* 的 3）',
    w.price('gpt-4.1')?.input === DEFAULT_PRICES['gpt-4.1'].input, JSON.stringify(w.price('gpt-4.1')));
  ok('不匹配的模型名 -> null（gptX4o 不以 gpt- 开头）',
    w.price('gptX4o') === null, JSON.stringify(w.price('gptX4o')));
  ok('模型名里的正则元字符不被当通配符（m+ / claude.3 不误匹配）',
    w.price('m+') === null && w.price('claude.3') === null);
  ok('空模型名 -> null', w.price('') === null && w.price(null) === null);
}

console.log('— 价格核心：单例装配 —');

{
  const s1 = configurePricing({ models: { 'gpt-4o': P(1, 1), x: P(1, 1) } });
  ok('configurePricing 返回 Pricing 实例', s1 instanceof Pricing);
  ok('配置覆盖内置同名价（内置 gpt-4o=2.5 被配置改成 1）',
    s1.price('gpt-4o')?.input === 1, JSON.stringify(s1.price('gpt-4o')));
  ok('未覆盖的内置价仍然可用（deepseek-chat）',
    s1.price('deepseek-chat')?.input === DEFAULT_PRICES['deepseek-chat'].input,
    JSON.stringify(s1.price('deepseek-chat')));

  const s2 = configurePricing({ currency: 'CNY', symbol: '¥', models: { y: P(5, 5) } });
  ok('可多次调用重建（热重载），返回新实例', s1 !== s2);
  ok('getPricing() 装配后返回同一实例', getPricing() === s2);
  ok('重建后旧实例语义不变（s1 里仍有 x）', s1.price('x')?.input === 1);
  ok('重建后新实例按新配置（y 生效、旧自定义 x 不在）',
    s2.price('y')?.input === 5 && s2.price('x') === null, JSON.stringify(s2.price('x')));
  ok('currency / symbol 可被配置覆盖', s2.currency === 'CNY' && s2.symbol === '¥');
  ok('默认币种与符号符合契约', DEFAULT_CURRENCY === 'USD' && DEFAULT_SYMBOL === '$');
}

console.log('— 价格核心：汇总 summarize —');

{
  const pr = new Pricing({ models: { a: P(1, 1), b: P(2, 2) } });

  const r0 = pr.summarize({});
  ok('summarize({}) 精确等于契约值',
    r0.cost === null && r0.complete === true && r0.priced === 0
    && r0.unpriced.length === 0 && r0.unpricedTokens === 0, JSON.stringify(r0));
  ok('summarize(undefined) 与空对象一致',
    JSON.stringify(pr.summarize(undefined)) === JSON.stringify(r0));

  const r1 = pr.summarize({ a: { inputTokens: 1e6, outputTokens: 0 }, b: { inputTokens: 0, outputTokens: 1e6 } });
  ok('全部配价：cost 为各模型之和（a=1 + b=2 = 3）、complete=true、priced=2',
    r1.cost === 3 && r1.complete === true && r1.priced === 2, JSON.stringify(r1));

  const r2 = pr.summarize({ a: { inputTokens: 1e6, outputTokens: 0 }, zz: { inputTokens: 5e5, outputTokens: 0 } });
  ok('部分未配价：cost 只含已配价部分（1）',
    r2.cost === 1, JSON.stringify(r2));
  ok('部分未配价：complete=false', r2.complete === false);
  ok('部分未配价：unpriced / unpricedTokens 正确',
    r2.unpriced.length === 1 && r2.unpriced[0] === 'zz' && r2.unpricedTokens === 5e5, JSON.stringify(r2));

  const rAllUnpriced = pr.summarize({ z1: { inputTokens: 10, outputTokens: 0 } });
  ok('全部未配价：cost=null（不是 0）且 complete=false（口径不变式 4）',
    rAllUnpriced.cost === null && rAllUnpriced.complete === false, JSON.stringify(rAllUnpriced));

  // unpriced 有界 + 降序 + unpricedTokens 不受截断影响
  const many = {};
  const total = UNPRICED_LIST_MAX + 5;
  let expectTokens = 0;
  for (let i = 0; i < total; i++) {
    many[`n${i}`] = { inputTokens: i * 10, outputTokens: 0 };
    expectTokens += i * 10;
  }
  const r3 = pr.summarize(many);
  ok(`unpriced 列表不超过 UNPRICED_LIST_MAX（${UNPRICED_LIST_MAX}）`,
    r3.unpriced.length === UNPRICED_LIST_MAX, String(r3.unpriced.length));
  ok('unpriced 按 token 降序（最贵的排最前）',
    r3.unpriced[0] === `n${total - 1}`, JSON.stringify(r3.unpriced.slice(0, 3)));
  ok('unpricedTokens 统计全部未配价 token（不受列表上限截断）',
    r3.unpricedTokens === expectTokens, `${r3.unpricedTokens} vs ${expectTokens}`);

  // 汇总把 byModel 里已配价与未配价混在一起时，priced 计数正确
  const r4 = pr.summarize({ a: { inputTokens: 0, outputTokens: 0 }, u1: {}, u2: { outputTokens: 5 } });
  ok('priced 只数配到价的模型（a=1，u1/u2 未配）',
    r4.priced === 1 && r4.unpriced.length === 2, JSON.stringify(r4));
}

console.log('— 价格核心：脏价目与 meta —');

{
  const dirty = new Pricing({
    models: {
      badNaN: { input: NaN, output: 1 },
      badInf: { input: Infinity, output: 1 },
      badNeg: { input: -1, output: 1 },
      badStr: { input: 'x', output: 1 },
      badNull: null,
      badNum: 5,
      good: P(1, 1),
    },
  });
  ok('NaN 价目被忽略 -> 未配价（cost null）',
    dirty.price('badNaN') === null && dirty.cost('badNaN', 1e6, 0) === null);
  ok('Infinity 价目被忽略', dirty.price('badInf') === null);
  ok('负数价目被忽略（不允许算出负金额）', dirty.price('badNeg') === null);
  ok('字符串价目被忽略', dirty.price('badStr') === null);
  ok('null / 非对象价目被忽略', dirty.price('badNull') === null && dirty.price('badNum') === null);
  ok('同一份配置里的合法条目不受脏条目影响', dirty.price('good')?.input === 1);
  ok('脏条目不计入 meta().models 条数',
    dirty.meta().models === Object.keys(DEFAULT_PRICES).length + 1,
    `${dirty.meta().models} vs ${Object.keys(DEFAULT_PRICES).length + 1}`);

  const m = new Pricing({ currency: 'CNY', symbol: '¥', models: { c: P(1, 1) } }).meta();
  ok('meta() 形状为 {currency,symbol,models}',
    typeof m.currency === 'string' && typeof m.symbol === 'string' && Number.isInteger(m.models),
    JSON.stringify(m));
  ok('meta().models = 内置 + 自定义（1 条）',
    m.models === Object.keys(DEFAULT_PRICES).length + 1, JSON.stringify(m));
  ok('未传 models 时元信息为内置表',
    getPricing().meta().models >= Object.keys(DEFAULT_PRICES).length);
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);