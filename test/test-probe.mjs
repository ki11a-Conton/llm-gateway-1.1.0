// 探活回归测试：probe 必须按「chat 接口真实可用性」判断，不能只探 models 接口
//   （models 200 ≠ chat 能用：TPM 429、chat 500、key 只对 chat 失效等场景下，models 探活会误把死渠道提前解融，
//     导致请求反复打向打不通的渠道——即「探活解除熔断后 agent 一直调用不到」）
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../lib/channels.mjs';
import { mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const NODE = process.execPath;
// P5：本文件只起 mock（不起网关），所以只需把配置里的 mock 上游地址按端口块平移
const mp = await mockUpstreamPorts();
const CFG = materializeConfig(path.join(HERE, 'probe.test.json'), { mockBase: mp.base });

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: mp.env });
await wait(800);
try {
  const mgr = new ChannelManager(CFG);
  mgr.load();
  const get = (n) => mgr.channels.find((c) => c.name === n);
  const good = get('good');
  const chatdead = get('chatdead');
  const nomodel = get('nomodel');

  // 三个渠道都强制进入冷却，再探活
  for (const c of [good, chatdead, nomodel]) c.cool(60000);
  ok('前置：三个渠道都在冷却', good.coolingDown && chatdead.coolingDown && nomodel.coolingDown);

  await mgr.probe();

  ok('good（chat 200）被解除熔断', good.coolingDown === false, `coolingDown=${good.coolingDown}`);
  ok('chatdead（chat 500 但 models 200）保持熔断', chatdead.coolingDown === true, `coolingDown=${chatdead.coolingDown}`);
  ok('nomodel（无绑定模型，走 models 探活）被解除熔断', nomodel.coolingDown === false, `coolingDown=${nomodel.coolingDown}`);
} catch (err) {
  console.error('TEST ERROR', err);
  fail++;
} finally {
  mock.kill();
  await wait(300);
}
console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);