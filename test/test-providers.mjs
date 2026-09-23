// 测试自定义 provider（preset）与模型拉取/回写
import { spawn, execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, existsSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort, mockUpstreamPorts, materializeConfig } from './lib/ports.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const NODE = process.execPath;
const PROVIDERS = path.join(ROOT, 'providers.json');
const CFG = path.join(HERE, 'preset.test.json');
const CFG_BACKUP = readFileSync(CFG, 'utf8');
// P5：端口全部运行时动态分配。mock 走 MOCK_PORT_BASE 整体平移；静态 preset.test.json 不改，
// 由 materializeConfig 复制一份（server.port → PORT，配置里的 91xx → mock 实际端口）作为 --config。
// （freePort()/mockUpstreamPorts() 的调用顺序无所谓：ports.mjs 会向上扫过保留段。）
const PORT = await freePort();
const MP = await mockUpstreamPorts();
const RUN_CFG = materializeConfig(CFG, { port: PORT, mockBase: MP.base });

let pass = 0;
let fail = 0;
const ok = (name, cond, extra = '') => {
  if (cond) { pass += 1; console.log(`  \x1b[32mPASS\x1b[0m ${name}`); }
  else { fail += 1; console.log(`  \x1b[31mFAIL\x1b[0m ${name} ${extra}`); }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 1) 写入自定义 provider 预设
writeFileSync(PROVIDERS, JSON.stringify({
  '_comment': '临时测试文件',
  'mock-preset': {
    protocol: 'openai',
    baseUrl: MP.url(9101),
    priority: 5,
    headers: { 'X-Test': '1' },
    description: '测试用自定义 provider',
  },
}, null, 2), 'utf8');

const mock = spawn(NODE, [path.join(HERE, 'mock-upstream.mjs')], { stdio: 'ignore', env: MP.env });
await sleep(700);

try {
  // 2) 自定义 preset 被识别
  //    必须显式传 --config：不传就会读 CWD 里的 config.json，干净 clone / CI 上没有这个文件，
  //    测试结果随环境变化（这也是发布包必须在无 config.json 环境下可跑的前提）。
  const list = execFileSync(NODE, [
    path.join(ROOT, 'server.mjs'),
    '--config', RUN_CFG,
    '--list-presets',
  ], { encoding: 'utf8' });
  ok('providers.json 的自定义 preset 被加载', list.includes('mock-preset') && list.includes('测试用自定义 provider'));
  ok('内置 preset 仍然可用', list.includes('deepseek') && list.includes('anthropic'));

  // 3) --refresh-models 拉取并写回 config
  const out = execFileSync(NODE, [
    path.join(ROOT, 'server.mjs'),
    '--config', RUN_CFG,
    '--refresh-models',
  ], { encoding: 'utf8' });

  ok('拉取报告显示成功', out.includes('from-preset') && /from-preset\s+✓|\✓\s+from-preset/.test(out), out.slice(-400));
  ok('提示已写回配置', /已写回 \d+ 个渠道/.test(out), out.slice(-300));

  const saved = JSON.parse(readFileSync(RUN_CFG, 'utf8'));
  const a = saved.channels.find((c) => c.name === 'from-preset');
  const b = saved.channels.find((c) => c.name === 'preset-overridden');

  ok('preset 的 baseUrl 生效（拉到 9101 的模型）',
    Array.isArray(a.models) && a.models.includes('test-model-1'), JSON.stringify(a.models));
  ok('渠道内字段覆盖 preset（拉到 9103 的模型）',
    Array.isArray(b.models) && b.models.includes('claude-test'), JSON.stringify(b.models));

  // 4) preset 里的 headers 合并到请求
  ok('preset 的 priority 生效', a.priority === undefined || a.priority === 5, String(a.priority));

  // 5) 启动服务验证 /api/discover 与 preset 生效
  const gw = spawn(NODE, [
    path.join(ROOT, 'server.mjs'), '--config', RUN_CFG, '--port', String(PORT), '--api-key', 'TESTKEY',
    '--log-level', 'warn', '--no-discover',
  ], { stdio: 'ignore' });

  let ready = false;
  for (let i = 0; i < 40 && !ready; i += 1) {
    await sleep(200);
    try { ready = (await fetch(`http://127.0.0.1:${PORT}/health`)).ok; } catch { /* retry */ }
  }
  ok('使用 preset 的网关可启动', ready);

  if (ready) {
    const st = await (await fetch(`http://127.0.0.1:${PORT}/api/status`)).json();
    const c1 = st.channels.find((c) => c.name === 'from-preset');
    ok('面板状态里带 preset 名', c1?.preset === 'mock-preset', JSON.stringify(c1?.preset));
    ok('preset 的 baseUrl 已解析', c1?.baseUrl === `${MP.url(9101)}/v1`, c1?.baseUrl);

    const dr = await (await fetch(`http://127.0.0.1:${PORT}/api/discover`, { method: 'POST' })).json();
    ok('/api/discover 返回每个渠道结果',
      Array.isArray(dr.results) && dr.results.some((r) => r.name === 'from-preset' && r.ok),
      JSON.stringify(dr.results));

    // preset 的自定义 header 是否真的发出去
    const hdr = await (await fetch(MP.url(9101, '/_last-headers'))).json();
    ok('preset 的 headers 随请求发出', hdr['x-test'] === '1', JSON.stringify(hdr).slice(0, 200));

    gw.kill('SIGTERM');
  }
} catch (err) {
  fail += 1;
  console.log(`  \x1b[31mFAIL\x1b[0m 测试异常: ${err.message}\n${err.stdout || ''}${err.stderr || ''}`);
} finally {
  mock.kill('SIGTERM');
  writeFileSync(CFG, CFG_BACKUP, 'utf8');
  if (existsSync(PROVIDERS)) unlinkSync(PROVIDERS);
  await sleep(300);
}

console.log(`\n结果: \x1b[32m${pass} 通过\x1b[0m, ${fail ? `\x1b[31m${fail} 失败\x1b[0m` : '0 失败'}\n`);
process.exit(fail ? 1 : 0);
