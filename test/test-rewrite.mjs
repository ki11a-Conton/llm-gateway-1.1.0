// 请求改写单元测试：渠道 minMaxTokens 下限 —— 上游对 max_tokens/max_completion_tokens 有下限要求时
// （如 api.b.ai 报 "max_completion_tokens must be greater than 2"），把客户端的极小值抬到渠道配置的下限
// 支持路由级全局默认（routing.minMaxTokens）+ 渠道级覆盖（cfg.minMaxTokens）
import { writeFileSync, rmSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openaiAdapter } from '../lib/adapters/openai.mjs';
import { anthropicAdapter } from '../lib/adapters/anthropic.mjs';
import { ChannelManager } from '../lib/channels.mjs';

let pass = 0;
let fail = 0;
function ok(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const chan = (minMaxTokens) => ({
  minMaxTokens,
  apiKey: 'k',
  baseUrl: 'http://127.0.0.1:9999/v1',
  headers: {},
});
const buildOpenAI = (min, body) => openaiAdapter.buildRequest({ channel: chan(min), model: 'm', body, stream: false }).payload;
const buildAnthropic = (min, body) => anthropicAdapter.buildRequest({ channel: chan(min), model: 'm', body, stream: false }).payload;

// ---- openai 适配器 ----
ok('openai: max_completion_tokens=1 抬到下限 8', buildOpenAI(8, { max_completion_tokens: 1 }).max_completion_tokens === 8);
ok('openai: max_tokens=1 抬到下限 8', buildOpenAI(8, { max_tokens: 1 }).max_tokens === 8);
ok('openai: max_tokens=2 抬到下限 8（上游要求 >2）', buildOpenAI(8, { max_tokens: 2 }).max_tokens === 8);
ok('openai: 大值不动（max_tokens=32768 保持）', buildOpenAI(8, { max_tokens: 32768 }).max_tokens === 32768);
ok('openai: 未传字段不添加', !('max_tokens' in buildOpenAI(8, {})) && !('max_completion_tokens' in buildOpenAI(8, {})));
ok('openai: 未配 minMaxTokens 不改写', buildOpenAI(undefined, { max_tokens: 1 }).max_tokens === 1);

// ---- anthropic 适配器 ----
ok('anthropic: max_tokens=1 抬到下限 8', buildAnthropic(8, { max_tokens: 1 }).max_tokens === 8);
ok('anthropic: max_completion_tokens=1 抬到下限 8', buildAnthropic(8, { max_completion_tokens: 1 }).max_tokens === 8);
ok('anthropic: 大值不动', buildAnthropic(8, { max_tokens: 32768 }).max_tokens === 32768);
ok('anthropic: 未配 minMaxTokens 不改写', buildAnthropic(undefined, { max_tokens: 1 }).max_tokens === 1);

// ---- 路由级全局默认（routing.minMaxTokens）----
{
  const tmpCfg = path.join(path.dirname(fileURLToPath(import.meta.url)), 'rewrite-tmp.test.json');
  writeFileSync(
    tmpCfg,
    JSON.stringify({
      routing: { minMaxTokens: 8, probeIntervalMs: 0, discoverIntervalMs: 0 },
      channels: [
        { name: 'inherit', protocol: 'openai', baseUrl: 'http://127.0.0.1:9101', apiKey: 'k', model: 'm' },
        { name: 'override', protocol: 'openai', baseUrl: 'http://127.0.0.1:9101', apiKey: 'k', model: 'm', minMaxTokens: 16 },
      ],
    }),
    'utf8',
  );
  const mgr = new ChannelManager(tmpCfg);
  mgr.load();
  ok('渠道未配时继承路由级默认 8', mgr.channels.find((c) => c.name === 'inherit')?.minMaxTokens === 8);
  ok('渠道配置覆盖路由级默认（16 优先）', mgr.channels.find((c) => c.name === 'override')?.minMaxTokens === 16);
  // 这个临时配置是一次性的：跑完就删，别在仓库里留生成物（每次跑测试都留一个文件属于卫生问题）
  rmSync(tmpCfg, { force: true });
}

console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
process.exit(fail ? 1 : 0);