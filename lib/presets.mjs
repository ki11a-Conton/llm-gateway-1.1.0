// 供应商预设：内置一批常见厂商，也支持用 providers.json 自定义/覆盖
// 渠道配置里写 {"preset":"deepseek","apiKey":"..."} 即可， preset 里的任何字段都能被同名配置覆盖

import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { log } from './logger.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export const PRESETS = {
  // ---------- 官方 ----------
  openai: {
    protocol: 'openai',
    baseUrl: 'https://api.openai.com/v1',
    priority: 20,
    description: 'OpenAI 官方',
  },
  anthropic: {
    protocol: 'anthropic',
    baseUrl: 'https://api.anthropic.com/v1',
    priority: 10,
    maxTokens: 8192,
    description: 'Anthropic 官方（原生协议）',
  },
  gemini: {
    protocol: 'openai',
    baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai',
    priority: 20,
    description: 'Google Gemini（OpenAI 兼容端点）',
  },
  groq: {
    protocol: 'openai',
    baseUrl: 'https://api.groq.com/openai/v1',
    priority: 30,
    description: 'Groq（推理极快）',
  },
  mistral: {
    protocol: 'openai',
    baseUrl: 'https://api.mistral.ai/v1',
    priority: 30,
    description: 'Mistral AI',
  },
  xai: {
    protocol: 'openai',
    baseUrl: 'https://api.x.ai/v1',
    priority: 30,
    description: 'xAI Grok',
  },
  together: {
    protocol: 'openai',
    baseUrl: 'https://api.together.xyz/v1',
    priority: 35,
    description: 'Together AI',
  },
  fireworks: {
    protocol: 'openai',
    baseUrl: 'https://api.fireworks.ai/inference/v1',
    priority: 35,
    description: 'Fireworks AI',
  },
  openrouter: {
    protocol: 'openai',
    baseUrl: 'https://openrouter.ai/api/v1',
    priority: 30,
    headers: { 'HTTP-Referer': 'http://localhost:8787', 'X-Title': 'local-gateway' },
    description: 'OpenRouter（几乎所有模型的兜底）',
  },

  // ---------- 国内厂商 ----------
  deepseek: {
    protocol: 'openai',
    baseUrl: 'https://api.deepseek.com/v1',
    priority: 10,
    description: 'DeepSeek 官方',
  },
  zhipu: {
    protocol: 'openai',
    baseUrl: 'https://open.bigmodel.cn/api/paas/v4',
    priority: 40,
    description: '智谱 GLM',
  },
  moonshot: {
    protocol: 'openai',
    baseUrl: 'https://api.moonshot.cn/v1',
    priority: 40,
    description: 'Moonshot / Kimi',
  },
  siliconflow: {
    protocol: 'openai',
    baseUrl: 'https://api.siliconflow.cn/v1',
    priority: 40,
    description: '硅基流动',
  },
  dashscope: {
    protocol: 'openai',
    baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    priority: 50,
    description: '阿里云百炼',
  },
  volcengine: {
    protocol: 'openai',
    baseUrl: 'https://ark.cn-beijing.volces.com/api/v3',
    priority: 50,
    description: '火山方舟（模型名填接入点 ID，如 ep-2024xxxx-xxxxx）',
  },
  baichuan: {
    protocol: 'openai',
    baseUrl: 'https://api.baichuan-ai.com/v1',
    priority: 50,
    description: '百川智能',
  },
  minimax: {
    protocol: 'openai',
    baseUrl: 'https://api.minimaxi.com/v1',
    priority: 50,
    description: 'MiniMax',
  },
  stepfun: {
    protocol: 'openai',
    baseUrl: 'https://api.stepfun.com/v1',
    priority: 50,
    description: '阶跃星辰',
  },
  lingyi: {
    protocol: 'openai',
    baseUrl: 'https://api.lingyiwanwu.com/v1',
    priority: 50,
    description: '零一万物',
  },
  tencent: {
    protocol: 'openai',
    baseUrl: 'https://api.hunyuan.cloud.tencent.com/v1',
    priority: 50,
    description: '腾讯混元',
  },

  // ---------- 腾讯 WorkBuddy（国际版）----------
  // access_token 授权，消耗账号额度；上游只认流式（forceStream），出站 body 需风控脱敏
  // （workbuddySanitize，11128 黑名单）；出站默认走本机代理（客户端实测 127.0.0.1:7897），
  // 账号渠道可用 proxy 字段覆盖或清空走直连。接入点/风控细节见 docs/workbuddy-intl-task.md。
  'workbuddy-intl': {
    protocol: 'openai',
    // baseUrl 带版本段 /v2：normalizeBaseUrl 对无版本段 baseUrl 会补 /v1（OpenAI 约定），
    // 这里直接写全版本段 + chatPath '/chat/completions' → 最终 https://www.workbuddy.ai/v2/chat/completions
    baseUrl: 'https://www.workbuddy.ai/v2',
    chatPath: '/chat/completions',
    priority: 60,
    forceStream: true,
    workbuddySanitize: true,
    proxy: 'http://127.0.0.1:7897',
    headers: {
      Accept: 'application/json, text/plain, */*',
      'X-Requested-With': 'XMLHttpRequest',
      'User-Agent': 'CLI/2.63.2 CodeBuddy/2.63.2',
      'X-Product': 'SaaS',
      'X-Domain': 'www.workbuddy.ai',
    },
    description: 'CodeBuddy / WorkBuddy（国际版）：access_token 授权，消耗账号额度',
  },

  // ---------- 本地 / 自建 ----------
  ollama: {
    protocol: 'openai',
    baseUrl: 'http://127.0.0.1:11434/v1',
    apiKey: 'ollama',
    priority: 90,
    description: '本机 Ollama',
  },
  'lm-studio': {
    protocol: 'openai',
    baseUrl: 'http://127.0.0.1:1234/v1',
    apiKey: 'lm-studio',
    priority: 90,
    description: '本机 LM Studio',
  },
  vllm: {
    protocol: 'openai',
    baseUrl: 'http://127.0.0.1:8000/v1',
    apiKey: 'EMPTY',
    priority: 90,
    description: '本机 vLLM',
  },
  'llama-cpp': {
    protocol: 'openai',
    baseUrl: 'http://127.0.0.1:8080/v1',
    apiKey: 'no-key',
    priority: 90,
    description: '本机 llama.cpp server',
  },
  xinference: {
    protocol: 'openai',
    baseUrl: 'http://127.0.0.1:9997/v1',
    apiKey: 'none',
    priority: 90,
    description: '本机 Xinference',
  },
};

/** 读取自定义 provider 定义（默认项目根的 providers.json，可用 GW_PROVIDERS 指定别处），忽略以 _ 开头的说明性键 */
export function loadUserPresets() {
  const file = process.env.GW_PROVIDERS || path.join(ROOT, 'providers.json');
  if (!existsSync(file)) return null;
  try {
    const json = JSON.parse(readFileSync(file, 'utf8'));
    const out = {};
    for (const [k, v] of Object.entries(json)) {
      if (k.startsWith('_') || !v || typeof v !== 'object') continue;
      out[k] = v;
    }
    if (Object.keys(out).length) log.info(`自定义 provider 已加载 ${Object.keys(out).length} 个 (providers.json)`);
    return out;
  } catch (err) {
    log.warn(`providers.json 解析失败，已忽略: ${err.message}`);
    return null;
  }
}

/** 合并用户自定义预设（providers.json）后的完整预设表 */
export function resolvePresets(userPresets) {
  return userPresets ? { ...PRESETS, ...userPresets } : PRESETS;
}

/**
 * 把渠道配置与预设合并：预设提供默认值，渠道配置里的同名字段优先
 * 注：headers 做浅合并，preset 里的头不会被整个覆盖掉
 */
export function applyPreset(cfg, presets) {
  if (!cfg.preset) return { ...cfg };
  const preset = presets?.[cfg.preset];
  if (!preset) {
    throw new Error(`渠道 ${cfg.name || '(未命名)'}: 找不到 preset "${cfg.preset}"，请检查拼写或在 providers.json 里定义`);
  }
  return {
    ...preset,
    ...cfg,
    headers: { ...(preset.headers || {}), ...(cfg.headers || {}) },
    description: cfg.description ?? preset.description,
  };
}
