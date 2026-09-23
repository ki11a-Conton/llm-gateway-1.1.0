// 工具调用正文泄漏守卫：上游没把函数调用走原生 tool_calls 时，模型会把调用"手写"进正文
// （如 <｜tool_calls｜>...、<invoke name=...>、<tool_call>...）。这类响应是 HTTP 200，
// 常规错误分类发现不了；守卫识别它并视为渠道失败，让路由层换下一个渠道。

/** 正文里出现即判泄漏的标记 */
export const TOOL_CALL_TEXT_MARKERS = [
  '<｜tool_calls｜>',        // DeepSeek 系特记号被当正文吐出（全角竖线 U+FF5C）
  '<｜tool▁calls▁begin｜>',  // DeepSeek 真实特记号的文本形态（U+2581）
  '<invoke name=',           // agent XML 工具协议被手写进正文
  '<tool_call>',             // Qwen 系文本工具调用格式
];

function hitMarker(text) {
  for (const m of TOOL_CALL_TEXT_MARKERS) if (text.includes(m)) return m;
  return null;
}

/**
 * 非流式：在转换后的响应 JSON（OpenAI 或 Anthropic 形状）正文里找泄漏标记
 * @returns 命中的 { marker, text } 或 null；响应已含原生 tool_calls / tool_use 时永远 null
 * （原生调用成功时正文里提到调用格式属于正常叙述，不算泄漏）
 */
export function findToolCallTextLeak(json) {
  if (!json || typeof json !== 'object') return null;
  const texts = [];
  if (Array.isArray(json.choices)) {
    for (const ch of json.choices) {
      const msg = ch?.message;
      if (Array.isArray(msg?.tool_calls) && msg.tool_calls.length) return null;
      if (typeof msg?.content === 'string') texts.push(msg.content);
      else if (Array.isArray(msg?.content)) {
        for (const p of msg.content) if (typeof p?.text === 'string') texts.push(p.text);
      }
    }
  } else if (Array.isArray(json.content)) {
    if (json.content.some((b) => b?.type === 'tool_use')) return null;
    for (const b of json.content) if (b?.type === 'text' && typeof b.text === 'string') texts.push(b.text);
  } else {
    return null;
  }
  const text = texts.join('\n');
  const marker = hitMarker(text);
  return marker ? { marker, text } : null;
}

// 流式检测：标记 token 可能被拆到相邻 chunk、也可能以 \uXXXX 转义出现，
// 必须把头部缓冲里的正文文本拼接还原后再匹配。
// "reasoning_content" 不会命中 "content"（前者 content 前是 _ 不是 "）。
const STREAM_CONTENT_RE = /"(?:content|text)"\s*:\s*"((?:[^"\\]|\\.)*)"/g;

/** 从 SSE/帧原文中提取正文文本（content / text 字段值拼接还原） */
export function extractStreamContentText(raw) {
  let out = '';
  for (const m of String(raw || '').matchAll(STREAM_CONTENT_RE)) {
    try { out += JSON.parse(`"${m[1]}"`); } catch { out += m[1]; }
  }
  return out;
}

/** 流式：给定拼接还原的正文文本，返回命中的标记或 null */
export function hitStreamLeak(contentText) {
  return hitMarker(contentText);
}
