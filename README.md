# 本地模型聚合网关（local-llm-gateway）

把多家大模型供应商聚合成**一个本地 OpenAI 兼容端点**。你只需要记一个 `base_url` 和一个 `api_key`，网关自动挑一家当前可用的供应商把请求发出去，失败就换下一家。

```
   你的各种客户端                      本地网关                          上游供应商
┌──────────────────────┐      ┌────────────────────────┐      ┌──────────────────────┐
│ Claude Code          │      │ POST /v1/chat/completions│      │ DeepSeek      (优先10)│
│ Cursor / Cline       │─────▶│ POST /v1/messages       │──┬──▶│ Anthropic     (优先10)│
│ Continue / Copilot   │      │ GET  /v1/models         │  │   │ OpenAI        (优先20)│
│ OpenAI SDK / curl    │      │                         │  ├──▶│ OpenRouter    (优先30)│
└──────────────────────┘      │  选路 → 熔断 → 自动切换 │  │   │ 硅基流动/智谱/Kimi... │
   base_url = http://         │  协议转换（双向）        │  └──▶│ Ollama (本地)         │
   127.0.0.1:8787/v1          └────────────────────────┘      └──────────────────────┘
   api_key  = PROXY_MANAGED
```

特性：

- **零依赖**：只用 Node 内置模块（node:http + fetch），`node server.mjs` 直接跑，不用 `npm install`
- **双协议对外**：`/v1/chat/completions`（OpenAI）和 `/v1/messages`（Anthropic 原生）都能接
- **双协议对内**：接 OpenAI 兼容供应商，也接 Anthropic 官方原生接口，流式 SSE 双向转译
- **自动故障切换**：网络错误 / 超时 / 429 / 5xx / 模型不存在 → 立刻换下一家
- **熔断 + 探活**：连续失败即熔断，指数退避，后台定时探活提前恢复
- **模型池**：渠道用 `model` 字段绑定单模型，同名模型 = 多个备用渠道，自动故障切换
- **模型自动发现**：启动时拉取各家的 `/v1/models`，不填白名单也能聚合；面板/接口可手动拉取并写回配置
- **模型点选填入**：编辑/添加渠道时可按该渠道拉出上游真实模型名，直接点选，避免手打模型名填错
- **自定义 provider 预设**：`providers.json` 里定义常用的供应商，渠道里一行 `preset` 引用，同名覆盖内置
- **模型名映射**：`alias` 做渠道内改名，`modelMap` 做全局入站改名
- **配置热重载**：改完 `config.json` 自动生效，不用重启
- **Web 状态面板**：实时看每家渠道的健康度、成功率、延迟、最近错误
- **WorkBuddy 国际版账号接入**：设备码授权（面板扫码/打开授权页）或手填 access_token，一账号一渠道，定时续期 + 余额聚合显示；出站自动走本机代理并做风控脱敏

---

## 1. 快速开始

要求：Node.js >= 20（用到 `AbortSignal.any`）

```bash
cd llm-gateway

# 1) 复制模板并填你的密钥
cp config.example.json config.json

# 2) 启动
node server.mjs
# 或者（PowerShell 7）
pwsh ./start.ps1
```

启动后：

```
base_url : http://127.0.0.1:8787/v1
api_key  : PROXY_MANAGED
状态面板 : http://127.0.0.1:8787/
```

验证：

```bash
curl http://127.0.0.1:8787/v1/models -H "Authorization: Bearer PROXY_MANAGED"

curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer PROXY_MANAGED" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"hi"}]}'
```

> 不填 `apiKey` 的渠道会自动停用（面板上会标"未配置 apiKey"）。**你填了哪几家，网关就用哪几家**，没填的不会拖慢请求。

命令行参数：

| 参数 | 说明 |
|---|---|
| `--port <n>` | 监听端口，默认 8787 |
| `--host <addr>` | 监听地址，默认 127.0.0.1（改 `0.0.0.0` 可让局域网其它机器连） |
| `--config <path>` | 指定配置文件 |
| `--api-key <key>` | 对外 API Key |
| `--log-level <lv>` | `debug`/`info`/`warn`/`error`/`silent` |
| `--no-discover` | 启动时不拉上游模型列表（离线时用） |

环境变量：`GW_PORT` / `GW_HOST` / `GW_API_KEY` / `GW_CONFIG` / `GW_LOG_LEVEL`。

---

## 2. 配置

`config.json` 结构：

```jsonc
{
  "server": {
    "host": "127.0.0.1",
    "port": 8787,
    "apiKey": "PROXY_MANAGED",   // 客户端用这个 key 连网关
    "panel": true                // / 状态面板开关
  },

  "routing": { /* 选路与熔断参数，见 2.2 */ },

  // 入站模型名归一化（客户端传的名字 -> 网关内部逻辑名）
  "modelMap": {
    "claude-sonnet-4-20250514": "claude-sonnet-4",
    "gpt-4o": "gpt-5"
  },

  // 显式指定某个模型优先走哪些渠道（按数组顺序）
  "routes": {
    "claude-sonnet-4": ["anthropic", "openrouter"]
  },

  "channels": [ /* 见 2.1 */ ]
}
```

### 2.1 channels（渠道）

每个渠道就是一家供应商：

```jsonc
{
  "name": "deepseek",                       // 唯一标识，日志和面板里显示
  "protocol": "openai",                     // openai | anthropic
  "baseUrl": "https://api.deepseek.com/v1", // 不带 /v1 也会自动补
  "apiKey": "${DEEPSEEK_API_KEY}",          // 支持 ${环境变量} 占位
  "priority": 10,                           // 越小越优先，默认 100
  "enabled": true,                          // 不写则"填了 key 就启用"
  "models": [],                             // 白名单，留空=自动发现；支持 "gpt-*" 通配
  "alias": { "my-smart": "deepseek-reasoner" }, // 渠道内模型改名
  "headers": { "HTTP-Referer": "http://localhost" }, // 额外请求头
  "timeoutMs": 180000,                      // 覆盖全局超时
  "maxTokens": 8192,                        // 仅 anthropic：OpenAI 请求没给 max_tokens 时的默认值
  "description": "DeepSeek 官方"
}
```

字段说明：

| 字段 | 必填 | 说明 |
|---|---|---|
| `name` | ✅ | 渠道唯一名 |
| `baseUrl` | ✅ | 上游根地址，`/v1` 可省 |
| `protocol` | | `openai`（默认）或 `anthropic` |
| `apiKey` | ✅ | 支持 `${ENV_VAR}`；为空则该渠道自动停用。也兼容写多行字符串（每行一个 key） |
| `apiKeys` | | **叠加 key**：同一 baseUrl 下的多个 key（数组）。内部调度由 `stackedKeyStrategy` 决定（默认 `race` 并行竞速）。见 §2.1.8 |
| `stackedKeyStrategy` | | 叠 Key **内部**调度策略（仅 `apiKeys.length > 1` 时生效）：`race`（默认，全量并发竞速）或 `rotate-429`（Key 内部轮转 + 429 快切）。**只影响这条渠道内部用哪个 Key，绝不参与渠道选路** |
| `priority` | | 数字越小越优先；同级按平均延迟排序 |
| `enabled` | | 显式开关。不写时按"有没有 key"自动判断 |
| `models` | | 白名单。留空 → 启动时自动发现。**发现成功的渠道只认自己列表里的模型**，不会抢别家的单 |
| `model` | | **模型池写法**：等价 `models:[model]`，且该渠道不参与模型拉取。语义 = "这家供应商只提供这一个模型" |
| `effort` | | 思考强度标签：`low` / `medium` / `high`。请求带 `reasoning_effort` 时优先路由到匹配档位的渠道 |
| `alias` | | `{ "对外模型名": "该渠道真实模型名" }` |

已内置模板的供应商：DeepSeek、Anthropic 官方、OpenAI、OpenRouter、硅基流动、智谱、Moonshot、阿里百炼、火山方舟、腾讯 WorkBuddy（国际版）、本机 Ollama（`node server.mjs --list-presets` 查看全部）。

**加一家新的 OpenAI 兼容供应商**，复制一段改三个字段就行：

```jsonc
{ "name": "myproxy", "protocol": "openai",
  "baseUrl": "https://my-proxy.example.com/v1", "apiKey": "sk-xxx", "priority": 60 }
```

### 2.1.1 模型池（推荐用法）

把网关当**一个大池子**：每家供应商渠道只绑**一个模型**（用 `model` 字段，等价于 `models:[model]`，且**不参与模型拉取**）。同一个模型名挂 N 家渠道，池子里就有 N 个备用，请求时自动挑当前健康的那家：

```jsonc
// config.json -> channels
{ "name": "ds-key1",  "preset": "deepseek",  "apiKey": "sk-xxx", "model": "deepseek-chat", "priority": 10 },
{ "name": "ds-key2",  "preset": "deepseek",  "apiKey": "sk-yyy", "model": "deepseek-chat", "priority": 20 },
{ "name": "ds-key3",  "preset": "deepseek",  "apiKey": "sk-zzz", "model": "deepseek-chat", "priority": 30 },
{ "name": "claude-a", "preset": "anthropic", "apiKey": "sk-aaa", "model": "claude-sonnet-4", "priority": 5 }
```

上例中池子里有 `deepseek-chat`（3 家）和 `claude-sonnet-4`（1 家）。客户端只要报 `model: "deepseek-chat"`，网关从池子里取 `ds-key1 → ds-key2 → ds-key3` 依次尝试，第 1 家挂了自动走下一家；**供应商加得越多，每个模型名的备用就越多**。面板「模型池」区块直接展示这种整池视图。

> `model` 与 `models` 同时出现时以 `model` 为准（单模型渠道不拉取模型列表，拉来的只会污染绑定）。

#### 2.1.2 自定义预设（providers.json）

同款供应商往往有固定的 baseUrl / 协议 / 请求头，渠道里每次手抄容易错。可以写成**预设**，渠道里用一行 `preset` 引用：

```jsonc
// providers.json（放在项目根目录，启动时自动加载）
{
  "myproxy": {
    "protocol": "openai",
    "baseUrl": "https://my-proxy.example.com/v1",
    "priority": 60,
    "headers": { "X-Custom-Token": "abc" },
    "description": "我的私有中转"
  }
}

// config.json -> channels 里
{ "name": "myproxy-cn", "preset": "myproxy", "apiKey": "sk-xxx" }
```

- 预设里的所有字段都能被渠道配置**逐字段覆盖**（上面只覆盖了 `apiKey`）
- 同名会覆盖内置预设，`--list-presets` 输出里带 `#` 的就是描述
- 模板见 `providers.example.json`，支持 `chatPath`/`modelsPath` 覆盖对话和模型列表路径（Azure 这类不走标准路径的供应商用得上）

#### 2.1.3 池子总入口（unifiedModel）—— "一个名字 = 整个池子"

agent 的模型下拉显示的是 `/v1/models` 的返回内容。想让 **agent 里只显示一个名字（比如 `auto`），调用它就路由到池子里任何一个能用的供应商**，配置：

```jsonc
"unifiedModel": "auto"
```

效果：

- `/v1/models` 只返回 `["auto"]` —— agent 列表里就这一个名字
- 调用 `model: "auto"` → 网关从**全部启用渠道**里按优先级挑（每家用它自己绑定的模型，单模型渠道就是 `model` 字段），失败自动换下一家，思考强度过滤照常生效
- 响应头 `x-gateway-upstream-model` 能看出这次实际用了哪个真实模型

不配 `unifiedModel` 则保持原行为：`/v1/models` 显示池子里全部真实模型名。

与 `fallbackModel` 的分工：`unifiedModel` 是"一个名字代表全池"；`fallbackModel` 是"陌生模型名落到指定模型"。两者可同时配置 —— agent 选 `auto` 走全池路由，发了其它奇怪名字则被 `fallbackModel` 接住。

#### 2.1.4 思考强度路由

给渠道打上 `effort` 标签，调用时传 OpenAI 标准的 `reasoning_effort` 参数，网关就按强度挑模型：

```jsonc
// config.json -> channels
{ "name": "ds-chat",     "preset": "deepseek", "apiKey": "sk-x", "model": "deepseek-chat",    "effort": "medium" },
{ "name": "ds-reasoner", "preset": "deepseek", "apiKey": "sk-x", "model": "deepseek-reasoner", "effort": "high" }
```

```bash
# 普通调用 -> 走 deepseek-chat
curl ... -d '{"model":"deepseek-chat","messages":[...]}'

# 要深度思考 -> reasoning_effort=high 经 effortMap 映射后走 deepseek-reasoner
curl ... -d '{"model":"deepseek-chat","reasoning_effort":"high","messages":[...]}'
```

工作方式（三层，按序生效）：

1. **`effortMap` 改写模型名**：同一供应商不同强度往往是不同模型名，配置 `"effortMap": { "deepseek-chat": { "high": "deepseek-reasoner" } }` 后，带 `reasoning_effort: "high"` 的请求会改调 `deepseek-reasoner`
2. **渠道 `effort` 标签过滤**：候选渠道按 `effort` 精确匹配排最前、未标记的兜底；全都没标记时不过滤（向后兼容）
3. **参数透传/转换**：`reasoning_effort` 原样透传给 OpenAI 兼容上游（o 系列等原生支持）；转发 Anthropic 渠道时自动转成 `thinking: {type:"enabled", budget_tokens}`（low=4096 / medium=16384 / high=32768，`max_tokens` 自动抬升），思考过程以 `reasoning_content` 回传

Anthropic 客户端（`/v1/messages`）传 `thinking: {type:"enabled", budget_tokens: N}` 同样生效，按 budget 折算档位。

#### 2.1.5 面板添加 / 编辑 / 删除供应商

面板右上角「**＋ 添加供应商**」：预设下拉选「自定义（手填 baseUrl）」时可选**协议**（OpenAI 兼容 / Anthropic 原生），再填渠道名 + apiKey + baseUrl + 绑定模型 + 思考强度 + 优先级，提交后直接写回 `config.json` 并热重载，不用手动编辑文件。**API Key 是多行文本域（每行一个 key）**——填多个即落成 §2.1.8 的叠加 key 渠道，渠道卡片上显示「叠加 N key」。

**编辑已保存的渠道**：渠道卡片右上角「**编辑**」按钮打开同一个弹窗，标题变成「编辑供应商」，并把该渠道**已保存的配置回填**（预设 / 渠道名 / baseUrl / 协议 / 绑定模型 / 思考强度 / 最高档位 / 优先级 / 备注 / 状态），改完点「保存修改」即写回 `config.json` 并热重载。几条约定：

- **密钥留空 = 保持不变**：面板**从不回显明文密钥**（`/api/status` 只回 `refreshToken` 的长度和 `userId`），所以 apiKey / refreshToken 输入框在编辑模式下是空的，**留空表示沿用原值**，粘贴新值才会替换；想清空就删掉这条渠道重建。
- **可以改名**：改名时 `routes` 里对该渠道的旧名引用会**自动同步改成新名**，不留悬空引用。
- **字段窄写**：只覆盖你实际改动的字段，其余（`headers` / `chatPath` / `timeoutMs` / 余额等运行时字段）原样保留；把「备注」「绑定模型」等清空 = 删除该字段（回到跟随全局 / 自动发现）。
- **状态可切换**：编辑弹窗里可把渠道设为「停用（不参与路由，配置保留）」，再改回「启用」。
- **补上 key 会自动重新启用**：之前因缺 apiKey 被自动停用的渠道，编辑时填上 key 保存后即恢复启用；只有未展开的 `${ENV}` 占位不会被写死成停用（否则环境变量注入后也永远起不来）。
- **落盘前预检**：坏配置（非法 preset、自定义却没填 baseUrl、重名）直接报错并拒绝写入，不会把坏渠道写进 `config.json`。
- **模型可以从上游拉出来点选**：模型字段旁的「**拉取上游模型**」按钮会去该渠道的上游 `/models` 拉真实模型名列成下拉，**点选即填入**——手打模型名是最容易填错的地方（上游 id 常带 `-reasoner` / `-v3.1` / `-preview` 之类后缀，错一个字符就是一路 404）。**添加渠道时（配置还没落盘）也能拉**；编辑时密钥框填了新 key 就用新 key，留空则沿用该渠道已保存的 key。**纯只读**，不改内存也不写配置，详见 §2.1.6。

渠道卡片右上角「×」可删除（同样写回配置，并自动清理 `routes` 里的引用）。

对应 HTTP 接口：

```
GET    /api/presets            列出可用预设
POST   /api/channels           添加渠道 {name, preset?, baseUrl?, apiKey?, apiKeys?, model?, effort?, priority?, description?}
                               （apiKey 支持多行/逗号分隔；多个 key -> apiKeys 叠加竞速）
PATCH  /api/channels/<name>    编辑渠道（部分更新；密钥字段留空/不传 = 保持不变，name 可改并同步 routes）
                               PUT 同义。可编辑 name/preset/protocol/baseUrl/apiKey/apiKeys/stackedKeyStrategy/
                               model/effort/maxEffort/priority/description/enabled/proxy/refreshToken/headers
DELETE /api/channels/<name>    删除渠道
POST   /api/models/fetch       拉取某渠道的上游模型列表（**只读**，不写配置、不改运行时状态）
                               body: {name} 用已保存渠道的端点/密钥；
                                     或 {preset} / {baseUrl, protocol, apiKey?, apiKeys?} 用表单里还没落盘的值
                               → {ok, models[], count, url}；上游失败时 ok:false + error（HTTP 非 2xx 才 400）
```

#### 2.1.6 拉取模型

**（a）面板上按渠道拉、点选填入（推荐，专治模型名手打填错）**

编辑/添加渠道的弹窗里，「模型」输入框右边有个「**拉取上游模型**」按钮：点它就去**该渠道自己的上游** `/models` 拉真实模型名，列成下拉框，**点选即填入**模型框。相比手打，这样拿到的一定是上游认可的 id。

- **添加渠道时也能拉**：配置还没落盘，按钮会把表单里的 `preset` / `baseUrl` / `protocol` 和刚填的 key 一起发给后端试拉。
- **编辑时密钥留空就沿用原 key**：面板从不回显明文密钥，所以密钥框留空时后端用该渠道已保存的 key 去拉；填了新 key 则优先用新 key（改完还没保存也能先试拉一次）。
- **纯只读**：不会改内存里的模型池，也不会写 `config.json`；换了预设或 baseUrl 后旧的列表会自动作废，避免拿着 A 家的列表去填 B 家。
- 失败会显示人能看懂的原因（上游 HTTP 状态码 + 响应片段；没配 key 会额外提示）。

对应接口：

```bash
# 用已保存的渠道拉（用该渠道自己的 key）
curl -X POST http://127.0.0.1:8787/api/models/fetch \
  -H 'content-type: application/json' -d '{"name":"deepseek"}'

# 配置还没落盘时也能拉（表单里的值）
curl -X POST http://127.0.0.1:8787/api/models/fetch \
  -H 'content-type: application/json' \
  -d '{"baseUrl":"https://api.example.com/v1","protocol":"openai","apiKey":"sk-xxx"}'
```

**（b）批量拉全部渠道并写回配置**

模型自动发现之外，也可以手动触发并**回写配置**：

```bash
# 只刷新内存（重启后恢复）
curl -X POST http://127.0.0.1:8787/api/discover

# 刷新并写回 config.json（把拉到的模型固化进该渠道的 models 白名单，下次启动直接用）
curl -X POST "http://127.0.0.1:8787/api/discover?save=1"

# 只拉某个渠道
curl -X POST "http://127.0.0.1:8787/api/discover?channel=deepseek"
```

#### 2.1.7 WorkBuddy 国际版账号（腾讯 CodeBuddy 海外站）

接入腾讯 WorkBuddy（workbuddy.ai，国际版）账号，把**账号额度**当一家供应商用。国际版**没有每日签到**，网关只做：定时续期 access_token + 定时刷新余额（每小时，渠道卡片上显示余额）。

**加号（主入口：面板「＋ WorkBuddy 国际版」→ 批量导入）**：把一堆 access token 粘进文本框（**每行一个**），一次生成 `wb-intl-1 / wb-intl-2 / …` 渠道，全部绑定同一模型（默认 `deepseek-v4.1-flash`）进池子——**同模型多账号 = 备用池**，调用时网关自动挑健康账号、挂了换下一个。对应接口：

```
POST /api/workbuddy/batch   {"tokens": ["tk1","tk2",...], "model": "deepseek-v4.1-flash", "priority": 60}
                            → { ok, added: [{name:"wb-intl-1", ok:true}, ...] }
```

**单账号备选（批量框里折叠的「单账号设备码授权」）**：

1. **设备码授权**：面板给出一个 **获取 access token 的网址**（上游签发的授权页）→ 浏览器打开并登录 → 网关每 5 秒轮询，授权成功后**自动落号渠道**进 config.json（一账号 = 一渠道）。对应接口：

   ```
   POST /api/workbuddy/auth/start        → { ok, state, authUrl, expiresIn: 900, interval: 5 }
   GET  /api/workbuddy/auth/poll?state=  → { status: "pending" } / { status: "authorized", channel }
   ```

2. **手动粘贴 access_token**（已在别处登录过）：渠道配置如下——`apiKey` 填 access token，`X-User-Id` 填用户 ID（可选但建议），有 refresh_token 就填 `refreshToken`（网关用它每小时续期，不然到期得手动换）：

   ```jsonc
   // config.json -> channels（一账号 = 一条）
   {
     "name": "wb-my-account",
     "preset": "workbuddy-intl",
     "apiKey": "你的 access_token",
     "model": "deepseek-v4.1-flash",     // 只绑这一个模型
     "priority": 60,
     "refreshToken": "你的 refresh_token（可选）",
     "headers": { "X-User-Id": "你的用户ID（可选）" }
   }
   ```

**preset 已默认做好的事**（`workbuddy-intl`）：聊天端点 `/v2/chat/completions`、只认流式（非流式客户端由网关自动聚合回标准 JSON）、出站 body 风控脱敏（上游黑名单：删除 Claude Code 身份句、`Claude`/`Codex`→`workbuddy`、剥离 `x-anthropic-billing-header`/`cc_*`、`developer`→`system`、tool_choice 归一）、出站默认走本机代理。

**出站代理**：workbuddy.ai 国际版需要代理才能直连。preset 默认 `proxy: "http://127.0.0.1:7897"`（客户端实测的 Clash 混合端口）；没有代理、或 mock 测试要直连时，渠道里写 `"proxy": ""` 清空。优先级：渠道 `proxy` > 配置 `routing.proxy` > 环境变量 `GW_PROXY`。网关内置零依赖 CONNECT 隧道（`lib/outbound-proxy.mjs`），不用装任何代理库。

> 其它细节：余额接口 `/v2/billing/meter/get-user-resource`；定时续期/刷新由渠道后台任务驱动，失败只记日志不阻塞；上游 `discover` 不需要（预置单模型），模型列表接口 401 属预期。

面板上的「拉取模型」和「拉取并保存」两个按钮就是这两个操作。注意：**没有配 apiKey 的渠道拉取会失败**，先填 key。（想按单个渠道拉、并且是**点选填入模型**而不是批量刷新，用 §2.1.6(a) 的「拉取上游模型」。）

#### 2.1.8 叠加 key（同一家多 key）——两种内部策略

同一家供应商（**同一个 baseUrl**）手里有多个 key 时，可以把它们**叠在一条渠道**里，由 `stackedKeyStrategy` 决定这条渠道**内部**怎么用这些 key。

> ⚠️ **两层轮转，务必分清**（这也是最容易搞混的地方）：
>
> - **渠道级轮询**（`routing.strategy` = `priority` / `round-robin` / `weighted` / `least-loaded`，以及两段式 `preferred`+`fallback`、渠道熔断、`sticky` / `sessionAffinity`）决定的是：**这次请求走哪一家渠道**。
> - **叠 Key 内部轮转**（`stackedKeyStrategy`）决定的是：**已经选中某一条渠道之后，用这条渠道里的哪个 Key**。
>
> 两个状态机**互不干扰**：Key 游标只在一条渠道内部转，**永远不会**改变渠道之间的选择顺序；反过来，渠道轮询怎么轮也不会动 Key 游标。

```
客户端请求
   ↓
渠道路由（routing.strategy）：A → B → A → B …        ← 决定“这次走哪一家渠道”
   ↓  本次选中 A
A 内部 Key（stackedKeyStrategy=rotate-429）：K1 → K2 → K3 → K4 → K5 → K1 …   ← 决定“用 A 的哪个 Key”
```

**策略一：`race`（默认，全量并发竞速）**

同一份请求**同时**发给全部 key，第一个返回成功（2xx）的 key 胜出，其余请求**立刻取消**（不再占连接、不再消耗上游生成）。效果 = "至少有一个 key 能成功"，用最快响应的那个 key 服务本次请求。**成功率高，但积分消耗与上游并发也高**（一个请求最多打出 N 份）。

**策略二：`rotate-429`（Key 内部轮转 + 429 快切）**

一次只用一个 Key，环形游标逐个消费：`K1 → K2 → K3 → K4 → K5 → K1 …`

- **规则 A**：只要一个 Key 真的被发出去，游标就**立刻**向前推进一格（不管它最后是 429 / 成功 / 网络失败）。
- **规则 B**：收到 **429**（以及 401 / 402 / 403 / 5xx 等"可切 Key 错误"）**零额外等待**立即换下一个 Key——不 sleep、不指数退避、不等 `retryWaitMs`。
- **规则 C**：**正常慢响应不切 Key**。上游"接了请求、慢慢生成"（商汤实测 9~17s，甚至约 20s）只受网关原有的首字节 / 总超时 / 静默超时约束，**不会**因为"慢"就启动下一个 Key。
- **规则 D**：成功后结束本轮扫描，**下一条请求从后一个 Key 开始**（游标不回 K1）。例：`K1 429 → K2 429 → K3 成功`，游标已经到 K4，下一条从 K4 起。
- **规则 E**：一次请求最多扫描 Key 池**一整圈**，绝不无限循环；整圈都失败才把错误交回**渠道级**降级逻辑。
- **规则 F**：普通业务错误（如 400 参数错误）**不盲扫**后续 Key——整池重试无意义，直接交回渠道级。

适合"共享池 + 快速 429 + 正常响应较慢"的供应商：**积分消耗与上游并发显著下降**（平均每请求用掉的 Key 数从 `race` 的 ≈ N 降到接近 1）。

```jsonc
// config.json -> channels（一条渠道 = 一个 baseUrl + 多个 key）
{
  "name": "myproxy-stacked",
  "protocol": "openai",
  "baseUrl": "https://my-proxy.example.com/v1",
  "apiKeys": ["sk-aaa", "sk-bbb", "sk-ccc"],   // 叠加的多个 key
  "stackedKeyStrategy": "rotate-429",          // race（默认）| rotate-429
  "model": "gpt-5",
  "priority": 40
}
```

- 只写 1 个 key 时仍用 `apiKey` 字符串，**旧配置形态与行为完全不变**（单 key 渠道不回 `x-gateway-key`，也不显示 Key 策略）。
- `apiKeys` 也兼容把 `apiKey` 直接写成多行字符串；面板「添加供应商」的 **API Key 文本域每行填一个** key 就走这个形态。落盘时 1 个 key → `apiKey`，多个 → `apiKeys`。
- **不写 `stackedKeyStrategy` 时默认 `race`**（保持旧行为）；非法值直接报配置错误。
- 响应头能看到本次用了哪个 Key、什么策略、尝试了几个（**只回序号 / 总数 / 策略名，绝不回 Key 明文**）：
  ```
  x-gateway-key: 3/5                 （本次最终命中的 Key 序号/总数）
  x-gateway-key-strategy: rotate-429 （本渠道的 Key 内部策略）
  x-gateway-key-attempts: 3          （本次从起点开始共尝试了几个 Key）
  ```
- `race` 模式下 key **不做单独的健康记忆**：每次请求都重发给全部 key，谁先成功用谁；某个 key 失效不影响整条渠道（它只是每次都"陪跑"）。
- 全部 Key 都失败时：`race` 优先拿**非鉴权/非余额**的那个错误继续降级；`rotate-429` 整圈扫完后同样交回渠道级。两者都避免单个坏 Key 把整条渠道判死（只有鉴权/余额错误才会让渠道被长冷却）。
- 面板渠道卡片上会显示「叠加 N key · 429快切 / 竞速」，渠道编辑区可选「叠 Key 内部策略」。

> 想要"同名模型、多家渠道、故障切换"用 §2.1.1 的模型池；想"同一家、多 key"用这里的叠加 key。两者可同时用——前者管**渠道之间**，后者管**渠道内部**。

### 2.2 routing（选路与熔断）

```jsonc
{
  "strategy": "priority",        // priority：优先级+延迟 | round-robin：每个请求轮换起点（须同时 sticky:false 才真正摊开） | weighted：按优先级加权轮询（高优先吃大头、低优先持续参与，输出交错无突发；配 priorityWeights 调档） | least-loaded：最少在途优先（多并发推荐）
  "maxAttempts": 4,              // 单轮最多尝试几个渠道
  "timeoutMs": 180000,           // 整条请求的总超时（兜底）
  "firstByteTimeoutMs": null,    // 首字节看门狗：发出请求 → 收到响应头的最长等待（null = 用该渠道的 timeoutMs）。与总超时/静默超时**并行计时、取最严**，不叠加。上游"接了连接却不给响应头"靠它兜住
  "streamIdleTimeoutMs": 120000, // 流式传输中，多久没数据算卡死
  "writeDrainTimeoutMs": null,   // 客户端"不读也不断开"时，等 drain 多久就判定客户端已断开（null = 用 streamIdleTimeoutMs）。判为 client_abort，**不计渠道失败**（上游没做错）
  "failThreshold": 2,            // 连续失败几次触发熔断
  "cooldownMs": 30000,           // 基础冷却时长（之后按指数退避）
  "maxCooldownMs": 600000,       // 冷却上限 10 分钟
  "probeIntervalMs": 60000,      // 后台探活间隔，0 关闭
  "discoverIntervalMs": 600000,  // 重新拉取模型列表间隔，0 关闭

  // ---- 两段式选路（tier）----
  "tiered": true,                // 第1段优先池用尽后，进第2段"其余供应商"
  "preferredBaseUrls": ["sensenova.cn", "api.b.ai"], // 按 baseUrl 域名自动进优先池；渠道可用 tier 字段强制指定
  "sticky": true,                // provider 钉选：上次成功的渠道反复复用，直至失败（least-loaded 策略下让位于负载摊开）
  "fallbackAttempts": 3,         // 钉选/随机池每家预算：1 次首次 + 2 次重试，仍失败才换下一家
  "fallbackRetryIntervalMs": 3000, //   每次重试之间固定等 3s
  "fallbackShuffle": true,       // 无钉选记录时随机洗牌（false = 按 priority）
  "priorityWeights": null,       // 仅 strategy=weighted 生效：优先级上界 → 权重。默认 { "10":8, "30":4, "60":2, "100":1 }（即 p1-10 拿约 8/15 流量）。想让所有渠道平均分就全填同一权重
  "halfOpenMs": 15000,           // 熔断到期后的半开窗口：窗口内该渠道最多放 halfOpenMaxInFlight 个在途去试探，其余请求跳过它——恢复了立刻回满份额，没好也不会被一批请求同时打爆。注意冷却中的渠道**始终留在候选列表里**（只垫到最后，不剔除）。0 = 关闭半开（等价旧行为）
  "halfOpenMaxInFlight": 1,      //   半开窗口内允许放行的在途数

  "attemptsPerChannel": 2,       // 两层重试第 1 层：优先池同一渠道最多尝试几次（含首次；渠道可用 retries 覆盖）
  "retryPerAttemptMs": null,     // 渠道内每次失败后的等待；null 回落到 retryWaitMs
  "retryLoop": true,             // 两层重试第 2 层：全池失败且都是可重试错误（429/5xx/网络）时
  "retryWaitMs": 15000,          //   等待 15s 再循环一轮，直到成功
  "retryMaxWaitMs": 120000,      //   累计等待上限 2 分钟，超过则返回 503；0 = 无上限
  "maxTotalWaitMs": 600000,      // 单请求总预算：超过必给客户端一个结果，绝不无限挂住 agent

  // ---- "重试也没用"类错误的快速隔离 ----
  "disableOnBalanceError": true, // 余额不足（insufficient balance/quota）：不撞第二枪，立刻换下一家
  "balanceCooldownMs": 900000,   //   并冷却 15 分钟（到期后台探活自动恢复）
  "disableOnAuthError": true,    // 鉴权失败（401/403 invalid key）：同样直接换家
  "authCooldownMs": 600000,      //   冷却 10 分钟

  // ---- 思考强度 ----
  "forceMaxEffort": true,        // 能思考的模型统一注入目标档位 reasoning_effort（deepseek-v4/gpt-5.x/claude 等，见 util.mjs）
  "maxEffort": "high",           // 目标档位 low|medium|high|xhigh。实测 xhigh 是各家普遍接受的最高档
                                 //   （sensenova 报错原文 "should be one of: low, medium, high, xhigh, none"，
                                 //    非法值 max 不在档位表里）；渠道可用 maxEffort 单独覆盖：
                                 //    xhigh 会把 max_tokens 全烧在思考上、正文返回空的渠道（如 wxctf）pin 到 high
                                 // 上游 400 报 ReasoningEffort invalid 时：同渠道自动降为 high 重试一次，
                                 //    不计渠道失败、不消耗重试预算，避免一个配错的值把整条渠道链打穿
  "reasoningGuard": true,        // 各家思考字段（reasoning/thinking/reasoning_details…）归一到 reasoning_content；
                                 //   混进正文的思维链自动拆回 reasoning_content，agent 只看到干净回答
                                 //   （渠道可用 guardReasoning:false 单独关闭）
  "reasoningGuardStream": true,  // 流式方向是否做思考守卫（拆分 + 空响应检测）。false = 流式完全跳过思考守卫，
                                 //   只保留工具守卫，换取更低首字延迟（TTFT）。实测见 §7"首字延迟量测"：
                                 //   默认开启时若上游已用 reasoning_content 承载思考，网关会立即放行，代价≈0
  "emptyMinChars": 1,            // "伪空"阈值：正文去掉空白/标点/markdown 装饰后剩余有效字符数低于该值
                                 //   即视为不可用（HTTP 200 但只有 "." / "```" 壳）-> 换下一家。
                                 //   默认 1 = 只拦"一个有效字符都不剩"，不误伤正常短回答

  "sessionAffinity": true,       // 会话亲和：同一子代理在 affinityTtlMs 内优先复用上次成功的渠道（同优先级带内）
  "affinityTtlMs": 600000,       // 亲和记录有效期
  "maxConcurrentPerAgent": 16,   // 每个子代理的在途配额，防止一个大代理吃光渠道，0 = 不限
  "maxConcurrent": 128,          // 全局在途上限，超出排队，0 = 不限
  "maxConcurrentPerChannel": 32, // 单渠道在途上限，避免打爆某一家上游，0 = 不限
  "queueTimeoutMs": 60000,       // 并发排队超时，超时返回 503（可重试）
  "failDedupMs": 2000,           // 失败去抖窗口：窗口内同一渠道的并发失败只计一次熔断计数

  "guardToolCallText": true      // 正文泄漏守卫：请求带 tools 时识别"工具调用被写成正文"的响应并换渠道（渠道可单独关闭）
}
```

**任务日志（routing 平级）**：

```jsonc
"taskLog": {
  "enabled": true,        // false = 只留内存环形缓冲，不落盘
  "dir": "logs",          // 相对启动目录或绝对路径
  "file": "tasks.jsonl",  // 每个请求一行 JSON：尝试链、每个错误的指纹、耗时、走没走通
  "ringMax": 500,         // 内存里保留最近多少条，供面板 / /api 查询
  "maxFileBytes": 33554432, // 单档体积上限（默认 32MB），超过就轮转
  "keepFiles": 5,         // 保留档数（含当前档）：tasks.jsonl + tasks.1.jsonl … tasks.4.jsonl
  "usageKeepDays": 120    // Token 用量按天文件（logs/usage/YYYY-MM-DD.jsonl）的保留天数
}
```

落盘的错误文本会**自动脱敏**（`sk-…`、`Authorization: Bearer …`、`apiKey=…` 等替换成 `***REDACTED***`）；
内存里的环形缓冲保留原文，方便排障时看到完整信息。查询支持时间窗：`GET /api/tasks?since=<ISO或毫秒>&until=<ISO或毫秒>`。

**Token 用量（taskLog 平级）**：每个成功请求的 token 消耗按天落盘 `logs/usage/YYYY-MM-DD.jsonl`（**本地时区**分天，每行 `{ ts, model, channel, input, output }`），保留 `taskLog.usageKeepDays` 天（默认 120）。查询走 `GET /api/usage`（一次返回 today / d1 / d7 / d30 / d90）。

记账口径（**自用统计，不是计费依据**）：

- 只统计**上游真的回报了非零 usage**的成功请求：OpenAI 口径 `prompt_tokens/completion_tokens` 与 Anthropic 口径 `input_tokens/output_tokens` 都认；上游不回 usage（或全 0）时**不产生记录**——不估算、不造假 0。失败 / 客户端中止的请求同样不产生记录（请求数照常进任务日志）
- 时间口径：`today` = 自然日（本地时区当天 0 点起）；`d1/d7/d30/d90` = 滚动 24h×N（按 `ts` 精确比较，不是"最近 N 个自然日"）
- 流式：OpenAI 协议渠道出站会**强制补 `stream_options.include_usage=true`**，所以 OpenAI 客户端收到的流末尾会**多一个 usage 帧**（`choices` 为空）——标准 SDK 接受，属本功能的预期行为变化；个别兼容上游若忽略或不认这个字段，该渠道的流式统计会偏低（请求本身不受影响；上游因此回 400 时归类 `bad_request`、不重试换家、不产生 token 记录，已有回归守住）
- 追加写失败只 warn（磁盘满等），统计会缺当日增量，绝不阻塞请求；`taskLog.enabled=false` 时用量库只做内存聚合、不落盘

**价格计费（pricing，taskLog 平级）**：把上面的 token 用量按**单价**折算成费用。配置在 `config.json` 的 `pricing` 段，单价单位是**每 100 万 token 的金额**：

```jsonc
"pricing": {
  "currency": "USD",     // 只影响显示，不做汇率换算
  "symbol": "$",
  "models": {
    "deepseek-chat": { "input": 0.28, "output": 0.42 },
    "my-model":      { "input": 1.2,  "output": 3.4 },
    "gpt-*":         { "input": 2.0,  "output": 8.0 }   // 通配：前缀匹配，多个命中取最长前缀
  }
}
```

整段不写就用 `lib/pricing.mjs` 的内置参考价目表（**只是价格快照、可能过期，以各家官方价目表为准**；同名条目被配置覆盖）。口径（**估算展示，不是扣费依据**）：

- 金额 = `(输入 × 单价in + 输出 × 单价out) / 1e6`，四舍五入到 6 位小数
- **未配价格的模型金额为 `null`，绝不按 0 计**（面板显示「未配置价格」）；范围级另有 `costComplete` 标注"这份金额是否完整"，不完整时面板加「（部分）」并在悬停里说明未计价模型数与 token 数
- 显式配置 `{"input":0,"output":0}` 是**有效的"免费"定价**（金额 `0`），与"没配价"（`null`）严格区分——0 是"确实不要钱"，null 是"不知道多少钱"，两者混用就等于把未知谎报成免费
- 脏价目（`NaN` / `Infinity` / 字符串 / 负数）直接忽略、当成没配这条，不让脏配置把金额算歪
- 金额**只在查询时计算、不落进 JSONL**：改价立即反映到全部历史范围，不需要迁移任何数据；热重载（`/api/reload` 或改配置文件）同样即时生效
- 范围级金额是"按模型明细之和"，所以 `byModel` 的**每天 200 个模型上界**同样限制金额完整性——被上界挤掉的模型既不在明细里、也不计入范围金额

### 2.3 故障切换是怎么工作的

1. 请求进来 → 按 `model` 从**池子**里找出所有能提供它的渠道（同名模型 = 多个备用）
2. **两段式排序**：第 1 段优先池（sensenova / api.b.ai）按 `priority`/策略排（`round-robin` / `weighted` 且 `sticky:false` 时**每个请求轮换起点**，避免永远只有第一家吃满流量）。`weighted` 是**平滑加权轮询**：高优先拿大头、低优先持续参与，输出是交错的（不会出现"前 N 个请求全给第一家"的突发）；第 2 段其余供应商——**上次成功的 provider 钉在最前反复复用**，无钉选记录时随机洗牌；**熔断中的排最后**，下一轮冷却到期进入半开窗口、只放 `halfOpenMaxInFlight` 个在途去试探，**但不把渠道从候选里剔除**
3. **两层重试**：优先池同一渠道内重试 `attemptsPerChannel` 次；随机池每家 `fallbackAttempts` 次（1 首次 + 2 重试，每次间隔 `fallbackRetryIntervalMs`）。429/5xx/网络错误会重试；**401/403、余额不足不撞第二枪**——记为渠道失败并长冷却，立刻换下一家
4. **空响应/伪空响应也算失败**：HTTP 200 但正文/工具调用/思考全空（GPT 系中转常见"假成功"），或者正文只有空白/单个标点/空代码围栏壳（去掉装饰后一个有效字符都不剩），同样按渠道失败换下一家，绝不把空壳丢给 agent。阈值由 `emptyMinChars` 控制（默认 1，保守）
5. 连续失败 `failThreshold` 次 → 该渠道熔断，冷却时长按 `30s → 60s → 120s…` 指数退避，上限 `maxCooldownMs`；高并发下一批请求撞上同一个故障时，`failDedupMs` 窗口内只计一次，避免瞬间打穿阈值
5. 后台每 `probeIntervalMs` 探活一次，可用的渠道**提前解除熔断**
6. 上游明确说"没有这个模型" → **临时**把该模型从这家的候选里摘掉，避免每次请求都白撞一遍；`unsupportedTtlMs`（默认 30 分钟）到期自动重新尝试，条目上限 `unsupportedMax`（默认 200，超出淘汰最旧）——一次误判不会永久损失一个渠道
7. 所有候选渠道都废了 → 开启 `retryLoop` 时等待 `retryWaitMs` 再循环一轮（每轮重新选路），直到成功或累计等待超过 `retryMaxWaitMs`；未开启或超限则返回 503，body 里带上每家的失败原因

流式请求的注意点：守卫开启时响应头会先攒在网关里（等正文够长/出现收尾信号/看到已分离的思考），**判定发生在响应头下发之前**。所以：

- 上游只发了一两帧就静默断流、而响应头**尚未下发**时，客户端一个字节都没收到，网关会**整体换到下一家**（不会把半截流丢给客户端）；
- 若已经下发过数据（响应头已发出），则无法再切渠道：网关补一个收尾帧让客户端优雅结束，同时记一次失败，让下一次请求换家。

开启 `retryLoop` 后客户端请求会挂起等待，agent 工具的超时设置要大于 `retryMaxWaitMs`。

### 2.4 高并发与多子代理

多个子代理（或多个工具）共用同一个网关时，网关做四件事：

- **三级并发闸门**：全局 `maxConcurrent` → 单渠道 `maxConcurrentPerChannel` → 单子代理 `maxConcurrentPerAgent`，超出配额的请求 FIFO 排队，`queueTimeoutMs` 后返回可重试的 503。上游被打爆和本地内存被打爆都不会发生
- **子代理识别**：按优先级从 `x-agent-id`（及 `x-session-id` 等头）→ `body.session_id` / `metadata.conversation_id` / `body.user` → API Key 指纹推断"这是哪个代理"，响应头 `x-gateway-agent` 回传
- **会话亲和**：同一子代理的连续请求优先复用上次成功的渠道（仅同优先级带内提升），多轮对话不换上游，上下文缓存友好
- **最少在途选路**：`strategy: "least-loaded"` 时请求自动摊到当前最空闲的渠道，空闲时自动退化为按优先级排序

观察当前负载：`GET /api/metrics` 返回全局/渠道/每代理的在途、排队、排队超时计数和内存用量。慢客户端由背压保护：下游写不进去时网关暂停读上游，内存不随响应体积膨胀（可用 `node test/bench-concurrency.mjs` 验证）。

---

## 3. 客户端接入

### OpenAI SDK（Python / Node）

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8787/v1", api_key="PROXY_MANAGED")
r = client.chat.completions.create(
    model="deepseek-chat",
    messages=[{"role": "user", "content": "hi"}],
    stream=True,
)
for chunk in r:
    print(chunk.choices[0].delta.content or "", end="")
```

### Claude Code / Anthropic SDK

网关直接暴露 `/v1/messages`，不用中间再套一层转换：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_AUTH_TOKEN=PROXY_MANAGED
export ANTHROPIC_MODEL=claude-sonnet-4
```

```python
import anthropic
client = anthropic.Anthropic(base_url="http://127.0.0.1:8787", api_key="PROXY_MANAGED")
```

### Cursor / Cline / Continue / 沉浸式翻译

填 OpenAI 兼容模式：

- Base URL：`http://127.0.0.1:8787/v1`
- API Key：`PROXY_MANAGED`
- Model：填面板 "可用模型" 里出现的名字

### DeepSeek Harness Desktop（DSH）

网关原生支持 DSH 的 openai-completions 调用。DSH Desktop
的 `settings.yaml`（`~/.dsh/settings.yaml`）里加一个 provider 指向网关即可：

```yaml
llm-pi-ai:
  providers:
    my-gateway:            # provider ID，小写
      displayName: 网关
      apiKeyEnv: GATEWAY_API_KEY    # 用环境变量注入密钥，或省略走 pi-ai 发现
      api: openai-completions
      baseURL: http://127.0.0.1:8787/v1
      models:
        - { id: auto }     # 网关 unifiedModel：调用 auto = 自动路由整个池子
```

- DSH 发任意模型名都能被服务：池子里没有的名字会自动兜底到 `unifiedModel`
  池入口（响应头 `x-gateway-fallback-from` 会标记原始模型名）。
- 流式响应带 `stream_options.include_usage` / `[DONE]` / `reasoning_content`，
  DSH 的 pi-ai 全部识别。
- 想固定走某一家模型，把 `models` 改成该 model ID 即可（需与池内渠道绑定一致）。

---

## 4. HTTP 接口

| 方法 | 路径 | 鉴权 | 说明 |
|---|---|---|---|
| POST | `/v1/chat/completions` | ✅ | OpenAI 对话接口（支持 `stream`） |
| POST | `/v1/messages` | ✅ | Anthropic 原生对话接口（支持 `stream`） |
| GET | `/v1/models` | ✅ | 聚合后的模型列表 |
| POST | `/v1/embeddings` | ✅ | 转发给 OpenAI 协议渠道 |
| GET | `/health` | ❌ | 健康检查 |
| GET | `/api/status` | loopback 免鉴权 | 渠道与模型状态 JSON（含每子代理在途/亲和视图） |
| GET | `/api/metrics` | loopback 免鉴权 | 并发/内存指标：全局与每渠道、每子代理在途与排队、排队超时计数。另有 `queueWait`（`queueWaitMsTotal` / `queueWaitMsMax` / `waitedCount`，**只在真正等到许可时结算**，排队超时的不虚增）与 `models`（每模型成功率、Top5 失败指纹、首字节 `ttfb` p50/p95）。某个耗时字段**没采到就不写这个键**，不会兜底成 0 |
| POST | `/api/probe` | loopback 免鉴权 | 立即探活 + 重新发现模型 |
| POST | `/api/discover` | loopback 免鉴权 | 拉取模型列表；`?save=1` 写回 config.json；`?channel=xx` 只拉指定渠道 |
| POST | `/api/models/fetch` | loopback 免鉴权 | 拉取**单个**渠道的上游模型列表并**原样返回**（只读，不写配置）：`{name}` 用已保存渠道，或 `{preset}` / `{baseUrl, protocol, apiKey?}` 用还没落盘的表单值 → `{ok, models, count, url}` |
| POST | `/api/reload` | loopback 免鉴权 | 重新加载 config.json |
| POST | `/api/workbuddy/auth/start` | loopback 免鉴权 | WorkBuddy 设备码授权：签发 state + authUrl（获取 access token 的网址）；默认代理 127.0.0.1:7897（可用 `config.proxy` / `GW_PROXY` 覆盖） |
| GET | `/api/workbuddy/auth/poll` | loopback 免鉴权 | 轮询授权结果：`?state=xx`；authorized 时自动落号渠道进 config.json |
| POST | `/api/workbuddy/batch` | loopback 免鉴权 | 批量导入：`{"tokens":["tk1","tk2",…]}` → 自动命名 wb-intl-1/2/3… 一账号一渠道，全部绑定同一模型进池子 |
| GET | `/api/tasks` | loopback 免鉴权 | 任务日志：`?limit=50` `?failed=1` `?fingerprint=auth` `?since=<ISO或毫秒>` `?until=<ISO或毫秒>`；含统计与各渠道/各错误指纹计数与尝试链 |
| GET | `/api/errors` | loopback 免鉴权 | 错误聚合视图：把每次尝试的错误摊平 + `byFingerprint` 计数（含"换家后整体成功"的尝试级错误） |
| GET | `/api/usage` | loopback 免鉴权 | Token 用量 + 金额：一次返回 `today` / `d1` / `d7` / `d30` / `d90`，每项为 `{ requests, inputTokens, outputTokens, totalTokens, cost, costComplete, unpricedTokens, unpriced, byModel }`（`byModel` 键为模型名，每项另有 `cost`）；顶层另有 `pricing: { currency, symbol, models }`。`cost` 为 `null` 表示"没配价 / 未采集"，`costComplete: false` 表示有模型未配价。口径见 §2.2「Token 用量」与「价格计费」 |
| GET | `/` | ❌ | Web 状态面板 |

> 说明：loopback 指 `127.0.0.1` / `::1` 本机来源；局域网其它机器访问这些管理接口会被拒绝（数据接口 `/v1/*` 仍要 API Key）。
>
> **管理接口的本机校验**（防止任意网页借浏览器读走 apiKey 或改写你的配置）：
> 1. `Host` 必须是本机名（`127.0.0.1` / `localhost` / `[::1]`，或 `--host` 显式绑定的地址），否则 403 —— 挡 DNS rebinding。`/health` 与 `/v1/*` 不做此校验，避免打断健康检查与局域网部署
> 2. `/api/*` 带非回环 `Origin`（含 `Origin: null` 与畸形值）一律 403，且**不返回任何 CORS 响应头** —— 挡跨站 CSRF。浏览器对跨站请求必带 `Origin`，所以外部网页 `fetch('http://127.0.0.1:8787/api/status')` 既拿不到数据也发不出写操作；本机面板与 `curl` 不受影响
> 3. `/api/status` 只回 `apiKeyMasked`（如 `sk-abc•••yz`），不回明文；`/v1/*` 的 401 响应体也不再回显真实 key
>
> CORS 现在**只发给 `/v1/*`**（模型客户端需要）。已知边界：`Origin` 只校验主机名属回环、不比对端口，所以本机另一个 web 服务（如 `127.0.0.1:9999`）理论上仍可发出同源请求；要堵这一层需比对 `Origin` 端口与监听端口，会破坏反代 / 多端口部署，故未启用。

响应头里会回传实际用到的渠道，方便排查：

```
x-gateway-channel: deepseek
x-gateway-upstream-model: deepseek-chat
x-gateway-protocol: openai
x-gateway-tier: preferred | fallback   （两段式选路命中的段）
x-gateway-effort: high                 （注入了思考强度时）
x-gateway-key: 2/3                     （叠加 key 渠道：本次命中的 key 序号/总数，只回序号）
x-gateway-key-strategy: rotate-429     （叠加 key 渠道：本条渠道的 Key 内部策略 race | rotate-429）
x-gateway-key-attempts: 3              （叠加 key 渠道：本次从起点开始共尝试了几个 key）
x-gateway-agent: my-agent        （识别出子代理身份时回传）
```

---

## 5. 状态面板

打开 <http://127.0.0.1:8787/>：每家渠道的健康/熔断/停用状态、成功率、平均延迟、模型数量、最近错误原因，5 秒自动刷新。按钮可以立即探活、重载配置。

每张渠道卡片右上角有「**编辑**」和「**×**」两个按钮：编辑会把该渠道已保存的配置回填到弹窗里改完写回 `config.json`（密钥留空即保持不变），× 删除该渠道——详见 §2.1.5。

弹窗里「模型」框旁边还有「**拉取上游模型**」按钮：按该渠道去上游拉真实模型名，**下拉点选即填入**，省得手打模型名填错——详见 §2.1.6(a)。

页面底部是**任务日志**区块：任务数 / 整体失败 / 尝试级错误三个统计、错误指纹聚合芯片（如 `insufficient_balance × 3`），以及最近 15 条请求的完整选路链（`a✗ → b✗ → c` 表示 a、b 失败后换到 c），带错误摘要与耗时。数据来自 `logs/tasks.jsonl`，用 `grep` / `jq` 直接查。

还有四块新增观测：

- **每模型健康度**：按模型聚合请求数 / 成功率 / 首字节 p50·p95 / 主要失败指纹（Top5）——一眼看出"是哪个模型在拖后腿"，而不是只看到渠道级的平均数字。跟踪表有 LRU 上界（200），因为模型名是客户端可控的。
- **最近一次请求的耗时分解**：把 `queueMs`（本地排队）/ `ttfbMs`（首字节）/ `bodyMs`（正文）画成堆叠条——慢在**排队**还是慢在**上游**，直接分得清。某段没采到就显示"未采集"，**绝不伪造 0ms**（0ms 是"真的没排队"这个有意义的值，不能被占用）。
- **全局排队等待**：`queueWaitMsTotal / queueWaitMsMax / waitedCount`（来自三级并发闸门，只在**真正等到许可**时结算，排队超时的不虚增），用来判断并发上限是否正在卡住请求。
- **Token 用量**：五个时间段卡片（今日 = 自然日；近 1 / 7 / 30 / 90 天 = 滚动 24h×N），每卡显示输入 / 输出 / 合计与请求数；下面一张**按模型柱状图**——一行一个模型，行内四个范围各一条横向堆叠柱（蓝 = 输入 `prompt_tokens`，绿 = 输出 `completion_tokens`），柱右标注该范围合计（`1.2k` / `3.4M` 紧凑格式，悬停看精确值），模型按近 90 天合计降序，**所有柱共用"全表最大用量"同一基准**所以柱长可直接横向比较；再小的用量也留 `min-width:2px` 保证看得见（窄屏下百分比下限会缩到 0.25px 等于没画，已由 `test-panel-layout.mjs` 守住）。数据来自 `GET /api/usage`，口径见 §2.2「Token 用量」：只统计上游真实回报 usage 的成功请求，某个范围或某个模型没有记录时显示**「未采集」而不是 0**（0 是"真的没用用量"这个有意义的值，不能被"没采到"占用），缺记录的格子只留空轨道、不画 0 长柱。渲染全程 `createElement` / `textContent`，不碰 `innerHTML`（模型名来自客户端与上游）。每张卡片在「输入 / 输出 / 请求」下方还有一行**费用**（`$0.0602`／未配价显示「未配置价格」／无记录显示「未采集」，金额不完整时加「（部分）」），柱状图每个范围格内也在 token 数下方标注该范围金额——币种符号取自响应里的 `pricing.symbol`，**不硬编码 `$`**。口径见 §2.2「价格计费」。

---

## 6. 排障

| 现象 | 原因 / 处理 |
|---|---|
| `/v1/models` 返回空 | 所有渠道都没填 key；或用 `--no-discover` 启动的。填 key 后点面板的"重载配置" |
| `model_not_found` | 没有任何渠道提供这个模型。检查渠道的 `models` 白名单和 `alias`。模型名是**区分大小写**的 |
| 一直走某一家，不切换 | provider 钉选（`sticky:true`）在起作用：上次成功的渠道会被反复复用直到失败——这是预期行为，失败重试 2 次仍不行会自动换家。想每轮重新洗牌就设 `"sticky": false` |
| 流量总是集中在前几家，后面的渠道从不被用 | 选路策略决定的：`priority` 只按优先级排（**不含轮换项**），`sticky`/`sessionAffinity` 还会把请求钉死在同一家。改成 `"strategy": "weighted"`（加权轮询，推荐）或 `"round-robin"`，并把 `sticky`、`sessionAffinity` 设为 `false`。注意 `tiered:true` 时第 2 段仍只在优先池用尽后才参与；想让整池一起摊开就设 `tiered:false` |
| 上游"接了连接却不给响应头"，一直干等到总超时 | 配 `firstByteTimeoutMs`（默认取该渠道的 `timeoutMs`）：首字节迟迟不来会按 `timeout` 记失败并换家，而不是等到 `timeoutMs` |
| 某模型成功率低 / 首字节很慢，但看不出是哪家上游 | 看面板的「每模型健康度」和「耗时分解」，或查 `/api/metrics` 的 `models` 段（成功率、Top5 失败指纹、`ttfb` p50/p95） |
| Claude Code 的 prompt caching 到底生效没有 | 网关对 `cache_control` 只做**保真透传**：目标是 Anthropic 协议渠道时按原块位置原样发出；目标是 OpenAI 协议渠道时会**剥掉**（OpenAI 不认这个字段，发过去零收益、纯风险）。至于是否真的命中缓存，要看上游返回的 `cache_creation_input_tokens` / `cache_read_input_tokens`——网关**不读也不改**这些字段 |
| agent 说"网关不理我"（请求挂着没响应） | 看 `/api/tasks` 里该请求的记录：`tries` / `rounds` / `waitedMs` 能看出卡在排队还是上游；总预算 `maxTotalWaitMs` 到点必返回结果。再配 `--log-level debug` 看完整尝试链 |
| agent 收到空回复 | 已被网关拦下：HTTP 200 但正文/工具调用/思考全空（GPT 系中转常见"假成功"）按"空调用"记失败并换家（kind=`empty_response`），`/api/errors` 里能看到是哪几家在返回空 |
| 正文里混着 "Step 1…" / "让我想想…" | reasoning-guard 会把思维链拆回 `reasoning_content`、正文只留正式回答；若误拆了正常回答，给该渠道配 `"guardReasoning": false` |
| 模型不思考 / 想得太久 | 默认 `forceMaxEffort:true`：能思考的模型统一注入 `routing.maxEffort`（默认 high，按模型名白名单判断）。渠道可配 `maxEffort:"xhigh"` 拉高档位、`maxEffort:"high"` 固定，或 `supportsThinking:false` 强制不加 |
| 换档位后上游 400「ReasoningEffort invalid」 | 网关在同渠道自动降为 high 重试一次（不计熔断、不耗重试预算）；仍失败才换下一家 |
| Anthropic 渠道报 400 | Anthropic 必填 `max_tokens`。给渠道加 `"maxTokens": 8192` |
| 端口被占用 | `node server.mjs --port 8788` |
| 想看每次请求的选路过程 | `node server.mjs --log-level debug` |

---

## 7. 自测

项目带一套回归（假上游 + 真网关，零依赖）。一次跑全量：

```bash
node test/run-all.mjs                  # ← 一把跑完全部套件：串行、实时打印，末尾汇总，退出码 = 失败套件数
npm test                               # 等价（package.json 的 scripts.test）
npm run test:one -- test-tiered.mjs    # 只跑一个套件
node test/run-all.mjs --list           # 列出会被跑到的套件
# 当前基线：50 个 suite / 1353 条断言 / 0 失败
```

> P5 起所有套件的端口都是**运行时动态分配**（`test/lib/ports.mjs` 的 `freePort()` /
> `mockUpstreamPorts()`），历史硬编码端口段（8793-8813 / 8877-8878 / 9101-9184 /
> 9241-9242 / 9251-9252 / 9270-9281 / 9300-9310）已全部消除；同一个套件反复跑、或先后跑不同套件
> 都不会再抢端口。**注意官方入口 `run-all.mjs` 仍然是串行的**：一是输出可读，二是少数套件会重建
> 仓库级的 `logs-test/` 目录，三是动态端口是"先申请、再交给子进程 bind"，极端并发（同时跑多个套件）
> 下仍有极小的 TOCTOU 窗口——串行可完全避开这些非端口性的相互干扰。单独跑任意一个套件也完全可用：
> `node test/test-tiered.mjs`。

各 suite（括号内为断言数）：

```bash
node test/smoke.mjs                        # 28  冒烟：鉴权、模型聚合、故障切换、熔断、双向协议转换、流式
node test/test-tiered.mjs                  # 50  两段式选路 / 余额不足·鉴权失败快速换家长冷却 / 空调用 / 思考强度注入 / 任务日志
node test/test-resilience.mjs              # 28  冷却渠道被探活救回 / 优先池全挂同轮落随机池 / 断流换家（已下发则不换家）/ 任务日志时间窗
node test/test-proxy-hardening.mjs         # 47  W4 加固：排队期断开必须归还许可 / 上游只回响应头不吐 body / 出站代理九类 bug
node test/test-proxy-timeouts.mjs          # 33  P2 验收：独立首字节超时 / waitDrain 可中断 / 出站代理看门狗 / 排队时长与三段耗时
node test/test-outbound-proxy-abort.mjs    # 30  P6：出站代理 abort 传导（CONNECT+TLS 隧道、socket 回收、并发许可归还）
node test/test-pseudo-empty.mjs            # 27  伪空响应拦截（只有空白·标点·围栏壳）+ 不误伤正常短回答
node test/test-anthropic-reasoning.mjs     # 23  OpenAI 上游 -> Anthropic 客户端：thinking 块顺序与流式帧合法性
node test/test-anthropic-thinking-compat.mjs # 57 W2：thinking 与互斥参数/强制工具选择的冲突清理、流式帧兼容
node test/test-tasklog-rotate.mjs          # 28  任务日志轮转 / 体积上限 / 落盘脱敏 / 时间窗过滤
node test/test-usage.mjs                   # 76  Token 用量 + 价格计费：按天持久化与聚合（today 自然日 / dN 滚动窗口）/ 跨天与跨重启 / 采集四条路径（非流式+流式 × OpenAI+Anthropic）/ 不造假 / 保留清理 / 上游 stream_options 400 降级 / 落盘副本脱敏 / /api/usage 与面板柱状图（含费用行、无 innerHTML 的安全渲染）/ API 安全 / 金额口径（未配价 null、显式 0 价、costComplete、JSONL 不落 cost）
node test/test-pricing.mjs                 # 50  价格计费核心：单价匹配（精确 + 最长前缀通配）/ 公式与 6 位小数 / 未配价 null 与显式 0 价 / 脏价目忽略 / 单例与热重载 / summarize 汇总与有界性
node test/test-pricing-verify.mjs          # 41  T4 独立对抗验收：金额手算核对 / §4.1 四种口径场景 / 通配与正则元字符 / 改价后历史范围立即变化 / 端到端真起网关验证 /api/usage / 面板 vm DOM stub 零 innerHTML / unpriced 有界
node test/test-panel-layout.mjs            # 24  面板柱状图的真实浏览器多分辨率布局回归（CDP 直连 Chromium，零依赖；无浏览器时自动跳过）
node test/test-reasoning-guard.mjs         # 32  各家思考字段归一、混进正文的思维链拆分（流式+非流式）、annotations 原样透传
node test/test-reasoning-guard-fp.mjs      # 35  W5：思考守卫的误切 / 去重 / 丢内容三类回归
node test/test-routing-hardening.mjs       # 31  路由加固：round-robin 真正轮换起点、冷却渠道垫到最后
node test/test-routing-notes.mjs           # 13  config 里的 `_note*` 说明键不污染选路与分层默认值
node test/test-routing-policy.mjs          # 15  两段排序、随机池洗牌、上下文路由、会话亲和带内约束、渠道内重试等待
node test/test-routing-weighted.mjs        # 32  P1 验收：优先级加权轮询 + 半开试探 + least-loaded 退化 + 每请求一次游标
node test/test-sticky-provider.mjs         # 12  provider 钉选：成功反复复用 / 失败重试 2 次才换家 / 失败后粘性失效
node test/test-multiagent.mjs              # 17  子代理识别 / 会话亲和 / 每代理配额 / 负载摊开
node test/test-concurrency.mjs             # 40  并发闸门：上限、FIFO、超时回滚、无泄漏
node test/test-toolcall-guard.mjs          # 16  正文泄漏守卫：工具调用被写成正文时换渠道
node test/test-budget.mjs                  # 24  渠道内重试预算 / 限流不降级
node test/test-effort.mjs                  # 16  思考强度映射与 effort 路由
node test/test-effort-levels.mjs           # 21  档位体系：routing.maxEffort / 渠道 maxEffort 覆盖 / 客户端显式值 / 上游拒绝档位时同渠道降档重试 / Anthropic thinking budget
node test/test-context.mjs                 # 12  上下文感知路由：窗口装不下的渠道被跳过
node test/test-rewrite.mjs                 # 12  请求改写：max_tokens 下限
node test/test-pool.mjs                    # 22  模型池：单模型渠道 / 同名多渠道路由
node test/test-multikey-race.mjs           # 21  叠加 key：同 baseUrl 多 key 并行竞速取最先成功者 / 落败请求取消 / x-gateway-key / 单 key 渠道零变化 / 全失败取非鉴权错误降级 / 面板多行 key 落 apiKeys
node test/test-multikey-rotate429.mjs      # 47  叠 Key rotate-429：环形游标 / 429 零等待快切 / 成功后游标前移 / 慢响应耐心等 / 整池 429 只扫一圈 / 401-402-403-5xx 换 Key / 普通 400 不盲扫 / 并发摊开 / race 零回归 / 响应头与叠 Key 指标
node test/test-multikey-rotate-routing.mjs # 41  渠道路由零影响：多 Key(rotate-429) vs 单 Key 对照在 priority/round-robin/weighted/least-loaded/sticky/sessionAffinity/tiered 下渠道顺序逐位一致；Key 游标推进后顺序不变；HTTP 级 x-gateway-channel 序列与对照逐位一致
node test/test-providers.mjs               # 12  自定义预设 + 模型拉取回写
node test/test-edit-channel.mjs            # 46  编辑已保存渠道：字段窄写 / 密钥留空=不变 / 改名同步 routes / 自动启用 / ${ENV} 占位保护 / CSRF
node test/test-fetch-models.mjs            # 34  按渠道拉上游模型：已保存渠道与未落盘表单值 / 自定义 modelsPath / Anthropic 头 / 401 与空列表 / 纯只读不改配置 / 不泄露密钥
node test/test-admin-security.mjs          # 34  管理面安全：跨站 Origin 被拒 / 非法 Host 403 / 面板照常可用 / key 不外泄
node test/test-metrics-observability.mjs   # 58  P3 验收：三段耗时分解 / 模型级指标 / 面板
node test/test-cache-control.mjs           # 51  P4 验收：prompt caching 透传 + top_k 转发 + 思考预算（纯单元，不占端口）
node test/test-openai-compat.mjs           # 50  openai 适配器兼容：stream_options / tool_calls / usage 等 6 个已确认 bug
node test/test-classify-hardening.mjs      # 30  util 加固：parseSSE 的 CRLF、普通 400 与 modelIssue/auth 的区分
node test/test-retry.mjs                   # 12  两层重试：渠道内重试 / 渠道耗尽切换 / 轮询上限 / 401 不重试
node test/test-unified-bug.mjs             # 8   统一池：429 TPM 不降级、404 渠道不再被选中
node test/test-probe.mjs                   # 4   探活口径：chat 500 但 models 200 不算健康
node test/test-nocap.mjs                   # 2   无重试上限时持续重试、客户端断开后网关存活
node test/test-workbuddy.mjs               # 69  WorkBuddy 国际版：批量导入自动落号 / 设备码授权 / 出站风控脱敏 / 余额与续期 / forceStream 聚合 / 出站代理（假 CONNECT 隧道）

# —— 以下 9 个由 2026-09-20 的代码审查（见 LLM-Gateway-Code-Review.md）与发布打包一并引入 ——
node test/test-stream-integrity.mjs        # 15  F1 流式完整性：上游流内错误 / 无终止信号的 EOF 不得被包装成「假成功」；兼容「有 finish_reason 但没发 [DONE]」的上游
node test/test-parallel-tools.mjs          # 14  F2 OpenAI→Anthropic：交错并行工具调用按 toolIndex 独立缓冲，不再被拆成多余残缺块
node test/test-total-deadline.mjs          # 10  F3 请求级绝对总时限贯穿排队 / 上游 / 读体 / 背压 / 重试等待；0 = 不限
node test/test-embeddings-lifecycle.mjs    # 9   F4 embeddings 接入客户端断开与请求级超时，取消后立即归还并发名额
node test/test-agent-admission.mjs         # 10  F6 排队请求不得预占全局 / 渠道名额（先拿 agent 名额再抢共享容量），不饿死其它 agent
node test/test-usage-nested.mjs            # 7   F5 usage 采集支持嵌套 details，成功请求不再漏记 token
node test/test-reload-concurrency.mjs      # 11  F7 热重载改并发上限不丢在途计数；调高立即唤醒等待者
node test/test-half-open-limit.mjs         # 9   F8 半开窗口的并发上限强制准入（不只是排序）；被挡下的请求不计渠道故障
node test/test-release-package.mjs         # 36  发布包守门：排除规则 / .gitignore 漂移检查 / zip 字节级复核（独立 CRC）/ 不含真实密钥
```

### 首字延迟量测（TTFT）

流式方向为了拆分"混进正文的思维链"和做空响应检测，会先把响应头攒在内存里再下发——这会推迟首字。
用 `node test/bench-ttft.mjs` 量测这个代价（上游先发一小段正文、停顿 400ms 再继续，取 7 次中位数）：

| 场景 | 首字延迟（median） |
|---|---|
| `reasoningGuardStream: true`（默认，攒头部） | **435 ms** |
| `reasoningGuardStream: false`（不做流式思考守卫） | **32 ms** |
| `reasoningGuardStream: true` + 上游已用 `reasoning_content` 承载思考 | **31 ms** |

结论：攒头部确实会把首字推迟（≈上游的停顿时长 403ms），但**只要上游用 `reasoning_content`/`thinking`
明确承载思考，网关就会立即放行，代价≈0**（31ms vs 32ms）。因此默认值保持 `true`——
换来的是"思维链不泄漏进正文"和"伪空响应被拦下"这两个对 agent 体验更重要的保证。
只有在极端追求首字延迟、且确认上游不会把思维链写进正文时，才建议关掉它。

### 并发压测

```bash
node test/bench-concurrency.mjs
# 场景 A 稳态吞吐：400 请求 / 0 失败 / ~140 req/s，上游峰值并发被压到单渠道上限 8
# 场景 B 上游全 500：120 请求全部快速返回 503（不挂死），熔断计数被并发去抖压到 42/360
# 场景 C 慢客户端背压：32 个 ~3.2MB 响应，网关 RSS 增长 ~16MB（背压生效，不随响应体积膨胀）
```


---

## 8. 目录结构

```
llm-gateway/
├── server.mjs               入口：HTTP 服务、鉴权、路由分发
├── config.json              你的配置（含密钥，已 gitignore）
├── config.example.json      配置模板
├── providers.json           自定义 provider 预设（可选，已 gitignore）
├── providers.example.json   预设模板
├── start.ps1                PowerShell 7 启动脚本
├── package.json            零依赖；type: module；scripts: test / test:one / start / package（无 dependencies）
├── LLM-Gateway-Code-Review.md 代码审查报告（F1–F8）+ 逐条修复记录与守门测试对照
├── lib/
│   ├── channels.mjs         渠道注册表：配置、模型发现、熔断、选路（含会话亲和）
│   ├── presets.mjs          供应商预设：内置 + providers.json 自定义（含 workbuddy-intl）
│   ├── proxy.mjs            转发：两段式选路、两层重试、并发闸门、背压、流式处理、错误分类、空响应拦截
│   ├── workbuddy.mjs        WorkBuddy 国际版：设备码授权 / 用户信息 / 余额聚合 / token 续期 / 出站脱敏 / SSE 聚合
│   ├── outbound-proxy.mjs   零依赖出站代理：HTTP CONNECT 隧道 + 绝对形式转发（gwFetch，Response 兼容）
│   ├── concurrency.mjs      并发闸门：全局/渠道/子代理三级信号量 + FIFO 队列
│   ├── agent.mjs            子代理识别：header / body / api-key 指纹
│   ├── retry.mjs            渠道内重试策略与等待
│   ├── toolcall-guard.mjs   正文泄漏守卫：识别"工具调用被写成正文"的上游响应
│   ├── reasoning-guard.mjs  思考守卫：各家思考字段归一 + 混进正文的思维链拆分（reasoning_content）
│   ├── tasklog.mjs          任务日志：每请求一条 JSONL（尝试链 / 错误指纹 / 耗时），内存环形缓冲供 API 查
│   ├── usage.mjs            Token 用量：按天 JSONL（本地时区分天）+ 按天聚合缓存（LRU），today/dN 两种口径
│   ├── pricing.mjs          价格计费：单价表（精确 + 最长前缀通配）+ 金额汇总，未配价返回 null 不当 0 算
│   ├── logger.mjs           日志（缓冲批量写，高并发不阻塞事件循环）
│   ├── util.mjs             URL / SSE / 工具 / 上游错误分类 / 思考能力白名单
│   └── adapters/
│       ├── openai.mjs       OpenAI 兼容协议（含反向转 Anthropic、各家思考参数兼容）
│       └── anthropic.mjs    Anthropic 原生协议（含双向消息/流式转换）
├── logs/tasks.jsonl         任务日志落盘（taskLog.dir/file 可改；超过 maxFileBytes 轮转为 tasks.1.jsonl…）
├── logs/usage/               Token 用量落盘（YYYY-MM-DD.jsonl，本地时区分天；保留 taskLog.usageKeepDays 天）
├── public/index.html        状态面板（含任务日志视图）
├── tools/package.mjs        发布打包器（零依赖：自建 ZIP + sha256 收据；排除规则见 §9）
├── dist/                    打包产物（已 gitignore）：llm-gateway-<version>.zip + .zip.sha256
└── test/
    ├── lib/ports.mjs       动态端口工具：freePort / freePortBlock / mockUpstreamPorts / materializeConfig
    ├── run-all.mjs         一把跑完全部 suite（串行、实时输出、末尾汇总，退出码 = 失败套件数）
    ├── mock-upstream.mjs   假上游（逻辑端口 9101-9144：正常/500/429/余额不足/鉴权失败/空响应/伪空/思考/断流/可复活…；9141-9142：WorkBuddy 全家桶 + 风控；9144：叠加 key 按 key 区分行为）
    │                       逻辑端口可经环境变量 MOCK_PORT_BASE 整体平移；测试运行时动态分配并传进去
    ├── *.test.json          各 suite 的网关配置（固定端口只是逻辑标识，运行时被 materializeConfig 改写成动态端口）
    ├── smoke.mjs            冒烟
    ├── test-*.mjs           回归 suite（见 §7）
    ├── bench-concurrency.mjs 并发压测：吞吐 / 上游峰值限流 / 熔断去抖 / 慢客户端背压
    └── bench-ttft.mjs       首字延迟量测：reasoningGuardStream 开/关的端到端 TTFT
```

---

## 9. 发布打包

`dist/llm-gateway-<version>.zip` 是可直接分发的发布包（版本号取自 `package.json`）。构建：

```bash
npm run package                     # 等价于 node tools/package.mjs
npm run package:list                # 只列出会打包哪些文件（不写盘）
node tools/package.mjs --out D:\tmp # 换输出目录
```

打包器**零依赖**——只用 Node 内置 `zlib` / `crypto`，ZIP 结构与 CRC32 自己写。两个理由：
项目本身零运行时依赖，打包链不该引入 7-Zip / Compress-Archive 这类外部工具；
更关键的是**排除规则要能被测试断言**，外部工具的命令行参数没法在这里做漂移检查。

产出两个文件：`llm-gateway-<version>.zip` 与 `llm-gateway-<version>.zip.sha256`
（收据，格式 `<hash>  <文件名>`，可 `sha256sum -c` ）。

### 9.1 打包内容

进包：`server.mjs`、`lib/`、`public/`、`test/`、`tools/`、`docs/`、`config.example.json`、
`providers.example.json`、`package.json`、`start.ps1`、`.gitignore` 以及各 Markdown 文档。

**不进包**（与 `.gitignore` 的「发布安全」段一一对应，改其一必须同步另一处）：

| 排除项 | 原因 |
|---|---|
| `config.json` | 你的真实配置，含密钥 |
| `providers.json` | 自定义 provider 预设，可能含密钥 |
| `config.wbtest.json` | 个人测试实例（含本机代理端口等个人环境信息） |
| `docs/workbuddy-intl-task.md` | 内部任务书：含第三方私有端点与逆向结论 |
| `.workbuddy-ai/` | AI 工具的记忆 / 会话元数据，不属于项目源码 |
| `logs/`、`logs-test/` | 任务日志（含上游错误文本），运行时产物 |
| `dist/` | 打包产物自身 |
| `handover-token-usage-dashboard.md`（根目录） | 旧副本；正式版是 `docs/handover-token-usage-dashboard.md` |
| `node_modules/`、`*.local.json`、`*.log`、`.DS_Store` | 依赖目录 / 运行时产物 / 系统文件 |

### 9.2 守门测试

`node test/test-release-package.mjs`（36 条断言，已并入 `npm test`）把设计文档里的
「生成的发布包不含任何真实 key」从**人工复验**变成**自动断言**，分三层：

1. **规则层**：排除表必须覆盖 `.gitignore` 的每一条（漂移检查）。往 `.gitignore` 新加一条敏感规则
   却忘了同步打包器，测试立刻变红；同时有 13 条反例断言，防止排除写宽把源码一起排掉。
2. **字节层**：真建一个 zip，自己解析中央目录 + inflate，逐文件与源树做**逐字节**比对，并用
   **独立实现的 CRC32**（按位算，不复用打包器的查表版）复核，避免同错同销；另校验 sha256 收据
   与实际字节一致、EOCD 位置、以及路径卫生（全部 `llm-gateway/` 前缀、无绝对路径 / `..` / 反斜杠）。
3. **内容层**：包内文本文件里出现的密钥特征必须全部落在「已知假 key」白名单里，白名单本身还受
   一条反向断言约束（名字是否名存实亡）；`config.example.json` 各渠道的 `apiKey` / `apiKeys`
   只允许 `${ENV}` / `PROXY_MANAGED` / `TESTKEY` 这类占位形态。

> **已知且刻意保留**：`test/test-outbound-proxy-abort.mjs` 内含一份 **throwaway 自签证书私钥**
> （CN=localhost，仅用于出站代理的 CONNECT + TLS 回归）。它是测试夹具、不是真实凭据，
> 测试里也断言了它只能出现在这一个文件中。介意的话打包前删掉该套件即可。
