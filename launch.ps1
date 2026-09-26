#Requires -Version 7.0
<#
  Local LLM Gateway — 一键启动脚本 (PowerShell 7+)

  做了什么：
    1. 若 config.json 不存在，自动从 config.example.json 复制一份
    2. 定位 node（>= 20）
    3. 后台拉起 server.mjs（日志实时打印到本窗口）
    4. 轮询 /health 直到就绪
    5. 自动用默认浏览器打开 Web 状态面板
    6. 按 Ctrl+C 干净停止网关（连带子进程一起退）

  用法：
    .\launch.ps1                       # 默认 127.0.0.1:8787，自动开面板
    .\launch.ps1 -Port 8788            # 换端口
    .\launch.ps1 -Bind 0.0.0.0         # 允许局域网其它机器连
    .\launch.ps1 -NoBrowser            # 不自动开浏览器
    .\launch.ps1 -Stop                 # 停止正在运行的网关（按端口匹配）
    .\launch.ps1 -LogLevel debug       # 调试日志
#>
param(
    [int]    $Port,
    [string] $Bind,
    [string] $Config,
    [string] $ApiKey,
    [ValidateSet('debug', 'info', 'warn', 'error', 'silent')]
    [string] $LogLevel = 'info',
    [switch] $NoDiscover,
    [switch] $NoBrowser,
    [switch] $Stop
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# ---------------------------------------------------------------------------
# 解析端口 / 绑定地址（参数优先，否则读 config；再否则用默认）
# ---------------------------------------------------------------------------
function Get-ResolvedServer {
    param([int]$PortArg, [string]$BindArg, [string]$ConfigArg)
    $cfgPath = if ($ConfigArg) { $ConfigArg } else { Join-Path $root 'config.json' }
    $host_ = '127.0.0.1'
    $port_ = 8787
    if (Test-Path $cfgPath) {
        try {
            $cfg = Get-Content $cfgPath -Raw -Encoding utf8 | ConvertFrom-Json
            if ($cfg.server.host) { $host_ = $cfg.server.host }
            if ($cfg.server.port) { $port_ = $cfg.server.port }
        } catch {
            Write-Warning "config.json 解析失败，回退到默认 127.0.0.1:8787（$_.Exception.Message）"
        }
    }
    if ($BindArg) { $host_ = $BindArg }
    if ($PortArg) { $port_ = $PortArg }
    [PSCustomObject]@{ Host = $host_; Port = $port_ }
}

# ---------------------------------------------------------------------------
# 一键停止：按端口找到监听进程并杀掉
# ---------------------------------------------------------------------------
if ($Stop) {
    $srv = Get-ResolvedServer -PortArg $Port -BindArg $Bind -ConfigArg $Config
    $listenHost = if ($srv.Host -eq '0.0.0.0') { '*' } else { $srv.Host }
    Write-Host "正在停止监听于 $($srv.Port) 的网关进程..." -ForegroundColor Yellow
    $conns = Get-NetTCPConnection -LocalPort $srv.Port -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq 'Listen' }
    if (-not $conns) {
        Write-Host "未找到占用端口 $($srv.Port) 的进程（可能未运行）。" -ForegroundColor DarkGray
        return
    }
    $killed = 0
    foreach ($pid_ in ($conns.OwningProcess | Sort-Object -Unique)) {
        try {
            $p = Get-Process -Id $pid_ -ErrorAction SilentlyContinue
            if ($p) {
                Write-Host "  停止 PID $pid_ ($($p.ProcessName))" -ForegroundColor DarkGray
                $p.Kill()
                $killed++
            }
        } catch {
            Write-Warning "  无法停止 PID $pid_：$_.Exception.Message"
        }
    }
    if ($killed -gt 0) {
        Write-Host "已停止 $killed 个网关进程。" -ForegroundColor Green
    } else {
        Write-Host "没有可停止的进程。" -ForegroundColor DarkGray
    }
    return
}

# ---------------------------------------------------------------------------
# 定位 node
# ---------------------------------------------------------------------------
$nodeCandidates = @(
    (Join-Path $env:USERPROFILE '.workbuddy\binaries\node\versions\22.22.2\node.exe'),
    (Join-Path $env:USERPROFILE '.workbuddy\binaries\node\versions\*\node.exe')
)
$node = $null
foreach ($c in $nodeCandidates) {
    $found = Get-Item $c -ErrorAction SilentlyContinue | Sort-Object { $_.FullName } -Descending | Select-Object -First 1
    if ($found) { $node = $found.FullName; break }
}
if (-not $node) { $node = (Get-Command node -ErrorAction SilentlyContinue).Source }
if (-not $node) { Write-Error 'node.exe not found. 请安装 Node.js >= 20 或加入 PATH。' }

Write-Host "node : $node" -ForegroundColor DarkGray
Write-Host ("ver  : " + (& $node -v)) -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# 配置引导：缺失则复制模板
# ---------------------------------------------------------------------------
if (-not $Config) { $Config = Join-Path $root 'config.json' }
if (-not (Test-Path $Config)) {
    $example = Join-Path $root 'config.example.json'
    if (Test-Path $example) {
        Copy-Item $example $Config
        Write-Host ''
        Write-Host "已从模板创建 config.json：$Config" -ForegroundColor Yellow
        Write-Host '  -> 如需接入供应商，填好 apiKey（或设置环境变量）后重新运行本脚本。' -ForegroundColor Yellow
        Write-Host '  -> 未填 key 的渠道会自动停用，面板仍可正常打开。' -ForegroundColor Yellow
        Write-Host ''
    }
}

# providers.json 引导：config 里部分渠道（如 my-provider-*）依赖自定义预设，
# 缺失会导致「找不到 preset」的配置加载失败。
$providersPath = Join-Path $root 'providers.json'
if (-not (Test-Path $providersPath)) {
    $providersExample = Join-Path $root 'providers.example.json'
    if (Test-Path $providersExample) {
        Copy-Item $providersExample $providersPath
        Write-Host "已从模板创建 providers.json：$providersPath" -ForegroundColor Yellow
    }
}

$srv = Get-ResolvedServer -PortArg $Port -BindArg $Bind -ConfigArg $Config
$panelHost = if ($srv.Host -eq '0.0.0.0') { '127.0.0.1' } else { $srv.Host }
$panelUrl  = "http://${panelHost}:$($srv.Port)/"

# ---------------------------------------------------------------------------
# 组装启动参数
# ---------------------------------------------------------------------------
$serverArgs = @((Join-Path $root 'server.mjs'))
if ($Port)       { $serverArgs += @('--port', "$Port") }
if ($Bind)       { $serverArgs += @('--host', $Bind) }
if ($Config)     { $serverArgs += @('--config', $Config) }
if ($ApiKey)     { $serverArgs += @('--api-key', $ApiKey) }
if ($LogLevel)   { $serverArgs += @('--log-level', $LogLevel) }
if ($NoDiscover) { $serverArgs += '--no-discover' }

$env:GW_LOG_LEVEL = $LogLevel

Write-Host "config: $Config" -ForegroundColor DarkGray
Write-Host "面板  : $panelUrl" -ForegroundColor Cyan
Write-Host "端点  : http://${panelHost}:$($srv.Port)/v1" -ForegroundColor Cyan
Write-Host ''
Write-Host '正在启动网关（日志实时输出，按 Ctrl+C 停止）...' -ForegroundColor Green
Write-Host ('-' * 60) -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# 后台启动（日志直接打到本窗口），保留进程对象以便退出时清理
# ---------------------------------------------------------------------------
$proc = Start-Process -FilePath $node -ArgumentList $serverArgs `
    -WorkingDirectory $root -NoNewWindow -PassThru

# 健康检查：轮询 /health（最多 30 秒）
$ready = $false
$deadline = (Get-Date).AddSeconds(30)
while ((Get-Date) -lt $deadline) {
    if ($proc.HasExited) {
        Write-Error "网关进程在就绪前退出（退出码 $($proc.ExitCode)）。请检查上面的日志。"
    }
    try {
        $r = Invoke-WebRequest -Uri "http://${panelHost}:$($srv.Port)/health" `
            -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($r.StatusCode -eq 200) { $ready = $true; break }
    } catch {
        # 还没起来，继续等
    }
    Start-Sleep -Milliseconds 500
}

if (-not $ready) {
    Write-Error "30 秒内未收到 /health 就绪信号。请查看上方日志排查。"
}

Write-Host ('=' * 60) -ForegroundColor DarkGray
Write-Host "网关已就绪 ✅" -ForegroundColor Green
Write-Host "面板地址：$panelUrl" -ForegroundColor Cyan

if (-not $NoBrowser) {
    try {
        Write-Host '正在打开浏览器面板...' -ForegroundColor DarkGray
        Start-Process $panelUrl
    } catch {
        Write-Warning "无法自动打开浏览器，请手动访问：$panelUrl"
    }
}

Write-Host ''
Write-Host '按 Ctrl+C 停止网关。' -ForegroundColor Yellow
Write-Host ('-' * 60) -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# 保持前台存活；Ctrl+C / 异常退出时清理子进程
# ---------------------------------------------------------------------------
try {
    while (-not $proc.HasExited) {
        Start-Sleep -Seconds 1
    }
    if ($proc.HasExited) {
        Write-Host "网关进程已自行退出（退出码 $($proc.ExitCode)）。" -ForegroundColor Yellow
    }
} finally {
    if ($proc -and -not $proc.HasExited) {
        Write-Host ''
        Write-Host '正在停止网关...' -ForegroundColor Yellow
        try { $proc.Kill() } catch { }
        try { $proc.WaitForExit(5000) } catch { }
    }
    Write-Host '已退出。' -ForegroundColor DarkGray
}
