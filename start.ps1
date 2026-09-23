#Requires -Version 7.0
<#
  Local LLM Gateway launcher (PowerShell 7+)
  Usage:
    .\start.ps1
    .\start.ps1 -Port 8788 -LogLevel debug
    .\start.ps1 -Config .\config.json
#>
param(
    [int]    $Port,
    [string] $Bind,
    [string] $Config,
    [string] $ApiKey,
    [ValidateSet('debug', 'info', 'warn', 'error', 'silent')]
    [string] $LogLevel = 'info',
    [switch] $NoDiscover
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# --- locate node ---
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
if (-not $node) { Write-Error 'node.exe not found. Install Node.js >= 20 or add it to PATH.' }

Write-Host "node : $node" -ForegroundColor DarkGray
Write-Host ("ver  : " + (& $node -v)) -ForegroundColor DarkGray

# --- config bootstrap ---
if (-not $Config) { $Config = Join-Path $root 'config.json' }
if (-not (Test-Path $Config)) {
    $example = Join-Path $root 'config.example.json'
    if (Test-Path $example) {
        Copy-Item $example $Config
        Write-Host ''
        Write-Host "config.json created from template: $Config" -ForegroundColor Yellow
        Write-Host '  -> fill in your apiKey (or set the env vars) then start again.' -ForegroundColor Yellow
        Write-Host ''
    }
}

# --- build args ---
$serverArgs = @((Join-Path $root 'server.mjs'))
if ($Port)         { $serverArgs += @('--port', "$Port") }
if ($Bind)         { $serverArgs += @('--host', $Bind) }
if ($Config)       { $serverArgs += @('--config', $Config) }
if ($ApiKey)       { $serverArgs += @('--api-key', $ApiKey) }
if ($LogLevel)     { $serverArgs += @('--log-level', $LogLevel) }
if ($NoDiscover)   { $serverArgs += '--no-discover' }

Write-Host "config: $Config" -ForegroundColor DarkGray
Write-Host ''

$env:GW_LOG_LEVEL = $LogLevel
& $node @serverArgs
