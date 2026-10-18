#!/usr/bin/env pwsh
<#
  openwa-rs real-engine check.

  Boots the gateway against the real Node/Baileys sidecar (`sidecar/`) instead of
  the mock, creates a session, starts it, and waits for WhatsApp to hand back a
  real pairing QR. Nothing is paired or sent, so no account is touched: the
  session is stopped and deleted at the end.

  This proves the full Phase 1 path: Rust gateway -> stdio JSON-RPC -> Baileys ->
  WhatsApp servers -> event back to the gateway.

  Usage:
      pwsh scripts/verify-real-sidecar.ps1
#>
[CmdletBinding()]
param(
    [int]$GatewayPort = 2791,
    [string]$ApiKey = 'real-sidecar-check',
    [int]$WaitSeconds = 60
)

$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$isWindowsHost = ($env:OS -eq 'Windows_NT')
$binName = if ($isWindowsHost) { 'openwa-gateway.exe' } else { 'openwa-gateway' }
$gatewayExe = Join-Path (Join-Path (Join-Path $repoRoot 'target') 'debug') $binName

$tempRoot = if ($env:TEMP) { $env:TEMP } else { [System.IO.Path]::GetTempPath() }
$runDir = Join-Path $tempRoot ('openwa-real-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$gatewayBase = "http://127.0.0.1:$GatewayPort"
$headers = @{ 'X-API-Key' = $ApiKey }
$failures = 0
$gatewayProc = $null
$sessionId = $null

function Write-Pass([string]$name) { Write-Host "PASS $name" }
function Write-Fail([string]$name, [string]$detail) {
    Write-Host "FAIL $name - $detail"
    $script:failures++
}
function Assert-That {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][bool]$Condition,
        [string]$Detail = 'assertion failed'
    )
    if ($Condition) { Write-Pass $Name } else { Write-Fail $Name $Detail }
}

try {
    Write-Host "openwa-rs real sidecar check (run dir: $runDir)"

    if (-not (Test-Path -LiteralPath (Join-Path $repoRoot 'sidecar\node_modules'))) {
        Write-Fail 'sidecar dependencies installed' 'run: npm --prefix sidecar install'
        exit 1
    }

    & cargo build -p openwa-gateway
    if ($LASTEXITCODE -ne 0) { Write-Fail 'cargo build -p openwa-gateway' "exit code $LASTEXITCODE"; exit 1 }
    Write-Pass 'cargo build -p openwa-gateway'

    $env:SIDECAR_DIR = Join-Path $repoRoot 'sidecar'
    $env:PORT = "$GatewayPort"
    $env:API_KEY = $ApiKey
    $env:DATA_DIR = $runDir
    $env:NODE_BIN = 'node'

    $gatewayProc = Start-Process -FilePath $gatewayExe -WorkingDirectory $repoRoot -PassThru -NoNewWindow `
        -RedirectStandardOutput (Join-Path $runDir 'gateway.out.log') `
        -RedirectStandardError (Join-Path $runDir 'gateway.err.log')

    $up = $false
    $deadline = (Get-Date).AddSeconds(30)
    while (-not $up -and (Get-Date) -lt $deadline) {
        if ($gatewayProc.HasExited) { break }
        try {
            $health = Invoke-RestMethod -Uri "$gatewayBase/api/health" -Headers $headers -TimeoutSec 5
            if ($health.sidecar -eq 'up') { $up = $true; break }
        }
        catch { }
        Start-Sleep -Milliseconds 300
    }
    Assert-That 'health reports the real Baileys sidecar up' $up "see $(Join-Path $runDir 'gateway.err.log')"

    if ($up) {
        $created = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions" -Headers $headers `
            -ContentType 'application/json' -Body (@{ name = 'real-check' } | ConvertTo-Json)
        $sessionId = $created.sessionId
        Assert-That 'session created' ([bool]$sessionId) "response: $($created | ConvertTo-Json -Compress)"

        $null = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/start" -Headers $headers

        # Baileys opens a socket to WhatsApp and emits a QR when the server offers one.
        $status = $null
        $qr = $null
        $deadline = (Get-Date).AddSeconds($WaitSeconds)
        while ((Get-Date) -lt $deadline) {
            $current = Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$sessionId" -Headers $headers
            $status = $current.status
            if ($status -in @('qr_ready', 'connected')) {
                $qr = (Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$sessionId/qr" -Headers $headers).qr
                if ($qr) { break }
            }
            Start-Sleep -Milliseconds 500
        }

        Assert-That 'session reached qr_ready (WhatsApp answered)' ($status -eq 'qr_ready' -or $status -eq 'connected') "status=$status after ${WaitSeconds}s"
        Assert-That 'gateway relayed a real pairing QR' ([bool]$qr) 'no QR payload came back'

        if ($qr) {
            $preview = if ($qr.Length -gt 40) { $qr.Substring(0, 40) + '...' } else { $qr }
            Write-Host "     qr preview: $preview"
        }

        $null = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/stop" -Headers $headers
        $null = Invoke-RestMethod -Method Delete -Uri "$gatewayBase/api/sessions/$sessionId" -Headers $headers
        Write-Pass 'session stopped and deleted'
    }
}
catch {
    Write-Fail 'unexpected error' $_.Exception.Message
}
finally {
    if ($null -ne $gatewayProc -and -not $gatewayProc.HasExited) {
        try { Stop-Process -Id $gatewayProc.Id -Force -ErrorAction SilentlyContinue } catch { }
    }
    Remove-Item Env:\SIDECAR_DIR, Env:\PORT, Env:\API_KEY, Env:\DATA_DIR, Env:\NODE_BIN -ErrorAction SilentlyContinue
}

if ($failures -gt 0) {
    Write-Host "REAL SIDECAR CHECK FAILED - $failures assertion(s)"
    exit 1
}
Write-Host 'REAL SIDECAR CHECK PASSED'
exit 0
