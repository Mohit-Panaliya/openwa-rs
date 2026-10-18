#!/usr/bin/env pwsh
<#
  openwa-rs memory measurement.

  Starts the gateway against the real Baileys sidecar and reports working-set
  memory for the Rust process and the Node engine process, first idle and then
  with N sessions holding a live WhatsApp multi-device socket.

  No account is paired: each session is left in qr_ready (a live socket to
  WhatsApp's servers waiting for a QR scan), which is the realistic steady state
  for an unpaired session. Nothing is sent.

  Usage:
      pwsh scripts/measure-memory.ps1
      pwsh scripts/measure-memory.ps1 -Sessions 3 -Configuration release
#>
[CmdletBinding()]
param(
    [int]$GatewayPort = 2792,
    [int]$Sessions = 1,
    [string]$ApiKey = 'memory-check',
    [ValidateSet('debug', 'release')]
    [string]$Configuration = 'release',
    [int]$WaitSeconds = 90
)

$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$isWindowsHost = ($env:OS -eq 'Windows_NT')
$binName = if ($isWindowsHost) { 'openwa-gateway.exe' } else { 'openwa-gateway' }
$gatewayExe = Join-Path (Join-Path (Join-Path $repoRoot 'target') $Configuration) $binName

$tempRoot = if ($env:TEMP) { $env:TEMP } else { [System.IO.Path]::GetTempPath() }
$runDir = Join-Path $tempRoot ('openwa-mem-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$gatewayBase = "http://127.0.0.1:$GatewayPort"
$headers = @{ 'X-API-Key' = $ApiKey }
$gatewayProc = $null
$sessionIds = @()
$nodeStart = $null

function Get-RssMb {
    param([Parameter(Mandatory)][string]$Name, [datetime]$Since)
    $procs = @(Get-Process -Name $Name -ErrorAction SilentlyContinue |
        Where-Object { $_.StartTime -ge $Since })
    if ($procs.Count -eq 0) { return @{ Count = 0; WorkingSet = 0; Private = 0 } }
    @{
        Count      = $procs.Count
        WorkingSet = [math]::Round((($procs | Measure-Object -Property WorkingSet64 -Sum).Sum) / 1MB, 1)
        Private    = [math]::Round((($procs | Measure-Object -Property PrivateMemorySize64 -Sum).Sum) / 1MB, 1)
    }
}

function Show-Sample {
    param([string]$Label, [datetime]$Since)
    $node = Get-RssMb -Name 'node' -Since $Since
    $gw = Get-RssMb -Name 'openwa-gateway' -Since $Since
    $total = [math]::Round($gw.WorkingSet + $node.WorkingSet, 1)
    Write-Host ("{0,-34} gateway {1,8} MB   node x{2} {3,8} MB   total {4,8} MB   (private: gateway {5} MB, node {6} MB)" -f `
            $Label, $gw.WorkingSet, $node.Count, $node.WorkingSet, $total, $gw.Private, $node.Private)
    return @{ Gateway = $gw.WorkingSet; Node = $node.WorkingSet; Total = $total }
}

try {
    Write-Host "openwa-rs memory measurement ($Configuration build, run dir: $runDir)"

    if (-not (Test-Path -LiteralPath $gatewayExe)) {
        throw "gateway binary not found at $gatewayExe - run: cargo build --release -p openwa-gateway"
    }
    if (-not (Test-Path -LiteralPath (Join-Path $repoRoot 'sidecar\node_modules'))) {
        throw 'sidecar dependencies missing - run: npm --prefix sidecar install'
    }

    $env:SIDECAR_DIR = Join-Path $repoRoot 'sidecar'
    $env:PORT = "$GatewayPort"
    $env:API_KEY = $ApiKey
    $env:DATA_DIR = $runDir
    $env:NODE_BIN = 'node'

    $started = Get-Date
    $gatewayProc = Start-Process -FilePath $gatewayExe -WorkingDirectory $repoRoot -PassThru -NoNewWindow `
        -RedirectStandardOutput (Join-Path $runDir 'gateway.out.log') `
        -RedirectStandardError (Join-Path $runDir 'gateway.err.log')

    $up = $false
    $deadline = (Get-Date).AddSeconds(30)
    while (-not $up -and (Get-Date) -lt $deadline) {
        if ($gatewayProc.HasExited) { throw "gateway exited early; see $runDir\gateway.err.log" }
        try {
            $health = Invoke-RestMethod -Uri "$gatewayBase/api/health" -Headers $headers -TimeoutSec 5
            if ($health.sidecar -eq 'up') { $up = $true }
        }
        catch { }
        if (-not $up) { Start-Sleep -Milliseconds 300 }
    }
    if (-not $up) { throw "sidecar did not come up; see $runDir\gateway.err.log" }

    Start-Sleep -Seconds 2
    $idle = Show-Sample -Label "idle (0 sessions)" -Since $started

    if ($Sessions -gt 0) {
        foreach ($n in 1..$Sessions) {
            $created = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions" -Headers $headers `
                -ContentType 'application/json' -Body (@{ name = "mem-$n" } | ConvertTo-Json)
            $sessionIds += $created.sessionId
            $null = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$($created.sessionId)/start" -Headers $headers
        }

        $ready = 0
        $deadline = (Get-Date).AddSeconds($WaitSeconds)
        while ($ready -lt $sessionIds.Count -and (Get-Date) -lt $deadline) {
            $ready = 0
            foreach ($id in $sessionIds) {
                $current = Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$id" -Headers $headers
                if ($current.status -in @('qr_ready', 'connected')) { $ready++ }
            }
            if ($ready -lt $sessionIds.Count) { Start-Sleep -Milliseconds 500 }
        }
        Write-Host "      sessions socket-ready: $ready/$($sessionIds.Count)"

        Start-Sleep -Seconds 2
        $loaded = Show-Sample -Label "$($sessionIds.Count) session(s) socket-ready" -Since $started

        if ($idle.Total -gt 0) {
            $perSession = [math]::Round(($loaded.Total - $idle.Total) / [math]::Max($sessionIds.Count, 1), 1)
            Write-Host ("      marginal cost per session: ~{0} MB" -f $perSession)
        }
    }

    foreach ($id in $sessionIds) {
        try { $null = Invoke-RestMethod -Method Delete -Uri "$gatewayBase/api/sessions/$id" -Headers $headers } catch { }
    }
}
finally {
    if ($null -ne $gatewayProc -and -not $gatewayProc.HasExited) {
        try { Stop-Process -Id $gatewayProc.Id -Force -ErrorAction SilentlyContinue } catch { }
    }
    Remove-Item Env:\SIDECAR_DIR, Env:\PORT, Env:\API_KEY, Env:\DATA_DIR, Env:\NODE_BIN -ErrorAction SilentlyContinue
}
