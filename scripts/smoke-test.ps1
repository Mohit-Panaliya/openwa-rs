#!/usr/bin/env pwsh
<#
  openwa-rs end-to-end smoke test.

  Builds the gateway, runs it against the deterministic mock sidecar
  (tests/mock-sidecar) with a throwaway webhook receiver, and exercises the
  core REST surface. No WhatsApp network traffic is involved.

  Usage:
      pwsh scripts/smoke-test.ps1

  Exits 0 when every assertion passes, 1 otherwise.
#>
[CmdletBinding()]
param(
    [int]$GatewayPort = 2790,
    [int]$WebhookPort = 2899,
    [string]$ApiKey = 'smoke-test-key'
)

$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$isWindowsHost = ($env:OS -eq 'Windows_NT')
$tempRoot = if ($env:TEMP) { $env:TEMP } elseif ($env:TMPDIR) { $env:TMPDIR } else { [System.IO.Path]::GetTempPath() }

$runDir = Join-Path $tempRoot ('openwa-smoke-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$capturePath = Join-Path $tempRoot 'openwa-webhook-capture.jsonl'
Remove-Item -LiteralPath $capturePath -Force -ErrorAction SilentlyContinue

$sidecarDir = Join-Path $repoRoot (Join-Path 'tests' 'mock-sidecar')
$binName = if ($isWindowsHost) { 'openwa-gateway.exe' } else { 'openwa-gateway' }
$gatewayExe = Join-Path (Join-Path (Join-Path $repoRoot 'target') 'debug') $binName
$gatewayOut = Join-Path $runDir 'gateway.out.log'
$gatewayErr = Join-Path $runDir 'gateway.err.log'
$webhookOut = Join-Path $runDir 'webhook.out.log'
$webhookErr = Join-Path $runDir 'webhook.err.log'

$gatewayBase = "http://127.0.0.1:$GatewayPort"
$headers = @{ 'X-API-Key' = $ApiKey }

$failures = 0
$gatewayProc = $null
$webhookProc = $null
$sessionId = $null

function Write-Pass([string]$name) {
    Write-Host "PASS $name"
}

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

# Returns the HTTP status code without throwing on 4xx/5xx. Uses Invoke-RestMethod
# so it behaves the same on Windows PowerShell 5.1 and PowerShell 7.
function Get-HttpStatus {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [hashtable]$Headers
    )
    try {
        $params = @{ Uri = $Uri; Method = 'GET'; TimeoutSec = 15 }
        if ($Headers) { $params.Headers = $Headers }
        Invoke-RestMethod @params | Out-Null
        return 200
    }
    catch {
        $response = $_.Exception.Response
        if ($null -ne $response -and $null -ne $response.StatusCode) {
            return [int]$response.StatusCode
        }
        return -1
    }
}

# Same as Get-HttpStatus but for any method/body. Returns the HTTP status code.
function Get-RequestStatus {
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        [hashtable]$Headers,
        [string]$Body
    )
    try {
        $params = @{ Uri = $Uri; Method = $Method; TimeoutSec = 15 }
        if ($Headers) { $params.Headers = $Headers }
        if ($Body) { $params.ContentType = 'application/json'; $params.Body = $Body }
        Invoke-RestMethod @params | Out-Null
        return 200
    }
    catch {
        $response = $_.Exception.Response
        if ($null -ne $response -and $null -ne $response.StatusCode) {
            return [int]$response.StatusCode
        }
        return -1
    }
}

# Test webhook receiver: appends every request body it receives to the capture
# file as one JSON line. Single-quote only so the script survives Start-Process
# argument quoting on both Windows and Linux.
$webhookJs = @'
const http = require('http');
const fs = require('fs');
const file = process.env.OPENWA_WEBHOOK_CAPTURE;
const port = Number(process.env.OPENWA_WEBHOOK_PORT || 2899);
http.createServer(function (req, res) {
  let body = '';
  req.on('data', function (chunk) { body += chunk; });
  req.on('end', function () {
    try { fs.appendFileSync(file, body + '\n'); } catch (e) { process.stderr.write(String(e) + '\n'); }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{}');
  });
}).listen(port, '127.0.0.1');
'@
$webhookJs = ($webhookJs -replace "(\r?\n)", ' ').Trim()

try {
    Write-Host "openwa-rs smoke test (run dir: $runDir)"

    # --- build ---
    Write-Host 'cargo build -p openwa-gateway'
    # Native stderr (cargo progress) surfaces as an ErrorRecord in Windows
    # PowerShell 5.1, which would otherwise trip $ErrorActionPreference = 'Stop'.
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & cargo build -p openwa-gateway
    $buildExit = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorAction
    if ($buildExit -ne 0) {
        Write-Fail 'cargo build -p openwa-gateway' "exit code $buildExit"
        exit 1
    }
    Write-Pass 'cargo build -p openwa-gateway'
    if (-not (Test-Path -LiteralPath $gatewayExe)) {
        Write-Fail 'gateway binary exists' "not found at $gatewayExe"
        exit 1
    }

    # --- webhook receiver ---
    $env:OPENWA_WEBHOOK_CAPTURE = $capturePath
    $env:OPENWA_WEBHOOK_PORT = "$WebhookPort"
    $webhookArgs = '-e "' + $webhookJs + '"'
    $webhookProc = Start-Process -FilePath 'node' -ArgumentList $webhookArgs -PassThru -NoNewWindow `
        -RedirectStandardOutput $webhookOut -RedirectStandardError $webhookErr
    Start-Sleep -Milliseconds 500

    # --- gateway ---
    $env:SIDECAR_DIR = $sidecarDir
    $env:PORT = "$GatewayPort"
    $env:API_KEY = $ApiKey
    $env:DATA_DIR = $runDir
    $env:NODE_BIN = 'node'
    $env:RUST_LOG = 'info'

    $gatewayProc = Start-Process -FilePath $gatewayExe -WorkingDirectory $repoRoot -PassThru -NoNewWindow `
        -RedirectStandardOutput $gatewayOut -RedirectStandardError $gatewayErr

    # --- wait for the sidecar handshake ---
    $sidecarUp = $false
    $deadline = (Get-Date).AddSeconds(30)
    while (-not $sidecarUp -and (Get-Date) -lt $deadline) {
        if ($gatewayProc.HasExited) { break }
        try {
            $health = Invoke-RestMethod -Uri "$gatewayBase/api/health" -Headers $headers -TimeoutSec 5
            if ($health.sidecar -eq 'up') { $sidecarUp = $true; break }
        }
        catch {
            # gateway not listening yet
        }
        Start-Sleep -Milliseconds 300
    }
    Assert-That -Name 'GET /api/health reports sidecar up' -Condition $sidecarUp `
        -Detail "sidecar stayed down for 30s; see $gatewayErr"

    # --- dashboard ---
    $dashboardStatus = Get-HttpStatus -Uri "$gatewayBase/"
    $appJsStatus = Get-HttpStatus -Uri "$gatewayBase/app.js"
    $styleStatus = Get-HttpStatus -Uri "$gatewayBase/style.css"
    Assert-That -Name 'dashboard index is served at / (200)' -Condition ($dashboardStatus -eq 200) `
        -Detail "expected 200, got $dashboardStatus"
    Assert-That -Name 'dashboard assets are served (app.js, style.css)' `
        -Condition ($appJsStatus -eq 200 -and $styleStatus -eq 200) `
        -Detail "app.js=$appJsStatus style.css=$styleStatus"

    # --- auth ---
    $noKey = Get-HttpStatus -Uri "$gatewayBase/api/sessions"
    Assert-That -Name 'GET /api/sessions without api key is 401' -Condition ($noKey -eq 401) `
        -Detail "expected 401, got $noKey"

    $withKey = Get-HttpStatus -Uri "$gatewayBase/api/sessions" -Headers $headers
    Assert-That -Name 'GET /api/sessions with api key is 200' -Condition ($withKey -eq 200) `
        -Detail "expected 200, got $withKey"

    # --- sessions ---
    $createBody = @{ name = 'smoke-test' } | ConvertTo-Json
    $created = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions" -Headers $headers `
        -ContentType 'application/json' -Body $createBody
    $sessionId = $created.sessionId
    Assert-That -Name 'POST /api/sessions returns a sessionId' -Condition ([bool]$sessionId) `
        -Detail "response: $($created | ConvertTo-Json -Compress)"

    $list = Invoke-RestMethod -Uri "$gatewayBase/api/sessions" -Headers $headers
    $listed = @($list.sessions | Where-Object { $_.sessionId -eq $sessionId }).Count -gt 0
    Assert-That -Name 'GET /api/sessions contains the new session' -Condition $listed `
        -Detail "sessionId $sessionId missing from list"

    # --- start + qr ---
    $started = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/start" -Headers $headers
    $startStatus = $started.status

    # The mock serves its QR for ~300ms after start and then clears it, so read
    # it immediately: retrying after that window would only ever see null.
    $qrResponse = Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$sessionId/qr" -Headers $headers
    $qr = $qrResponse.qr

    Assert-That -Name 'POST /api/sessions/{id}/start reports qr_ready' -Condition ($startStatus -eq 'qr_ready') `
        -Detail "status=$startStatus"
    Assert-That -Name 'GET /api/sessions/{id}/qr returns mock-qr-data' -Condition ($qr -eq 'mock-qr-data') `
        -Detail "qr=$qr"

    # --- webhooks ---
    $hookBody = @{
        url    = "http://127.0.0.1:$WebhookPort/hook"
        events = @('message.received')
        secret = 's3cret'
    } | ConvertTo-Json
    $hook = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/webhooks" -Headers $headers `
        -ContentType 'application/json' -Body $hookBody
    Assert-That -Name 'POST /api/sessions/{id}/webhooks registers a webhook' -Condition ([bool]$hook.id) `
        -Detail "response: $($hook | ConvertTo-Json -Compress)"

    # --- wait for the mock's simulated link-up ---
    # The mock reaches "connected" ~300ms after start, and sendText is rejected
    # until then, so wait for it explicitly rather than racing the timer.
    $sessionStatus = $null
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $current = Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$sessionId" -Headers $headers
        $sessionStatus = $current.status
        if ($sessionStatus -eq 'connected') { break }
        Start-Sleep -Milliseconds 200
    }
    Assert-That -Name 'session reaches connected' -Condition ($sessionStatus -eq 'connected') `
        -Detail "status=$sessionStatus"

    # --- send-text ---
    $sendBody = @{ chatId = '628123456789@c.us'; text = 'Hello from openwa-rs smoke test' } | ConvertTo-Json
    $sent = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/messages/send-text" `
        -Headers $headers -ContentType 'application/json' -Body $sendBody
    Assert-That -Name 'POST /api/sessions/{id}/messages/send-text returns a messageId' -Condition ([bool]$sent.messageId) `
        -Detail "response: $($sent | ConvertTo-Json -Compress)"

    # --- webhook delivery ---
    $eventType = $null
    $deadline = (Get-Date).AddSeconds(10)
    while (-not $eventType -and (Get-Date) -lt $deadline) {
        if (Test-Path -LiteralPath $capturePath) {
            foreach ($line in @(Get-Content -LiteralPath $capturePath)) {
                if ([string]::IsNullOrWhiteSpace($line)) { continue }
                try { $capturedEvent = $line | ConvertFrom-Json } catch { continue }
                if ($capturedEvent.type -eq 'message.received') { $eventType = $capturedEvent.type; break }
            }
        }
        if ($eventType) { break }
        Start-Sleep -Milliseconds 250
    }
    # Fallback for a pretty-printed (multi-line) body that no single line parses as.
    if (-not $eventType -and (Test-Path -LiteralPath $capturePath)) {
        try {
            foreach ($chunk in @((Get-Content -LiteralPath $capturePath -Raw) | ConvertFrom-Json)) {
                if ($chunk.type -eq 'message.received') { $eventType = $chunk.type; break }
            }
        }
        catch { }
    }
    Assert-That -Name 'webhook receiver got a message.received event' -Condition ($eventType -eq 'message.received') `
        -Detail "nothing matched in $capturePath within 10s"

    # --- phase 1.5: message store ---
    $chatId = '628123456789@c.us'
    $stored = Invoke-RestMethod -Uri "$gatewayBase/api/sessions/$sessionId/messages?chatId=$chatId&limit=50" -Headers $headers
    $matching = @($stored.messages | Where-Object { $_.text -eq 'Hello from openwa-rs smoke test' })
    Assert-That -Name 'GET messages returns the sent message' -Condition ($matching.Count -ge 1) `
        -Detail "response: $($stored | ConvertTo-Json -Compress -Depth 4)"
    Assert-That -Name 'outgoing message is stored exactly once' -Condition ($matching.Count -eq 1) `
        -Detail "duplicates stored: $($matching.Count)"
    Assert-That -Name 'stored message is marked fromMe' -Condition (@($matching)[0].fromMe -eq $true) `
        -Detail "fromMe=$(@($matching)[0].fromMe)"

    # --- phase 1.5: media store ---
    $pixelPng = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg=='
    $mediaBody = @{ chatId = $chatId; mediaType = 'image'; mimetype = 'image/png'; base64 = $pixelPng; filename = 'pixel.png' } | ConvertTo-Json
    $mediaSent = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$sessionId/messages/send-media" `
        -Headers $headers -ContentType 'application/json' -Body $mediaBody
    Assert-That -Name 'POST send-media stores the blob and returns a mediaId' -Condition ([bool]$mediaSent.mediaId) `
        -Detail "response: $($mediaSent | ConvertTo-Json -Compress -Depth 3)"
    if ($mediaSent.mediaId) {
        $mediaResponse = Invoke-WebRequest -Uri "$gatewayBase/api/media/$($mediaSent.mediaId)?apiKey=$ApiKey" -UseBasicParsing
        Assert-That -Name 'GET /api/media/{id} serves the stored blob' -Condition ($mediaResponse.StatusCode -eq 200) `
            -Detail "status=$($mediaResponse.StatusCode)"
    }

    # --- phase 1.5: stats ---
    $stats = Invoke-RestMethod -Uri "$gatewayBase/api/stats" -Headers $headers
    Assert-That -Name 'GET /api/stats reports store counts' `
        -Condition ($stats.messages -ge 2 -and $stats.media -ge 1 -and $stats.webhooks -ge 1 -and $stats.sessions -ge 1) `
        -Detail "stats: $($stats | ConvertTo-Json -Compress)"

    # --- phase 1.5: webhook delivery log ---
    $deliveries = $null
    $deliveryCount = 0
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $deliveries = Invoke-RestMethod -Uri "$gatewayBase/api/webhooks/deliveries?sessionId=$sessionId&limit=50" -Headers $headers
        $deliveryCount = @($deliveries.deliveries | Where-Object { $_.delivered -eq $true }).Count
        if ($deliveryCount -ge 1) { break }
        Start-Sleep -Milliseconds 250
    }
    Assert-That -Name 'delivery log records a successful delivery' -Condition ($deliveryCount -ge 1) `
        -Detail "deliveries: $($deliveries | ConvertTo-Json -Compress -Depth 4)"

    $unknownRetry = Get-RequestStatus -Method POST -Uri "$gatewayBase/api/webhooks/deliveries/999999/retry" -Headers $headers
    Assert-That -Name 'retrying an unknown delivery is 404' -Condition ($unknownRetry -eq 404) `
        -Detail "expected 404, got $unknownRetry"

    # --- phase 1.5: API keys, roles, scoping ---
    $viewerBody = @{ name = 'smoke-viewer'; role = 'viewer'; allowedSessions = @($sessionId) } | ConvertTo-Json
    $viewer = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/keys" -Headers $headers -ContentType 'application/json' -Body $viewerBody
    Assert-That -Name 'POST /api/keys returns a plaintext key once' -Condition ([bool]$viewer.apiKey -and $viewer.role -eq 'viewer') `
        -Detail "response: $($viewer | ConvertTo-Json -Compress)"
    $viewerHeaders = @{ 'X-API-Key' = $viewer.apiKey }

    $viewerList = Invoke-RestMethod -Uri "$gatewayBase/api/sessions" -Headers $viewerHeaders
    $unexpected = @($viewerList.sessions | Where-Object { $_.sessionId -ne $sessionId })
    Assert-That -Name 'scoped key only sees its own session' -Condition ($unexpected.Count -eq 0) `
        -Detail "leaked sessions: $($unexpected | ConvertTo-Json -Compress)"

    $viewerStats = Get-RequestStatus -Method GET -Uri "$gatewayBase/api/stats" -Headers $viewerHeaders
    Assert-That -Name 'viewer key is refused on /api/stats (403)' -Condition ($viewerStats -eq 403) `
        -Detail "expected 403, got $viewerStats"

    $viewerSend = Get-RequestStatus -Method POST -Uri "$gatewayBase/api/sessions/$sessionId/messages/send-text" `
        -Headers $viewerHeaders -Body (@{ chatId = $chatId; text = 'not allowed' } | ConvertTo-Json)
    Assert-That -Name 'viewer key cannot send messages (403)' -Condition ($viewerSend -eq 403) `
        -Detail "expected 403, got $viewerSend"

    $outOfScope = Get-RequestStatus -Method GET -Uri "$gatewayBase/api/sessions/not-my-session" -Headers $viewerHeaders
    Assert-That -Name 'scoped key is refused on an unlisted session (403)' -Condition ($outOfScope -eq 403) `
        -Detail "expected 403, got $outOfScope"

    $viewerKeys = Get-RequestStatus -Method GET -Uri "$gatewayBase/api/keys" -Headers $viewerHeaders
    Assert-That -Name 'viewer key cannot list API keys (403)' -Condition ($viewerKeys -eq 403) `
        -Detail "expected 403, got $viewerKeys"

    $rotated = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/keys/$($viewer.id)/rotate" -Headers $headers
    Assert-That -Name 'POST /api/keys/{id}/rotate issues a fresh key' -Condition ([bool]$rotated.apiKey -and $rotated.apiKey -ne $viewer.apiKey) `
        -Detail "response: $($rotated | ConvertTo-Json -Compress)"
    $oldKeyStatus = Get-RequestStatus -Method GET -Uri "$gatewayBase/api/sessions" -Headers $viewerHeaders
    Assert-That -Name 'rotated-away key is rejected (401)' -Condition ($oldKeyStatus -eq 401) `
        -Detail "expected 401, got $oldKeyStatus"

    $keys = Invoke-RestMethod -Uri "$gatewayBase/api/keys" -Headers $headers
    $adminKeyId = @($keys.keys | Where-Object { $_.role -eq 'admin' })[0].id
    $lastAdminDelete = Get-RequestStatus -Method DELETE -Uri "$gatewayBase/api/keys/$adminKeyId" -Headers $headers
    Assert-That -Name 'the last admin key cannot be deleted (400)' -Condition ($lastAdminDelete -eq 400) `
        -Detail "expected 400, got $lastAdminDelete"

    # --- phase 1.5: rendered pairing QR ---
    $qrSession = Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions" -Headers $headers `
        -ContentType 'application/json' -Body (@{ name = 'smoke-qr' } | ConvertTo-Json)
    Invoke-RestMethod -Method Post -Uri "$gatewayBase/api/sessions/$($qrSession.sessionId)/start" -Headers $headers | Out-Null
    $qrImage = Invoke-WebRequest -Uri "$gatewayBase/api/sessions/$($qrSession.sessionId)/qr.png?apiKey=$ApiKey" -UseBasicParsing
    Assert-That -Name 'GET /api/sessions/{id}/qr.png renders the pairing QR' `
        -Condition ($qrImage.StatusCode -eq 200 -and "$($qrImage.Headers['Content-Type'])" -like 'image/*') `
        -Detail "status=$($qrImage.StatusCode) type=$($qrImage.Headers['Content-Type'])"
    Invoke-RestMethod -Method Delete -Uri "$gatewayBase/api/sessions/$($qrSession.sessionId)" -Headers $headers | Out-Null

    # --- delete ---
    $deleted = Invoke-RestMethod -Method Delete -Uri "$gatewayBase/api/sessions/$sessionId" -Headers $headers
    Assert-That -Name 'DELETE /api/sessions/{id} returns deleted:true' -Condition ($deleted.deleted -eq $true) `
        -Detail "response: $($deleted | ConvertTo-Json -Compress)"
}
catch {
    Write-Fail 'unexpected error' $_.Exception.Message
}
finally {
    foreach ($proc in @($gatewayProc, $webhookProc)) {
        if ($null -ne $proc -and -not $proc.HasExited) {
            try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch { }
        }
    }
    Remove-Item Env:\SIDECAR_DIR, Env:\PORT, Env:\API_KEY, Env:\DATA_DIR, Env:\NODE_BIN -ErrorAction SilentlyContinue
    Remove-Item Env:\OPENWA_WEBHOOK_CAPTURE, Env:\OPENWA_WEBHOOK_PORT -ErrorAction SilentlyContinue
}

if ($failures -gt 0) {
    Write-Host "SMOKE TEST FAILED - $failures assertion(s)"
    exit 1
}
Write-Host 'SMOKE TEST PASSED'
exit 0
