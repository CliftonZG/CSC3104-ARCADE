# End-to-end failover test through the proxy (Windows PowerShell 5.1 or 7).
#
# Simulated players (cmd/wsclient) send a message every -IntervalMs through the
# proxy to match-000. After -KillAfterSec the script force-deletes the game
# primary of match-000, so the standby must detect it, take over and the proxy
# must retarget the players. Per run it reports:
#   disconnects        players whose WebSocket was closed (target: 0)
#   longest reply gap  the longest time a player waited for a reply, i.e. the
#                      failover pause as a player sees it
#   unanswered         messages that never got a reply (in flight at the crash)
#
#   .\test\ha\client-continuity.ps1                 # 1 run, 20 players
#   .\test\ha\client-continuity.ps1 -Runs 5
param(
    [string]$Namespace = "arcade",
    [string]$Match = "match-000",
    [int]$Runs = 1,
    [int]$Clients = 20,
    [int]$IntervalMs = 100,
    [int]$KillAfterSec = 10,
    [int]$DurationSec = 30,
    [int]$LocalPort = 18080
)
$ErrorActionPreference = "Continue"   # kubectl warnings on stderr must not abort the script

New-Item -ItemType Directory -Force bin, test/ha/results | Out-Null
Write-Host "Building cmd/wsclient..."
go build -o bin/wsclient.exe ./cmd/wsclient
if ($LASTEXITCODE -ne 0) { throw "go build failed" }

# The match id is <prefix><index>; with every client in one match the index is 0.
$prefix = $Match.Substring(0, $Match.Length - 1)
$results = @()

for ($run = 1; $run -le $Runs; $run++) {
    Write-Host "`n=== Run $run of $Runs ===" -ForegroundColor Cyan
    $pf = Start-Process kubectl -ArgumentList "-n $Namespace port-forward svc/arcade-proxy ${LocalPort}:8080" `
        -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 3

    $out = "test/ha/results/wsclient-run$run.txt"
    $client = Start-Process -FilePath "bin/wsclient.exe" -PassThru -NoNewWindow -RedirectStandardOutput $out `
        -ArgumentList "-url ws://127.0.0.1:$LocalPort/ws -clients $Clients -players-per-match $Clients -match-prefix $prefix -interval $($IntervalMs)ms -duration $($DurationSec)s"

    Start-Sleep -Seconds $KillAfterSec
    $victim = kubectl -n $Namespace get pods -l "arcade.io/match=$Match,arcade.io/role=primary" -o jsonpath="{.items[0].metadata.name}"
    if (-not $victim) { Write-Host "  no primary found for $Match" -ForegroundColor Red }
    else {
        Write-Host "  killing primary $victim at +$KillAfterSec s"
        kubectl -n $Namespace delete pod $victim --grace-period=0 --force 2>$null | Out-Null
    }

    $client.WaitForExit()
    Stop-Process -Id $pf.Id -Force -ErrorAction SilentlyContinue
    $text = Get-Content $out -Raw
    Write-Host $text

    $disc = if ($text -match "disconnects:\s+(\d+)") { [int]$Matches[1] } else { -1 }
    $gap = if ($text -match "longest reply gap:\s+(\S+)") { $Matches[1] } else { "?" }
    $una = if ($text -match "(\d+) unanswered") { [int]$Matches[1] } else { -1 }
    $sent = if ($text -match "messages:\s+(\d+) sent") { [int]$Matches[1] } else { -1 }
    $results += [pscustomobject]@{ Run = $run; Killed = $victim; Clients = $Clients; Sent = $sent
        Unanswered = $una; Disconnects = $disc; LongestReplyGap = $gap }

    if ($run -lt $Runs) {
        Write-Host "  waiting 60 s for the pair to heal..."
        Start-Sleep -Seconds 60
    }
}

Write-Host "`n=== Summary ===" -ForegroundColor Cyan
$results | Format-Table -AutoSize
$csv = "test/ha/results/client-continuity-$(Get-Date -Format yyyyMMdd-HHmmss).csv"
$results | Export-Csv -NoTypeInformation $csv
Write-Host "Saved $csv"
