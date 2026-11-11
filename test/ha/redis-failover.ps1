# Redis Sentinel failover test for k8s/overlays/ha (Windows PowerShell 5.1 or 7).
#
# Kills the current Redis primary pod while the gameservers self-play, then checks:
#   1. Sentinel promotes a replica                 -> failover time
#   2. The match keeps committing moves            -> resume time, sequence still rising
#   3. The fencing epoch did not go backwards      -> fencing survives a Redis failover
#   4. The killed pod comes back as a replica      -> the group heals itself
#
#   .\test\ha\redis-failover.ps1                    # pair 0 (match-000), 1 run
#   .\test\ha\redis-failover.ps1 -Match match-001 -Runs 3
#
# Times are measured by polling through kubectl exec, so they are accurate to
# about +-0.5 s. Use them as an upper bound, not a precise latency.
param(
    [string]$Namespace = "arcade",
    [string]$Match = "match-000",
    [int]$Runs = 1,
    [int]$TimeoutSec = 60
)
$ErrorActionPreference = "Continue"

$pods = @("arcade-redis-0", "arcade-redis-1", "arcade-redis-2")
$epochKey = "arcade:match:{$Match}:epoch"
$snapKey = "arcade:match:{$Match}:snapshot"

function Invoke-Redis([string]$Pod, [string[]]$RedisArgs) {
    $out = & kubectl -n $Namespace exec $Pod -c redis -- redis-cli @RedisArgs 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $out
}

# The primary according to the Sentinels, as a pod name. Skips $Except, the
# pod that was just killed, because its Sentinel died with it.
function Get-Primary([string]$Except = "") {
    foreach ($p in $pods) {
        if ($p -eq $Except) { continue }
        $out = & kubectl -n $Namespace exec $p -c sentinel -- redis-cli -p 26379 SENTINEL get-master-addr-by-name arcade 2>$null
        if ($LASTEXITCODE -eq 0 -and $out) {
            $hostName = @($out)[0]
            return $hostName.Split(".")[0]          # arcade-redis-1.arcade-redis-headless... -> arcade-redis-1
        }
    }
    return $null
}

function Get-Seq([string]$Pod) {
    $v = Invoke-Redis $Pod @("HGET", $snapKey, "seq")
    if ($v) { return [int64]@($v)[0] } else { return -1 }
}

function Get-Epoch([string]$Pod) {
    $v = Invoke-Redis $Pod @("GET", $epochKey)
    if ($v) { return [int64]@($v)[0] } else { return -1 }
}

function Show-Roles {
    foreach ($p in $pods) {
        $role = Invoke-Redis $p @("ROLE")
        $r = if ($role) { @($role)[0] } else { "down" }
        Write-Host ("    {0}: {1}" -f $p, $r)
    }
}

$results = @()
for ($run = 1; $run -le $Runs; $run++) {
    Write-Host "`n=== Run $run of $Runs ($Match) ===" -ForegroundColor Cyan
    $old = Get-Primary
    if (-not $old) { throw "No Sentinel answered. Is k8s/overlays/ha deployed and are all arcade-redis pods 2/2 Ready?" }
    $epoch0 = Get-Epoch $old
    $seq0 = Get-Seq $old
    if ($seq0 -lt 0) { throw "No snapshot for $Match in Redis. Is the pair running with AUTOPLAY_INTERVAL set?" }
    Write-Host "  before: primary=$old epoch=$epoch0 seq=$seq0"
    Show-Roles

    Write-Host "  killing $old (force delete)..." -ForegroundColor Yellow
    $sw = [Diagnostics.Stopwatch]::StartNew()
    & kubectl -n $Namespace delete pod $old --grace-period=0 --force 2>$null | Out-Null

    # 1. Wait for Sentinel to name a different primary.
    $new = $null
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $p = Get-Primary -Except $old
        if ($p -and $p -ne $old) { $new = $p; break }
        Start-Sleep -Milliseconds 200
    }
    if (-not $new) { throw "Sentinel did not promote a replica within $TimeoutSec s" }
    $failoverMs = $sw.ElapsedMilliseconds
    Write-Host "  sentinel promoted $new after $failoverMs ms"

    # 2. Wait until the match commits a newer move on the new primary.
    $resumeMs = -1
    $seq1 = -1
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $seq1 = Get-Seq $new
        if ($seq1 -gt $seq0) { $resumeMs = $sw.ElapsedMilliseconds; break }
        Start-Sleep -Milliseconds 200
    }
    if ($resumeMs -lt 0) { Write-Host "  match did NOT resume within $TimeoutSec s (seq still $seq1)" -ForegroundColor Red }
    else { Write-Host "  moves resumed after $resumeMs ms (seq $seq0 -> $seq1)" }

    # 3. The epoch must not go backwards (it may go up if a gameserver also failed over).
    $epoch1 = Get-Epoch $new
    $epochOk = $epoch1 -ge $epoch0
    Write-Host ("  epoch {0} -> {1}: {2}" -f $epoch0, $epoch1, $(if ($epochOk) { "OK, not lost" } else { "WENT BACKWARDS" }))

    # 4. The killed pod should rejoin as a replica of the new primary.
    Write-Host "  waiting for $old to rejoin..."
    & kubectl -n $Namespace wait --for=condition=Ready "pod/$old" --timeout=120s 2>$null | Out-Null
    Start-Sleep -Seconds 3
    $oldRole = Invoke-Redis $old @("ROLE")
    $rejoined = $oldRole -and @($oldRole)[0] -eq "slave"
    Write-Host ("  {0} is now: {1}" -f $old, $(if ($oldRole) { @($oldRole)[0] + " of " + @($oldRole)[1] } else { "down" }))
    Show-Roles

    $results += [pscustomobject]@{
        Run = $run; Killed = $old; NewPrimary = $new
        FailoverMs = $failoverMs; ResumeMs = $resumeMs
        SeqBefore = $seq0; SeqAfter = $seq1
        EpochBefore = $epoch0; EpochAfter = $epoch1
        EpochOk = $epochOk; OldPodRejoined = $rejoined
    }
    if ($run -lt $Runs) { Start-Sleep -Seconds 10 }
}

Write-Host "`n=== Summary ===" -ForegroundColor Cyan
$results | Format-Table -AutoSize
$csv = "redis-failover-$(Get-Date -Format yyyyMMdd-HHmmss).csv"
$results | Export-Csv -NoTypeInformation $csv
Write-Host "Saved $csv"

Write-Host "`nGameserver log lines about Redis in the last 2 minutes:"
foreach ($gs in @("arcade-gs-a-0", "arcade-gs-b-0")) {
    Write-Host "  [$gs]"
    & kubectl -n $Namespace logs $gs --since=2m 2>$null |
        Select-String -Pattern "REPLICATION|gave up|not acknowledged|FENCED|Stepping down|TAKEOVER" |
        Select-Object -Last 8 | ForEach-Object { "    $_" }
}
