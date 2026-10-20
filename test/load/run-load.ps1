<#
.SYNOPSIS
  Phase 2 load test: horizontal scalability & per-pair resource profiling (Windows).
  Owner: Tan Yan Ting.  Linux/macOS/Git Bash: run-load.sh does the same steps.

.DESCRIPTION
  For each fleet size N (default 10, 50, 100 Primary/Standby pairs) it:
    1. scales arcade-gs-a and arcade-gs-b to N replicas
    2. waits until every gameserver pod is Ready, then a warm-up period
    3. runs the in-cluster load probe Job (cmd/loadprobe) for -Window
    4. saves the probe report (JSON + table), `kubectl top` (if metrics-server
       is installed) and pprof heap/CPU profiles of pair 0 (primary + standby)
  and finally renders charts with plot_results.py (needs Python + matplotlib).

.EXAMPLE
  .\test\load\run-load.ps1                                  # 10, 50, 100 pairs
  .\test\load\run-load.ps1 -Pairs 10 -Window 30s -Warmup 15  # quick smoke run
  .\test\load\run-load.ps1 -Pairs 10,50 -MoveInterval 20ms   # stress: 50 moves/s per match
#>
[CmdletBinding()]
param(
    [string[]] $Pairs = @("10", "50", "100"),
    [string] $Window = "60s",
    [int]    $Warmup = 30,
    [string] $MoveInterval = "200ms",   # one self-play move per match per interval (5 moves/s)
    [string] $OutDir = "",
    [string] $Kubectl = "kubectl",
    [string] $Namespace = "arcade",
    [switch] $SkipApply,
    [switch] $NoPprof
)

$ErrorActionPreference = "Stop"
# Accept -Pairs 10,50,100 as well as -Pairs "10 50 100" (e.g. via powershell -File).
$Pairs = @($Pairs | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ } | ForEach-Object { [int]$_ })
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Resolve-Path (Join-Path $here "../..")
if (-not $OutDir) { $OutDir = Join-Path $here ("results/" + (Get-Date -Format "yyyyMMdd-HHmmss")) }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# -Kubectl may be a command with arguments, e.g. "minikube kubectl --".
$kubectlParts = @($Kubectl -split '\s+' | Where-Object { $_ })
$kubectlExe = $kubectlParts[0]
$kubectlPre = @()
if ($kubectlParts.Count -gt 1) { $kubectlPre = $kubectlParts[1..($kubectlParts.Count - 1)] }

# Plain functions (no param block) so kubectl flags like -n, -l, -o reach
# kubectl untouched instead of being parsed as PowerShell parameters.
function Invoke-Kube {
    # Windows PowerShell 5.1 turns native stderr (e.g. kubectl warnings) into
    # terminating errors under "Stop"; judge success by the exit code instead.
    $ErrorActionPreference = "Continue"
    $out = & $kubectlExe @kubectlPre @args 2>&1 | ForEach-Object { "$_" }
    if ($LASTEXITCODE -ne 0) { throw "kubectl $($args -join ' ') failed:`n$($out | Out-String)" }
    return $out
}
function K { Invoke-Kube -n $Namespace @args }
function Log([string] $msg) { Write-Host ""; Write-Host ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg) -ForegroundColor Cyan }

Log "Results -> $OutDir"
$envFile = Join-Path $OutDir "environment.txt"
@(
    "date: $(Get-Date -Format o)"
    "pairs: $($Pairs -join ' ') window: $Window warmup: ${Warmup}s interval: $MoveInterval"
    (Invoke-Kube version | Out-String)
    (Invoke-Kube get nodes -o wide | Out-String)
) | Set-Content -Encoding UTF8 $envFile

if (-not $SkipApply) {
    Log "Applying k8s/overlays/load"
    Invoke-Kube apply -k (Join-Path $root "k8s/overlays/load") | Write-Host
}
# Always set the move rate explicitly: an env var added by `kubectl set env`
# survives later `kubectl apply`s, so relying on the overlay's value is unsafe.
Log "Setting AUTOPLAY_INTERVAL=$MoveInterval"
K set env statefulset/arcade-gs-a statefulset/arcade-gs-b "AUTOPLAY_INTERVAL=$MoveInterval" | Write-Host
K rollout status statefulset/arcade-redis --timeout=5m | Write-Host

$jobTemplate = Get-Content -Raw (Join-Path $root "k8s/load/loadprobe-job.yaml")

foreach ($n in $Pairs) {
    Log "=== $n pairs ($(2 * $n) gameserver pods) ==="
    K scale statefulset/arcade-gs-a statefulset/arcade-gs-b "--replicas=$n" | Write-Host
    K rollout status statefulset/arcade-gs-a --timeout=15m | Out-Null
    K rollout status statefulset/arcade-gs-b --timeout=15m | Out-Null
    # Poll readyReplicas instead of `kubectl wait pod -l ...`: when scaling down,
    # that would also wait on pods that are being deleted and hang.
    $deadline = (Get-Date).AddMinutes(15)
    while ($true) {
        $ready = @("arcade-gs-a", "arcade-gs-b") | ForEach-Object { [int]("0" + ((K get statefulset $_ -o "jsonpath={.status.readyReplicas}") -join "")) }
        if (($ready | Where-Object { $_ -ne $n }).Count -eq 0) { break }
        if ((Get-Date) -gt $deadline) { throw "pods not Ready after 15 minutes (ready: $($ready -join '/') of $n)" }
        Start-Sleep -Seconds 2
    }
    Log "All pods Ready; warming up ${Warmup}s"
    Start-Sleep -Seconds $Warmup

    K get pods -o wide -L "arcade.io/role,arcade.io/match" | Set-Content -Encoding UTF8 (Join-Path $OutDir "pairs-$n-pods.txt")
    try {
        K top pods --no-headers | Set-Content -Encoding UTF8 (Join-Path $OutDir "pairs-$n-kubectl-top.txt")
    } catch {
        "(kubectl top unavailable: install metrics-server)" | Set-Content (Join-Path $OutDir "pairs-$n-kubectl-top.txt")
    }

    $job = "arcade-loadprobe-$n"
    K delete job $job --ignore-not-found | Out-Null
    $manifest = Join-Path $OutDir "loadprobe-$n.yaml"
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($manifest, $jobTemplate.Replace("__PAIRS__", "$n").Replace("__WINDOW__", $Window), $utf8NoBom)
    Invoke-Kube apply -f $manifest | Out-Null
    Log "Load probe running ($Window window)"
    try {
        K wait --for=condition=complete "job/$job" --timeout=20m | Out-Null
    } catch {
        try { K logs "job/$job" | Write-Host } catch { Write-Warning "$_" }
        throw "load probe for $n pairs failed"
    }
    $logText = (K logs "job/$job") -join "`n"
    $logText | Set-Content -Encoding UTF8 (Join-Path $OutDir "pairs-$n.log")
    $m = [regex]::Match($logText, '(?s)=== LOADPROBE JSON BEGIN ===\s*(.*?)\s*=== LOADPROBE JSON END ===')
    if (-not $m.Success) { throw "no JSON report in probe output for $n pairs" }
    # Write without a BOM so every JSON parser accepts it.
    [System.IO.File]::WriteAllText((Join-Path $OutDir "pairs-$n.json"), $m.Groups[1].Value, $utf8NoBom)
    $t = [regex]::Match($logText, '(?s)(ARCADE load probe.*?)=== LOADPROBE JSON BEGIN ===')
    if ($t.Success) { Write-Host $t.Groups[1].Value }

    if (-not $NoPprof) {
        foreach ($pod in @("arcade-gs-a-0", "arcade-gs-b-0")) {
            $pfArgs = @($kubectlPre) + @("-n", $Namespace, "port-forward", "pod/$pod", "18081:8081")
            $spArgs = @{ FilePath = $kubectlExe; ArgumentList = $pfArgs; PassThru = $true }
            if ($PSVersionTable.PSEdition -eq "Desktop" -or $IsWindows) {
                $spArgs.WindowStyle = "Hidden"
            } else {
                $spArgs.RedirectStandardOutput = Join-Path $OutDir "port-forward.log"
            }
            $pf = Start-Process @spArgs
            try {
                $ok = $false
                for ($i = 0; $i -lt 50 -and -not $ok; $i++) {
                    try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 "http://127.0.0.1:18081/healthz" | Out-Null; $ok = $true } catch { Start-Sleep -Milliseconds 200 }
                }
                if (-not $ok) { Write-Warning "port-forward to $pod did not come up"; continue }
                Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:18081/debug/pprof/heap" -OutFile (Join-Path $OutDir "pairs-$n-$pod-heap.pb.gz")
                Invoke-WebRequest -UseBasicParsing -TimeoutSec 60 "http://127.0.0.1:18081/debug/pprof/profile?seconds=15" -OutFile (Join-Path $OutDir "pairs-$n-$pod-cpu.pb.gz")
                Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:18081/role" -OutFile (Join-Path $OutDir "pairs-$n-$pod-role.json")
            } catch {
                Write-Warning "pprof capture for ${pod}: $_"
            } finally {
                Stop-Process -Id $pf.Id -Force -ErrorAction SilentlyContinue
            }
        }
        Log "pprof step done (view: go tool pprof -http=:0 <results>\pairs-$n-arcade-gs-a-0-cpu.pb.gz)"
    }
}

$python = $null
foreach ($candidate in @("python", "py", "python3")) {
    if (Get-Command $candidate -ErrorAction SilentlyContinue) { $python = $candidate; break }
}
if ($python) {
    Log "Rendering charts"
    & $python (Join-Path $here "plot_results.py") $OutDir
    if ($LASTEXITCODE -ne 0) { Write-Warning "plotting failed (pip install matplotlib)" }
} else {
    Write-Warning "Python not found: run  python test\load\plot_results.py `"$OutDir`"  later"
}
Log "Done. Results in $OutDir"
