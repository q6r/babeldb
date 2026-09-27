<#
.SYNOPSIS
  Summarizes benches/engine.rs JSON lines: median (and min..max) over the
  repetitions (--tag) of the main metric of every phase, grouped by backend,
  variant, scenario, phase and thread count.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/summarize.ps1 -Path bench-results/engine.jsonl
  powershell -ExecutionPolicy Bypass -File scripts/summarize.ps1 -Phase get,mt-get -Csv bench-results/summary.csv
  (-Csv writes long format: one row per group and metric, with median/min/max.)
#>
param(
    [string]$Path = 'bench-results/engine.jsonl',
    [string[]]$Phase = @(),
    [string]$RunIdPrefix = '',
    [string]$Csv = ''
)

$ErrorActionPreference = 'Stop'

function Get-Median([double[]]$xs) {
    if ($xs.Count -eq 0) { return $null }
    $s = @($xs | Sort-Object)
    $n = $s.Count
    if ($n % 2 -eq 1) { return $s[($n - 1) / 2] }
    return ($s[$n / 2 - 1] + $s[$n / 2]) / 2
}

# (column, how to read it from a line); latencies are converted to microseconds.
function Get-Metrics($line) {
    $m = [ordered]@{}
    switch ($line.phase) {
        'load' {
            $m['MB/s'] = $line.mb_per_s
            $m['rec/s'] = $line.records_per_s
            $m['batch_p99_us'] = $line.batch_latency_ns.p99 / 1000
        }
        { $_ -in 'space', 'space-final' } {
            $m['alloc_MB'] = if ($null -ne $line.file_allocated_bytes) { $line.file_allocated_bytes / 1e6 } else { $null }
            $m['amp_alloc'] = $line.amplification_allocated
            if ($null -ne $line.engine) { $m['payload_MB'] = $line.engine.payload_bytes / 1e6 }
        }
        'mixed' {
            $m['ops/s'] = $line.ops_per_s
            $m['read_p99_us'] = $line.read_latency_ns.p99 / 1000
            $m['write_p99_us'] = $line.write_latency_ns.p99 / 1000
        }
        default {
            $m['p50_us'] = $line.latency_ns.p50 / 1000
            $m['p99_us'] = $line.latency_ns.p99 / 1000
            $m['p999_us'] = $line.latency_ns.p999 / 1000
            $m['ops/s'] = $line.ops_per_s
            if ($null -ne $line.read_amplification) { $m['read_amp'] = $line.read_amplification }
        }
    }
    return $m
}

$lines = Get-Content -LiteralPath $Path | Where-Object { $_.Trim() -ne '' } | ForEach-Object { $_ | ConvertFrom-Json }
if ($RunIdPrefix -ne '') { $lines = $lines | Where-Object { $_.run_id.StartsWith($RunIdPrefix) } }
if ($Phase.Count -gt 0) { $lines = $lines | Where-Object { $Phase -contains $_.phase } }
$errors = @($lines | Where-Object { $_.phase -eq 'error' })
$lines = $lines | Where-Object { $_.phase -ne 'error' }

$groups = $lines | Group-Object -Property backend, variant, scenario, phase, { if ($null -ne $_.threads) { $_.threads } else { 1 } }
$long = New-Object System.Collections.Generic.List[object]
$rows = foreach ($g in $groups) {
    $first = $g.Group[0]
    $row = [ordered]@{
        backend  = $first.backend
        variant  = $first.variant
        scenario = $first.scenario
        phase    = $first.phase
        threads  = if ($null -ne $first.threads) { $first.threads } else { 1 }
        reps     = $g.Count
    }
    $perRun = @($g.Group | ForEach-Object { Get-Metrics $_ })
    foreach ($name in $perRun[0].Keys) {
        $vals = @($perRun | ForEach-Object { $_[$name] } | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
        if ($vals.Count -eq 0) { $row[$name] = '-'; continue }
        $med = Get-Median $vals
        $min = ($vals | Measure-Object -Minimum).Minimum
        $max = ($vals | Measure-Object -Maximum).Maximum
        $inv = [Globalization.CultureInfo]::InvariantCulture
        $long.Add([pscustomobject][ordered]@{
                backend = $row.backend; variant = $row.variant; scenario = $row.scenario; phase = $row.phase
                threads = $row.threads; reps = $vals.Count; metric = $name
                median = $med.ToString($inv); min = $min.ToString($inv); max = $max.ToString($inv)
            })
        $row[$name] = if ($vals.Count -gt 1) {
            [string]::Format($inv, '{0:G4} ({1:G4}..{2:G4})', $med, $min, $max)
        } else {
            [string]::Format($inv, '{0:G4}', $med)
        }
    }
    [pscustomobject]$row
}

# One table per phase: each phase has its own metric columns.
$rows | Group-Object phase | Sort-Object Name | ForEach-Object {
    Write-Output "== $($_.Name)"
    $_.Group | Sort-Object scenario, variant, threads | Format-Table -AutoSize | Out-String -Width 400 | Write-Output
}
if ($errors.Count -gt 0) {
    Write-Output "$($errors.Count) failed run(s):"
    $errors | ForEach-Object { Write-Output "  $($_.backend) $($_.variant) $($_.scenario): $($_.error)" }
}
if ($Csv -ne '') {
    # Long format (one metric per row): phases have different metric columns.
    $long | Export-Csv -LiteralPath $Csv -NoTypeInformation -Encoding UTF8
    Write-Output "wrote $Csv ($($long.Count) rows)"
}
