<#
.SYNOPSIS
  Runs the babeldb engine benchmark matrix with independent repetitions:
  one fresh process per (repetition, variant, scenario), all appending to one
  JSON-lines file with --tag repN.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/bench-matrix.ps1 -Reps 5
  powershell -ExecutionPolicy Bypass -File scripts/bench-matrix.ps1 -Reps 5 -Scenarios s3 -Records 200k -ValueSize 512 -Extra '--dist','zipf','--mix','95-5'
  powershell -ExecutionPolicy Bypass -File scripts/bench-matrix.ps1 -Backend lmdb -Reps 3
#>
param(
    [int]$Reps = 3,
    [string[]]$Variants = @('raw-backend', 'engine-raw', 'babel-pure', 'lz4', 'zstd', 'adaptive-nodedupe', 'adaptive'),
    [string[]]$Scenarios = @('s1', 's2', 's3', 's4', 's5', 's6'),
    [string]$Backend = 'redb',
    [string]$Records = '20k',
    [string]$ValueSize = '1024',
    [string]$Out = 'bench-results/engine.jsonl',
    [string]$Note = '',
    [string[]]$Extra = @()
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$features = @()
if ($Backend -eq 'lmdb') { $features = @('--features', 'lmdb') }

# Build once; every run below is then a fresh process of the same binary.
& cargo bench --bench engine @features --no-run
if ($LASTEXITCODE -ne 0) { throw 'cargo bench --no-run failed' }

$common = @('--backend', $Backend, '--records', $Records, '--value-size', $ValueSize, '--out', $Out)
# Windows PowerShell 5.1 drops empty-string arguments: pass --note only when set.
if ($Note -ne '') { $common += @('--note', $Note) }
$common += $Extra

$failures = 0
for ($r = 1; $r -le $Reps; $r++) {
    # Rotate the variant order each repetition so slow drifts (temperature,
    # OS cache, background activity) do not always hit the same variant.
    $k = ($r - 1) % $Variants.Count
    $order = @($Variants[$k..($Variants.Count - 1)])
    if ($k -gt 0) { $order += @($Variants[0..($k - 1)]) }
    foreach ($v in $order) {
        foreach ($s in $Scenarios) {
            Write-Host "== rep $r/$Reps backend=$Backend variant=$v scenario=$s"
            $argList = @('bench', '--bench', 'engine') + $features + @('--') + $common + @('--variant', $v, '--scenario', $s, '--tag', "rep$r")
            & cargo @argList
            if ($LASTEXITCODE -ne 0) { $failures++ }
        }
    }
}
Write-Host "done: $failures failed run(s); results appended to $Out"
if ($failures -gt 0) { exit 1 }
