Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

git -C $repoRoot config core.hooksPath .githooks

Write-Host "Configured git hooks path to .githooks"
