Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

if ($env:CU_SKIP_CODEX_REVIEW -eq "1") {
    Write-Host "[codex-review] skipped because CU_SKIP_CODEX_REVIEW=1"
    exit 0
}

if (-not (Get-Command codex -ErrorAction SilentlyContinue)) {
    Write-Error "codex CLI not found in PATH."
}

$stagedFiles = @(git -C $repoRoot diff --cached --name-only)
if ($stagedFiles.Count -eq 0) {
    Write-Host "[codex-review] no staged files, skipping"
    exit 0
}

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("cu-codex-review-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempDir | Out-Null

$resultFile = Join-Path $tempDir "review-result.json"
$schemaFile = Join-Path $repoRoot "scripts\git-hooks\review-schema.json"

$prompt = @'
You are the pre-commit review agent for this repository.

You must read and follow these repository skill files first:
- skill/code_review/instructions.md
- skill/doc_review/instructions.md

Review requirements:
- Review only the current staged changes
- Read git diff --cached, git diff --cached --stat, related file contents, pyproject.toml, claude.md, and README.md first
- If the staged changes are mostly docs, also perform a documentation consistency review
- Use a PoC/MVP standard, but still block on security issues, correctness issues, broken hooks, or obviously misleading docs

The final output must match the provided JSON schema:
- status: "ok" or "block"
- summary: one sentence
- findings: array of strings; return an empty array if there are no findings

Decision rules:
- Return status="block" if there is any issue that should stop this commit
- Return status="ok" if there are no blocking issues
- Do not output any fields beyond the schema
'@

try {
    & codex exec `
        --ephemeral `
        --sandbox read-only `
        -C $repoRoot `
        --output-schema $schemaFile `
        -o $resultFile `
        $prompt

    if ($LASTEXITCODE -ne 0) {
        throw "codex exec failed with exit code $LASTEXITCODE"
    }

    $result = Get-Content -Raw $resultFile | ConvertFrom-Json

    Write-Host "[codex-review] $($result.summary)"
    foreach ($finding in $result.findings) {
        Write-Host "[codex-review] - $finding"
    }

    if ($result.status -ne "ok") {
        exit 1
    }
}
finally {
    if (Test-Path $tempDir) {
        Remove-Item -LiteralPath $tempDir -Recurse -Force
    }
}

exit 0
