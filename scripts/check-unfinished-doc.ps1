$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$docPath = Join-Path $repoRoot "docs/UNFINISHED_FEATURES_IMPLEMENTATION_GUIDE.md"

if (-not (Test-Path $docPath)) {
    throw "Missing file: $docPath"
}

$content = Get-Content -Path $docPath -Raw -Encoding UTF8

$bannedPatterns = @(
    '已具备基础',
    '已实现但历史文档曾写',
    '本文件只保留“仍未完成项”与推进策略，不再描述已落地细节'
)

$violations = @()
foreach ($pattern in $bannedPatterns) {
    if ($content -match [regex]::Escape($pattern)) {
        $violations += $pattern
    }
}

if ($violations.Count -gt 0) {
    Write-Error "UNFINISHED guide contains implemented-content markers: $($violations -join ', ')"
    exit 1
}

Write-Host "UNFINISHED guide check passed." -ForegroundColor Green
