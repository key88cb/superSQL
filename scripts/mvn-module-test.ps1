param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("java-master", "java-regionserver", "java-client")]
    [string]$Module,

    [Parameter(Mandatory = $false)]
    [string]$TestClass,

    [Parameter(Mandatory = $false)]
    [string[]]$AdditionalMavenArgs
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    $args = @("-pl", $Module, "-am", "test")

    if (-not [string]::IsNullOrWhiteSpace($TestClass)) {
        $args += "-Dtest=$TestClass"
        $args += "-Dsurefire.failIfNoSpecifiedTests=false"
    }

    if ($AdditionalMavenArgs) {
        $args += $AdditionalMavenArgs
    }

    Write-Host "Running: mvn $($args -join ' ')" -ForegroundColor Cyan
    & mvn @args
    if ($LASTEXITCODE -ne 0) {
        throw "Maven test execution failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
