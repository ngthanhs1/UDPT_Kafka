param(
    [string]$Module = "clients",
    [string]$Task = "jar"
)

# run_build.ps1 - tự động chọn gradle wrapper hoặc gradle system và chạy task
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $repoRoot
try {
    $gradlewBat = Join-Path $repoRoot 'gradlew.bat'
    $gradlewSh = Join-Path $repoRoot 'gradlew'

    $taskArg = ":$Module:$Task"

    if (Test-Path $gradlewBat) {
        Write-Host "Using gradlew.bat to run $taskArg"
        & $gradlewBat $taskArg
        exit $LASTEXITCODE
    }

    # If gradlew (sh) exists and bash is available (Git Bash / WSL), use it
    if (Test-Path $gradlewSh) {
        $bash = Get-Command bash -ErrorAction SilentlyContinue
        if ($bash) {
            Write-Host "Using bash ./gradlew to run $taskArg"
            & bash -lc "cd \"$repoRoot\" && ./gradlew $taskArg"
            exit $LASTEXITCODE
        }
    }

    # Fallback to system gradle if installed
    $gradleCmd = Get-Command gradle -ErrorAction SilentlyContinue
    if ($gradleCmd) {
        Write-Host "Using system 'gradle' to run $taskArg"
        & gradle $taskArg
        exit $LASTEXITCODE
    }

    Write-Error "No Gradle wrapper found and 'gradle' isn't in PATH. Please run this script from the project root and ensure Gradle or Git Bash is installed."
    exit 2
}
finally {
    Pop-Location
}
