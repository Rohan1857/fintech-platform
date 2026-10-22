# Run the same assertion-based demo as the Bash entrypoint.
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    go run ./cmd/demo
    if ($LASTEXITCODE -ne 0) { throw "Payment demo failed (exit $LASTEXITCODE)" }
} finally {
    Pop-Location
}
