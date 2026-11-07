<#
.SYNOPSIS
    Download MapReduce Results from MinIO Object Storage for Project BIGDATA_SIBI.
.DESCRIPTION
    Retrieves MapReduce result sets stored in bucket 'sibi-output' under a specific Run ID
    and places them safely in output/mapreduce/<run_id>/ on the local Windows host.
.PARAMETER RunId
    The identifier of the MapReduce execution run (e.g., run_primary_20261104_120000).
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$RunId,
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path "$ScriptDir\..\..").Path
$LocalOutputDir = "$ProjectRoot\output\mapreduce\$RunId"

if (-not $EnvFile) {
    $EnvFile = "$ProjectRoot\configs\minio\minio.env"
}

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " [BIGDATA_SIBI] Download MapReduce Results from MinIO" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Target Run ID   : $RunId"
Write-Host " Local Destination: $LocalOutputDir"

# Safety check: do not overwrite
if (Test-Path $LocalOutputDir) {
    $existing = Get-ChildItem -Path $LocalOutputDir -File
    if ($existing.Count -gt 0) {
        Write-Warning "Local destination already contains files: $LocalOutputDir"
        Write-Warning "Skipping download to prevent overwriting existing data."
        exit 0
    }
} else {
    New-Item -ItemType Directory -Path $LocalOutputDir -Force | Out-Null
}

# Read credentials
$MinioUser = ""
$MinioPass = ""
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line.Split("=", 2)
        if ($parts.Count -eq 2) {
            if ($parts[0].Trim() -eq "MINIO_ROOT_USER") { $MinioUser = $parts[1].Trim() }
            if ($parts[0].Trim() -eq "MINIO_ROOT_PASSWORD") { $MinioPass = $parts[1].Trim() }
        }
    }
}

$ContainerRunning = docker ps --filter "name=sibi_minio" --filter "status=running" --format "{{.Names}}"
if (-not $ContainerRunning) {
    Write-Warning "Container 'sibi_minio' is not running. Please start MinIO before downloading."
    exit 0
}

# Download via Docker MinIO client
Write-Host " Downloading results from sibi_local/sibi-output/$RunId to $LocalOutputDir..." -ForegroundColor Cyan
docker run --rm --network sibi_network `
    -v "$LocalOutputDir:/download:rw" `
    minio/mc:RELEASE.2024-05-09T17-04-24Z `
    sh -c "mc alias set sibi_local http://minio:9000 '$MinioUser' '$MinioPass' && mc cp --recursive sibi_local/sibi-output/$RunId/ /download/"

Write-Host "========================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] Download completed. Local files in: $LocalOutputDir" -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green

