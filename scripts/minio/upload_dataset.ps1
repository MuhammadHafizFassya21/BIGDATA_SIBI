<#
.SYNOPSIS
    Dataset Upload Script to MinIO Object Storage for Project BIGDATA_SIBI.
.DESCRIPTION
    Safely uploads project assets to MinIO buckets under timestamped run prefixes:
    - sibi-raw       : Textbook PDFs and books metadata
    - sibi-processed : Cleaned text tokens, TSV inputs, and BoW manifests
    Never uploads .env files, credentials, or internal scripts.
#>

[CmdletBinding()]
param (
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path "$ScriptDir\..\..").Path

if (-not $EnvFile) {
    $EnvFile = "$ProjectRoot\configs\minio\minio.env"
}

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " [BIGDATA_SIBI] Dataset Upload to MinIO" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Project Root : $ProjectRoot"

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
    Write-Warning "Container 'sibi_minio' is not running. Please start MinIO before uploading."
    exit 0
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$UploadPrefix = "upload_$Timestamp"
Write-Host " Upload Run Prefix: $UploadPrefix" -ForegroundColor Yellow

# Configure Docker alias
docker run --rm --network sibi_network minio/mc:RELEASE.2024-05-09T17-04-24Z `
    alias set sibi_local http://minio:9000 "$MinioUser" "$MinioPass" | Out-Null

# 1. Upload to sibi-raw
Write-Host "`n>> [1/2] Uploading Raw PDFs and Metadata to sibi-raw/$UploadPrefix..." -ForegroundColor Cyan
# Copy files into a temporary staging volume or mount directly
docker run --rm --network sibi_network `
    -v "$ProjectRoot\dataset\raw:/data/raw:ro" `
    -v "$ProjectRoot\dataset\optional:/data/optional:ro" `
    -v "$ProjectRoot\metadata:/data/metadata:ro" `
    minio/mc:RELEASE.2024-05-09T17-04-24Z `
    cp --recursive /data/ "sibi_local/sibi-raw/$UploadPrefix/"

# 2. Upload to sibi-processed
Write-Host "`n>> [2/2] Uploading Processed Datasets to sibi-processed/$UploadPrefix..." -ForegroundColor Cyan
docker run --rm --network sibi_network `
    -v "$ProjectRoot\extracted_text\cleaned:/data/cleaned:ro" `
    -v "$ProjectRoot\dataset\processed:/data/processed:ro" `
    minio/mc:RELEASE.2024-05-09T17-04-24Z `
    cp --recursive /data/ "sibi_local/sibi-processed/$UploadPrefix/"

Write-Host "========================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] Datasets uploaded successfully to MinIO under prefix: $UploadPrefix" -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green

