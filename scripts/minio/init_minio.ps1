<#
.SYNOPSIS
    MinIO Initialization Script for Project BIGDATA_SIBI.
.DESCRIPTION
    Initializes MinIO object storage buckets:
    - sibi-raw        : Raw PDFs and metadata
    - sibi-processed  : Cleaned tokens, TSV inputs, manifests
    - sibi-output     : MapReduce output data and execution logs
    Execution supports both docker container 'mc' (MinIO client) or local host 'mc.exe'.
#>

[CmdletBinding()]
param (
    [string]$Endpoint = "http://127.0.0.1:9000",
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path "$ScriptDir\..\..").Path

if (-not $EnvFile) {
    $EnvFile = "$ProjectRoot\configs\minio\minio.env"
}

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " [BIGDATA_SIBI] MinIO Bucket Initialization" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Project Root : $ProjectRoot"
Write-Host " Config File  : $EnvFile"

if (-not (Test-Path $EnvFile)) {
    Write-Error "MinIO config file not found: $EnvFile"
    exit 1
}

# Parse environment file
$MinioUser = ""
$MinioPass = ""

Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line.Split("=", 2)
        if ($parts.Count -eq 2) {
            $k = $parts[0].Trim()
            $v = $parts[1].Trim()
            if ($k -eq "MINIO_ROOT_USER") { $MinioUser = $v }
            if ($k -eq "MINIO_ROOT_PASSWORD") { $MinioPass = $v }
        }
    }
}

if (-not $MinioUser -or -not $MinioPass -or $MinioPass -eq "REPLACE_WITH_UNIQUE_PASSWORD_MIN_8_CHARS") {
    Write-Warning "MinIO credentials are not configured or still using default placeholder."
    Write-Warning "Please configure a valid password in $EnvFile before initializing buckets."
}

# Check if Docker container sibi_minio is running
$UseDockerClient = $false
$ContainerRunning = docker ps --filter "name=sibi_minio" --filter "status=running" --format "{{.Names}}"

if ($ContainerRunning) {
    Write-Host " Detected running 'sibi_minio' container." -ForegroundColor Green
    $UseDockerClient = $true
} else {
    Write-Host " MinIO container is not running locally. Checking local 'mc' CLI..." -ForegroundColor Yellow
    if (Get-Command mc -ErrorAction SilentlyContinue) {
        Write-Host " Found local 'mc' executable." -ForegroundColor Green
    } else {
        Write-Warning "Neither MinIO container nor local 'mc' CLI is currently active."
        Write-Warning "Run this script after starting containers: docker compose up -d"
        exit 0
    }
}

$Buckets = @("sibi-raw", "sibi-processed", "sibi-output")

if ($UseDockerClient) {
    # Check if minio-client container is available, else use minio container directly or docker run
    Write-Host " Initializing MinIO alias 'sibi_local' via Docker..."
    docker run --rm --network sibi_network minio/mc:RELEASE.2024-05-09T17-04-24Z `
        alias set sibi_local http://minio:9000 "$MinioUser" "$MinioPass" | Out-Null

    foreach ($bucket in $Buckets) {
        Write-Host " Creating bucket '$bucket' (idempotent)..."
        docker run --rm --network sibi_network minio/mc:RELEASE.2024-05-09T17-04-24Z `
            mb --ignore-existing "sibi_local/$bucket"
    }
} else {
    Write-Host " Initializing MinIO alias 'sibi_local' via local host mc..."
    mc alias set sibi_local $Endpoint "$MinioUser" "$MinioPass"
    foreach ($bucket in $Buckets) {
        Write-Host " Creating bucket '$bucket' (idempotent)..."
        mc mb --ignore-existing "sibi_local/$bucket"
    }
}

Write-Host "========================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] MinIO Buckets Initialization Completed: $($Buckets -join ', ')" -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green

