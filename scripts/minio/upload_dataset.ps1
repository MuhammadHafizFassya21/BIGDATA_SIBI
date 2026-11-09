<#
.SYNOPSIS
    Uploads BIGDATA_SIBI source and processed datasets to MinIO.
.DESCRIPTION
    Uploads to timestamped prefixes:
      sibi-raw       : dataset/raw, dataset/optional, metadata
      sibi-processed : extracted_text/cleaned, dataset/processed
    Does not upload .env files, credentials, scripts, or MapReduce output.
    A temporary Docker volume preserves the mc alias between containers
    and is removed when the script finishes.
#>

[CmdletBinding()]
param (
    [string]$Network = "sibi_network",
    [string]$McImage = "quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z",
    [string]$Endpoint = "http://minio:9000",
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path

if (-not $EnvFile) {
    $EnvFile = Join-Path $ProjectRoot "configs\minio\minio.env"
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " BIGDATA_SIBI - Dataset Upload to MinIO" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Project root : $ProjectRoot"
Write-Host "Docker network: $Network"
Write-Host "MinIO endpoint: $Endpoint"

if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) {
    throw "MinIO environment file not found: $EnvFile"
}

# Read credentials without displaying them.
$MinioUser = ""
$MinioPass = ""

Get-Content -LiteralPath $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line.Split("=", 2)
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim()
            if ($key -eq "MINIO_ROOT_USER") { $MinioUser = $value }
            if ($key -eq "MINIO_ROOT_PASSWORD") { $MinioPass = $value }
        }
    }
}

if ([string]::IsNullOrWhiteSpace($MinioUser) -or
    [string]::IsNullOrWhiteSpace($MinioPass) -or
    $MinioPass -eq "REPLACE_WITH_UNIQUE_PASSWORD_MIN_8_CHARS") {
    throw "MINIO_ROOT_USER or MINIO_ROOT_PASSWORD is missing or still uses the placeholder. Check configs/minio/minio.env locally."
}

# Verify Docker network and MinIO container.
docker network inspect $Network *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker network '$Network' does not exist."
}

$runningState = docker inspect --format '{{.State.Running}}' sibi_minio 2>$null
if ($LASTEXITCODE -ne 0 -or (($runningState -join "").Trim() -ne "true")) {
    throw "Container sibi_minio is not running. Start it with: docker compose up -d minio"
}

# Required source directories.
$UploadItems = @(
    @{ Path = "dataset\raw"; Bucket = "sibi-raw"; Key = "raw"; Label = "Raw textbook PDFs" },
    @{ Path = "metadata"; Bucket = "sibi-raw"; Key = "metadata"; Label = "Book metadata" },
    @{ Path = "extracted_text\cleaned"; Bucket = "sibi-processed"; Key = "cleaned"; Label = "Cleaned text" },
    @{ Path = "dataset\processed"; Bucket = "sibi-processed"; Key = "processed"; Label = "Processed TSV and manifests" }
)

# Optional comparison book directory is included only if it exists and has files.
$OptionalPath = Join-Path $ProjectRoot "dataset\optional"
if (Test-Path -LiteralPath $OptionalPath -PathType Container) {
    $optionalFiles = @(Get-ChildItem -LiteralPath $OptionalPath -File -Recurse -ErrorAction Stop)
    if ($optionalFiles.Count -gt 0) {
        $UploadItems += @{ Path = "dataset\optional"; Bucket = "sibi-raw"; Key = "optional"; Label = "Optional comparison textbook" }
    }
}

# Validate every selected source directory before starting the upload.
foreach ($item in $UploadItems) {
    $absolutePath = Join-Path $ProjectRoot $item.Path
    if (-not (Test-Path -LiteralPath $absolutePath -PathType Container)) {
        throw "Required upload directory not found: $absolutePath"
    }

    $files = @(Get-ChildItem -LiteralPath $absolutePath -File -Recurse -ErrorAction Stop)
    if ($files.Count -eq 0) {
        throw "Upload directory is empty: $absolutePath"
    }

    $item["AbsolutePath"] = (Resolve-Path -LiteralPath $absolutePath).Path
    $item["FileCount"] = $files.Count
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$UploadPrefix = "upload_$Timestamp"
$McConfigVolume = "sibi_mc_upload_$([guid]::NewGuid().ToString('N'))"
$VolumeCreated = $false

function Invoke-Mc {
    param (
        [Parameter(Mandatory = $true)]
        [string[]]$McArgs,

        [string[]]$Mounts = @()
    )

    $dockerArgs = @(
        "run", "--rm",
        "--network", $Network,
        "--env", "MC_CONFIG_DIR=/mc-config",
        "--mount", "type=volume,source=$McConfigVolume,target=/mc-config"
    )

    foreach ($mount in $Mounts) {
        $dockerArgs += @("--mount", $mount)
    }

    $dockerArgs += @($McImage)
    $dockerArgs += $McArgs

    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "MinIO Client command failed (exit code $LASTEXITCODE). Review the Docker error above. Command arguments are suppressed to protect credentials."
    }
}

try {
    docker volume create $McConfigVolume | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create temporary Docker volume for MinIO Client configuration."
    }
    $VolumeCreated = $true

    Write-Host "`nConfiguring MinIO Client alias..." -ForegroundColor Cyan
    Invoke-Mc -McArgs @("alias", "set", "sibi_local", $Endpoint, $MinioUser, $MinioPass)

    Write-Host "`nStarting upload with prefix: $UploadPrefix" -ForegroundColor Yellow

    $index = 0
    foreach ($item in $UploadItems) {
        $index++
        $target = "sibi_local/$($item.Bucket)/$UploadPrefix/$($item.Key)/"
        $mount = "type=bind,source=$($item.AbsolutePath),target=/data,readonly"

        Write-Host "`n[$index/$($UploadItems.Count)] $($item.Label)" -ForegroundColor Cyan
        Write-Host "Files detected: $($item.FileCount)"
        Write-Host "Destination: $target"

        Invoke-Mc -Mounts @($mount) -McArgs @("cp", "--recursive", "/data/", $target)
    }

    Write-Host "`nVerifying uploaded raw objects..." -ForegroundColor Green
    Invoke-Mc -McArgs @("ls", "sibi_local/sibi-raw/$UploadPrefix/")

    Write-Host "`nVerifying uploaded processed objects..." -ForegroundColor Green
    Invoke-Mc -McArgs @("ls", "sibi_local/sibi-processed/$UploadPrefix/")

    Write-Host "`n[SUCCESS] Dataset upload commands completed." -ForegroundColor Green
    Write-Host "Upload prefix: $UploadPrefix" -ForegroundColor Yellow
    Write-Host "Raw bucket: sibi-raw/$UploadPrefix/"
    Write-Host "Processed bucket: sibi-processed/$UploadPrefix/"
}
finally {
    if ($VolumeCreated) {
        docker volume rm $McConfigVolume | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Temporary MinIO Client volume could not be removed: $McConfigVolume"
        }
    }
}
