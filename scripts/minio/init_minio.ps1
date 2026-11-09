<#
.SYNOPSIS
    Initializes MinIO buckets for BIGDATA_SIBI.
.DESCRIPTION
    Uses a temporary Docker volume for MinIO Client configuration.
    The alias and credentials persist across short-lived mc containers
    during this script, then the temporary volume is removed.
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
Write-Host " BIGDATA_SIBI - MinIO Bucket Initialization" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Project root : $ProjectRoot"
Write-Host "Docker network: $Network"
Write-Host "MinIO endpoint: $Endpoint"
Write-Host "Environment file: $EnvFile"

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

$Buckets = @("sibi-raw", "sibi-processed", "sibi-output")
$McConfigVolume = "sibi_mc_init_$([guid]::NewGuid().ToString('N'))"
$VolumeCreated = $false

function Invoke-Mc {
    param (
        [Parameter(Mandatory = $true)]
        [string[]]$McArgs
    )

    $dockerArgs = @(
        "run", "--rm",
        "--network", $Network,
        "--env", "MC_CONFIG_DIR=/mc-config",
        "--mount", "type=volume,source=$McConfigVolume,target=/mc-config",
        $McImage
    )
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

    Write-Host "`nChecking MinIO connection..." -ForegroundColor Cyan
    Invoke-Mc -McArgs @("ls", "sibi_local")

    foreach ($bucket in $Buckets) {
        Write-Host "`nEnsuring bucket exists: $bucket" -ForegroundColor Cyan
        Invoke-Mc -McArgs @("mb", "--ignore-existing", "sibi_local/$bucket")
    }

    Write-Host "`nFinal bucket listing:" -ForegroundColor Green
    Invoke-Mc -McArgs @("ls", "sibi_local")

    Write-Host "`n[SUCCESS] All required MinIO buckets are available." -ForegroundColor Green
}
finally {
    if ($VolumeCreated) {
        docker volume rm $McConfigVolume | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Temporary MinIO Client volume could not be removed: $McConfigVolume"
        }
    }
}
