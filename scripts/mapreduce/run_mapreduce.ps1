<#
.SYNOPSIS
    Host PowerShell controller to orchestrate Hadoop MapReduce execution for BIGDATA_SIBI.
.DESCRIPTION
    Manages end-to-end execution of Hadoop Streaming WordCount in LocalJobRunner mode:
    1. Validates prerequisites (Docker container status).
    2. Selects target dataset (primary [default] or all).
    3. Generates a timestamped Run ID.
    4. Stages input files to HDFS logically distributed storage.
    5. Executes Hadoop Streaming LocalJobRunner inside NameNode container.
    6. Extracts resulting part-00000 file to output/mapreduce/<run_id>/.
    7. Retains complete run history without overwriting previous outputs.
.PARAMETER DatasetType
    Specifies dataset scope: 'primary' (Books 1, 2, 3 - default) or 'all' (Books 1, 2, 3, 4).
.EXAMPLE
    .\run_mapreduce.ps1 -DatasetType primary
.EXAMPLE
    .\run_mapreduce.ps1 -DatasetType all
#>

[CmdletBinding()]
param (
    [ValidateSet("primary", "all")]
    [string]$DatasetType = "primary"
)

$ErrorActionPreference = "Stop"

# Define project directories relative to script location
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path "$ScriptDir\..\..").Path
$ProcessedDir = "$ProjectRoot\dataset\processed"
$OutputDir = "$ProjectRoot\output\mapreduce"

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " [BIGDATA_SIBI] Hadoop MapReduce Execution Controller (LocalJobRunner)" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Project Root : $ProjectRoot"
Write-Host " Dataset Scope: $DatasetType"

# 1. Determine Input File
if ($DatasetType -eq "primary") {
    $LocalInputFilename = "bow_input_primary.tsv"
} else {
    $LocalInputFilename = "bow_input.tsv"
}

$LocalInputPath = "$ProcessedDir\$LocalInputFilename"
if (-not (Test-Path $LocalInputPath)) {
    Write-Error "Target input file not found: $LocalInputPath"
    exit 1
}

$InputLines = (Get-Content $LocalInputPath | Measure-Object -Line).Lines
Write-Host " Target Input : $LocalInputFilename ($InputLines records)" -ForegroundColor Green

# 2. Check NameNode Container Status
$ContainerName = "sibi_namenode"
Write-Host " Checking container '$ContainerName' status..."
$ContainerRunning = docker ps --filter "name=$ContainerName" --filter "status=running" --format "{{.Names}}"
if (-not $ContainerRunning) {
    Write-Error "Container '$ContainerName' is not running! Please ensure 'docker compose up -d' was executed first."
    exit 1
}
Write-Host " Container '$ContainerName' is running." -ForegroundColor Green

# 3. Generate Timestamped Run ID
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$RunId = "run_${DatasetType}_${Timestamp}"
$HdfsInputDir = "/sibi/input"
$HdfsInputFile = "$HdfsInputDir/$LocalInputFilename"
$HdfsOutputDir = "/sibi/output/$RunId"
$LocalRunOutputDir = "$OutputDir\$RunId"

Write-Host " Run ID       : $RunId" -ForegroundColor Yellow
Write-Host " HDFS Input   : $HdfsInputFile"
Write-Host " HDFS Output  : $HdfsOutputDir"
Write-Host " Local Output : $LocalRunOutputDir"

# 4. Prepare HDFS Directory and Stage Input
Write-Host "`n>> Step 1: Staging input data into HDFS..." -ForegroundColor Cyan
docker exec $ContainerName hdfs dfs -mkdir -p $HdfsInputDir
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create HDFS input directory $HdfsInputDir"
    exit $LASTEXITCODE
}

docker exec $ContainerName hdfs dfs -put -f "/workspace/dataset/processed/$LocalInputFilename" $HdfsInputFile
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to upload $LocalInputFilename to HDFS"
    exit $LASTEXITCODE
}
Write-Host ">> Successfully staged $LocalInputFilename to $HdfsInputFile" -ForegroundColor Green

# 5. Execute MapReduce Job inside NameNode Container
Write-Host "`n>> Step 2: Executing Hadoop Streaming MapReduce Job (LocalJobRunner)..." -ForegroundColor Cyan
docker exec $ContainerName /bin/bash /workspace/scripts/mapreduce/run_mapreduce.sh "$HdfsInputFile" "$HdfsOutputDir"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Hadoop MapReduce execution failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

# 6. Retrieve Results from HDFS to Local Host
Write-Host "`n>> Step 3: Downloading results from HDFS to local output folder..." -ForegroundColor Cyan
if (-not (Test-Path $LocalRunOutputDir)) {
    New-Item -ItemType Directory -Path $LocalRunOutputDir -Force | Out-Null
}

docker exec $ContainerName hdfs dfs -get "$HdfsOutputDir/*" "/workspace/output/mapreduce/$RunId/"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to download results from HDFS to host directory $LocalRunOutputDir"
    exit $LASTEXITCODE
}

# Check result file
$LocalResultFile = "$LocalRunOutputDir\part-00000"
if (Test-Path $LocalResultFile) {
    $ResultLines = (Get-Content $LocalResultFile | Measure-Object -Line).Lines
    Write-Host "========================================================================" -ForegroundColor Green
    Write-Host " [SUCCESS] MapReduce Execution Finished Successfully!" -ForegroundColor Green
    Write-Host " Run ID           : $RunId" -ForegroundColor Green
    Write-Host " Output Directory : $LocalRunOutputDir" -ForegroundColor Green
    Write-Host " Output File      : part-00000 ($ResultLines distinct word records)" -ForegroundColor Green
    Write-Host "========================================================================" -ForegroundColor Green
} else {
    Write-Warning "Output directory created but part-00000 not found in $LocalRunOutputDir."
}

