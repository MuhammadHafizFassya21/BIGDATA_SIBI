#!/bin/bash

set -euo pipefail

# ==========================================
# BIGDATA_SIBI - HADOOP STREAMING MAPREDUCE
# Execution Mode: LocalJobRunner
# Hadoop Version: 3.2.1
# ==========================================

HDFS_INPUT="${1:-/user/sibi/input/bow_input_primary.tsv}"
HDFS_OUTPUT="${2:-/user/sibi/output/run_$(date +%Y%m%d_%H%M%S)}"

MAPPER_PATH="/workspace/scripts/mapreduce/mapper.py"
REDUCER_PATH="/workspace/scripts/mapreduce/reducer.py"

STREAMING_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

echo "=========================================="
echo " BIGDATA_SIBI - MAPREDUCE EXECUTION"
echo "=========================================="

echo "Input       : ${HDFS_INPUT}"
echo "Output      : ${HDFS_OUTPUT}"
echo "Framework   : LocalJobRunner"
echo "Reducers    : 1"

# 1. Check Python
echo ""
echo ">> Checking Python..."

python3 --version

# 2. Check required files
echo ""
echo ">> Checking required files..."

test -f "${MAPPER_PATH}"
test -f "${REDUCER_PATH}"
test -f "${STREAMING_JAR}"

echo "Mapper: FOUND"
echo "Reducer: FOUND"
echo "Streaming JAR: FOUND"

# 3. Check HDFS input
echo ""
echo ">> Checking HDFS input..."

hdfs dfs -test -e "${HDFS_INPUT}"

echo "HDFS input: FOUND"

# 4. Prevent output overwrite
if hdfs dfs -test -e "${HDFS_OUTPUT}"; then
    echo "ERROR: Output directory already exists!" >&2
    exit 1
fi

# 5. Execute Hadoop Streaming
echo ""
echo ">> Starting Hadoop Streaming..."

START_TIME=$(date +%s)

if hadoop jar "${STREAMING_JAR}" \
    -D mapreduce.framework.name=local \
    -D mapreduce.job.reduces=1 \
    -D stream.num.map.output.key.fields=2 \
    -files "${MAPPER_PATH},${REDUCER_PATH}" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "${HDFS_INPUT}" \
    -output "${HDFS_OUTPUT}"; then

    END_TIME=$(date +%s)

    DURATION=$((END_TIME - START_TIME))

    echo ""
    echo "=========================================="
    echo " MAPREDUCE EXECUTION SUCCESSFUL"
    echo "=========================================="

    echo "Execution time: ${DURATION} seconds"
    echo "HDFS Output: ${HDFS_OUTPUT}"

    echo ""
    echo ">> Output files:"

    hdfs dfs -ls "${HDFS_OUTPUT}"

else

    EXIT_CODE=$?

    echo ""
    echo "=========================================="
    echo " MAPREDUCE EXECUTION FAILED"
    echo "=========================================="

    echo "Exit code: ${EXIT_CODE}" >&2

    exit "${EXIT_CODE}"

fi

echo ""
echo "=========================================="
echo " BIGDATA_SIBI - JOB FINISHED"
echo "=========================================="