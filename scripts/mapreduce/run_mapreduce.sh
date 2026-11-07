#!/bin/bash
# ==============================================================================
# Hadoop Streaming MapReduce Runner (Inside NameNode Container)
# Project: BIGDATA_SIBI
# Mode: LocalJobRunner Execution (mapreduce.framework.name=local, reduces=1)
# ==============================================================================

set -euo pipefail

echo "========================================================================"
echo " [BIGDATA_SIBI] Starting Hadoop Streaming Execution (LocalJobRunner)"
echo "========================================================================"

# 1. Parse Arguments or defaults
HDFS_INPUT="${1:-/sibi/input/bow_input_primary.tsv}"
HDFS_OUTPUT="${2:-/sibi/output/run_$(date +%Y%m%d_%H%M%S)}"

echo ">> Target HDFS Input : ${HDFS_INPUT}"
echo ">> Target HDFS Output: ${HDFS_OUTPUT}"

# 2. Verify Python 3 Availability
if command -v python3 >/dev/null 2>&1; then
    PY_BIN=$(command -v python3)
    echo ">> Python 3 runtime found at: ${PY_BIN} ($(${PY_BIN} --version))"
else
    echo ">> [FATAL ERROR] Python 3 is not installed inside container!" >&2
    exit 1
fi

# 3. Locate Hadoop Streaming Jar
STREAMING_JAR=""
CANDIDATE_PATHS=(
    "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
    "${HADOOP_HOME:-/opt/hadoop}/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
    "${HADOOP_HOME:-/opt/hadoop}/share/hadoop/tools/lib/hadoop-streaming"*.jar
    "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming"*.jar
)

for jar in "${CANDIDATE_PATHS[@]}"; do
    if [ -f "${jar}" ]; then
        STREAMING_JAR="${jar}"
        break
    fi
done

if [ -z "${STREAMING_JAR}" ] || [ ! -f "${STREAMING_JAR}" ]; then
    echo ">> [FATAL ERROR] Hadoop Streaming jar file not found!" >&2
    exit 1
fi
echo ">> Hadoop Streaming JAR found at: ${STREAMING_JAR}"

# 4. Verify Local Script Files
MAPPER_PATH="/workspace/scripts/mapreduce/mapper.py"
REDUCER_PATH="/workspace/scripts/mapreduce/reducer.py"

if [ ! -f "${MAPPER_PATH}" ]; then
    echo ">> [FATAL ERROR] Mapper script missing at: ${MAPPER_PATH}" >&2
    exit 1
fi

if [ ! -f "${REDUCER_PATH}" ]; then
    echo ">> [FATAL ERROR] Reducer script missing at: ${REDUCER_PATH}" >&2
    exit 1
fi
echo ">> Mapper script verified : ${MAPPER_PATH}"
echo ">> Reducer script verified: ${REDUCER_PATH}"

# 5. Verify HDFS Health and Input Existence
echo ">> Verifying HDFS connectivity..."
if ! hdfs dfs -test -e "${HDFS_INPUT}"; then
    echo ">> [FATAL ERROR] HDFS Input file does not exist: ${HDFS_INPUT}" >&2
    exit 1
fi
echo ">> HDFS Input file verified: ${HDFS_INPUT}"

# 6. Safety Check: Verify HDFS Output Does NOT Already Exist
if hdfs dfs -test -e "${HDFS_OUTPUT}"; then
    echo ">> [FATAL ERROR] HDFS Output directory already exists: ${HDFS_OUTPUT}" >&2
    echo ">> Aborting job to avoid overwriting or collision." >&2
    exit 1
fi
echo ">> Verified HDFS Output directory is clear: ${HDFS_OUTPUT}"

# 7. Execute Hadoop Streaming Job via LocalJobRunner
echo ">> Submitting Hadoop Streaming MapReduce Job (LocalJobRunner)..."
start_time=$(date +%s)

hadoop jar "${STREAMING_JAR}" \
    -D mapreduce.framework.name=local \
    -D mapreduce.job.reduces=1 \
    -D stream.num.map.output.key.fields=2 \
    -files "${MAPPER_PATH},${REDUCER_PATH}" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "${HDFS_INPUT}" \
    -output "${HDFS_OUTPUT}"

exit_code=$?
end_time=$(date +%s)
duration=$((end_time - start_time))

echo "========================================================================"
if [ ${exit_code} -eq 0 ]; then
    echo ">> [SUCCESS] MapReduce Job completed successfully in ${duration} seconds."
    echo ">> HDFS Output Directory: ${HDFS_OUTPUT}"
    echo ">> Listing output files:"
    hdfs dfs -ls "${HDFS_OUTPUT}"
else
    echo ">> [FAILED] MapReduce Job failed with exit code: ${exit_code} (Duration: ${duration}s)" >&2
    exit ${exit_code}
fi
echo "========================================================================"

