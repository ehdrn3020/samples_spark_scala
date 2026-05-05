#!/bin/bash
# 실행 : /home/dgk/clean_spark_history.sh 1000

set -u
set -o pipefail

HDFS_BIN="/rnd/hadoop/default/bin/hdfs"
TARGET_DIR="/user/spark/applicationHistory"
LOG_DIR="/home/dgk/logs"
LOG_FILE="${LOG_DIR}/clean_spark_history_$(date '+%Y%m%d').log"
# 첫 번째 인자: 몇 개 처리할지
LIMIT="${1:-1000}"

mkdir -p "$LOG_DIR"

echo "===== $(date '+%F %T') START =====" >> "$LOG_FILE"

# 삭제 대상 추출
FILES=$($HDFS_BIN dfs -ls "$TARGET_DIR" 2>>"$LOG_FILE" | head -n "$LIMIT" | awk '{print $8}')

if [ -z "$FILES" ]; then
  echo "$(date '+%F %T') No files found." >> "$LOG_FILE"
  echo "===== $(date '+%F %T') END =====" >> "$LOG_FILE"
  exit 0
fi

echo "$(date '+%F %T') Delete target list:" >> "$LOG_FILE"
echo "$FILES" >> "$LOG_FILE"

# 실제 삭제
echo "$FILES" | xargs -r -n 1 $HDFS_BIN dfs -rm >> "$LOG_FILE" 2>&1

RC=$?

echo "$(date '+%F %T') Exit code: $RC" >> "$LOG_FILE"
echo "===== $(date '+%F %T') END =====" >> "$LOG_FILE"

exit $RC