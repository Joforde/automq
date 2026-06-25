#!/usr/bin/env bash
# Launch rebalance-storm-bench with JVM heap sized for N concurrent consumers.
#
# Usage:
#   ./run.sh [jar-args...]
#   CONSUMERS=100 ./run.sh topic=big-test alive.ms=2000
#
# Heap formula (big-test / high partition count):
#   Xmx ≈ 512MB base + 48MB × consumers  (capped 8g)
#   Xms = min(Xmx, max(1g, Xmx / 2))

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${SCRIPT_DIR}/target/rebalance-storm-bench.jar"

if [[ ! -f "$JAR" ]]; then
  echo "Jar not found. Run: mvn -q clean package" >&2
  exit 1
fi

CONSUMERS="${CONSUMERS:-100}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-big-test}"
GROUP_ID="${GROUP_ID:-rebalance-storm-bench}"

# Parse consumers= from args if present (overrides env)
for arg in "$@"; do
  if [[ "$arg" == consumers=* ]]; then
    CONSUMERS="${arg#consumers=}"
  fi
done

heap_mb=$((512 + CONSUMERS * 48))
if (( heap_mb > 8192 )); then
  heap_mb=8192
fi
if (( heap_mb < 1024 )); then
  heap_mb=1024
fi

xms_mb=$((heap_mb / 2))
if (( xms_mb < 1024 )); then
  xms_mb=1024
fi

JAVA_OPTS="${JAVA_OPTS:-}"
JAVA_OPTS+=" -Xms${xms_mb}m -Xmx${heap_mb}m"
JAVA_OPTS+=" -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
JAVA_OPTS+=" -XX:+HeapDumpOnOutOfMemoryError"
JAVA_OPTS+=" -XX:HeapDumpPath=/tmp/rebalance-storm-bench-heapdump.hprof"
# 100 consumers → 100 NetworkClient I/O threads; cap native thread stack footprint
JAVA_OPTS+=" -Xss512k"

echo "JVM: -Xms${xms_mb}m -Xmx${heap_mb}m (consumers=${CONSUMERS})"

exec java ${JAVA_OPTS} -jar "$JAR" \
  bootstrap="${BOOTSTRAP}" \
  topic="${TOPIC}" \
  group.id="${GROUP_ID}" \
  consumers="${CONSUMERS}" \
  "$@"
