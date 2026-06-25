#!/usr/bin/env bash
# Launch steady-consumer-bench with heap sized for N long-lived consumers.
#
# Usage:
#   ./run.sh [jar-args...]
#   CONSUMERS=110 ./run.sh mode=assign topic=big-test fetch.max.wait.ms=500
#   CONSUMERS=9   ./run.sh mode=subscribe heartbeat.interval.ms=3000
#
# Heap formula: Xmx ~= 512MB base + 40MB x consumers (capped 8g).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${SCRIPT_DIR}/target/steady-consumer-bench.jar"

if [[ ! -f "$JAR" ]]; then
  echo "Jar not found. Build first: ./build.sh   (or: mvn -q clean package)" >&2
  exit 1
fi

CONSUMERS="${CONSUMERS:-110}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-big-test}"

for arg in "$@"; do
  if [[ "$arg" == consumers=* ]]; then
    CONSUMERS="${arg#consumers=}"
  fi
done

heap_mb=$((512 + CONSUMERS * 40))
if (( heap_mb > 8192 )); then heap_mb=8192; fi
if (( heap_mb < 1024 )); then heap_mb=1024; fi
xms_mb=$((heap_mb / 2))
if (( xms_mb < 512 )); then xms_mb=512; fi

JAVA_OPTS="${JAVA_OPTS:-}"
JAVA_OPTS+=" -Xms${xms_mb}m -Xmx${heap_mb}m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xss512k"

echo "JVM: -Xms${xms_mb}m -Xmx${heap_mb}m (consumers=${CONSUMERS})"

exec java ${JAVA_OPTS} -jar "$JAR" \
  bootstrap="${BOOTSTRAP}" \
  topic="${TOPIC}" \
  consumers="${CONSUMERS}" \
  "$@"
