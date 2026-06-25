#!/usr/bin/env bash
# Launch raw-metadata-bench.
#
# Usage:
#   ./run.sh [jar-args...]
#   ./run.sh mode=alltopics rate=200 inflight=8 connections=4
#
# inflight=8  -> reproduces AutoMQ server-side pipeline (SocketServer mutes at >=8)
# inflight=1  -> standard-Kafka style (1 outstanding request per connection)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${SCRIPT_DIR}/target/raw-metadata-bench.jar"

if [[ ! -f "$JAR" ]]; then
  echo "Jar not found. Build first: ./build.sh   (or: mvn -q clean package)" >&2
  exit 1
fi

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

JAVA_OPTS="${JAVA_OPTS:-}"
JAVA_OPTS+=" -Xms512m -Xmx2g -XX:+UseG1GC"

exec java ${JAVA_OPTS} -jar "$JAR" host="${BOOTSTRAP}" "$@"
