#!/usr/bin/env bash
# Maven-free build for raw-metadata-bench.
#
# This box has no `mvn`, so we compile against the kafka-clients jar already in
# ~/.m2 and assemble a runnable fat-jar by hand. If you DO have maven, just run
# `mvn -q clean package` instead - the pom.xml produces the same jar.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
M2="${HOME}/.m2/repository"
KAFKA_VER="3.9.0"

find_jar() { find "${M2}/$1" -name "$2" 2>/dev/null | grep -v sources | grep -v javadoc | head -1; }

KAFKA_CLIENTS="$(find_jar org/apache/kafka/kafka-clients/${KAFKA_VER} "kafka-clients-${KAFKA_VER}.jar")"
SLF4J_API="$(find_jar org/slf4j/slf4j-api "slf4j-api-*.jar")"
SLF4J_SIMPLE="$(find_jar org/slf4j/slf4j-simple "slf4j-simple-*.jar")"
LZ4="$(find_jar org/lz4/lz4-java "lz4-java-*.jar")"
SNAPPY="$(find_jar org/xerial/snappy/snappy-java "snappy-java-*.jar")"
ZSTD="$(find_jar com/github/luben/zstd-jni "zstd-jni-*.jar")"

if [[ -z "${KAFKA_CLIENTS}" ]]; then
  echo "ERROR: kafka-clients-${KAFKA_VER}.jar not found under ${M2}" >&2
  exit 1
fi

DEPS=("${KAFKA_CLIENTS}" "${SLF4J_API}" "${SLF4J_SIMPLE}" "${LZ4}" "${SNAPPY}" "${ZSTD}")
CP="$(IFS=:; echo "${DEPS[*]}")"

BUILD="${SCRIPT_DIR}/target"
CLASSES="${BUILD}/classes"
FATDIR="${BUILD}/fat"
JAR="${BUILD}/raw-metadata-bench.jar"
MAIN="io.automq.bench.RawMetadataBench"

rm -rf "${CLASSES}" "${FATDIR}" "${JAR}"
mkdir -p "${CLASSES}" "${FATDIR}"

echo "Compiling..."
find "${SCRIPT_DIR}/src/main/java" -name "*.java" > "${BUILD}/sources.txt"
javac -cp "${CP}" -d "${CLASSES}" @"${BUILD}/sources.txt"

echo "Assembling fat-jar..."
for dep in "${DEPS[@]}"; do
  [[ -n "${dep}" ]] && (cd "${FATDIR}" && jar xf "${dep}")
done
cp -R "${CLASSES}/." "${FATDIR}/"
# Drop signature files that break a merged jar.
rm -rf "${FATDIR}/META-INF/"*.SF "${FATDIR}/META-INF/"*.DSA "${FATDIR}/META-INF/"*.RSA 2>/dev/null || true

(cd "${FATDIR}" && jar cfe "${JAR}" "${MAIN}" .)
echo "Built: ${JAR}"
