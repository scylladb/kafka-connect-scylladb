#!/usr/bin/env bash
# Drives the Aerospike CDC lab. See README.md.
set -euo pipefail

CALLER_DIR="${PWD}"
LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${LAB_DIR}/../../.." && pwd)"
cd "${LAB_DIR}"

# One sink connector per outbound format, so a failure on one topic does not
# stop the other.
CONNECTORS=(json flatjson)

compose() { docker compose "$@"; }
aql() { compose run --rm -T tools aql -h aerospike -c "$1"; }

# Copies the sink package of the current project version, so stale
# packages of older versions never land on the Connect plugin path.
# Empties .plugin/ in place: removing it would leave a running Connect
# container with a dangling bind mount.
stage_plugin() {
  local version pkg
  version="$(cd "${REPO_DIR}" && mvn -q help:evaluate -Dexpression=project.version -DforceStdout)"
  pkg="${REPO_DIR}/target/components/packages/scylladb-kafka-connect-scylladb-${version}"
  if [[ ! -d "${pkg}" ]]; then
    echo "Missing ${pkg}; run 'mvn -DskipTests package' in ${REPO_DIR} first." >&2
    exit 1
  fi
  mkdir -p .plugin
  find .plugin -mindepth 1 -delete
  cp -r "${pkg}" .plugin/
}

up() {
  local connect_running name body
  connect_running="$(compose ps -q --status running connect)"
  stage_plugin
  # Connect loads plugins only at startup.
  if [[ -n "${connect_running}" ]]; then
    compose restart connect
  fi
  compose up -d --wait
  compose exec -T scylla cqlsh -f /opt/lab/schema.cql
  for name in "${CONNECTORS[@]}"; do
    if ! body="$(curl -sS --fail-with-body -X PUT -H 'Content-Type: application/json' \
      --data @"connect/scylladb-sink-${name}.json" \
      "localhost:8083/connectors/scylladb-sink-${name}/config")"; then
      echo "Registering scylladb-sink-${name} failed: ${body}" >&2
      exit 1
    fi
  done
  echo "Lab is up (FORMAT=${FORMAT:-$(sed -n 's/^FORMAT=//p' .env)})."
}

# Writes one record with a stored user key, so the key carries it too.
write() {
  local pk="${1:-u1}"
  aql "SET KEY_SEND true; INSERT INTO test.users (PK, name, age) VALUES ('${pk}', 'alice', 30)"
}

# Durable delete leaves a tombstone, and XDR ships it.
delete() {
  local pk="${1:-u1}"
  aql "SET DURABLE_DELETE true; DELETE FROM test.users WHERE PK = '${pk}'"
}

# Dumps every message of a topic (timestamp, partition, offset, headers, key,
# value). A relative dir is resolved against the caller's directory. The
# previous capture is kept unless the new one succeeds.
capture() {
  local topic="${1:?topic}" out="${2:-${LAB_DIR}/captures}" file err
  [[ "${out}" = /* ]] || out="${CALLER_DIR}/${out}"
  # Checked first, because consuming a missing topic auto-creates it.
  if ! compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
    --describe --topic "${topic}" >/dev/null; then
    echo "Cannot capture ${topic}: topic missing or Kafka unreachable." >&2
    exit 1
  fi
  mkdir -p "${out}"
  file="${out}/${topic}.txt"
  err="$(mktemp)"
  # The consumer stops after 5 s without new messages.
  if ! compose exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 --topic "${topic}" --from-beginning \
    --timeout-ms 5000 \
    --property print.partition=true --property print.offset=true \
    --property print.timestamp=true --property print.headers=true \
    --property print.key=true --property key.separator=' | ' \
    >"${file}.tmp" 2>"${err}" \
    || ! grep -q '^Processed a total of' "${err}"; then
    cat "${err}" >&2
    rm -f "${file}.tmp" "${err}"
    echo "Capture of ${topic} failed." >&2
    exit 1
  fi
  rm -f "${err}"
  mv "${file}.tmp" "${file}"
  echo "$(wc -l <"${file}") message(s) -> ${file}"
}

status() {
  local name
  for name in "${CONNECTORS[@]}"; do
    curl -s "localhost:8083/connectors/scylladb-sink-${name}/status"
    echo
  done
}

down() {
  compose --profile '*' down -v --remove-orphans
}

cmd="${1:-}"
shift || true
case "${cmd}" in
  up | write | delete | capture | status | down) "${cmd}" "$@" ;;
  *)
    echo "Usage: $0 {up|write [pk]|delete [pk]|capture <topic> [dir]|status|down}" >&2
    exit 2
    ;;
esac
