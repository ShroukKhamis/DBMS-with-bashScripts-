#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export BASHDB_DIR="${ROOT_DIR}/.testdata"
rm -rf "${BASHDB_DIR}"
mkdir -p "${BASHDB_DIR}"

echo "[tests] Using data dir: ${BASHDB_DIR}"

"${ROOT_DIR}/bin/bashdb" --init
"${ROOT_DIR}/bin/bashdb" -f "${ROOT_DIR}/examples/demo.sql" --echo

echo
echo "[tests] Smoke SELECT after demo:"
"${ROOT_DIR}/bin/bashdb" -f <(echo "SELECT name, age FROM users ORDER BY age DESC LIMIT 2;")

echo "[tests] OK"


