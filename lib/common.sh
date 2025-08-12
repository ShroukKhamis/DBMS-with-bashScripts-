#!/usr/bin/env bash
set -euo pipefail

common::die() {
  echo "Error: $*" >&2
  exit 1
}

common::warn() {
  echo "Warn: $*" >&2
}

common::trim() {
  local s="$1"
  # shellcheck disable=SC2001
  s="$(echo -n "${s}" | sed -e 's/^\s\+//' -e 's/\s\+$//')"
  echo -n "${s}"
}

common::upper() {
  echo -n "$1" | tr '[:lower:]' '[:upper:]'
}

common::init_env() {
  # Default DB_DIR
  DB_DIR_DEFAULT="${BASHDB_ROOT:-$(pwd)}/data"
  if [[ -f "${BASHDB_ROOT}/bashdb.conf" ]]; then
    # shellcheck disable=SC1090
    source "${BASHDB_ROOT}/bashdb.conf"
  fi
  export DB_DIR="${BASHDB_DIR:-${DB_DIR:-${DB_DIR_DEFAULT}}}"
  mkdir -p "${DB_DIR}"
}

common::split_csv_simple() {
  # Splits a comma-separated list into lines, no quoting support
  # Usage: common::split_csv_simple "a, b, c" | while read -r token; do ...; done
  echo "$1" | awk -v RS=',' '{gsub(/^\s+|\s+$/, ""); print}'
}

common::join_by_tab() {
  local IFS=$'\t'
  echo "$*"
}

common::is_integer() { [[ "$1" =~ ^-?[0-9]+$ ]]; }
common::is_float() { [[ "$1" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; }



