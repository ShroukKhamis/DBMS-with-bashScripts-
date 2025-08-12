#!/usr/bin/env bash
set -euo pipefail

source "${BASHDB_ROOT}/lib/common.sh"

storage::schema_path() { echo -n "${DB_DIR}/$1.schema"; }
storage::data_path() { echo -n "${DB_DIR}/$1.tsv"; }

storage::ensure_safe_table_name() {
  local name="$1"
  [[ "${name}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || common::die "Invalid table name: ${name}"
}

storage::table_exists() {
  local t="$1"
  [[ -f "$(storage::schema_path "${t}")" && -f "$(storage::data_path "${t}")" ]]
}

storage::get_header() {
  local t="$1"
  head -n 1 "$(storage::data_path "${t}")"
}

storage::get_types_line() {
  local t="$1"
  awk -F '\t' '{print $2}' "$(storage::schema_path "${t}")" | paste -sd $'\t' -
}

storage::atomic_write() {
  local tmp_file="$1"; shift
  local dest_file="$1"; shift
  mv "${tmp_file}" "${dest_file}"
}
