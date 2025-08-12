#!/usr/bin/env bash
set -euo pipefail

source "${BASHDB_ROOT}/lib/common.sh"
source "${BASHDB_ROOT}/lib/storage.sh"

ddl::create_table() {
  local sql="$1"

  # Extract table name (token after CREATE TABLE)
  local table
  table=$(echo "${sql}" | awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){if(toupper($i)=="CREATE" && toupper($(i+1))=="TABLE"){print $(i+2); exit}}}')
  table="${table%%(*}"
  table="$(common::trim "${table}")"
  [[ -n "${table}" ]] || common::die "CREATE TABLE: could not parse table name"
  storage::ensure_safe_table_name "${table}"
  storage::table_exists "${table}" && common::die "Table already exists: ${table}"

  # Extract columns section between first ( and last )
  local inner
  inner="${sql#*\(}"
  inner="${inner%)*}"
  inner="$(common::trim "${inner}")"
  [[ -n "${inner}" ]] || common::die "CREATE TABLE: column definitions required"

  local cols=()
  local types=()
  while IFS= read -r tok; do
    [[ -z "${tok}" ]] && continue
    # Split into name and type (first two tokens)
    local name type
    name=$(echo "${tok}" | awk '{print $1}')
    type=$(echo "${tok}" | awk '{print $2}')
    name="$(common::trim "${name}")"
    type="$(common::upper "$(common::trim "${type}")")"
    [[ -n "${name}" && -n "${type}" ]] || common::die "Invalid column spec: ${tok}"
    [[ "${type}" =~ ^(INT|FLOAT|TEXT)$ ]] || common::die "Unsupported type for ${name}: ${type}"
    [[ "${name}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || common::die "Invalid column name: ${name}"
    cols+=("${name}")
    types+=("${type}")
  done < <(common::split_csv_simple "${inner}")

  ((${#cols[@]} > 0)) || common::die "CREATE TABLE: no columns parsed"

  local schema_file data_file
  schema_file="$(storage::schema_path "${table}")"
  data_file="$(storage::data_path "${table}")"
  {
    for i in "${!cols[@]}"; do
      printf '%s\t%s\n' "${cols[$i]}" "${types[$i]}"
    done
  } > "${schema_file}"

  {
    local first=1
    for c in "${cols[@]}"; do
      if [[ ${first} -eq 1 ]]; then
        printf '%s' "${c}"
        first=0
      else
        printf '\t%s' "${c}"
      fi
    done
    printf '\n'
  } > "${data_file}"

  echo "OK"
}

ddl::drop_table() {
  local sql="$1"
  local table
  table=$(echo "${sql}" | awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){if(toupper($i)=="DROP" && toupper($(i+1))=="TABLE"){print $(i+2); exit}}}')
  table="$(common::trim "${table}")"
  table="${table%;}"
  [[ -n "${table}" ]] || common::die "DROP TABLE: could not parse table name"
  storage::ensure_safe_table_name "${table}"
  local schema_file data_file
  schema_file="$(storage::schema_path "${table}")"
  data_file="$(storage::data_path "${table}")"
  [[ -f "${schema_file}" || -f "${data_file}" ]] || common::die "Table not found: ${table}"
  rm -f "${schema_file}" "${data_file}"
  echo "OK"
}
