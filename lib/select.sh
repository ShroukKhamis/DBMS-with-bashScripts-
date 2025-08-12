#!/usr/bin/env bash
set -euo pipefail

source "${BASHDB_ROOT}/lib/common.sh"
source "${BASHDB_ROOT}/lib/storage.sh"

select::_parse_table() {
  local sql="$1"
  awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){ if (toupper($i)=="FROM") {print $(i+1); exit}}}' <<< "${sql}"
}

select::_parse_columns() {
  local sql="$1"
  awk 'BEGIN{IGNORECASE=1} {if (match($0,/SELECT[[:space:]]*(.*)[[:space:]]+FROM/,m)) print m[1]}' <<< "${sql}"
}

select::_parse_where() {
  local sql="$1"
  local rest
  rest=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/WHERE[[:space:]]*(.*)/,m)) print m[1]}' <<< "${sql}")
  rest="${rest%%ORDER BY*}"
  rest="${rest%%LIMIT*}"
  rest="${rest%%OFFSET*}"
  common::trim "${rest}"
}

select::_parse_order_by() {
  local sql="$1"
  local ob
  ob=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/ORDER[[:space:]]+BY[[:space:]]*([^ ]+)/,m)) print m[1]}' <<< "${sql}")
  echo -n "${ob}"
}

select::_parse_order_dir() {
  local sql="$1"
  local dir
  dir=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/ORDER[[:space:]]+BY[[:space:]]*[^ ]+[[:space:]]+(ASC|DESC)/,m)) print toupper(m[1])}' <<< "${sql}")
  echo -n "${dir:-ASC}"
}

select::_parse_limit() {
  local sql="$1"
  awk 'BEGIN{IGNORECASE=1} {if (match($0,/LIMIT[[:space:]]*([0-9]+)/,m)) print m[1]}' <<< "${sql}"
}

select::_parse_offset() {
  local sql="$1"
  awk 'BEGIN{IGNORECASE=1} {if (match($0,/OFFSET[[:space:]]*([0-9]+)/,m)) print m[1]}' <<< "${sql}"
}

select::run_select() {
  local sql="$1"
  local table
  table="$(select::_parse_table "${sql}")"
  table="$(common::trim "${table}")"
  storage::ensure_safe_table_name "${table}"
  storage::table_exists "${table}" || common::die "SELECT: table not found: ${table}"

  local col_expr where_clause order_col order_dir limit_val offset_val
  col_expr="$(select::_parse_columns "${sql}")"
  where_clause="$(select::_parse_where "${sql}")"
  order_col="$(select::_parse_order_by "${sql}")"
  order_dir="$(select::_parse_order_dir "${sql}")"
  limit_val="$(select::_parse_limit "${sql}")"
  offset_val="$(select::_parse_offset "${sql}")"

  local header types data_file schema_file tmp_body tmp_sorted tmp_sliced
  data_file="$(storage::data_path "${table}")"
  schema_file="$(storage::schema_path "${table}")"
  header="$(head -n 1 "${data_file}")"
  types="$(storage::get_types_line "${table}")"

  tmp_body="${data_file}.body.$$"
  tmp_sorted="${data_file}.sorted.$$"
  tmp_sliced="${data_file}.sliced.$$"

  # Filter rows per WHERE to tmp_body (no header)
  awk -v FS='\t' -v OFS='\t' -v SCH="${schema_file}" -v COND="${where_clause}" '
    BEGIN{
      IGNORECASE=1
      while ((getline line < SCH)>0){ split(line,a, "\t"); schema_names[++sn]=a[1]; schema_types[sn]=toupper(a[2]) }
    }
    NR==1{
      for (i=1;i<=NF;i++){ u=toupper($i); colidx[u]=i; coltype[i]=schema_types[i] }
      next
    }
    {
      cond_ok = 1
      if (COND != ""){
        n = split(COND, parts, /[[:space:]]+[Aa][Nn][Dd][[:space:]]+/)
        for (j=1;j<=n;j++){
          part = parts[j]
          if (!match(part, /(<=|>=|!=|=|<|>|[Ll][Ii][Kk][Ee])/, m)) { cond_ok=0; break }
          op = m[1]
          l = substr(part, 1, RSTART-1)
          r = substr(part, RSTART+RLENGTH)
          gsub(/^\s+|\s+$/, "", l)
          gsub(/^\s+|\s+$/, "", r)
          lu = toupper(l)
          idx = colidx[lu]
          if (!idx) { cond_ok=0; break }
          val = $idx
          if (r ~ /^".*"$/ || r ~ /^'.*'$/) { r = substr(r,2,length(r)-2) }
          typ = coltype[idx]
          if (op ~ /^[Ll][Ii][Kk][Ee]$/){
            pat = r
            gsub(/([\^$.|?*+()\[\]{}])/, "\\&", pat)
            gsub(/%/, ".*", pat)
            if (val !~ ("^" pat "$")) { cond_ok=0; break }
          } else if (op == "=" || op == "!="){
            if (op=="=" && val != r) { cond_ok=0; break }
            if (op=="!=" && val == r) { cond_ok=0; break }
          } else {
            if (typ=="INT" || typ=="FLOAT") { v1=val+0; v2=r+0 } else { v1=val; v2=r }
            if (op=="<" && !(v1<v2)) { cond_ok=0; break }
            if (op==">" && !(v1>v2)) { cond_ok=0; break }
            if (op=="<=" && !(v1<=v2)) { cond_ok=0; break }
            if (op==">=" && !(v1>=v2)) { cond_ok=0; break }
          }
        }
      }
      if (cond_ok) print $0
    }
  ' "${data_file}" > "${tmp_body}"

  # Determine sort options if ORDER BY
  local sort_opts=""
  local order_idx=""
  if [[ -n "${order_col}" ]]; then
    local idx=1 typ="TEXT"
    local i=1
    while IFS=$'\t' read -r -a arr1; do
      true
    done <<< "${header}"
    while IFS=$'\t' read -r -a arr2; do
      true
    done <<< "${types}"
    # Find index of order_col
    i=1
    IFS=$'\t' read -r -a hcols <<< "${header}"
    IFS=$'\t' read -r -a tcols <<< "${types}"
    for ((i=0;i<${#hcols[@]};i++)); do
      if [[ "$(common::upper "${hcols[$i]}")" == "$(common::upper "${order_col}")" ]]; then
        idx=$((i+1))
        typ="$(common::upper "${tcols[$i]}")"
        break
      fi
    done
    order_idx="${idx}"
    sort_opts="-t $'\t' -k${idx},${idx}"
    if [[ "${typ}" == "INT" || "${typ}" == "FLOAT" ]]; then sort_opts+=" -n"; fi
    if [[ "${order_dir}" == "DESC" ]]; then sort_opts+=" -r"; fi
    # shellcheck disable=SC2086
    sort ${sort_opts} "${tmp_body}" > "${tmp_sorted}"
  else
    cp "${tmp_body}" "${tmp_sorted}"
  fi

  # Apply OFFSET and LIMIT
  local offset="${offset_val:-0}"
  local limit="${limit_val:-0}"
  if [[ -n "${offset}" && "${offset}" -gt 0 ]]; then
    tail -n +$((offset+1)) "${tmp_sorted}" > "${tmp_sliced}"
  else
    cp "${tmp_sorted}" "${tmp_sliced}"
  fi
  if [[ -n "${limit}" && "${limit}" -gt 0 ]]; then
    head -n "${limit}" "${tmp_sliced}" > "${tmp_sliced}.lim"
    mv "${tmp_sliced}.lim" "${tmp_sliced}"
  fi

  # Build selected columns indexes
  local selected="${col_expr}"
  local -a sel_idx=()
  local -a sel_names=()
  if [[ "${selected}" == "*" || -z "${selected}" ]]; then
    IFS=$'\t' read -r -a hcols <<< "${header}"
    for ((i=0;i<${#hcols[@]};i++)); do sel_idx+=("$((i+1))"); sel_names+=("${hcols[$i]}"); done
  else
    while IFS= read -r c; do
      c="$(common::trim "${c}")"
      sel_names+=("${c}")
      # find index
      IFS=$'\t' read -r -a hcols <<< "${header}"
      local found=0
      for ((i=0;i<${#hcols[@]};i++)); do
        if [[ "$(common::upper "${hcols[$i]}")" == "$(common::upper "${c}")" ]]; then sel_idx+=("$((i+1))"); found=1; break; fi
      done
      [[ ${found} -eq 1 ]] || common::die "SELECT: column not found: ${c}"
    done < <(common::split_csv_simple "${selected}")
  fi

  # Print header of selected columns
  {
    local first=1
    for name in "${sel_names[@]}"; do
      if [[ ${first} -eq 1 ]]; then printf '%s' "${name}"; first=0; else printf '\t%s' "${name}"; fi
    done
    printf '\n'
  }

  # Print rows with selected columns
  local idx_csv
  idx_csv=$(IFS=, ; echo "${sel_idx[*]}")
  awk -v FS='\t' -v OFS='\t' -v IDX="${idx_csv}" '
    BEGIN{
      n=split(IDX, idxs, ",")
    }
    {
      first=1
      for (i=1;i<=n;i++){
        v = $(idxs[i])
        if (first==1){ printf "%s", v; first=0 } else { printf "\t%s", v }
      }
      printf "\n"
    }
  ' "${tmp_sliced}"

  rm -f "${tmp_body}" "${tmp_sorted}" "${tmp_sliced}"
}


