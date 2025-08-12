#!/usr/bin/env bash
set -euo pipefail

source "${BASHDB_ROOT}/lib/common.sh"
source "${BASHDB_ROOT}/lib/storage.sh"

dml::parse_values_inside_parens() {
  local sql="$1"
  awk 'BEGIN{IGNORECASE=1} {if (match($0,/VALUES[[:space:]]*\(([^)]*)\)/,m)) {print m[1]}}' <<< "${sql}"
}

dml::parse_columns_inside_parens_after_table() {
  local sql="$1"; local table="$2"
  awk -v tbl="${table}" 'BEGIN{IGNORECASE=1} {
    pat="[[:space:]]*" tbl "[[:space:]]*\(([^)]*)\)";
    if (match($0,pat,m)) {print m[1]}
  }' <<< "${sql}"
}

dml::strip_quotes() {
  local s="$1"
  s="$(common::trim "${s}")"
  if [[ "${s}" == "''" || "${s}" == '""' ]]; then echo -n ""; return; fi
  if [[ ( "${s}" == "'"*"'" ) || ( "${s}" == '"'*'"' ) ]]; then
    echo -n "${s:1:${#s}-2}"
  else
    echo -n "${s}"
  fi
}

dml::insert_into() {
  local sql="$1"

  local table
  table=$(echo "${sql}" | awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){if(toupper($i)=="INSERT" && toupper($(i+1))=="INTO"){print $(i+2); exit}}}')
  table="${table%%(*}"
  table="$(common::trim "${table}")"
  storage::ensure_safe_table_name "${table}"
  storage::table_exists "${table}" || common::die "INSERT: table not found: ${table}"

  local header types
  header="$(storage::get_header "${table}")"
  types="$(storage::get_types_line "${table}")"

  local col_list
  col_list="$(dml::parse_columns_inside_parens_after_table "${sql}" "${table}")"

  local values_list
  values_list="$(dml::parse_values_inside_parens "${sql}")"
  [[ -n "${values_list}" ]] || common::die "INSERT: VALUES (...) not found"

  local -a cols=()
  if [[ -n "${col_list}" ]]; then
    while IFS= read -r c; do
      c="$(common::trim "${c}")"
      cols+=("${c}")
    done < <(common::split_csv_simple "${col_list}")
  else
    while IFS=$'\t' read -r -a arr; do
      for c in "${arr[@]}"; do cols+=("${c}"); done
    done <<< "${header}"
  fi

  declare -A col_to_val
  local -a input_vals=()
  while IFS= read -r v; do
    v="$(dml::strip_quotes "${v}")"
    input_vals+=("${v}")
  done < <(common::split_csv_simple "${values_list}")

  ((${#cols[@]} == ${#input_vals[@]})) || common::die "INSERT: number of columns and values differ"
  for i in "${!cols[@]}"; do
    col_to_val["${cols[$i]}"]="${input_vals[$i]}"
  done

  local -a header_cols=()
  while IFS=$'\t' read -r -a arr; do
    for c in "${arr[@]}"; do header_cols+=("${c}"); done
  done <<< "${header}"

  local first=1
  for c in "${header_cols[@]}"; do
    local v="${col_to_val[$c]:-}"
    if [[ ${first} -eq 1 ]]; then
      printf '%s' "${v}"
      first=0
    else
      printf '\t%s' "${v}"
    fi
  done >> "$(storage::data_path "${table}")"
  printf '\n' >> "$(storage::data_path "${table}")"
  echo "OK"
}

dml::update_table() {
  local sql="$1"
  local table
  table=$(echo "${sql}" | awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){if(toupper($i)=="UPDATE"){print $(i+1); exit}}}')
  table="$(common::trim "${table}")"
  storage::ensure_safe_table_name "${table}"
  storage::table_exists "${table}" || common::die "UPDATE: table not found: ${table}"

  local set_clause
  set_clause=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/SET[[:space:]]*(.*)/,m)) print m[1]}' <<< "${sql}")
  set_clause="${set_clause%%WHERE*}"
  set_clause="$(common::trim "${set_clause}")"
  [[ -n "${set_clause}" ]] || common::die "UPDATE: SET clause not found"

  local where_clause
  where_clause=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/WHERE[[:space:]]*(.*)/,m)) print m[1]}' <<< "${sql}")
  where_clause="$(common::trim "${where_clause}")"

  local data_file schema_file tmp_file
  data_file="$(storage::data_path "${table}")"
  schema_file="$(storage::schema_path "${table}")"
  tmp_file="${data_file}.tmp.$$"

  local set_map=""
  while IFS= read -r pair; do
    [[ -z "${pair}" ]] && continue
    local name value
    name=$(echo "${pair}" | awk -F '=' '{print $1}')
    value=$(echo "${pair}" | awk -F '=' '{print substr($0, index($0,$2))}')
    name="$(common::upper "$(common::trim "${name}")")"
    value="$(dml::strip_quotes "${value}")"
    if [[ -z "${set_map}" ]]; then set_map="${name}=${value}"; else set_map+=";${name}=${value}"; fi
  done < <(common::split_csv_simple "${set_clause}")

  awk -v FS='\t' -v OFS='\t' -v SCH="${schema_file}" -v COND="${where_clause}" -v SETMAP="${set_map}" '
    BEGIN{
      IGNORECASE=1
      while ((getline line < SCH)>0){ split(line,a, "\t"); schema_names[++sn]=a[1]; schema_types[sn]=toupper(a[2]) }
    }
    NR==1{
      for (i=1;i<=NF;i++){ u=toupper($i); colidx[u]=i; coltype[i]=schema_types[i] }
      print $0; next
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
      if (!cond_ok) { print $0; next }
      rest = SETMAP
      while (length(rest) > 0) {
        semi = index(rest, ";");
        if (semi == 0) seg = rest; else seg = substr(rest, 1, semi-1)
        if (semi == 0) rest = ""; else rest = substr(rest, semi+1)
        if (seg == "") continue
        eq = index(seg, "=")
        if (eq == 0) continue
        key = toupper(substr(seg, 1, eq-1))
        valset = substr(seg, eq+1)
        idx = colidx[key]
        if (!idx) continue
        $idx = valset
      }
      print $0
    }
  ' "${data_file}" > "${tmp_file}"

  storage::atomic_write "${tmp_file}" "${data_file}"
  echo "OK"
}

dml::delete_from() {
  local sql="$1"
  local table
  table=$(echo "${sql}" | awk 'BEGIN{IGNORECASE=1} {for (i=1;i<=NF;i++){if(toupper($i)=="FROM"){print $(i+1); exit}}}')
  table="$(common::trim "${table}")"
  storage::ensure_safe_table_name "${table}"
  storage::table_exists "${table}" || common::die "DELETE: table not found: ${table}"

  local where_clause
  where_clause=$(awk 'BEGIN{IGNORECASE=1} {if (match($0,/WHERE[[:space:]]*(.*)/,m)) print m[1]}' <<< "${sql}")
  where_clause="$(common::trim "${where_clause}")"

  local data_file schema_file tmp_file
  data_file="$(storage::data_path "${table}")"
  schema_file="$(storage::schema_path "${table}")"
  tmp_file="${data_file}.tmp.$$"

  awk -v FS='\t' -v OFS='\t' -v SCH="${schema_file}" -v COND="${where_clause}" '
    BEGIN{
      IGNORECASE=1
      while ((getline line < SCH)>0){ split(line,a, "\t"); schema_names[++sn]=a[1]; schema_types[sn]=toupper(a[2]) }
    }
    NR==1{
      for (i=1;i<=NF;i++){ u=toupper($i); colidx[u]=i; coltype[i]=schema_types[i] }
      print $0; next
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
      if (!cond_ok) { print $0 }
    }
  ' "${data_file}" > "${tmp_file}"

  storage::atomic_write "${tmp_file}" "${data_file}"
  echo "OK"
}


