## BashDB

**BashDB** is a tiny database engine that implements a pragmatic subset of SQL in pure Bash. It stores tables as tab-separated files (TSV) with a header row and a simple schema file per table. It is great for quick prototyping, small datasets, CI examples, or learning.

### Features

- **DDL**: `CREATE TABLE`, `DROP TABLE`
- **DML**: `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- **WHERE** with `=, !=, <, <=, >, >=`, and simple `LIKE` (% wildcard)
- **ORDER BY** single column (numeric or text)
- **LIMIT/OFFSET**
- **REPL** and script execution (`-f file.sql`)

### Non-goals / Limitations

- Not ACID; no concurrency control.
- No joins, no multi-table queries.
- `WHERE` supports clauses joined by `AND` only (no `OR`, no parentheses).
- `INSERT` values cannot contain unescaped commas; prefer simple strings.
- Types are advisory: `TEXT`, `INT`, `FLOAT`.

### Quick start

```bash
./bin/bashdb --init
./bin/bashdb
bashdb> CREATE TABLE users (id INT, name TEXT, age INT);
bashdb> INSERT INTO users (id, name, age) VALUES (1, 'Ada', 34);
bashdb> INSERT INTO users (id, name, age) VALUES (2, 'Linus', 53);
bashdb> SELECT id, name FROM users WHERE age > 40 ORDER BY id DESC LIMIT 1;
```

### Running SQL files

```bash
./bin/bashdb -f examples/demo.sql
```

### Data layout

- Tables live under `data/` by default.
- Each table has `data/<table>.tsv` (header + rows) and `data/<table>.schema` (one `name<TAB>TYPE` per line).

### Configure data directory

- Use env var `BASHDB_DIR=/path/to/dir ./bin/bashdb ...` or set in `bashdb.conf` at repo root:

```
DB_DIR=/absolute/path/to/data
```

### Supported SQL grammar (subset)

- CREATE TABLE
  - `CREATE TABLE tbl (col1 TYPE, col2 TYPE, ...)`
- DROP TABLE
  - `DROP TABLE tbl`
- INSERT
  - `INSERT INTO tbl (col1, col2) VALUES (v1, v2)`
  - `INSERT INTO tbl VALUES (v1, v2, ...)` (column order must match header)
- SELECT
  - `SELECT *|col[, col...] FROM tbl [WHERE ...] [ORDER BY col [ASC|DESC]] [LIMIT N] [OFFSET M]`
  - WHERE: `col op value` joined by `AND`, operators: `=, !=, <, <=, >, >=, LIKE`
- UPDATE
  - `UPDATE tbl SET col1 = v1[, col2 = v2...] [WHERE ...]`
- DELETE
  - `DELETE FROM tbl [WHERE ...]`

### Testing

```bash
bash tests/run.sh
```

### License

MIT



