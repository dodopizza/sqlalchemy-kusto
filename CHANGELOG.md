# Changelog

## 4.0.0

The KQL compiler (`sqlalchemy_kusto/dialect_kql.py`) is rewritten from scratch. It
walks the SQLAlchemy expression tree with visitor methods instead of fixing compiled
SQL text with regular expressions. The output changes for every statement, hence the
major version.

Fixed:

- Saved metrics without extra arguments compiled to `dcount(["x"]None)`.
- `HAVING` was silently dropped.
- `DISTINCT` was silently dropped (filter values had duplicates).
- Series limit: the join on a grouped subquery was dropped for a physical table and
  crashed for a virtual one.
- A virtual dataset starting with a `//` comment before `let` produced a syntax error.
- `=`, `'` and `"` inside string literals were rewritten by the text-based `WHERE` conversion.
- `LIKE '%x%'` compiled to `has_cs` (whole-term match); it is now `contains_cs` (substring).
- `COUNT(col)` compiled to `count(["col"])`, which Kusto rejects; it is now `countif(isnotnull(col))`.
- `ORDER BY` without a direction sorted descending (the KQL default); `asc` is now explicit.
- Group-by columns produced `extend ["X"] = ["X"]` noise; aliases now live in `summarize ... by alias = expr`.

Changed output:

- String literals are double-quoted (`"ru"`), not single-quoted.
- `text()` in `FROM` passes through as written; `schema.table` is no longer converted to `database("schema").["table"]` (pass `schema=` on the table instead).
- Table aliases are ignored (KQL has none); column references never carry a table prefix.
- Bind parameters are always rendered inline; `supports_statement_cache` is off.

Added:

- `kustokql+http://` for the Kusto emulator (`KustoKqlHttpDialect`).
- `OFFSET` via `serialize | where row_number() > n`.
- Superset's escaped colons in `text()` (`datetime(2026-01-01T00\:00\:00)`) are unescaped.
- Uppercased function names in KQL text (`TOLOWER(x)`, from Superset's sqlglot sanitizer) are lowercased.
