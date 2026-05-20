from sqlalchemy import types
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import compiler
from sqlalchemy.sql.functions import GenericFunction

from sqlalchemy_kusto.dialect_base import KustoBaseDialect

# PostgreSQL date_trunc grain → T-SQL datepart name
_GRAIN_TO_TSQL: dict[str, str] = {
    "microseconds": "millisecond",  # T-SQL has no microsecond
    "milliseconds": "millisecond",
    "second": "second",
    "minute": "minute",
    "hour": "hour",
    "day": "day",
    "week": "week",
    "week_sun": "week",  # extension: Sunday-based week (not in PostgreSQL)
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}

# Safe DATEDIFF base epoch per grain.
# For sub-day grains, epoch 0 (1900-01-01) causes integer overflow; use 2000-01-01.
# week_sun uses epoch -1 = 1899-12-31 (Sunday), so DATEADD/DATEDIFF stay in Sunday-aligned weeks.
_GRAIN_EPOCH: dict[str, str] = {
    "microseconds": "'2000-01-01'",
    "milliseconds": "'2000-01-01'",
    "second": "'2000-01-01'",
    "minute": "'2000-01-01'",
    "hour": "'2000-01-01'",
    "day": "0",
    "week": "0",
    "week_sun": "-1",
    "month": "0",
    "quarter": "0",
    "year": "0",
}


def _translate_raw_date_trunc(sql: str) -> str:
    """Translate date_trunc('grain', expr) calls in a raw SQL string to T-SQL DATEADD/DATEDIFF.

    Only the kustosql dialect calls this.  The kql dialect has its own compiler path.
    Grain matching is case-insensitive.  Unknown grains are left unchanged.
    """
    marker = "date_trunc("
    result: list[str] = []
    i = 0
    sql_lower = sql.lower()

    while i < len(sql):
        pos = sql_lower.find(marker, i)
        if pos == -1:
            result.append(sql[i:])
            break

        result.append(sql[i:pos])

        # Walk forward tracking depth to find the matching ')'.
        depth = 1
        j = pos + len(marker)
        while j < len(sql) and depth > 0:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
            j += 1

        if depth != 0:
            # Unbalanced parentheses — leave the rest unchanged.
            result.append(sql[pos:])
            i = len(sql)
            break

        interior = sql[pos + len(marker) : j - 1]

        # Split interior at the first top-level comma to get grain and expression.
        comma_pos: int | None = None
        inner_depth = 0
        for k, ch in enumerate(interior):
            if ch == "(":
                inner_depth += 1
            elif ch == ")":
                inner_depth -= 1
            elif ch == "," and inner_depth == 0:
                comma_pos = k
                break

        if comma_pos is None:
            # Malformed call — leave unchanged.
            result.append(sql[pos:j])
            i = j
            continue

        grain_raw = interior[:comma_pos].strip().strip("'\"")
        grain = grain_raw.lower()
        expr = interior[comma_pos + 1 :].strip()

        part = _GRAIN_TO_TSQL.get(grain)
        if part is None:
            # Unknown grain — leave the whole call unchanged.
            result.append(sql[pos:j])
            i = j
            continue

        epoch = _GRAIN_EPOCH.get(grain, "0")
        if grain == "week":
            # T-SQL DATEDIFF(week,...) counts Sunday boundaries, but epoch 0 is Monday.
            # Shift the input back 1 day so Sunday stays inside its Mon-Sat week,
            # producing ISO-standard Monday-based truncation (matches PostgreSQL semantics).
            result.append(
                f"DATEADD({part}, DATEDIFF({part}, {epoch}, DATEADD(day, -1, {expr})), {epoch})"
            )
        else:
            result.append(f"DATEADD({part}, DATEDIFF({part}, {epoch}, {expr}), {epoch})")
        i = j

    return "".join(result)


class date_trunc(GenericFunction):  # noqa: N801
    """PostgreSQL-style date_trunc compiled to T-SQL DATEADD/DATEDIFF for Kusto."""

    name = "date_trunc"
    type = types.DateTime()
    inherit_cache = True


@compiles(date_trunc, "kustosql")
def _compile_date_trunc(element, compiler, **kw):
    grain_clause, col_clause = list(element.clauses)

    if hasattr(grain_clause, "value"):
        grain = str(grain_clause.value).strip("'\" ").lower()
    else:
        grain = compiler.process(grain_clause, literal_binds=True, **kw).strip("'\" ").lower()

    t_sql_part = _GRAIN_TO_TSQL.get(grain, grain)
    epoch = _GRAIN_EPOCH.get(grain, "0")
    col = compiler.process(col_clause, **kw)

    if grain == "week":
        # T-SQL DATEDIFF(week,...) counts Sunday boundaries, but epoch 0 is Monday.
        # Shift the input back 1 day so Sunday stays inside its Mon-Sat week,
        # producing ISO-standard Monday-based truncation (matches PostgreSQL semantics).
        return f"DATEADD({t_sql_part}, DATEDIFF({t_sql_part}, {epoch}, DATEADD(day, -1, {col})), {epoch})"

    return f"DATEADD({t_sql_part}, DATEDIFF({t_sql_part}, {epoch}, {col}), {epoch})"


class KustoSqlCompiler(compiler.SQLCompiler):
    def get_select_precolumns(self, select, **kw) -> str:
        """Kusto uses TOP instead of LIMIT; also requires TOP when ORDER BY has no LIMIT."""
        select_precolumns = super().get_select_precolumns(select, **kw)

        if select._limit_clause is not None:
            kw["literal_execute"] = True
            select_precolumns += f"TOP {self.process(select._limit_clause, **kw)} "
        elif select._order_by_clauses:
            # Kusto T-SQL requires a TOP clause when ORDER BY is present without LIMIT
            select_precolumns += f"TOP {self.dialect.max_top_n} "

        return select_precolumns

    def limit_clause(self, select, **kw):
        """Do not add LIMIT to the end of the query."""
        return ""

    def visit_sequence(self, sequence, **kw):
        pass

    def visit_empty_set_expr(self, element_types):
        pass

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass

    def delete_extra_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass


class KustoSqlHttpsDialect(KustoBaseDialect):
    name = "kustosql"
    statement_compiler = KustoSqlCompiler
    supports_native_boolean = False
    # For some reason supports_statement_cache
    # doesn't work when defined in the KustoBaseDialect.
    # Need to investigate why it happens.
    supports_statement_cache = True

    def __init__(self, max_top_n: int = 500_000, **kwargs):
        super().__init__(**kwargs)
        self.max_top_n = max_top_n

    def do_execute(self, cursor, statement, parameters, context=None) -> None:
        """Translate date_trunc() in raw SQL before the cursor sends it to Kusto."""
        cursor.execute(_translate_raw_date_trunc(statement), parameters)
