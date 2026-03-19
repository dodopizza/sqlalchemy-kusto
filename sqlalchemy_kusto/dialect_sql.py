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
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}

# Safe DATEDIFF base epoch per grain.
# For sub-day grains, epoch 0 (1900-01-01) causes integer overflow; use 2000-01-01.
_GRAIN_EPOCH: dict[str, str] = {
    "microseconds": "'2000-01-01'",
    "milliseconds": "'2000-01-01'",
    "second": "'2000-01-01'",
    "minute": "'2000-01-01'",
    "hour": "'2000-01-01'",
    "day": "0",
    "week": "0",
    "month": "0",
    "quarter": "0",
    "year": "0",
}


class date_trunc(GenericFunction):  # noqa: N801
    """PostgreSQL-style date_trunc compiled to T-SQL DATEADD/DATEDIFF for Kusto."""

    name = "date_trunc"
    type = types.DateTime()
    inherit_cache = True


@compiles(date_trunc, "kustosql")
def _compile_date_trunc(element, compiler, **kw):  # noqa: ANN001, ANN201
    grain_clause, col_clause = list(element.clauses)

    if hasattr(grain_clause, "value"):
        grain = str(grain_clause.value).strip("'\" ").lower()
    else:
        grain = compiler.process(grain_clause, literal_binds=True, **kw).strip("'\" ").lower()

    t_sql_part = _GRAIN_TO_TSQL.get(grain, grain)
    epoch = _GRAIN_EPOCH.get(grain, "0")
    col = compiler.process(col_clause, **kw)

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

    def update_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        pass

    def delete_extra_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        pass


class KustoSqlHttpsDialect(KustoBaseDialect):
    name = "kustosql"
    statement_compiler = KustoSqlCompiler
    # For some reason supports_statement_cache
    # doesn't work when defined in the KustoBaseDialect.
    # Need to investigate why it happens.
    supports_statement_cache = True

    def __init__(self, max_top_n: int = 500_000, **kwargs):
        super().__init__(**kwargs)
        self.max_top_n = max_top_n
