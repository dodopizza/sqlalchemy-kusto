from sqlalchemy import types
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import compiler
from sqlalchemy.sql.functions import GenericFunction

from sqlalchemy_kusto.dbapi import _GRAIN_EPOCH, _GRAIN_TO_TSQL, _translate_raw_date_trunc  # noqa: F401
from sqlalchemy_kusto.dialect_base import KustoBaseDialect


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
