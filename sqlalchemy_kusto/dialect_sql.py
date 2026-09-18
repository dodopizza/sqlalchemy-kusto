from sqlalchemy import exc, types
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import compiler
from sqlalchemy.sql.functions import GenericFunction

from sqlalchemy_kusto.dbapi import _date_trunc_expr
from sqlalchemy_kusto.dialect_base import KustoBaseDialect


class date_trunc(GenericFunction):  # noqa: N801
    """PostgreSQL-style date_trunc compiled to T-SQL DATEADD/DATEDIFF for Kusto."""

    name = "date_trunc"
    type = types.DateTime()
    inherit_cache = True


@compiles(date_trunc, "kustosql")
def _compile_date_trunc(element, compiler, **kw):
    grain_clause, col_clause = list(element.clauses)
    grain = getattr(grain_clause, "value", None)
    if grain is None:
        grain = compiler.process(grain_clause, literal_binds=True, **kw)

    expression = _date_trunc_expr(str(grain), compiler.process(col_clause, **kw))
    if expression is None:
        raise exc.CompileError(f"Unsupported date_trunc grain for Kusto: {grain}")
    return expression


class KustoSqlCompiler(compiler.SQLCompiler):
    def get_select_precolumns(self, select, **kw) -> str:
        """Kusto uses TOP instead of LIMIT; also requires TOP when ORDER BY has no LIMIT."""
        select_precolumns = super().get_select_precolumns(select, **kw)

        if select._limit_clause is not None:
            kw["literal_execute"] = True
            select_precolumns += f"TOP {self.process(select._limit_clause, **kw)} "
        elif select._order_by_clauses and len(self.stack) > 1:
            # Kusto rejects ORDER BY inside a nested SELECT unless it also carries a
            # TOP (verified against the Kusto emulator), and Superset's WRAP_SQL limit
            # method produces exactly that shape. A top-level ORDER BY needs no TOP, so
            # it must not get one — that would cap an otherwise unbounded query.
            # ponytail: caps a nested sort at max_top_n rows; raise it via create_engine.
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
    supports_native_boolean = False
    # For some reason supports_statement_cache
    # doesn't work when defined in the KustoBaseDialect.
    # Need to investigate why it happens.
    supports_statement_cache = True

    def __init__(self, max_top_n: int = 500_000, **kwargs):
        """Row cap used for a nested ORDER BY that has no LIMIT of its own.

        Override per engine: create_engine(url, max_top_n=1_000_000). In Superset it
        goes into the database's Advanced → Other → Engine Parameters.
        """
        super().__init__(**kwargs)
        self.max_top_n = max_top_n


class KustoSqlHttpDialect(KustoSqlHttpsDialect):
    """Plain-HTTP variant, for the Kusto emulator: no TLS, no authentication."""

    driver = "http"
    # SQLAlchemy looks this up on the concrete dialect class, not on its bases,
    # which is also why KustoSqlHttpsDialect repeats it instead of inheriting it.
    supports_statement_cache = True
