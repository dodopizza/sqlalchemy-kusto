from sqlalchemy.sql import compiler

from sqlalchemy_kusto.dialect_base import KustoBaseDialect


class KustoSqlCompiler(compiler.SQLCompiler):
    def get_select_precolumns(self, select, **kw) -> str:
        """Kusto uses TOP instead of LIMIT"""
        select_precolumns = super().get_select_precolumns(select, **kw)

        if select._limit_clause is not None:
            kw["literal_execute"] = True
            select_precolumns += "TOP %s " % self.process(select._limit_clause, **kw)

        return select_precolumns

    def limit_clause(self, select, **kw):
        """Do not add LIMIT to the end of the query"""
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
