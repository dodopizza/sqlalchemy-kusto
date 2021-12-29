import logging
from typing import List, Optional, Tuple

from sqlalchemy import Column
from sqlalchemy.sql import compiler, selectable, operators
from sqlalchemy.sql.compiler import OPERATORS

from sqlalchemy_kusto import NotSupportedError
from sqlalchemy_kusto.dialect_base import KustoBaseDialect

logger = logging.getLogger(__name__)

aggregates_sql_to_kql = {
    "count(*)": "count()",
}


class UniversalSet:
    def __contains__(self, item):
        return True


class KustoKqlIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()

    def __init__(self, dialect, **kw):
        super(KustoKqlIdentifierPreparer, self).__init__(
            dialect, initial_quote='["', final_quote='"]', **kw
        )


class KustoKqlCompiler(compiler.SQLCompiler):
    OPERATORS[operators.and_] = " and "

    def visit_select(
        self,
        select: selectable.Select,
        asfrom=False,
        parens=True,
        fromhints=None,
        compound_index: int = 0,
        nested_join_translation=False,
        select_wraps_for=None,
        lateral=False,
        **kwargs,
    ):
        logger.debug(f"Incoming query {select}")
        logger.debug(type(select.froms[0]))

        if len(select.froms) != 1:
            raise NotSupportedError("Only 1 from query is supported in kql compiler")

        compiled_query_lines = []

        from_object = select.froms[0]
        if hasattr(from_object, "element"):
            query = self._get_most_inner_element(from_object.element)
            (main, lets) = self._extract_let_statements(query.text)
            compiled_query_lines.extend(lets)
            compiled_query_lines.append(f"let {from_object.name} = ({main});")
            compiled_query_lines.append(from_object.name)
        elif hasattr(from_object, "name"):
            compiled_query_lines.append(from_object.name)
        else:
            compiled_query_lines.append(from_object.text)

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                compiled_query_lines.append(f"| where {t}")

        projections = self._get_projection_or_summarize(select)
        if projections:
            compiled_query_lines.append(projections)

        if select._limit_clause is not None:  # pylint: disable=protected-access
            kwargs["literal_execute"] = True
            compiled_query_lines.append(
                f"| take {self.process(select._limit_clause, **kwargs)}"
            )  # pylint: disable=protected-access

        compiled_query_lines = list(filter(None, compiled_query_lines))

        compiled_query = "\n".join(compiled_query_lines)
        logger.debug(f"Compiled query: {compiled_query}")
        return compiled_query

    def limit_clause(self, select, **kw):
        return ""

    def _get_projection_or_summarize(self, select: selectable.Select) -> str:
        columns = select.inner_columns
        if columns is not None:
            column_labels = []
            is_summarize = False
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)

                if column_name in aggregates_sql_to_kql:
                    is_summarize = True
                    column_labels.append(
                        self._build_column_projection(
                            aggregates_sql_to_kql[column_name], column_alias
                        )
                    )
                else:
                    column_labels.append(
                        self._build_column_projection(column_name, column_alias)
                    )

            if column_labels:
                projection_type = "summarize" if is_summarize else "project"
                return f"| {projection_type} {', '.join(column_labels)}"
        return ""

    def _get_most_inner_element(self, clause):
        inner_element = getattr(clause, "element", None)
        if inner_element is not None:
            return self._get_most_inner_element(inner_element)
        else:
            return clause

    @staticmethod
    def _extract_let_statements(clause) -> Tuple[str, List[str]]:
        rows = [s.strip() for s in clause.split(";")]
        main = next(filter(lambda row: not row.startswith("let"), rows), None)
        lets = [row + ";" for row in rows if row.startswith("let")]
        return main, lets

    @staticmethod
    def _extract_column_name_and_alias(column: Column) -> Tuple[str, Optional[str]]:
        if hasattr(column, "element"):
            return column.element.name, column.name
        else:
            return column.name, None

    @staticmethod
    def _build_column_projection(column_name: str, column_alias: str = None):
        return f"{column_alias} = {column_name}" if column_alias else column_name


class KustoKqlHttpsDialect(KustoBaseDialect):
    name = "kustokql"
    statement_compiler = KustoKqlCompiler
    preparer = KustoKqlIdentifierPreparer
