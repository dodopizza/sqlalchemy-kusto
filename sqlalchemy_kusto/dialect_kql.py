import logging
from typing import List, Optional, Tuple

from sqlalchemy import Column, exc
from sqlalchemy.sql import compiler, operators, selectable
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
    # We want to quote all table and column names to prevent unconventional names usage
    reserved_words = UniversalSet()

    def __init__(self, dialect, **kw):
        super().__init__(dialect, initial_quote='["', final_quote='"]', **kw)


class KustoKqlCompiler(compiler.SQLCompiler):
    OPERATORS[operators.and_] = " and "

    delete_extra_from_clause = None
    update_from_clause = None
    visit_empty_set_expr = None
    visit_sequence = None

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
        logger.debug("Incoming query: %s", select)

        if len(select.froms) != 1:
            raise NotSupportedError('Only single "select from" query is supported in kql compiler')

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
            where_clause = select._whereclause._compiler_dispatch(self, **kwargs)
            if where_clause:
                compiled_query_lines.append(f"| where {where_clause}")

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
        logger.debug("Compiled query: %s", compiled_query)
        return compiled_query

    def limit_clause(self, select, **kw):
        return ""

    def _get_projection_or_summarize(self, select: selectable.Select) -> str:
        """Builds the ending part of the query either project or summarize"""
        columns = select.inner_columns
        if columns is not None:
            column_labels = []
            is_summarize = False
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)

                if column_name in aggregates_sql_to_kql:
                    is_summarize = True
                    column_labels.append(
                        self._build_column_projection(aggregates_sql_to_kql[column_name], column_alias)
                    )
                else:
                    column_labels.append(self._build_column_projection(column_name, column_alias))

            if column_labels:
                projection_type = "summarize" if is_summarize else "project"
                return f"| {projection_type} {', '.join(column_labels)}"
        return ""

    def _get_most_inner_element(self, clause):
        """Finds the most nested element in clause"""
        inner_element = getattr(clause, "element", None)
        if inner_element is not None:
            return self._get_most_inner_element(inner_element)

        return clause

    @staticmethod
    def _extract_let_statements(clause) -> Tuple[str, List[str]]:
        """Separates the final query from let statements"""
        rows = [s.strip() for s in clause.split(";")]
        main = next(filter(lambda row: not row.startswith("let"), rows), None)

        if main is None:
            raise exc.InvalidRequestError("The query doesn't contain body.")

        lets = [row + ";" for row in rows if row.startswith("let")]
        return main, lets

    @staticmethod
    def _extract_column_name_and_alias(column: Column) -> Tuple[str, Optional[str]]:
        if hasattr(column, "element"):
            return column.element.name, column.name

        return column.name, None

    @staticmethod
    def _build_column_projection(column_name: str, column_alias: str = None):
        """Generates column alias semantic for project statement"""
        return f"{column_alias} = {column_name}" if column_alias else column_name


class KustoKqlHttpsDialect(KustoBaseDialect):
    name = "kustokql"
    statement_compiler = KustoKqlCompiler
    preparer = KustoKqlIdentifierPreparer
