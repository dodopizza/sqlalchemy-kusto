import logging
import re
from typing import List, Optional, Tuple

from sqlalchemy import Column, exc
from sqlalchemy.sql import compiler, operators, selectable
from sqlalchemy.sql.compiler import OPERATORS

from sqlalchemy_kusto import NotSupportedError
from sqlalchemy_kusto.dialect_base import KustoBaseDialect

logger = logging.getLogger(__name__)

aggregates_sql_to_kql = {
    "count(*)": "count()",
    "count": "count",
    "count(distinct": "dcount",
    "sum": "sum",
    "avg": "avg",
    "min": "min",
    "max": "max",
}
AGGREGATE_PATTERN = r"(\w+)\s*\(\s*(DISTINCT\s*)?(\*|\w+)\s*\)"


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
        select_stmt: selectable.Select,
        asfrom=False,
        insert_into=True,
        fromhints=None,
        compound_index: int = 0,
        select_wraps_for=None,
        lateral=False,
        from_linter=None,
        **kwargs,
    ):
        logger.debug("Incoming query: %s", select_stmt)

        if len(select_stmt.get_final_froms()) != 1:
            raise NotSupportedError('Only single "select from" query is supported in kql compiler')

        compiled_query_lines = []

        from_object = select_stmt.get_final_froms()[0]
        if hasattr(from_object, "element"):
            query = self._get_most_inner_element(from_object.element)
            (main, lets) = self._extract_let_statements(query.text)
            compiled_query_lines.extend(lets)
            compiled_query_lines.append(f"let {from_object.name} = ({self._convert_schema_in_statement(main)});")
            compiled_query_lines.append(from_object.name)
        elif hasattr(from_object, "name"):
            if from_object.schema is not None:
                unquoted_schema = from_object.schema.strip("\"'")
                compiled_query_lines.append(f'database("{unquoted_schema}").')
            unquoted_name = from_object.name.strip("\"'")
            compiled_query_lines.append(f'["{unquoted_name}"]')
        else:
            compiled_query_lines.append(self._convert_schema_in_statement(from_object.text))

        if select_stmt._whereclause is not None:
            where_clause = select_stmt._whereclause._compiler_dispatch(self, **kwargs)
            if where_clause:
                compiled_query_lines.append(f"| where {where_clause}")

        projections = self._get_projection_or_summarize(select_stmt)
        if projections:
            compiled_query_lines.append(projections)

        if select_stmt._limit_clause is not None:  # pylint: disable=protected-access
            kwargs["literal_execute"] = True
            compiled_query_lines.append(
                f"| take {self.process(select_stmt._limit_clause, **kwargs)}"
            )  # pylint: disable=protected-access

        compiled_query_lines = list(filter(None, compiled_query_lines))

        compiled_query = "\n".join(compiled_query_lines)
        logger.warning("Compiled query: %s", compiled_query)
        return compiled_query

    def limit_clause(self, select, **kw):
        return ""

    def _get_projection_or_summarize(self, select: selectable.Select) -> str:
        """Builds the ending part of the query either project or summarize"""
        projection_statement = ""
        columns = select.inner_columns
        group_by_cols = select._group_by_clauses  # pylint: disable=protected-access
        # If there are no columns, we don't need to project anything.
        # The following is a valid query and this does not have a computed aggregated column.
        # SELECT "EventTime" / time(1d) AS "EventInfoTime","ActiveUsers" FROM
        # ActiveUsersLastMonth GROUP BY "EventInfo_Time" / time(1d)
        has_agg_cols = False
        has_group_by = group_by_cols is not None and len(group_by_cols) > 0  # pylint: disable=protected-access
        if columns is not None:
            column_labels = []
            aggregate_columns = []
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)
                match = re.match(AGGREGATE_PATTERN, column_name, re.IGNORECASE)
                if match:
                    has_agg_cols = True
                    aggregate_func, distinct_keyword, agg_column_name = match.groups()
                    is_distinct = bool(distinct_keyword)
                    kql_agg = self._sql_to_kql_aggregate(aggregate_func, agg_column_name, is_distinct)
                    aggregate_columns.append(self._build_column_projection(kql_agg, column_alias))
                    column_labels.append(column_alias)
                else:
                    if has_group_by and has_agg_cols:
                        column_labels.append(column_alias)
                    else:
                        column_labels.append(self._build_column_projection(column_name, column_alias))

            if has_group_by or has_agg_cols:
                if aggregate_columns:
                    # only group_by_cols is not empty prepend by and join the columns in one line
                    group_by_columns_projection = ""
                    if group_by_cols:
                        group_by_columns_projection = "by " + ", ".join(
                            map(lambda c: self._escape_and_quote_columns(str(c)), group_by_cols)
                        )
                    agg_cols = ", ".join(map(lambda c: self._escape_and_quote_columns(str(c)), aggregate_columns))
                    projection_statement = (
                        f"{projection_statement} | summarize {agg_cols}  {group_by_columns_projection}"
                    )
            if column_labels:
                projection_statement = f"{projection_statement}| project {', '.join(column_labels)}"
        return projection_statement

    @staticmethod
    def _escape_and_quote_columns(name: str):
        return re.sub(r'"([^"]+)"', r'["\1"]', name)

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
            return KustoKqlCompiler._escape_and_quote_columns(str(column.element)), column.name
        return column.name, None

    @staticmethod
    def _build_column_projection(column_name: str, column_alias: str = None):
        """Generates column alias semantic for project statement"""
        return f"{column_alias} = {column_name}" if column_alias else column_name

    @staticmethod
    def _convert_schema_in_statement(query: str) -> str:
        """
        Converts schema in the query from SQL notation to KQL notation. Returns converted query.

        Examples:
            - schema.table                -> database("schema").["table"]
            - schema."table.name"         -> database("schema").{"table.name"]
            - "schema.name".table         -> database("schema.name").["table"]
            - "schema.name"."table.name"  -> database("schema.name").["table.name"]
            - "schema name"."table name"  -> database("schema name").["table name"]
            - "table.name"                -> ["table.name"]
            - MyTable                     -> ["MyTable"]
            - ["schema"].["table"]        -> database("schema").["table"]
            - ["table"]                   -> ["table"]
        """

        pattern = r"^\[?([a-zA-Z0-9]+\b|\"[a-zA-Z0-9 \-_.]+\")?\]?\.?\[?([a-zA-Z0-9]+\b|\"[a-zA-Z0-9 \-_.]+\")\]?"
        match = re.search(pattern, query)
        if not match:
            return query

        original = match.group(0)
        unquoted_table = match.group(2).strip("\"'")

        if not match.group(1):
            return query.replace(original, f'["{unquoted_table}"]', 1)

        unquoted_schema = match.group(1).strip("\"'")
        return query.replace(original, f'database("{unquoted_schema}").["{unquoted_table}"]', 1)

    @staticmethod
    def _sql_to_kql_aggregate(sql_agg: str, column_name: str = None, is_distinct: bool = False) -> str:
        """
        Converts SQL aggregate function to KQL equivalent.
        If a column name is provided, applies it to the aggregate.
        """
        has_column = column_name is not None and column_name.strip() != ""
        column_name_escaped = KustoKqlCompiler._escape_and_quote_columns(column_name) if has_column else ""
        return_value = None
        # The count function is a special case because it can be used with or without a column name
        # We can also use it in count(Distinct column_name) format. This has to be handled separately
        if "count" in sql_agg:
            if "*" in sql_agg or column_name == "*":
                return_value = aggregates_sql_to_kql["count(*)"]
            elif is_distinct:
                return_value = f"dcount({column_name_escaped})"
            else:
                return_value = f"count({column_name_escaped})"
        if return_value:
            return return_value
        # Other summarize operators have to be looked up
        aggregate_function = aggregates_sql_to_kql.get(sql_agg.split("(")[0])
        if aggregate_function:
            return_value = f"{aggregate_function}({column_name_escaped})"
        return return_value


class KustoKqlHttpsDialect(KustoBaseDialect):
    name = "kustokql"
    statement_compiler = KustoKqlCompiler
    preparer = KustoKqlIdentifierPreparer
    supports_statement_cache = True
