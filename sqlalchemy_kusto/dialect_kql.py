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
    "count_distinct": "dcount",
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
        """

        With Columns :
            - Do we have a group by clause ? --Yes---> Do we have aggregate columns ? --Yes---> Summarize new column(s)
                       |                                   |                                        with by clause
                       N                                   N --> Add to projection
                       |
                       |
            - Do the columns have aliases ? --Yes---> Extend with aliases
                       |
                       N---> Add to projection
        """
        if columns is not None:
            summarize_columns = set()
            extend_columns = set()
            projection_columns = set()
            by_columns = set()
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)
                # Do we have a group by clause ?
                match_agg_cols = re.match(AGGREGATE_PATTERN, column_name, re.IGNORECASE)
                # Do we have aggregate columns ?
                if match_agg_cols:
                    aggregate_func, distinct_keyword, agg_column_name = match_agg_cols.groups()
                    is_distinct = bool(distinct_keyword) or aggregate_func.casefold() == "count_distinct"
                    kql_agg = self._sql_to_kql_aggregate(aggregate_func, agg_column_name, is_distinct)
                    summarize_columns.add(self._build_column_projection(kql_agg, column_alias))
                    if column_alias:
                        projection_columns.add(self._escape_and_quote_columns(column_alias))
                # No group by clause
                else:
                    # Do the columns have aliases ?
                    if column_alias:
                        extend_columns.add(self._build_column_projection(column_name, column_alias, True))
                        projection_columns.add(self._escape_and_quote_columns(column_alias))
                    else:
                        projection_columns.add(self._build_column_projection(column_name, column_alias, True))

            # group by columns
            for column in [c for c in group_by_cols]:
                column_name, column_alias = self._extract_column_name_and_alias(column)
                by_columns.add(column_name)

            if summarize_columns:
                # escape each column with _escape_and_quote_columns
                projection_statement = f"| summarize {', '.join(sorted(summarize_columns))} "
                if by_columns:
                    by_columns_escaped = [self._escape_and_quote_columns(c) for c in by_columns]
                    projection_statement = f"{projection_statement} by {', '.join(sorted(by_columns_escaped))}"
            if extend_columns:
                # escape each column with _escape_and_quote_columns
                extend = f"| extend {', '.join(sorted(extend_columns))}"
                projection_statement = f"{projection_statement} {extend}" if projection_statement else extend

            if projection_columns:
                # escape each column with _escape_and_quote_columns
                project = (
                    f"| project {', '.join(sorted(projection_columns, key=lambda x: x.split('=')[0], reverse=False))}"
                )
                projection_statement = f"{projection_statement} {project}" if projection_statement else project

        return projection_statement

    @staticmethod
    def _escape_and_quote_columns(name: str):
        name = name.strip()
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
        # First, check if the name is already wrapped in ["ColumnName"] (escaped format)
        if name.startswith('["') and name.endswith('"]'):
            return name  # Return as is if already properly escaped
        # Remove surrounding spaces
        # Handle mathematical operations (wrap only the column part before operators)
        # Find the position of the first operator or space that separates the column name
        for operator in ['/', '+', '-', '*']:
            if operator in name:
                # Split the name at the first operator and wrap the left part
                parts = name.split(operator, 1)
                # Remove quotes if they exist at the edges
                col_part = parts[0].strip()
                if col_part.startswith('"') and col_part.endswith('"'):
                    return f'["{col_part[1:-1].strip()}"] {operator} {parts[1].strip()}'
                else:
                    return f'["{col_part}"] {operator} {parts[1].strip()}'  # Wrap the column part
            else:
                # No operators found, just wrap the entire name

                return f'["{name}"]'

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
            return str(column.element), column.name
        elif hasattr(column, "name"):
            return str(column.name), column.name
        else:
            return str(column), None

    @staticmethod
    def _build_column_projection(column_name: str, column_alias: str = None, is_extend: bool = False) -> str:
        """Generates column alias semantic for project statement"""
        if is_extend:
            return (
                f"{column_alias} = {KustoKqlCompiler._escape_and_quote_columns(column_name)}"
                if column_alias
                else KustoKqlCompiler._escape_and_quote_columns(column_name)
            )

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
        if sql_agg and ("count" in sql_agg or "COUNT" in sql_agg):
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
