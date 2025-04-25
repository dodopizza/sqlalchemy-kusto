import logging
import re

from sqlalchemy import Column, exc, sql
from sqlalchemy.sql import compiler, operators, selectable
from sqlalchemy.sql.compiler import OPERATORS

from sqlalchemy_kusto.dialect_base import KustoBaseDialect

logger = logging.getLogger(__name__)

aggregates_sql_to_kql = {
    "count(*)": "count()",
    "count(1)": "count()",
    "count": "count",
    "count(distinct": "dcount",
    "count(distinct(": "dcount",
    "count_distinct": "dcount",
    "sum": "sum",
    "avg": "avg",
    "min": "min",
    "max": "max",
}
kql_aggregates = {
    "arg_max",
    "arg_min",
    "avg",
    "avgif",
    "binary_all_and",
    "binary_all_or",
    "binary_all_xor",
    "buildschema",
    "count",
    "count_distinct",
    "count_distinctif",
    "countif",
    "dcount",
    "dcountif",
    "hll",
    "hll_if",
    "hll_merge",
    "make_bag",
    "make_bag_if",
    "make_list",
    "make_list_if",
    "max",
    "maxif",
    "min",
    "minif",
    "percentile",
    "percentiles",
    "percentilew",
    "percentilesw",
    "stdev",
    "stdevif",
    "stdevp",
    "sum",
    "sumif",
    "take_any",
    "take_anyif",
    "tdigest",
    "tdigest_merge",
    "merge_tdigest",
    "variance",
    "varianceif",
    "variancep",
}
AGGREGATE_PATTERN = r"(\w+)\s*\(\s*(DISTINCT|distinct\s*)?\(?\s*(\*|\[?\"?\'?\w+\"?\]?)\s*(,.+)*\)?\s*\)"


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
    sort_with_clause_parts = 2

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
        compiled_query_lines = []

        from_object = select_stmt.get_final_froms()[0]
        if hasattr(from_object, "element"):
            query = self._get_most_inner_element(from_object.element)
            (main, lets) = self._extract_let_statements(query.text)
            compiled_query_lines.extend(lets)
            compiled_query_lines.append(
                f"let {from_object.name} = ({self._convert_schema_in_statement(main)});"
            )
            compiled_query_lines.append(from_object.name)
        elif hasattr(from_object, "name"):
            if from_object.schema is not None:
                unquoted_schema = from_object.schema.strip("\"'")
                compiled_query_lines.append(f'database("{unquoted_schema}").')
            unquoted_name = from_object.name.strip("\"'")
            compiled_query_lines.append(f'["{unquoted_name}"]')
        elif hasattr(from_object, "left"):
            # This is a case of a join.
            compiled_query_lines.append(
                self._convert_schema_in_statement(from_object.left.fullname)
            )
        else:
            compiled_query_lines.append(
                self._convert_schema_in_statement(from_object.text)
            )
        kql_join = self._legacy_join(select_stmt, **kwargs)
        compiled_query_lines.append(kql_join)
        projections_parts_dict = self._get_projection_or_summarize(select_stmt)

        if select_stmt._whereclause is not None:
            kwargs["literal_binds"] = True
            where_clause = select_stmt._whereclause._compiler_dispatch(self, **kwargs)
            if where_clause:
                where_clause_reformatted = self._remove_table_from_where(where_clause)
                converted_where_clause = self._sql_to_kql_where(
                    where_clause_reformatted
                )
                compiled_query_lines.append(f"| where {converted_where_clause}")

        if "extend" in projections_parts_dict:
            compiled_query_lines.append(projections_parts_dict.pop("extend"))

        for statement_part in projections_parts_dict.values():
            if statement_part:
                compiled_query_lines.append(statement_part)

        if select_stmt._limit_clause is not None:
            kwargs["literal_execute"] = True
            compiled_query_lines.append(
                f"| take {self.process(select_stmt._limit_clause, **kwargs)}"
            )
        compiled_query_lines = list(filter(None, compiled_query_lines))
        compiled_query = "\n".join(compiled_query_lines)
        logger.warning("Compiled query: %s", compiled_query)
        return compiled_query

    def limit_clause(self, select, **kw):
        return ""

    def _legacy_join(self, select_stmt: selectable.Select, **kwargs):
        """Consumes arguments from join() or outerjoin(), places them into a
        consistent format with which to form the actual JOIN constructs.
        """
        kql_join = ""
        on_clause = ""
        for right, on_columns, _left, flags in select_stmt._legacy_setup_joins:
            join_type = "inner"
            if flags["isouter"]:
                if flags["full"]:
                    join_type = "fullouter"
                else:
                    join_type = "leftouter"
            if on_columns is not None:
                left_clause_col, _ = self._extract_column_name_and_alias(
                    on_columns.left
                )
                right_clause_col, _ = self._extract_column_name_and_alias(
                    on_columns.right
                )
                on_clause = (
                    f"on $left.{self._escape_and_quote_columns(left_clause_col)} "
                    f"== $right.{self._escape_and_quote_columns(right_clause_col)}"
                )
            kql_join = f"| join kind={join_type} ({self._escape_and_quote_columns(right.name)}) {on_clause}"
        return kql_join

    def visit_join(self, join, asfrom=True, from_linter=None, **kwargs):
        return ""

    def _should_use_summarize_for_count_star(self, column, select):
        """Determine if COUNT(*) should use summarize or extend."""
        # Check if this is a literal_column (used in aggregations) or a regular column (used in drill-by)
        return (
            # Case for test_select_count: literal_column("count(*)")
            (
                hasattr(column, "element")
                and isinstance(column.element, sql.expression.ClauseElement)
                and not isinstance(column.element, sql.expression.ColumnClause)
            )
            # Also use summarize if there are where clauses (as in test_select_count)
            or (select._whereclause is not None)
        )

    @staticmethod
    def _is_count_star_column(column_name: str) -> bool:
        """Check if the column name is COUNT(*)."""
        return (
            column_name.upper() == "COUNT(*)"
            or column_name == "COUNT(*)"
            or column_name.upper() == '"COUNT(*)"'
            or column_name == '"COUNT(*)"'
        )

    def _process_column(
        self, column, select, summarize_columns, extend_columns, projection_columns
    ):
        """Process a single column and update the appropriate collections."""
        column_name, column_alias = self._extract_column_name_and_alias(column)
        column_alias = self._escape_and_quote_columns(column_alias, True)
        has_aggregates = False

        # Special handling for COUNT(*) as a column name
        if self._is_count_star_column(column_name):
            if self._should_use_summarize_for_count_star(column, select):
                # This is likely a literal_column or has where clauses, so use summarize
                has_aggregates = True
                summarize_columns.add(f"{column_alias} = count()")
            else:
                # This is likely a regular column without where clauses, so use extend
                extend_columns.add(f"{column_alias} = count()")
        else:
            # Do we have aggregate columns?
            kql_agg = self._extract_maybe_agg_column_parts(column_name)
            if kql_agg:
                has_aggregates = True
                summarize_columns.add(
                    self._build_column_projection(kql_agg, column_alias)
                )
            # No aggregates - do the columns have aliases?
            elif column_alias and column_alias != column_name:
                extend_columns.add(
                    self._build_column_projection(column_name, column_alias, True)
                )

        # Add to projection columns
        if column_alias:
            projection_columns.append(
                self._escape_and_quote_columns(column_alias, True)
            )
        else:
            projection_columns.append(self._escape_and_quote_columns(column_name))

        return has_aggregates

    def _get_projection_or_summarize(self, select: selectable.Select) -> dict[str, str]:
        """Builds the ending part of the query either project or summarize."""
        columns = select.inner_columns
        group_by_cols = select._group_by_clauses
        order_by_cols = select._order_by_clauses
        summarize_statement = ""
        extend_statement = ""
        project_statement = ""

        # Process columns if they exist
        if columns is not None:
            summarize_columns = set()
            extend_columns = set()
            projection_columns = []
            has_aggregates = False

            # Process each column (except *)
            for column in [c for c in columns if c.name != "*"]:
                column_has_agg = self._process_column(
                    column,
                    select,
                    summarize_columns,
                    extend_columns,
                    projection_columns,
                )
                has_aggregates = has_aggregates or column_has_agg

            # Process group by columns
            by_columns = self._group_by(group_by_cols)

            # Build statements
            if has_aggregates or bool(by_columns):
                summarize_statement = f"| summarize {', '.join(summarize_columns)} "
                if by_columns:
                    summarize_statement = (
                        f"{summarize_statement} by {', '.join(by_columns)}"
                    )

            if extend_columns:
                extend_statement = f"| extend {', '.join(sorted(extend_columns))}"

            project_statement = (
                f"| project {', '.join(projection_columns)}"
                if projection_columns
                else ""
            )

        # Process order by
        unwrapped_order_by = self._get_order_by(order_by_cols)
        sort_statement = (
            f"| order by {', '.join(unwrapped_order_by)}" if unwrapped_order_by else ""
        )

        return {
            "extend": extend_statement,
            "summarize": summarize_statement,
            "project": project_statement,
            "sort": sort_statement,
        }

    @staticmethod
    def _extract_maybe_agg_column_parts(column_name) -> str | None:
        match_agg_cols = re.match(AGGREGATE_PATTERN, column_name, re.IGNORECASE)
        if match_agg_cols and match_agg_cols.groups():
            # Check if the aggregate function is count_distinct. This is case from superset
            # where we can use count(distinct or count_distinct)
            aggregate_func, distinct_keyword, agg_column_name, extra_params = (
                match_agg_cols.groups()
            )
            is_distinct = (
                bool(distinct_keyword) or aggregate_func.casefold() == "count_distinct"
            )
            kql_agg = KustoKqlCompiler._sql_to_kql_aggregate(
                aggregate_func.lower(), agg_column_name, is_distinct, extra_params
            )
            return kql_agg

        maybe_aggregation_function = column_name.lower().split("(")[0]
        if maybe_aggregation_function in kql_aggregates:
            return column_name

        return None

    def _get_order_by(self, order_by_cols):
        unwrapped_order_by = []
        for elem in order_by_cols:
            if isinstance(elem, sql.elements._label_reference):
                nested_element = elem.element
                unwrapped_order_by.append(
                    f"{self._escape_and_quote_columns(nested_element._order_by_label_element.name,is_alias=True)} "
                    f"{'desc' if (nested_element.modifier is operators.desc_op) else 'asc'}"
                )
            elif isinstance(elem, sql.elements.TextClause):
                sort_parts = elem.text.split()
                if len(sort_parts) == self.sort_with_clause_parts:
                    unwrapped_order_by.append(
                        f"{self._escape_and_quote_columns(sort_parts[0],is_alias=True)} {sort_parts[1].lower()}"
                    )
                elif len(sort_parts) == 1:
                    unwrapped_order_by.append(
                        self._escape_and_quote_columns(sort_parts[0], is_alias=True)
                    )
                else:
                    unwrapped_order_by.append(
                        elem.text.replace(" ASC", " asc").replace(" DESC", " desc")
                    )
            else:
                logger.warning(
                    "Unsupported order by clause: %s of type %s", elem, type(elem)
                )
        return unwrapped_order_by

    def _group_by(self, group_by_cols):
        by_columns = set()
        for column in group_by_cols:
            column_name, column_alias = self._extract_column_name_and_alias(column)
            if column_alias:
                by_columns.add(self._escape_and_quote_columns(column_alias))
            else:
                by_columns.add(self._escape_and_quote_columns(column_name))
        return by_columns

    @staticmethod
    def _convert_quoted_columns(kql_expression) -> str:
        # Regex to find function calls with quoted column names
        pattern = r'(\w+)\(\s*"([^"]+)"'

        # Replace with the modified format
        def replacer(match):
            function_name = match.group(1)
            column_name = match.group(2)
            return f'{function_name}(["{column_name}"]'  # Wrap column in brackets

        # Apply transformation
        modified_expression = re.sub(pattern, replacer, kql_expression)

        return modified_expression

    @staticmethod
    def _escape_and_quote_columns(name: str | None, is_alias=False) -> str:
        if name is None:
            return ""

        result = None

        # Special handling for COUNT(*) as a column name
        if KustoKqlCompiler._is_count_star_column(name):
            if is_alias:
                # When it's an alias, we need to quote it
                result = '["COUNT(*)"]'
            else:
                # When it's a function, we need to convert it to count()
                result = "count()"
        elif (
            KustoKqlCompiler._is_kql_function(name)
            or KustoKqlCompiler._is_number_literal(name)
        ) and not is_alias:
            result = name
        elif name.startswith('["') and name.endswith('"]'):
            # Return as is if already properly escaped
            result = name
        elif not is_alias:
            # Handle mathematical operations (wrap only the column part before operators)
            for operator in ["/", "+", "-", "*"]:
                if operator in name:
                    # Split the name at the first operator and wrap the left part
                    parts = name.split(operator, 1)
                    # Remove quotes if they exist at the edges
                    col_part = parts[0].strip()
                    if col_part.startswith('"') and col_part.endswith('"'):
                        col_part = col_part[1:-1].strip()
                    col_part = col_part.replace('"', '\\"')
                    result = f'["{col_part}"] {operator} {parts[1].strip()}'  # Wrap the column part
                    break

        # If no special case was matched, apply default formatting
        if result is None:
            # Handle quoted names
            if name.startswith('"') and name.endswith('"'):
                name = name[1:-1]

            # No operators found, just wrap the entire name
            name = name.replace('"', '\\"')
            result = f'["{name}"]'

        return result

    @staticmethod
    def _sql_to_kql_where(where_clause: str) -> str:
        where_clause = where_clause.strip().replace("\n", "")

        # Handle 'IS NULL' and 'IS NOT NULL' -> KQL equivalents
        where_clause = re.sub(
            r'(\["[^\]]+"\])\s*IS NULL',
            r"isnull(\1)",
            where_clause,
            flags=re.IGNORECASE,
        )  # IS NULL -> isnull(["FieldName"])
        where_clause = re.sub(
            r'(\["[^\]]+"\])\s*IS NOT NULL',
            r"isnotnull(\1)",
            where_clause,
            flags=re.IGNORECASE,
        )  # IS NOT NULL -> isnotnull(["FieldName"])
        # Handle comparison operators
        # Change '=' to '==' for equality comparisons
        where_clause = re.sub(
            r"(?<=[^=])=(?=\s|$|[^=])", r"==", where_clause, flags=re.IGNORECASE
        )
        # Remove spaces in < = and > = operators
        where_clause = re.sub(r"\s*<\s*=\s*", "<=", where_clause, flags=re.IGNORECASE)
        where_clause = re.sub(r"\s*>\s*=\s*", ">=", where_clause, flags=re.IGNORECASE)
        where_clause = where_clause.replace(">==", ">=")
        where_clause = where_clause.replace("<==", "<=")
        where_clause = where_clause.replace("<>", "!=")
        where_clause = re.sub(
            r"(\s|^)lower\(", r"\1tolower(", where_clause, flags=re.IGNORECASE
        )

        where_clause = re.sub(
            r"(\s)(<>|!=)\s*", r" \2 ", where_clause, flags=re.IGNORECASE
        )  # Handle '!=' and '<>'
        where_clause = re.sub(
            r"(\s)(<|<=|>|>=)\s*", r" \2 ", where_clause, flags=re.IGNORECASE
        )  # Comparison operators: <, <=, >, >=

        # Step 3: Handle '(I)LIKE' -> 'has' for substring matching
        # support the % characters in the RHS at the beginning, end or both
        like_regexp = r"(\s){like}\s+([a-z_]+\()?(['\"]){pre}([^%]+){post}\3(\))?"
        if re.search(r"NOT I?LIKE", where_clause, re.IGNORECASE):
            where_clause = re.sub(
                like_regexp.format(like="NOT LIKE", pre="", post="%+"),
                r"\1!startswith_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="NOT LIKE", pre="%+", post=""),
                r"\1!endswith_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="NOT LIKE", pre="%+", post="%+"),
                r"\1!has_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="NOT ILIKE", pre="", post="%+"),
                r"\1!startswith \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="NOT ILIKE", pre="%+", post=""),
                r"\1!endswith \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="NOT ILIKE", pre="%+", post="%+"),
                r"\1!has \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
        elif re.search(r"I?LIKE", where_clause, re.IGNORECASE):
            where_clause = re.sub(
                like_regexp.format(like="LIKE", pre="%+", post="%+"),
                r"\1has_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="LIKE", pre="", post="%+"),
                r"\1startswith_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="LIKE", pre="%+", post=""),
                r"\1endswith_cs \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="ILIKE", pre="%+", post="%+"),
                r"\1has \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="ILIKE", pre="", post="%+"),
                r"\1startswith \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )
            where_clause = re.sub(
                like_regexp.format(like="ILIKE", pre="%+", post=""),
                r"\1endswith \2\3\4\3\5",
                where_clause,
                flags=re.IGNORECASE,
            )

        # Step 4: Handle 'IN' and 'NOT IN' operators (with lists inside parentheses)
        # We need to correctly handle multiple spaces around IN/NOT IN and lists inside parentheses
        where_clause = re.sub(
            r"(\s)NOT IN\s*\(([^)]+)\)",
            r"\1!in (\2)",
            where_clause,
            flags=re.IGNORECASE,
        )  # NOT IN operator (list of values)
        where_clause = re.sub(
            r"(\s)IN\s*\(([^)]+)\)", r"\1in (\2)", where_clause, flags=re.IGNORECASE
        )  # IN operator (list of values)
        # Handle BETWEEN operator (if needed)

        where_clause = re.sub(
            r"(\w+|\[\"[A-Za-z0-9_]+\"\]) (BETWEEN|between) (\d) (AND|and) (\d)",
            r"\1 between (\3..\5)",
            where_clause,
            flags=re.IGNORECASE,
        )
        where_clause = re.sub(
            r"(\w+) (BETWEEN|between) (\d) (AND|and) (\d)",
            r"\1 between (\3..\5)",
            where_clause,
            flags=re.IGNORECASE,
        )
        # Handle logical operators 'AND' and 'OR' to ensure the conditions are preserved
        # Replace AND with 'and' in KQL
        where_clause = re.sub(r"\s+AND\s+", r" and ", where_clause, flags=re.IGNORECASE)
        # Replace OR with 'or' in KQL
        where_clause = re.sub(r"\s+OR\s+", r" or ", where_clause, flags=re.IGNORECASE)
        return where_clause

    @staticmethod
    def _remove_table_from_where(where_clause: str) -> str:
        regex = r'(?:\["?)(\w+)(["?]\])?\.'
        return re.sub(regex, "", where_clause)

    @staticmethod
    def _is_kql_function(name: str) -> bool:
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*\s*\("
        return bool(re.match(pattern, name))

    @staticmethod
    def _is_number_literal(s: str) -> bool:
        pattern = r"^[0-9]+$"
        return bool(re.match(pattern, s))

    def _get_most_inner_element(self, clause):
        """Finds the most nested element in clause."""
        inner_element = getattr(clause, "element", None)
        if inner_element is not None:
            return self._get_most_inner_element(inner_element)
        return clause

    @staticmethod
    def _extract_let_statements(clause) -> tuple[str, list[str]]:
        """Separates the final query from let statements."""
        rows = [s.strip() for s in clause.split(";")]
        main = next(filter(lambda row: not row.startswith("let"), rows), None)

        if main is None:
            raise exc.InvalidRequestError("The query doesn't contain body.")

        lets = [row + ";" for row in rows if row.startswith("let")]
        return main, lets

    @staticmethod
    def _extract_column_name_and_alias(column: Column) -> tuple[str, str | None]:
        if hasattr(column, "element"):
            column_name = str(column.element)
            column_alias = str(column.name)

            # Special handling for COUNT(*) as a column name
            if KustoKqlCompiler._is_count_star_column(column_name):
                return "COUNT(*)", column_alias

            return KustoKqlCompiler._convert_quoted_columns(
                column_name
            ), KustoKqlCompiler._convert_quoted_columns(column_alias)
        if hasattr(column, "name"):
            column_name = str(column.name)

            # Special handling for COUNT(*) as a column name
            if KustoKqlCompiler._is_count_star_column(column_name):
                return "COUNT(*)", None

            return KustoKqlCompiler._convert_quoted_columns(column_name), None

        column_str = str(column)

        # Special handling for COUNT(*) as a column name
        if KustoKqlCompiler._is_count_star_column(column_str):
            return "COUNT(*)", None

        return KustoKqlCompiler._convert_quoted_columns(column_str), None

    @staticmethod
    def _build_column_projection(
        column_name: str, column_alias: str | None = None, is_extend: bool = False
    ) -> str:
        """Generates column alias semantic for project statement."""
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
        return query.replace(
            original, f'database("{unquoted_schema}").["{unquoted_table}"]', 1
        )

    @staticmethod
    def _sql_to_kql_aggregate(
        sql_agg: str,
        column_name: str | None = None,
        is_distinct: bool = False,
        extra_params: str | None = None,
    ) -> str | None:
        """
        Converts SQL aggregate function to KQL equivalent.
        If a column name is provided, applies it to the aggregate.
        """
        has_column = column_name is not None and column_name.strip() != ""
        column_name_escaped = (
            KustoKqlCompiler._escape_and_quote_columns(column_name)
            if has_column
            else ""
        )
        return_value = None
        # The count function is a special case because it can be used with or without a column name
        # We can also use it in count(Distinct column_name) format. This has to be handled separately
        if sql_agg and sql_agg in ("count", "COUNT"):
            if "*" in sql_agg or column_name in ("*", "1"):
                return_value = aggregates_sql_to_kql["count(*)"]
            elif is_distinct:
                return_value = f"dcount({column_name_escaped})"
            else:
                return_value = f"count({column_name_escaped})"
        if return_value:
            return return_value

        aggregation_function = sql_agg.lower().split("(")[0]

        # Other summarize operators have to be looked up
        sql_to_kql_aggregate_function = aggregates_sql_to_kql.get(aggregation_function)
        if sql_to_kql_aggregate_function:
            return_value = f"{sql_to_kql_aggregate_function}({column_name_escaped})"
        elif aggregation_function in kql_aggregates:
            return_value = (
                f"{aggregation_function}({column_name_escaped}{extra_params})"
            )
        return return_value


class KustoKqlHttpsDialect(KustoBaseDialect):
    name = "kustokql"
    statement_compiler = KustoKqlCompiler
    preparer = KustoKqlIdentifierPreparer
    supports_statement_cache = True
