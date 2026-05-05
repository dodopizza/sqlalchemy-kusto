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
# Pre-compiled regex for aggregate function matching (performance optimization).
# Compiled once at module load to avoid recompiling on every call, which significantly
# improves performance for query-heavy workloads.
KQL_AGG_PATTERN = re.compile(
    r"\b(" + "|".join(kql_aggregates) + r")\s*\(", re.IGNORECASE
)


class _ParseState:
    """Tracks parsing state while scanning through text."""

    __slots__ = ("in_double_quote", "in_single_quote", "in_bracket", "paren_depth")

    def __init__(self):
        self.in_double_quote = False
        self.in_single_quote = False
        self.in_bracket = False
        self.paren_depth = 0

    def update(self, ch: str, prev_ch: str | None) -> None:
        """Update state based on current and previous character."""
        # Handle quotes (only if not escaped and not in conflicting context)
        if (
            ch == '"'
            and prev_ch != "\\"
            and not self.in_single_quote
            and not self.in_bracket
        ):
            self.in_double_quote = not self.in_double_quote
        elif (
            ch == "'"
            and prev_ch != "\\"
            and not self.in_double_quote
            and not self.in_bracket
        ):
            self.in_single_quote = not self.in_single_quote
        # Handle brackets and parens (only if not in quotes)
        elif not self.in_double_quote and not self.in_single_quote:
            if ch == "[":
                self.in_bracket = True
            elif ch == "]":
                self.in_bracket = False
            elif not self.in_bracket:
                if ch == "(":
                    self.paren_depth += 1
                elif ch == ")":
                    self.paren_depth -= 1

    @property
    def in_quotes_or_brackets(self) -> bool:
        """Check if currently inside quotes or brackets."""
        return self.in_double_quote or self.in_single_quote or self.in_bracket


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

    @staticmethod
    def _find_top_level_operator(text: str, operator: str) -> int:
        """Find position of operator at depth 0 (not inside quotes, brackets, or parens).

        Args:
            text: The string to search in
            operator: The single-character operator to find (e.g., '+', '-', '*', '/')

        Returns:
            The position of the operator at depth 0, or -1 if not found.
            Returns -1 when the operator only appears inside quotes, brackets, or nested parens.
        """
        state = _ParseState()
        for i, ch in enumerate(text):
            if (
                ch == operator
                and state.paren_depth == 0
                and not state.in_quotes_or_brackets
            ):
                return i
            state.update(ch, text[i - 1] if i > 0 else None)
        return -1

    @staticmethod
    def _is_inside_quotes_or_brackets(text: str, pos: int) -> bool:
        """Check if a position in text is inside quotes or brackets."""
        if pos >= len(text):
            return False

        state = _ParseState()
        for i in range(pos):
            state.update(text[i], text[i - 1] if i > 0 else None)
        return state.in_quotes_or_brackets

    @staticmethod
    def _find_matching_paren(text: str, start_pos: int) -> int:
        """Find the matching closing parenthesis for an opening paren at start_pos."""
        if start_pos >= len(text) or text[start_pos] != "(":
            return -1

        state = _ParseState()
        state.paren_depth = 1  # Start with depth 1 since we're at opening paren

        for i in range(start_pos + 1, len(text)):
            ch = text[i]
            state.update(ch, text[i - 1] if i > 0 else None)
            if state.paren_depth == 0:
                return i
        return -1

    @staticmethod
    def _has_operators_outside_quotes(expr: str) -> bool:
        """Check if expression has arithmetic operators outside of quoted strings and brackets."""
        return any(
            KustoKqlCompiler._find_top_level_operator(expr, op) != -1 for op in "+-*/"
        )

    @staticmethod
    def _count_outer_parens(text: str) -> tuple[int, str]:
        """Count and strip outer parentheses from text. Returns (count, stripped_text)."""
        text = text.strip()
        count = 0
        while len(text) > 1 and text[0] == "(" and text[-1] == ")":
            depth = 0
            for ch in text[:-1]:  # Scan all but last char
                depth += (ch == "(") - (ch == ")")
                if depth == 0:
                    return count, text  # First '(' closed before end
            count += 1
            text = text[1:-1].strip()
        return count, text

    @staticmethod
    def _extract_and_replace_aggregates(
        expr: str, measure_name: str, existing_aggs: dict[str, str] | None = None
    ) -> tuple[str, list[tuple[str, str]]]:
        """Extract aggregate functions from an expression and replace with references.

        Args:
            expr: The expression to process (may contain aggregates, operators)
            measure_name: Name of the parent measure (used for generating ref names)
            existing_aggs: Dict mapping kql_agg (lowercase) -> ref_name for reuse.

        Returns:
            A tuple of:
            - modified expression with aggregates replaced by references like ["__measure_1"]
            - list of (ref_name, kql_aggregate) tuples to add to summarize (only NEW ones)
        """
        if existing_aggs is None:
            existing_aggs = {}

        new_aggregates: list[tuple[str, str]] = []
        agg_counter = 0

        # Collect replacements: (start, end, ref_name)
        replacements: list[tuple[int, int, str]] = []

        for match in KQL_AGG_PATTERN.finditer(expr):
            start = match.start()
            paren_end = KustoKqlCompiler._find_matching_paren(expr, match.end() - 1)
            kql_agg = (
                KustoKqlCompiler._extract_maybe_agg_column_parts(
                    expr[start : paren_end + 1]
                )
                if paren_end != -1
                else None
            )

            # Skip invalid matches
            if (
                KustoKqlCompiler._is_inside_quotes_or_brackets(expr, start)
                or paren_end == -1
                or not kql_agg
            ):
                continue

            # Reuse existing aggregate or create new one
            if kql_agg.lower() in existing_aggs:
                ref_name = existing_aggs[kql_agg.lower()]
            else:
                agg_counter += 1
                clean_name = measure_name.strip('[]"')
                ref_name = f'["__{clean_name}_{agg_counter}"]'
                existing_aggs[kql_agg.lower()] = ref_name
                new_aggregates.append((ref_name, kql_agg))

            replacements.append((start, paren_end + 1, ref_name))

        # Apply replacements from right to left so positions stay valid
        result = expr
        for start, end, ref_name in reversed(replacements):
            result = result[:start] + ref_name + result[end:]

        return result, new_aggregates

    @staticmethod
    def _contains_aggregate_function(expr: str) -> bool:
        """Check if expression contains an aggregate function call."""
        for match in KQL_AGG_PATTERN.finditer(expr):
            if not KustoKqlCompiler._is_inside_quotes_or_brackets(expr, match.start()):
                return True
        return False

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
            main, lets = self._extract_let_statements(query.text)
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

        # Add summarize first if it exists
        if "summarize" in projections_parts_dict:
            compiled_query_lines.append(projections_parts_dict.pop("summarize"))

        # Then add extend after summarize
        if "extend" in projections_parts_dict:
            compiled_query_lines.append(projections_parts_dict.pop("extend"))

        # Add remaining parts (project, sort)
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

    def _get_projection_or_summarize(self, select: selectable.Select) -> dict[str, str]:
        """Builds the ending part of the query either project or summarize."""
        columns = select.inner_columns
        group_by_cols = select._group_by_clauses
        order_by_cols = select._order_by_clauses
        summarize_statement = ""
        extend_statement = ""
        project_statement = ""
        has_aggregates = False
        # The following is the logic
        # With Columns :
        #     - Do we have a group by clause ? --Yes---> Do we have aggregate columns ? --Yes--> Summarize new column(s)
        #                |                                   |                                        with by clause
        #                N                                   N --> Add to projection
        #                |
        #                |
        #     - Do the columns have aliases ? --Yes---> Extend with aliases
        #                |
        #                N---> Add to projection
        if columns is not None:
            summarize_columns = []
            extend_columns = []
            projection_columns = []
            existing_aggs: dict[str, str] = {}
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)
                column_alias = self._escape_and_quote_columns(column_alias, True)
                column_name = re.sub(
                    r'(?:[a-zA-Z_][a-zA-Z0-9_]*|\["[^"]+"\])\.', "", column_name
                )  # Remove table qualifiers from column name for processing
                # Do we have a group by clause ?
                # Do we have aggregate columns ?
                kql_agg = self._extract_maybe_agg_column_parts(column_name)
                has_inline_aggregates = self._contains_aggregate_function(column_name)

                # Case 1: Simple aggregate (e.g., count(), sum(col))
                if kql_agg and not self._has_operators_outside_quotes(column_name):
                    has_aggregates = True
                    summarize_entry = self._build_column_projection(
                        kql_agg, column_alias
                    )
                    summarize_columns.append(summarize_entry)
                    projection_columns.append(column_alias)
                    if column_alias:
                        existing_aggs[kql_agg.lower()] = column_alias

                # Case 2 & 3: Expressions with aggregates or aliased columns (both go to extend)
                # No group by clause
                # Do the columns have aliases ?
                # Add additional and to handle case where : SELECT column_name as column_name
                elif has_inline_aggregates or (
                    column_alias
                    and column_alias != self._escape_and_quote_columns(column_name)
                ):
                    # If contains aggregates, extract them first
                    if has_inline_aggregates:
                        has_aggregates = True
                        column_name, extracted_aggs = (
                            self._extract_and_replace_aggregates(
                                column_name, column_alias or "expr", existing_aggs
                            )
                        )

                        # Add extracted aggregates to summarize
                        for ref_name, kql_agg_extracted in extracted_aggs:
                            summarize_entry = self._build_column_projection(
                                kql_agg_extracted, ref_name
                            )
                            summarize_columns.append(summarize_entry)

                    # Build extend entry (common for both cases)
                    escaped_expr = self._escape_and_quote_columns(column_name)
                    extend_entry = f"{column_alias} = {escaped_expr}"
                    extend_columns.append(extend_entry)
                    projection_columns.append(column_alias)

                # Case 4: Simple column reference
                else:
                    projection_columns.append(
                        self._escape_and_quote_columns(column_name)
                    )
            # group by columns
            by_columns = self._group_by(group_by_cols)
            if has_aggregates or bool(
                by_columns
            ):  # Summarize can happen with or without aggregate being created
                summarize_statement = f"| summarize {', '.join(summarize_columns)} "
                if by_columns:
                    summarize_statement = (
                        f"{summarize_statement} by {', '.join(by_columns)}"
                    )
            if extend_columns:
                extend_statement = f"| extend {', '.join(extend_columns)}"

            project_statement = (
                f"| project {', '.join(projection_columns)}"
                if projection_columns
                else ""
            )
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
        if (
            KustoKqlCompiler._is_kql_function(name)
            or KustoKqlCompiler._is_number_literal(name)
        ) and not is_alias:
            return name
        # First, check if the name is already wrapped in ["ColumnName"] (escaped format)
        if name.startswith('["') and name.endswith('"]'):
            return name  # Return as is if already properly escaped
        # Handle arithmetic expressions by recursively processing operands
        if not is_alias:
            outer_paren_count, inner = KustoKqlCompiler._count_outer_parens(name)
            for operator in ["/", "+", "-", "*"]:
                pos = KustoKqlCompiler._find_top_level_operator(inner, operator)
                if pos != -1:
                    left = KustoKqlCompiler._escape_and_quote_columns(
                        inner[:pos].strip()
                    )
                    right = KustoKqlCompiler._escape_and_quote_columns(
                        inner[pos + 1 :].strip()
                    )
                    return (
                        "(" * outer_paren_count
                        + left
                        + " "
                        + operator
                        + " "
                        + right
                        + ")" * outer_paren_count
                    )
            # No operators - just process inner content (don't re-add unnecessary parens)
            if outer_paren_count > 0:
                return KustoKqlCompiler._escape_and_quote_columns(inner)
        # No operators found - strip surrounding quotes if present, then wrap
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
        name = name.replace('"', '\\"')
        return f'["{name}"]'

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

        where_clause = re.sub(
            r"!=\s*=",
            "!=",
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
        pattern = r"^\d+(\.\d+)?$"
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
            return KustoKqlCompiler._convert_quoted_columns(
                str(column.element)
            ), KustoKqlCompiler._convert_quoted_columns(column.name)
        if hasattr(column, "name"):
            return KustoKqlCompiler._convert_quoted_columns(str(column.name)), None
        return KustoKqlCompiler._convert_quoted_columns(str(column)), None

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
