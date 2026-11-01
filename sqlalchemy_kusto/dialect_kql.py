"""KQL dialect: compiles a SQLAlchemy ``Select`` into a Kusto Query Language pipeline.

The compiler walks the expression tree with the regular SQLAlchemy visitor methods
and emits pipeline operators in a fixed order::

    <let statements of the virtual table>
    <source>                      ["Table"] | database("db").["Table"] | virtual_table
    | join kind=inner (<subquery>) on $left.["k"] == $right.["k__"]
    | where <where>
    | extend <alias> = <expr>     (only without summarize)
    | summarize <alias> = <agg>() by <alias> = <expr>
    | where <having>
    | order by <col> desc
    | project <cols>  |  distinct <cols>
    | take <limit>

It covers the statement shapes Apache Superset builds for charts, datasets and
filter values. Raw text (``text()``, ``literal_column()``) is passed through as KQL.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import exc
from sqlalchemy.sql import (
    compiler,
    elements,
    functions,
    operators,
    selectable,
    sqltypes,
)

from sqlalchemy_kusto.dialect_base import KustoBaseDialect

KQL_AGGREGATES = frozenset(
    {
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
        "make_set",
        "make_set_if",
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
)

# SQL spellings of aggregates that Superset or a user may write, mapped to KQL names.
FUNCTION_ALIASES = {"count_distinct": "dcount"}

BOOLEAN_OPERATORS = {operators.and_: " and ", operators.or_: " or "}

# Scans KQL text: string literals are opaque, everything else yields function calls.
_KQL_TEXT = re.compile(
    r"""(?P<str>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')
      |(?<![\w.])(?:
          (?P<count_all>count\s*\(\s*(?:\*|1)?\s*\))
         |(?P<count_distinct>count\s*\(\s*distinct\s+|count_distinct\s*\()
         |(?P<call>[A-Za-z_]\w*)\s*\(
      )""",
    re.IGNORECASE | re.VERBOSE,
)
_TRAILING_DIRECTION = re.compile(
    r"\s+(asc|desc)(\s+nulls\s+(?:first|last))?\s*$", re.IGNORECASE
)
# One name of a table reference: ["name"] (this dialect's quoting), "name" or bare.
_TABLE_NAME_PART = r'\["(?:[^"\\]|\\.)*"\]|"(?:[^"\\]|\\.)*"|[A-Za-z_][\w-]*'
# A FROM text that is nothing but [schema.]table, the way Superset's select_star
# spells it: text(quote_schema(schema) + "." + quote(table)).
_TABLE_REFERENCE = re.compile(
    rf"\s*(?:(?P<schema>{_TABLE_NAME_PART})\s*\.\s*)?(?P<table>{_TABLE_NAME_PART})\s*"
)


def _normalize_kql_text(text: str) -> str:
    """Fix the SQL habits Superset bakes into KQL text: COUNT(*), COUNT(DISTINCT x), SUM(x), TOLOWER(x).

    KQL function names are case-sensitive, ``count()`` takes no argument and distinct
    counting is ``dcount()``. String literals are left untouched.
    """

    def replace(match: re.Match) -> str:
        if match.group("str"):
            return match.group(0)
        if match.group("count_all"):
            return "count()"
        if match.group("count_distinct"):
            return "dcount("
        name = match.group("call")
        if name.lower() in KQL_AGGREGATES or (name.isupper() and len(name) > 1):
            # Built-in KQL functions are all lowercase; Superset's sqlglot sanitizer
            # shouts every call it sees (tolower → TOLOWER).
            return match.group(0).replace(name, name.lower(), 1)
        return match.group(0)

    return _KQL_TEXT.sub(replace, text)


def _mentions_aggregate(text: str) -> bool:
    """Whether the KQL text calls an aggregate function outside string literals."""
    for match in _KQL_TEXT.finditer(text):
        if match.group("count_all") or match.group("count_distinct"):
            return True
        name = match.group("call")
        if name and name.lower() in KQL_AGGREGATES:
            return True
    return False


def _string_end(script: str, start: int, verbatim: bool) -> int:
    """Index just past the string literal opening at ``start``."""
    quote = script[start]
    i = start + 1
    while i < len(script):
        char = script[i]
        if char == quote:
            if verbatim and script[i + 1 : i + 2] == quote:
                i += 2  # doubled quote inside @"…"
                continue
            return i + 1
        if char == "\\" and not verbatim:
            i += 2
            continue
        i += 1
    return len(script)


def split_statements(script: str) -> list[str]:
    """Split a KQL script on top-level ``;`` and drop ``//`` comments.

    String literals (``"…"``, ``'…'``, verbatim ``@"…"`` and multi-line ```…```)
    are opaque: a ``;`` or ``//`` inside one is data. Comments are removed because a
    trailing one would swallow the ``);`` that closes a ``let`` body.
    """
    statements: list[str] = []
    buffer: list[str] = []
    i = 0
    while i < len(script):
        char = script[i]
        if script.startswith("//", i):
            end = script.find("\n", i)
            i = len(script) if end == -1 else end  # keep the newline itself
        elif script.startswith("```", i):
            end = script.find("```", i + 3)
            end = len(script) if end == -1 else end + 3
            buffer.append(script[i:end])
            i = end
        elif char in "\"'":
            end = _string_end(script, i, verbatim=i > 0 and script[i - 1] == "@")
            buffer.append(script[i:end])
            i = end
        elif char == ";":
            statements.append("".join(buffer))
            buffer = []
            i += 1
        else:
            buffer.append(char)
            i += 1
    statements.append("".join(buffer))
    return [statement.strip() for statement in statements if statement.strip()]


class UniversalSet:
    def __contains__(self, item: object) -> bool:
        return True


class KustoKqlIdentifierPreparer(compiler.IdentifierPreparer):
    # Quote every identifier as ["name"]: it is valid for any name, including
    # spaces, dots, Cyrillic and KQL keywords.
    reserved_words = UniversalSet()

    def __init__(self, dialect: Any, **kw: Any) -> None:
        super().__init__(dialect, initial_quote='["', final_quote='"]', **kw)

    def quote(self, ident: str, force: Any = None) -> str:
        if ident.startswith('["') and ident.endswith('"]'):
            return ident  # already quoted, e.g. by Superset via this same preparer
        return super().quote(ident, force)

    def _escape_identifier(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _unescape_identifier(self, value: str) -> str:
        return value.replace('\\"', '"').replace("\\\\", "\\")


@dataclass
class _Column:
    """One entry of the SELECT list, rendered."""

    alias: str | None  # label name
    reference: str  # how later operators refer to it: ["alias"] or the expression
    expression: str  # rendered KQL of the underlying expression
    aggregate: bool
    grouped: bool = False

    @property
    def assignment(self) -> str:
        if self.alias and self.reference != self.expression:
            return f"{self.reference} = {self.expression}"
        return self.expression


class KustoKqlCompiler(compiler.SQLCompiler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Ordered set of top-level statements hoisted from virtual tables; KQL allows
        # `let` only at the top of the script, never inside parentheses.
        self._lets: dict[str, None] = {}
        super().__init__(*args, **kwargs)

    # ------------------------------------------------------------------ statement

    def visit_select(
        self,
        select_stmt: selectable.Select,
        asfrom: bool = False,
        insert_into: bool = False,
        fromhints: Any = None,
        compound_index: int | None = None,
        select_wraps_for: Any = None,
        lateral: bool = False,
        from_linter: Any = None,
        **kwargs: Any,
    ) -> str:
        kwargs["within_columns_clause"] = False
        # Kusto has no bind parameters: every value is rendered inline.
        kwargs["literal_binds"] = True

        compile_state = select_stmt._compile_state_factory(select_stmt, self, **kwargs)
        select_stmt = compile_state.statement
        toplevel = not self.stack
        if toplevel and not self.compile_state:  # type: ignore[has-type]
            self.compile_state = compile_state
        entry = self._default_stack_entry if toplevel else self.stack[-1]
        froms = self._setup_select_stack(
            select_stmt, compile_state, entry, asfrom, lateral, compound_index
        )
        try:
            lines = self._pipeline(select_stmt, compile_state, froms, **kwargs)
        finally:
            self.stack.pop(-1)
        if toplevel:
            lines = [*self._lets, *lines]
        return "\n".join(lines)

    def _pipeline(  # noqa: PLR0912
        self,
        select: selectable.Select,
        compile_state: Any,
        froms: list,
        **kw: Any,
    ) -> list[str]:
        if len(froms) != 1:
            raise exc.CompileError(
                f"KQL needs exactly one source table or subquery, got {len(froms)}"
            )
        lines = [self._source(froms[0], **kw)]
        where = self._conjunction(select._where_criteria, **kw)
        if where:
            lines.append("| where " + where)

        columns = self._columns(compile_state, **kw)
        self.stack[-1]["kql_columns"] = columns  # for visit_label_reference
        by = self._group_by(select, columns, **kw)
        aggregates = [c for c in columns if c.aggregate and not c.grouped]
        if aggregates or by:
            stray = [c.reference for c in columns if not c.aggregate and not c.grouped]
            if stray:
                raise exc.CompileError(
                    f"{', '.join(stray)}: not an aggregate and not in GROUP BY, "
                    "KQL summarize cannot carry it"
                )
            parts = ["summarize", ", ".join(c.assignment for c in aggregates)]
            if by:
                parts += ["by", ", ".join(by)]
            lines.append("| " + " ".join(part for part in parts if part))
        else:
            extends = [c.assignment for c in columns if c.assignment != c.expression]
            if extends:
                lines.append("| extend " + ", ".join(extends))

        having = self._having(select._having_criteria, aggregates, **kw)
        if having:
            lines.append("| where " + having)

        order_by = self._order_by(select._order_by_clauses, **kw)
        if order_by and not select._distinct:
            lines.append(order_by)

        projection = [c.reference for c in columns]
        if select._distinct:
            lines.append("| distinct " + (", ".join(projection) if projection else "*"))
            if order_by:
                lines.append(order_by)
        elif projection and all(c.alias or not c.aggregate for c in columns):
            lines.append("| project " + ", ".join(projection))

        if select._offset_clause is not None:
            offset = self.process(select._offset_clause, **kw)
            lines += ["| serialize", f"| where row_number() > {offset}"]
        if select._limit_clause is not None:
            lines.append(f"| take {self.process(select._limit_clause, **kw)}")
        return lines

    def _conjunction(self, criteria: tuple, **kw: Any) -> str:
        rendered = (
            self.process(criterion.self_group(against=operators.and_), **kw)
            for criterion in criteria
        )
        return " and ".join(part for part in rendered if part)  # and_() renders empty

    def _having(self, criteria: tuple, aggregates: list[_Column], **kw: Any) -> str:
        """SQL HAVING repeats the aggregate (``count(*) > 5``); after summarize only the alias exists."""
        having = _normalize_kql_text(self._conjunction(criteria, **kw))
        for column in sorted(aggregates, key=lambda c: len(c.expression), reverse=True):
            if column.alias:
                having = having.replace(column.expression, column.reference)
        return having

    def _columns(self, compile_state: Any, **kw: Any) -> list[_Column]:
        columns = []
        for _, _, _, column, _ in compile_state.columns_plus_names:
            if isinstance(column, elements.ColumnClause) and column.name == "*":
                continue
            alias, expr = (
                (column.name, column.element)
                if isinstance(column, elements.Label)
                else (None, column)
            )
            expression = self.process(expr, **kw)
            reference = self.preparer.quote(alias) if alias else expression
            columns.append(
                _Column(alias, reference, expression, self._is_aggregate(expr))
            )
        return columns

    def _group_by(
        self, select: selectable.Select, columns: list[_Column], **kw: Any
    ) -> list[str]:
        by: list[str] = []
        for clause in select._group_by_clauses:
            expression = self.process(clause, **kw)
            # Superset selects the same expression under several labels
            # ("Timestamp" AS __timestamp, "Timestamp" AS "Timestamp"): all of them are grouped.
            matches = [c for c in columns if c.expression == expression]
            if not matches and isinstance(clause, elements.Label):
                matches = [c for c in columns if c.alias == clause.name]
            if not matches and isinstance(
                clause, elements.TextClause
            ):  # group_by(text("Col"))
                matches = [
                    c
                    for c in columns
                    if c.expression == self.preparer.quote(expression)
                ]
            if not matches:
                by.append(expression)
            for column in matches:
                column.grouped = True
                if column.assignment not in by:
                    by.append(column.assignment)
        return by

    def _order_by(self, clauses: tuple, **kw: Any) -> str | None:
        if not clauses:
            return None
        parts = []
        for clause in clauses:
            if isinstance(clause, elements.TextClause):
                # forgive the SQL habit of shouting the direction
                part = _TRAILING_DIRECTION.sub(
                    lambda m: " " + m.group(1).lower() + (m.group(2) or "").lower(),
                    clause.text,
                )
            else:
                part = self.process(clause, **kw)
            if not _TRAILING_DIRECTION.search(part):
                part += " asc"  # KQL sorts descending by default, SQL ascending
            parts.append(part)
        return "| order by " + ", ".join(parts)

    def visit_label_reference(
        self,
        element: elements._label_reference,
        within_columns_clause: bool = False,
        **kw: Any,
    ) -> str:
        """ORDER BY a label renders the alias of the matching SELECT entry.

        Superset builds fresh Label objects for ORDER BY, so SQLAlchemy's lineage
        check never passes; and for the series-limit subquery the label name differs
        (``count`` vs ``mme_inner__``) while the expression is the same. After
        summarize only the alias exists, so the alias is what must be rendered.
        """
        label = element.element._order_by_label_element
        columns = self.stack[-1].get("kql_columns", []) if self.stack else []
        if label is not None and columns:
            match = next((c for c in columns if c.alias == label.name), None)
            if match is None:
                expression = self.process(label.element, **kw)
                match = next(
                    (c for c in columns if c.alias and c.expression == expression), None
                )
            if match is not None:
                kw["kql_label_as"] = (label, match.reference)
        return self.process(
            element.element, within_columns_clause=within_columns_clause, **kw
        )

    def visit_label(
        self, label: elements.Label, kql_label_as: tuple | None = None, **kw: Any
    ) -> str:
        if kql_label_as is not None and kql_label_as[0] is label:
            return kql_label_as[1]
        return super().visit_label(label, **kw)

    def _is_aggregate(self, expr: Any) -> bool:
        if isinstance(expr, functions.FunctionElement):
            name = getattr(expr, "name", "").lower()
            if FUNCTION_ALIASES.get(name, name) in KQL_AGGREGATES:
                return True
            return any(self._is_aggregate(clause) for clause in expr.clauses)
        if isinstance(expr, elements.TextClause):
            return _mentions_aggregate(expr.text)
        if isinstance(expr, elements.ColumnClause):
            return bool(expr.is_literal) and _mentions_aggregate(expr.name)
        return any(self._is_aggregate(child) for child in expr.get_children())

    # -------------------------------------------------------------------- sources

    def _source(self, from_obj: Any, **kw: Any) -> str:  # noqa: PLR0911
        if isinstance(from_obj, selectable.Join):
            return self._join(from_obj, **kw)
        if isinstance(from_obj, selectable.FromGrouping):
            return self._source(from_obj.element, **kw)
        if isinstance(from_obj, selectable.TableClause):
            name = self.preparer.quote(from_obj.name)
            if from_obj.schema:
                database = self.render_literal_value(from_obj.schema, sqltypes.String())
                return f"database({database}).{name}"
            return name
        if isinstance(from_obj, selectable.AliasedReturnsRows):
            element = from_obj.element
            if isinstance(element, selectable.TextualSelect):
                self._add_let(from_obj.name, element.element.text)
                return from_obj.name
            if isinstance(element, selectable.SelectBase):
                return "(" + self.process(element, asfrom=True, **kw) + ")"
            return self._source(element, **kw)  # KQL has no table aliases
        if isinstance(from_obj, elements.TextClause):
            return self._text_source(from_obj.text)
        raise exc.CompileError(
            f"Unsupported FROM clause for KQL: {type(from_obj).__name__}"
        )

    def _text_source(self, text: str) -> str:
        """FROM text: a bare [schema.]table becomes a KQL table reference, anything else is KQL.

        Superset builds the SQL Lab table preview as select * from text('["events"].["T"]').
        KQL has no schema.table syntax (SEM0139), so a text that is only a table name is
        rendered like a TableClause: database("events").["T"].
        """
        match = _TABLE_REFERENCE.fullmatch(text)
        if match is None:
            return text.strip()
        name = self.preparer.quote(self._unquote_name(match.group("table")))
        if match.group("schema") is None:
            return name
        schema = self._unquote_name(match.group("schema"))
        return f"database({self.render_literal_value(schema, sqltypes.String())}).{name}"

    def _unquote_name(self, part: str) -> str:
        if part.startswith('["'):
            return self.preparer._unescape_identifier(part[2:-2])
        if part.startswith('"'):
            return part[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        return part

    def _add_let(self, name: str, script: str) -> None:
        statements = split_statements(script)
        if not statements:
            raise exc.CompileError(f"Virtual table {name} has no KQL body")
        *prelude, body = statements
        for statement in prelude:
            self._lets.setdefault(statement + ";", None)
        self._lets.setdefault(f"let {name} = ({body});", None)

    def _join(self, join: selectable.Join, **kw: Any) -> str:
        kind = "fullouter" if join.full else "leftouter" if join.isouter else "inner"
        extends: list[str] = []
        on_clause = self._on_clause(join.onclause, join.right, extends, **kw)
        right = self._source(join.right, **kw)
        if not right.startswith("("):
            right = f"({right})"
        lines = [self._source(join.left, **kw)]
        if extends:
            lines.append("| extend " + ", ".join(extends))
        lines.append(f"| join kind={kind} {right} on {on_clause}")
        return "\n".join(lines)

    def _on_clause(self, clause: Any, right: Any, extends: list[str], **kw: Any) -> str:
        if isinstance(clause, elements.Grouping):
            return self._on_clause(clause.element, right, extends, **kw)
        if (
            isinstance(clause, elements.BooleanClauseList)
            and clause.operator is operators.and_
        ):
            return " and ".join(
                self._on_clause(part, right, extends, **kw) for part in clause.clauses
            )
        if (
            isinstance(clause, elements.BinaryExpression)
            and clause.operator is operators.eq
        ):
            left_key, right_key = clause.left, clause.right
            if self._belongs_to(left_key, right) and not self._belongs_to(
                right_key, right
            ):
                left_key, right_key = right_key, left_key
            return (
                f"$left.{self._join_key(left_key, extends, **kw)} == "
                f"$right.{self._join_key(right_key, extends, **kw)}"
            )
        raise exc.CompileError("KQL join supports only column equality conditions")

    @staticmethod
    def _belongs_to(expr: Any, selectable_: Any) -> bool:
        if not isinstance(expr, elements.ColumnClause):
            return False
        if expr.table is not None:
            return expr.table is selectable_
        return expr.name in selectable_.c

    def _join_key(self, expr: Any, extends: list[str], **kw: Any) -> str:
        if isinstance(expr, elements.Label):
            inner = expr.element
            if isinstance(inner, elements.ColumnClause) and not inner.is_literal:
                return self.preparer.quote(inner.name)
            # a computed key must exist as a column on the left side before the join
            name = self.preparer.quote(expr.name)
            extends.append(f"{name} = {self.process(inner, **kw)}")
            return name
        if isinstance(expr, elements.ColumnClause) and not expr.is_literal:
            return self.preparer.quote(expr.name)
        raise exc.CompileError("KQL join keys must be columns or labelled expressions")

    # ---------------------------------------------------------------- expressions

    def visit_column(
        self,
        column: elements.ColumnClause,
        add_to_result_map: Any = None,
        include_table: bool = True,
        result_map_targets: tuple = (),
        **kw: Any,
    ) -> str:
        if column.is_literal:
            return _normalize_kql_text(column.name)
        # KQL has no table prefix for columns.
        return super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=False,
            result_map_targets=result_map_targets,
            **kw,
        )

    def visit_textclause(
        self, textclause: elements.TextClause, add_to_result_map: Any = None, **kw: Any
    ) -> str:
        if not textclause._bindparams:
            # KQL as written; a ':' in it is not a bind marker. Superset still escapes
            # colons the SQLAlchemy way (datetime(2026-01-01T00\:00\:00)), so undo that.
            return compiler.BIND_PARAMS_ESC.sub(lambda m: m.group(1), textclause.text)
        return super().visit_textclause(
            textclause, add_to_result_map=add_to_result_map, **kw
        )

    def post_process_text(self, text: str) -> str:
        return text  # no parameters are sent, so '%' needs no doubling

    def escape_literal_column(self, text: str) -> str:
        return text

    def visit_function(
        self, func: functions.FunctionElement, add_to_result_map: Any = None, **kw: Any
    ) -> str:
        name = func.name.lower()
        name = FUNCTION_ALIASES.get(name, name)
        args = list(func.clauses)
        if name == "count":
            if not args or self._is_star(args[0]):
                return "count()"
            arg = args[0]
            if (
                isinstance(arg, elements.UnaryExpression)
                and arg.operator is operators.distinct_op
            ):
                return f"dcount({self.process(arg.element, **kw)})"
            if (
                isinstance(arg, functions.FunctionElement)
                and arg.name.lower() == "distinct"
            ):
                return f"dcount({self._arguments(arg.clauses, **kw)})"
            # SQL COUNT(col) skips nulls; KQL count() takes no argument.
            return f"countif(isnotnull({self.process(arg, **kw)}))"
        return f"{name}({self._arguments(args, **kw)})"

    def _arguments(self, args: Any, **kw: Any) -> str:
        return ", ".join(self.process(arg, **kw) for arg in args)

    @staticmethod
    def _is_star(arg: Any) -> bool:
        if isinstance(arg, elements.ColumnClause):
            return arg.name == "*"
        return isinstance(arg, elements.BindParameter) and arg.value in ("*", 1)

    def visit_clauselist(self, clauselist: elements.ClauseList, **kw: Any) -> str:
        separator = BOOLEAN_OPERATORS.get(clauselist.operator)
        if separator is None:
            return super().visit_clauselist(clauselist, **kw)
        return self._generate_delimited_list(clauselist.clauses, separator, **kw)

    def visit_eq_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._generate_generic_binary(binary, " == ", **kw)

    def visit_ne_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._generate_generic_binary(binary, " != ", **kw)

    def visit_is__binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        if isinstance(binary.right, elements.Null):
            return f"isnull({self.process(binary.left, **kw)})"
        return self._generate_generic_binary(binary, " == ", **kw)

    def visit_is_not_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        if isinstance(binary.right, elements.Null):
            return f"isnotnull({self.process(binary.left, **kw)})"
        return self._generate_generic_binary(binary, " != ", **kw)

    def visit_in_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        if self._is_empty_list(binary.right):
            return "false"
        return self._generate_generic_binary(binary, " in ", **kw)

    def visit_not_in_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        if self._is_empty_list(binary.right):
            return "true"
        return self._generate_generic_binary(binary, " !in ", **kw)

    @staticmethod
    def _is_empty_list(expr: Any) -> bool:
        return (
            isinstance(expr, elements.BindParameter)
            and expr.expanding
            and not expr.effective_value
        )

    def visit_between_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._between(binary, "between", **kw)

    def visit_not_between_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._between(binary, "!between", **kw)

    def _between(
        self, binary: elements.BinaryExpression, keyword: str, **kw: Any
    ) -> str:
        low, high = binary.right.clauses
        return (
            f"{self.process(binary.left, **kw)} {keyword} "
            f"({self.process(low, **kw)} .. {self.process(high, **kw)})"
        )

    def visit_like_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._like(binary, negate=False, case_sensitive=True, **kw)

    def visit_not_like_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._like(binary, negate=True, case_sensitive=True, **kw)

    def visit_ilike_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._like(binary, negate=False, case_sensitive=False, **kw)

    def visit_not_ilike_op_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._like(binary, negate=True, case_sensitive=False, **kw)

    def _like(
        self,
        binary: elements.BinaryExpression,
        negate: bool,
        case_sensitive: bool,
        **kw: Any,
    ) -> str:
        """LIKE by pattern shape: %x% → contains, x% → startswith, %x → endswith, x → ==.

        KQL has no LIKE. A '%' in the middle of the pattern becomes a regular
        expression; '_' is taken literally, as column values with underscores are far
        more common in Kusto than single-character wildcards.
        """
        pattern = getattr(binary.right, "value", None)
        if not isinstance(pattern, str):
            raise exc.CompileError("KQL LIKE needs a literal string pattern")
        left = self.process(binary.left, **kw)
        body = pattern.strip("%")
        not_ = "!" if negate else ""
        if "%" in body:
            regex = ".*".join(re.escape(part) for part in pattern.split("%"))
            if not case_sensitive:
                regex = "(?i)" + regex
            return f"{left} {not_}matches regex {self._string(f'^{regex}$')}"
        starts, ends = pattern.startswith("%"), pattern.endswith("%")
        if starts and ends:
            keyword = "contains"
        elif ends:
            keyword = "startswith"
        elif starts:
            keyword = "endswith"
        else:
            keyword = (
                ("!=" if negate else "==")
                if case_sensitive
                else ("!~" if negate else "=~")
            )
            return f"{left} {keyword} {self._string(body)}"
        if case_sensitive:
            keyword += "_cs"
        return f"{left} {not_}{keyword} {self._string(body)}"

    def visit_inv_unary_operator(
        self, unary: elements.UnaryExpression, operator: Any, **kw: Any
    ) -> str:
        element = unary.element
        if isinstance(element, elements.Grouping):
            element = element.element  # not() already brackets its argument
        return f"not({self.process(element, **kw)})"

    def visit_mod_binary(
        self, binary: elements.BinaryExpression, operator: Any, **kw: Any
    ) -> str:
        return self._generate_generic_binary(
            binary, " % ", **kw
        )  # never doubled: no parameters

    def visit_asc_op_unary_modifier(
        self, unary: elements.UnaryExpression, modifier: Any, **kw: Any
    ) -> str:
        return f"{self.process(unary.element, **kw)} asc"

    def visit_desc_op_unary_modifier(
        self, unary: elements.UnaryExpression, modifier: Any, **kw: Any
    ) -> str:
        return f"{self.process(unary.element, **kw)} desc"

    def visit_nulls_first_op_unary_modifier(
        self, unary: elements.UnaryExpression, modifier: Any, **kw: Any
    ) -> str:
        return f"{self.process(unary.element, **kw)} nulls first"

    def visit_nulls_last_op_unary_modifier(
        self, unary: elements.UnaryExpression, modifier: Any, **kw: Any
    ) -> str:
        return f"{self.process(unary.element, **kw)} nulls last"

    # ------------------------------------------------------------------- literals

    def render_literal_value(self, value: Any, type_: Any) -> str:
        if isinstance(value, str):
            return self._string(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, datetime | date):
            return f"datetime({value.isoformat()})"
        return super().render_literal_value(value, type_)

    @staticmethod
    def _string(value: str) -> str:
        escaped = (
            value.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
        return f'"{escaped}"'


class KustoKqlHttpsDialect(KustoBaseDialect):
    name = "kustokql"
    statement_compiler = KustoKqlCompiler
    preparer = KustoKqlIdentifierPreparer
    # Literal values are rendered into the query text, so a cached compilation would
    # replay the values of the first execution.
    supports_statement_cache = False


class KustoKqlHttpDialect(KustoKqlHttpsDialect):
    """Plain-HTTP variant, for the Kusto emulator: no TLS, no authentication."""

    driver = "http"
    supports_statement_cache = False
