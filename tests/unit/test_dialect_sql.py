from unittest.mock import MagicMock

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    create_engine,
    exc,
    func,
    select,
)

from sqlalchemy_kusto.dbapi import Cursor, _translate_raw_functions, is_tsql_query

engine = create_engine("kustosql+https://localhost/testdb")

DAY_TRUNCATIONS_IN_NESTED_MIX = 2

metadata = MetaData()
orders = Table(
    "Orders",
    metadata,
    Column("TotalAmount", Integer),
    Column("IsCorporateOrder", Boolean),
    schema="test",
)


def _compile(query, bind=engine) -> str:
    return str(query.compile(bind, compile_kwargs={"literal_binds": True})).replace(
        "\n", " "
    )


def test_boolean_false_renders_as_zero():
    """Kusto T-SQL does not support `false`/`true` literals; they must be 1/0."""
    query = select(orders.c.TotalAmount).where(
        orders.c.IsCorporateOrder == False  # noqa: E712
    )
    sql = _compile(query)
    assert "false" not in sql.lower()
    assert "0" in sql


def test_boolean_true_renders_as_one():
    query = select(orders.c.TotalAmount).where(
        orders.c.IsCorporateOrder == True  # noqa: E712
    )
    sql = _compile(query)
    assert "true" not in sql.lower()
    assert "1" in sql


def test_boolean_filter_full_query():
    """Mirrors the exact query from the bug report."""
    query = (
        select(orders.c.TotalAmount)
        .where(orders.c.IsCorporateOrder == False)  # noqa: E712
        .group_by(orders.c.TotalAmount)
    )
    sql = _compile(query)
    assert "false" not in sql.lower()
    assert "true" not in sql.lower()


class TestIlikeLowerFolding:
    """Kusto's LIKE pattern must be a string literal (OTR0001) and LIKE on a
    column is case-sensitive, so the stock ILIKE form lower(x) LIKE lower(y)
    must keep its left lower() but fold the right one into a lowered literal.
    """

    def test_lower_literal_folds(self):
        """Mirrors the Superset filter-search query from the bug report."""
        sql = "SELECT Name FROM t WHERE lower(Name) LIKE lower('%ФрЕш%')"
        expected = "SELECT Name FROM t WHERE lower(Name) LIKE '%фреш%'"
        assert _translate_raw_functions(sql) == expected

    def test_lower_of_column_is_left_alone(self):
        """lower(column) is valid Kusto; only constants can fold."""
        sql = "SELECT lower(Name) FROM t WHERE lower(Name) = 'x'"
        assert _translate_raw_functions(sql) == sql

    def test_lower_with_non_literal_expression_is_left_alone(self):
        sql = "SELECT 1 WHERE lower('a' + Name) = 'ab'"
        assert _translate_raw_functions(sql) == sql

    def test_folded_literal_keeps_quote_escapes(self):
        assert _translate_raw_functions("lower('О''Хара')") == "'о''хара'"


class TestOrderByWithoutLimit:
    """Kusto rejects ORDER BY in a nested SELECT that has no TOP; top level is fine."""

    events = Table("events", MetaData(), Column("score", Integer))

    def _ordered(self):
        return select([self.events.c.score]).order_by(self.events.c.score.desc())

    def test_top_level_order_by_gets_no_top(self):
        """Kusto sorts a top-level ORDER BY on its own: a TOP would only cap the result."""
        sql = _compile(self._ordered())
        assert "ORDER BY" in sql
        assert "TOP" not in sql

    def test_order_by_with_limit_uses_limit_not_double_top(self):
        sql = _compile(self._ordered().limit(10))
        assert sql.count("TOP") == 1
        assert "TOP 10" in sql

    def test_select_without_order_by_no_extra_top(self):
        assert "TOP" not in _compile(select([self.events.c.score]))

    def test_nested_order_by_gets_top(self):
        """Superset's WRAP_SQL shape: the inner ORDER BY needs a TOP of its own."""
        sql = _compile(select([self._ordered().alias("virtual_table")]).limit(100))
        assert "TOP 500000" in sql
        assert "TOP 100" in sql
        assert "ORDER BY" in sql

    def test_nested_order_by_with_own_limit_keeps_it(self):
        inner = self._ordered().limit(20).alias("virtual_table")
        sql = _compile(select([inner]).limit(100))
        assert "TOP 20" in sql
        assert "TOP 500000" not in sql

    def test_nested_select_without_order_by_gets_no_top(self):
        inner = select([self.events.c.score]).alias("virtual_table")
        assert "TOP" not in _compile(select([inner]))

    def test_top_fallback_value_is_configurable(self):
        """max_top_n must be settable the way callers actually set it."""
        custom_engine = create_engine(
            "kustosql+https://localhost/testdb", max_top_n=999
        )
        query = select([self._ordered().alias("virtual_table")]).limit(100)
        assert "TOP 999" in _compile(query, custom_engine)


class TestDateTruncCompiled:
    """func.date_trunc() goes through the dialect hook."""

    events = Table("events", MetaData(), Column("ts", DateTime))

    def _query(self, grain: str) -> str:
        return _compile(select([func.date_trunc(grain, self.events.c.ts)]))

    @pytest.mark.parametrize(
        ("grain", "part"),
        [
            ("second", "second"),
            ("minute", "minute"),
            ("hour", "hour"),
            ("day", "day"),
            ("month", "month"),
            ("quarter", "quarter"),
            ("year", "year"),
        ],
    )
    def test_grain_maps_to_datepart(self, grain: str, part: str):
        sql = self._query(grain)
        assert f"DATEADD({part}," in sql
        assert f"DATEDIFF({part}," in sql
        assert "date_trunc(" not in sql.lower()

    def test_week_is_monday_based(self):
        sql = self._query("week")
        assert "DATEADD(day, -1," in sql  # ISO week shift
        assert ", 0," in sql

    def test_week_sun_uses_sunday_epoch(self):
        sql = self._query("week_sun")
        assert "DATEADD(week," in sql
        assert ", -1," in sql
        assert "DATEADD(day, -1," not in sql

    def test_sub_day_grain_uses_safe_epoch(self):
        assert "2000-01-01" in self._query("minute")

    def test_grain_is_case_insensitive(self):
        assert "DATEADD(day," in self._query("DAY")

    def test_unsupported_grain_fails_loudly(self):
        with pytest.raises(exc.CompileError):
            self._query("decade")


class TestTranslateRawDateTrunc:
    """_translate_raw_functions rewrites unsupported calls in a raw SQL string."""

    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            (
                "SELECT date_trunc('day', event_time) FROM events",
                "SELECT DATEADD(day, DATEDIFF(day, 0, event_time), 0) FROM events",
            ),
            (
                "SELECT date_trunc('month', ts) FROM t",
                "SELECT DATEADD(month, DATEDIFF(month, 0, ts), 0) FROM t",
            ),
            (
                "SELECT date_trunc('year', ts) FROM t",
                "SELECT DATEADD(year, DATEDIFF(year, 0, ts), 0) FROM t",
            ),
            (
                "SELECT date_trunc('quarter', ts) FROM t",
                "SELECT DATEADD(quarter, DATEDIFF(quarter, 0, ts), 0) FROM t",
            ),
            (
                "SELECT date_trunc('week', ts) FROM t",
                "SELECT DATEADD(week, DATEDIFF(week, 0, DATEADD(day, -1, ts)), 0) FROM t",
            ),
            (
                "SELECT date_trunc('week_sun', ts) FROM t",
                "SELECT DATEADD(week, DATEDIFF(week, -1, ts), -1) FROM t",
            ),
            (
                "SELECT date_trunc('hour', ts) FROM logs",
                "SELECT DATEADD(hour, DATEDIFF(hour, '2000-01-01', ts), '2000-01-01') FROM logs",
            ),
            (
                # nested parentheses in the expression must survive intact
                "SELECT date_trunc('day', CAST(ts AS DATETIME)) FROM logs",
                "SELECT DATEADD(day, DATEDIFF(day, 0, CAST(ts AS DATETIME)), 0) FROM logs",
            ),
            (
                # nesting is translated inside out
                "SELECT date_trunc('day', date_trunc('hour', ts)) FROM t",
                "SELECT DATEADD(day, DATEDIFF(day, 0, DATEADD(hour, "
                "DATEDIFF(hour, '2000-01-01', ts), '2000-01-01')), 0) FROM t",
            ),
        ],
    )
    def test_translation(self, sql: str, expected: str):
        assert _translate_raw_functions(sql) == expected

    def test_grain_capitalisation_normalised(self):
        for grain in ("Day", "DAY", "dAy"):
            result = _translate_raw_functions(
                f"SELECT date_trunc('{grain}', ts) FROM t"
            )
            assert "DATEADD(day," in result, f"failed for grain={grain!r}: {result}"

    def test_multiple_calls_in_one_query(self):
        result = _translate_raw_functions(
            "SELECT date_trunc('day', a), date_trunc('hour', b) FROM t"
        )
        assert "DATEADD(day," in result
        assert "DATEADD(hour," in result
        assert "date_trunc(" not in result.lower()

    @pytest.mark.parametrize(
        "sql",
        [
            # a value that merely looks like a call must not be rewritten
            "SELECT * FROM t WHERE note = 'date_trunc(''day'', x)'",
            "SELECT * FROM t WHERE note = \"date_trunc('day', x)\"",
            # sub-second grains have no overflow-free form, so they are not supported
            "SELECT date_trunc('milliseconds', ts) FROM logs",
            "SELECT date_trunc('microseconds', ts) FROM logs",
            # unrecognised grain: leave it for Kusto to reject
            "SELECT date_trunc('decade', ts) FROM t",
            # malformed calls stay as they are
            "SELECT date_trunc('day' ts) FROM t",
            "SELECT date_trunc('day', ts FROM t",
            # nothing to do
            "SELECT id, name FROM users WHERE active = 1",
        ],
    )
    def test_left_unchanged(self, sql: str):
        assert _translate_raw_functions(sql) == sql

    def test_literal_next_to_a_real_call_is_preserved(self):
        result = _translate_raw_functions(
            "SELECT date_trunc('day', ts) FROM t WHERE note = 'date_trunc(''day'', x)'"
        )
        assert result == (
            "SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t "
            "WHERE note = 'date_trunc(''day'', x)'"
        )


class TestTranslateOtherFunctions:
    """Calls Kusto does not implement, rewritten to the form it does.

    Every shape here was taken from production SQLLab queries and its replacement
    executed against the Kusto emulator.
    """

    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            # T-SQL 2022 spelling, and the bare (unquoted) grain both analysts use
            (
                "SELECT DATETRUNC('hour', ts) FROM t",
                "SELECT DATEADD(hour, DATEDIFF(hour, '2000-01-01', ts), '2000-01-01') FROM t",
            ),
            (
                "SELECT DATETRUNC(month, ts) FROM t",
                "SELECT DATEADD(month, DATEDIFF(month, 0, ts), 0) FROM t",
            ),
            # date_part in both spellings -> DATEPART, whose unit is bare in T-SQL
            ("SELECT DATE_PART(hour, ts) FROM t", "SELECT DATEPART(hour, ts) FROM t"),
            ("SELECT date_part('year', ts) FROM t", "SELECT DATEPART(year, ts) FROM t"),
            # KQL habits that do not exist in T-SQL
            (
                "SELECT startofday(ts) FROM t",
                "SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t",
            ),
            (
                "SELECT startofmonth(ts) FROM t",
                "SELECT DATEADD(month, DATEDIFF(month, 0, ts), 0) FROM t",
            ),
            (
                "SELECT startofweek(ts) FROM t",  # KQL weeks start on Sunday
                "SELECT DATEADD(week, DATEDIFF(week, -1, ts), -1) FROM t",
            ),
            # MySQL habits
            ("SELECT IFNULL(a, 0) FROM t", "SELECT COALESCE(a, 0) FROM t"),
            ("SELECT ifnull(a , 'x') FROM t", "SELECT COALESCE(a, 'x') FROM t"),
            # DATE()/to_date() must drop the time: Kusto's CAST to DATE keeps it
            (
                "SELECT DATE(ts) FROM t",
                "SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t",
            ),
            (
                "SELECT date (ts) FROM t",  # whitespace before the paren
                "SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t",
            ),
            (
                "SELECT to_date(sale_date) FROM t",
                "SELECT DATEADD(day, DATEDIFF(day, 0, sale_date), 0) FROM t",
            ),
        ],
    )
    def test_translation(self, sql: str, expected: str):
        assert _translate_raw_functions(sql) == expected

    @pytest.mark.parametrize(
        "sql",
        [
            # no exact equivalent exists: let Kusto say so instead of guessing
            "SELECT DATE_FORMAT(ts, '%Y-%m-%d') FROM t",
            "SELECT FORMAT(ts, 'yyyy-MM') FROM t",
            "SELECT TRY_CAST(note AS INT) FROM t",
            "SELECT JSON_VALUE(payload, '$.a') FROM t",
            # forms Kusto already understands must not be touched
            "SELECT CONVERT(DATE, ts) FROM t",
            "SELECT CAST(ts AS DATE) FROM t",
            "SELECT DATEADD(day, 1, ts) FROM t",
            "SELECT DATEPART(year, ts) FROM t",
            # a name that merely ends with a translated one
            "SELECT my_date(ts), sale.date(x) FROM t",
            # wrong arity: not the function we know
            "SELECT date(ts, 'HOUR') FROM t",
            "SELECT ifnull(a) FROM t",
            # a column called date is not a call
            "SELECT date, to_date FROM t ORDER BY date",
        ],
    )
    def test_left_unchanged(self, sql: str):
        assert _translate_raw_functions(sql) == sql

    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            # an apostrophe in a comment used to be read as a string literal, which
            # swallowed the rest of the query and left the real call untranslated
            (
                "-- don't ask\nSELECT date_trunc('day', ts) FROM t",
                "-- don't ask\nSELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t",
            ),
            (
                "/* it's a header */ SELECT date_trunc('day', ts) FROM t",
                "/* it's a header */ SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t",
            ),
            # a call inside a comment stays a comment, untouched
            (
                "SELECT 1 -- date_trunc('day', x)\nFROM t",
                "SELECT 1 -- date_trunc('day', x)\nFROM t",
            ),
            (
                "SELECT /* date_trunc('day', x) */ 1 FROM t",
                "SELECT /* date_trunc('day', x) */ 1 FROM t",
            ),
        ],
    )
    def test_comments_are_opaque(self, sql: str, expected: str):
        assert _translate_raw_functions(sql) == expected

    def test_nested_mix_is_translated_throughout(self):
        result = _translate_raw_functions(
            "SELECT IFNULL(date_trunc('day', startofmonth(ts)), DATE(other)) FROM t"
        )
        assert "COALESCE(" in result
        # two day-truncations: date_trunc('day', ...) and DATE(other)
        assert result.count("DATEADD(day") == DAY_TRUNCATIONS_IN_NESTED_MIX
        assert "DATEADD(month" in result
        assert "ifnull" not in result.lower()
        assert "startofmonth" not in result.lower()


class TestIsTsqlQuery:
    """The query language is chosen from the text; a CTE is T-SQL, not KQL."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select 1",
            "WITH t AS (SELECT 1 x) SELECT x FROM t",
            "with t as (select 1 x) select x from t",
            ";WITH t AS (SELECT 1 x) SELECT x FROM t",
            "-- a comment\nSELECT 1",
            "/* header */ WITH t AS (SELECT 1 x) SELECT x FROM t",
        ],
    )
    def test_recognised_as_tsql(self, sql: str):
        assert is_tsql_query(sql) is True

    @pytest.mark.parametrize(
        "sql",
        [
            "events | take 10",
            "let x = 1; print x",
            ".show tables",
            "print now()",
            "withdrawals | count",  # starts with "with" but is not a CTE
        ],
    )
    def test_recognised_as_kql(self, sql: str):
        assert is_tsql_query(sql) is False

    def test_long_comment_header_is_linear(self):
        """A repeated-alternation regex here used to backtrack catastrophically."""
        sql = "\n".join(f"-- note {i}" for i in range(2000)) + "\nevents | take 1"
        assert is_tsql_query(sql) is False


class TestCursorDateTruncTranslation:
    """Cursor.execute translates T-SQL only, and leaves KQL alone."""

    @staticmethod
    def _cursor() -> tuple[Cursor, MagicMock]:
        client = MagicMock()
        response = MagicMock()
        response.primary_results = [
            MagicMock(columns=[], **{"__iter__": lambda _: iter([])})
        ]
        client.execute.return_value = response
        return Cursor(client, "testdb"), client

    def test_date_trunc_translated_before_kusto(self):
        cursor, client = self._cursor()
        cursor.execute("SELECT date_trunc('day', ts) FROM t")
        received_sql = client.execute.call_args[0][1]
        assert "DATEADD(day," in received_sql
        assert "date_trunc(" not in received_sql.lower()

    def test_sql_without_date_trunc_forwarded_unchanged(self):
        cursor, client = self._cursor()
        sql = "SELECT id, name FROM users WHERE active = 1"
        cursor.execute(sql)
        assert client.execute.call_args[0][1] == sql

    def test_bound_ilike_pattern_folds_after_parameters(self):
        """Parameters land first, then the rewrite, so a bound pattern can fold."""
        cursor, client = self._cursor()
        cursor.execute(
            "SELECT Name FROM t WHERE lower(Name) LIKE lower(%(pat)s)",
            {"pat": "%ФрЕш%"},
        )
        received_sql = client.execute.call_args[0][1]
        assert received_sql == "SELECT Name FROM t WHERE lower(Name) LIKE '%фреш%'"

    def test_kql_query_is_never_rewritten(self):
        """A KQL query is not T-SQL; DATEADD would be invalid there."""
        cursor, client = self._cursor()
        kql = "events | extend d = date_trunc('day', ts) | take 10"
        cursor.execute(kql)
        assert client.execute.call_args[0][1] == kql
