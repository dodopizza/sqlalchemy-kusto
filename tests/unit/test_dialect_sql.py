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

from sqlalchemy_kusto.dbapi import Cursor, _translate_raw_date_trunc

engine = create_engine("kustosql+https://localhost/testdb")

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
    """_translate_raw_date_trunc rewrites date_trunc() in a raw SQL string."""

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
        assert _translate_raw_date_trunc(sql) == expected

    def test_grain_capitalisation_normalised(self):
        for grain in ("Day", "DAY", "dAy"):
            result = _translate_raw_date_trunc(
                f"SELECT date_trunc('{grain}', ts) FROM t"
            )
            assert "DATEADD(day," in result, f"failed for grain={grain!r}: {result}"

    def test_multiple_calls_in_one_query(self):
        result = _translate_raw_date_trunc(
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
        assert _translate_raw_date_trunc(sql) == sql

    def test_literal_next_to_a_real_call_is_preserved(self):
        result = _translate_raw_date_trunc(
            "SELECT date_trunc('day', ts) FROM t WHERE note = 'date_trunc(''day'', x)'"
        )
        assert result == (
            "SELECT DATEADD(day, DATEDIFF(day, 0, ts), 0) FROM t "
            "WHERE note = 'date_trunc(''day'', x)'"
        )


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

    def test_kql_query_is_never_rewritten(self):
        """A KQL query is not T-SQL; DATEADD would be invalid there."""
        cursor, client = self._cursor()
        kql = "events | extend d = date_trunc('day', ts) | take 10"
        cursor.execute(kql)
        assert client.execute.call_args[0][1] == kql
