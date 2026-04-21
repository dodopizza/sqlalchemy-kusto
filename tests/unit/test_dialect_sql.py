"""Unit tests for KustoSql dialect — ORDER BY and date_trunc compilation."""
from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, create_engine, func, select

from sqlalchemy_kusto.dialect_sql import KustoSqlHttpsDialect, _translate_raw_date_trunc

engine = create_engine("kustosql+https://localhost/testdb")


def _compile(query) -> str:
    return str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", " ")


class TestOrderByWithoutLimit:
    def test_order_by_without_limit_inserts_top_fallback(self):
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc())
        sql = _compile(query)
        assert "TOP" in sql
        assert "ORDER BY" in sql

    def test_order_by_with_limit_uses_limit_not_double_top(self):
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc()).limit(10)
        sql = _compile(query)
        assert sql.count("TOP") == 1
        assert "TOP 10" in sql

    def test_select_without_order_by_no_extra_top(self):
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score])
        sql = _compile(query)
        assert "TOP" not in sql

    def test_top_fallback_value_is_configurable(self):
        custom_engine = create_engine("kustosql+https://localhost/testdb", connect_args={})
        custom_engine.dialect.max_top_n = 999
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc())
        sql = str(query.compile(custom_engine, compile_kwargs={"literal_binds": True}))
        assert "TOP 999" in sql


class TestDateTrunc:
    def _query(self, grain: str) -> str:
        t = Table("events", MetaData(), Column("ts", DateTime))
        query = select([func.date_trunc(grain, t.c.ts)])
        return _compile(query)

    def test_date_trunc_day(self):
        sql = self._query("day")
        assert "DATEADD(day" in sql
        assert "DATEDIFF(day" in sql

    def test_date_trunc_month(self):
        sql = self._query("month")
        assert "DATEADD(month" in sql
        assert "DATEDIFF(month" in sql

    def test_date_trunc_year(self):
        sql = self._query("year")
        assert "DATEADD(year" in sql
        assert "DATEDIFF(year" in sql

    def test_date_trunc_hour(self):
        sql = self._query("hour")
        assert "DATEADD(hour" in sql
        assert "DATEDIFF(hour" in sql

    def test_date_trunc_minute(self):
        sql = self._query("minute")
        assert "DATEADD(minute" in sql
        assert "DATEDIFF(minute" in sql

    def test_date_trunc_week(self):
        sql = self._query("week")
        assert "DATEADD(week" in sql
        assert "DATEDIFF(week" in sql

    def test_date_trunc_quarter(self):
        sql = self._query("quarter")
        assert "DATEADD(quarter" in sql
        assert "DATEDIFF(quarter" in sql

    def test_date_trunc_not_passed_through_verbatim(self):
        sql = self._query("day")
        assert "date_trunc(" not in sql.lower()

    def test_date_trunc_minute_uses_safe_epoch(self):
        sql = self._query("minute")
        assert "2000-01-01" in sql

    def test_date_trunc_day_uses_zero_epoch(self):
        sql = self._query("day")
        assert ", 0," in sql or ", 0)" in sql


class TestTranslateRawDateTrunc:
    """_translate_raw_date_trunc rewrites date_trunc() in a raw SQL string."""

    def test_day_grain(self):
        sql = "SELECT date_trunc('day', event_time) FROM events"
        assert _translate_raw_date_trunc(sql) == (
            "SELECT DATEADD(day, DATEDIFF(day, 0, event_time), 0) FROM events"
        )

    def test_month_grain(self):
        sql = "SELECT date_trunc('month', ts) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert result == "SELECT DATEADD(month, DATEDIFF(month, 0, ts), 0) FROM t"

    def test_year_grain(self):
        sql = "SELECT date_trunc('year', ts) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert result == "SELECT DATEADD(year, DATEDIFF(year, 0, ts), 0) FROM t"

    def test_week_grain(self):
        sql = "SELECT date_trunc('week', ts) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert result == "SELECT DATEADD(week, DATEDIFF(week, 0, ts), 0) FROM t"

    def test_quarter_grain(self):
        sql = "SELECT date_trunc('quarter', ts) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert result == "SELECT DATEADD(quarter, DATEDIFF(quarter, 0, ts), 0) FROM t"

    def test_hour_grain_uses_safe_epoch(self):
        """Sub-day grains must use '2000-01-01' to avoid DATEDIFF integer overflow."""
        sql = "SELECT date_trunc('hour', ts) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert result == (
            "SELECT DATEADD(hour, DATEDIFF(hour, '2000-01-01', ts), '2000-01-01') FROM logs"
        )

    def test_minute_grain_uses_safe_epoch(self):
        sql = "SELECT date_trunc('minute', ts) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(minute" in result
        assert "'2000-01-01'" in result

    def test_second_grain_uses_safe_epoch(self):
        sql = "SELECT date_trunc('second', ts) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(second" in result
        assert "'2000-01-01'" in result

    def test_milliseconds_grain_maps_to_millisecond(self):
        """'milliseconds' (plural) maps to T-SQL 'millisecond' (singular)."""
        sql = "SELECT date_trunc('milliseconds', ts) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(millisecond," in result
        assert "'2000-01-01'" in result

    def test_microseconds_grain_maps_to_millisecond(self):
        """'microseconds' maps to 'millisecond' — T-SQL has no microsecond datepart."""
        sql = "SELECT date_trunc('microseconds', ts) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(millisecond," in result

    def test_grain_capitalisation_normalised(self):
        """Grain is case-insensitive — 'Day' and 'DAY' must work like 'day'."""
        for grain in ("Day", "DAY", "dAy"):
            sql = f"SELECT date_trunc('{grain}', ts) FROM t"
            result = _translate_raw_date_trunc(sql)
            assert "DATEADD(day" in result, f"Failed for grain={grain!r}: {result}"

    def test_nested_parens_in_expr(self):
        """Expressions containing parentheses must be preserved intact."""
        sql = "SELECT date_trunc('day', CAST(ts AS DATETIME)) FROM logs"
        result = _translate_raw_date_trunc(sql)
        assert result == (
            "SELECT DATEADD(day, DATEDIFF(day, 0, CAST(ts AS DATETIME)), 0) FROM logs"
        )

    def test_multiple_calls_in_one_query(self):
        """Every occurrence is translated."""
        sql = "SELECT date_trunc('day', a), date_trunc('hour', b) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(day" in result
        assert "DATEADD(hour" in result
        assert "date_trunc(" not in result.lower()

    def test_unknown_grain_left_unchanged(self):
        """An unrecognised grain is left as-is so Kusto surfaces the error."""
        sql = "SELECT date_trunc('decade', ts) FROM t"
        assert _translate_raw_date_trunc(sql) == sql

    def test_sql_without_date_trunc_returned_unchanged(self):
        sql = "SELECT id, name FROM users WHERE active = 1"
        assert _translate_raw_date_trunc(sql) == sql

    def test_nested_date_trunc_outer_only_translated(self):
        """Nested date_trunc — only the outer call is translated; inner remains as-is.

        This is a known limitation of the single-pass scanner. In practice, nesting
        date_trunc calls is not a valid SQL pattern in analytics SQL.
        """
        sql = "SELECT date_trunc('day', date_trunc('hour', ts)) FROM t"
        result = _translate_raw_date_trunc(sql)
        assert "DATEADD(day" in result
        assert "date_trunc('hour', ts)" in result


class TestKustoSqlDialectDoExecute:
    """KustoSqlHttpsDialect.do_execute must translate date_trunc before the cursor sees the SQL."""

    def _dialect(self) -> KustoSqlHttpsDialect:
        return KustoSqlHttpsDialect()

    def test_date_trunc_translated_before_cursor(self):
        cursor = MagicMock()
        self._dialect().do_execute(
            cursor, "SELECT date_trunc('day', ts) FROM t", [], context=None
        )
        cursor.execute.assert_called_once()
        received_sql: str = cursor.execute.call_args[0][0]
        assert "DATEADD(day" in received_sql
        assert "date_trunc(" not in received_sql.lower()

    def test_parameters_forwarded_unchanged(self):
        cursor = MagicMock()
        params = {"id": 42}
        self._dialect().do_execute(cursor, "SELECT id FROM t", params, context=None)
        cursor.execute.assert_called_once_with("SELECT id FROM t", params)

    def test_sql_without_date_trunc_forwarded_unchanged(self):
        cursor = MagicMock()
        sql = "SELECT id, name FROM users WHERE active = 1"
        self._dialect().do_execute(cursor, sql, [], context=None)
        cursor.execute.assert_called_once_with(sql, [])
