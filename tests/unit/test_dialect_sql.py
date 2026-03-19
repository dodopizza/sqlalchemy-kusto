"""Unit tests for KustoSql dialect — ORDER BY and date_trunc compilation."""
from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, func, select

engine = create_engine("kustosql+https://localhost/testdb")


def _compile(query) -> str:
    return str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", " ")


# ---------------------------------------------------------------------------
# ORDER BY without LIMIT
# ---------------------------------------------------------------------------

class TestOrderByWithoutLimit:
    def test_order_by_without_limit_inserts_top_fallback(self):
        """SELECT … ORDER BY without LIMIT must emit TOP <max_top_n> for Kusto."""
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc())
        sql = _compile(query)

        # Before fix: no TOP → Kusto may reject or silently ignore ORDER BY
        # After fix: TOP max_top_n appears before the column list
        assert "TOP" in sql, "Expected TOP fallback when ORDER BY has no LIMIT"
        assert "ORDER BY" in sql

    def test_order_by_with_limit_uses_limit_not_double_top(self):
        """When both LIMIT and ORDER BY are present, only one TOP must appear."""
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc()).limit(10)
        sql = _compile(query)

        assert sql.count("TOP") == 1, "Expected exactly one TOP clause"
        assert "TOP 10" in sql

    def test_select_without_order_by_no_extra_top(self):
        """A plain SELECT without ORDER BY must not gain a spurious TOP."""
        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score])
        sql = _compile(query)

        assert "TOP" not in sql

    def test_top_fallback_value_is_configurable(self):
        """The max_top_n fallback must come from the dialect, not be hard-coded."""
        custom_engine = create_engine(
            "kustosql+https://localhost/testdb",
            connect_args={},  # required by some SA versions
        )
        # Reach in and change the dialect's max_top_n
        custom_engine.dialect.max_top_n = 999

        t = Table("events", MetaData(), Column("score", Integer))
        query = select([t.c.score]).order_by(t.c.score.desc())
        sql = str(query.compile(custom_engine, compile_kwargs={"literal_binds": True}))

        assert "TOP 999" in sql


# ---------------------------------------------------------------------------
# date_trunc → DATEADD / DATEDIFF
# ---------------------------------------------------------------------------

class TestDateTrunc:
    def _query(self, grain: str) -> str:
        t = Table("events", MetaData(), Column("ts", DateTime))
        query = select([func.date_trunc(grain, t.c.ts)])
        return _compile(query)

    def test_date_trunc_day(self):
        """date_trunc('day', col) → DATEADD(day, DATEDIFF(day, …, col), …)"""
        sql = self._query("day")
        assert "DATEADD(day" in sql, f"Got: {sql}"
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
        """date_trunc must NOT appear as a function call — Kusto T-SQL doesn't know it."""
        sql = self._query("day")
        # SQLAlchemy produces an alias like "AS date_trunc_1" — that's fine.
        # What must NOT appear is the function call form: date_trunc(
        assert "date_trunc(" not in sql.lower(), (
            f"date_trunc was passed through as a function call — needs T-SQL translation. Got: {sql}"
        )

    def test_date_trunc_minute_uses_safe_epoch(self):
        """Minute/second grains must use a recent epoch to avoid DATEDIFF overflow."""
        sql = self._query("minute")
        # epoch 0 (1900-01-01) would overflow DATEDIFF(minute, 0, ...) for modern dates
        # After fix: uses '2000-01-01' as the epoch
        assert "2000-01-01" in sql, (
            f"Minute grain should use safe epoch '2000-01-01', got: {sql}"
        )

    def test_date_trunc_day_uses_zero_epoch(self):
        """Day/month/year grains can safely use epoch 0 (no overflow risk)."""
        sql = self._query("day")
        assert ", 0," in sql or ", 0)" in sql, (
            f"Day grain should use epoch 0, got: {sql}"
        )
