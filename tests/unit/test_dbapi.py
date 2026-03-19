"""Unit tests for DBAPI Cursor correctness and performance."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from sqlalchemy_kusto.dbapi import Cursor


def _cursor(rows: list) -> Cursor:
    """Return a Cursor with _results pre-populated, bypassing network calls."""
    cursor = Cursor(MagicMock(), "test_db")
    cursor._results = list(rows)
    cursor.description = []
    return cursor


# ---------------------------------------------------------------------------
# rowcount
# ---------------------------------------------------------------------------

class TestRowcount:
    def test_returns_total_row_count(self):
        cursor = _cursor([(1,), (2,), (3,)])
        assert cursor.rowcount == 3

    def test_unchanged_after_fetchone(self):
        """rowcount must reflect the total, not the remaining rows."""
        cursor = _cursor([(1,), (2,), (3,)])
        cursor.fetchone()
        assert cursor.rowcount == 3

    def test_rowcount_does_not_copy_results_list(self):
        """rowcount must use len(), not list() — list() is O(n) and unnecessary."""
        results = [(i,) for i in range(5)]
        cursor = _cursor(results)

        # Poison _results with a spy that tracks list() coercion.
        # After fix: only len() is called (O(1)); list() must NOT be called.
        original = cursor._results

        class ListSpy(list):
            def __init__(self, src):
                super().__init__(src)
                self.list_copy_calls = 0

            def __iter__(self):
                # list() construction calls __iter__; track that
                self.list_copy_calls += 1
                return super().__iter__()

        spy = ListSpy(original)
        cursor._results = spy
        spy.list_copy_calls = 0  # reset after __init__ iteration

        _ = cursor.rowcount

        # Before fix: list(self._results) triggers __iter__ → list_copy_calls == 1
        # After fix:  len(self._results) does NOT trigger __iter__ → 0
        assert spy.list_copy_calls == 0, (
            "rowcount called list() on _results — this is O(n) and must be removed"
        )


# ---------------------------------------------------------------------------
# fetchone
# ---------------------------------------------------------------------------

class TestFetchone:
    def test_returns_rows_in_order(self):
        cursor = _cursor([(1, "a"), (2, "b"), (3, "c")])
        assert cursor.fetchone() == (1, "a")
        assert cursor.fetchone() == (2, "b")
        assert cursor.fetchone() == (3, "c")

    def test_returns_none_when_exhausted(self):
        cursor = _cursor([(1,)])
        cursor.fetchone()
        assert cursor.fetchone() is None

    def test_sequential_calls_advance_position(self):
        cursor = _cursor([(i,) for i in range(10)])
        for i in range(10):
            assert cursor.fetchone() == (i,)
        assert cursor.fetchone() is None

    def test_fetchone_does_not_call_rowcount_property(self):
        """fetchone must not go through the rowcount property (avoids O(n) path)."""
        cursor = _cursor([(1,), (2,)])

        call_count = 0
        original_prop = type(cursor).rowcount.fget  # noqa: B009

        def counting_rowcount(self):
            nonlocal call_count
            call_count += 1
            return original_prop(self)

        type(cursor).rowcount = property(counting_rowcount)
        try:
            cursor.fetchone()
            cursor.fetchone()
        finally:
            # Restore original property so other tests are unaffected
            type(cursor).rowcount = property(original_prop)

        # Before fix: fetchone calls self.rowcount → call_count == 2
        # After fix:  fetchone uses len(self._results) directly → 0
        assert call_count == 0, (
            f"fetchone called rowcount {call_count} time(s) — must use len() directly"
        )


# ---------------------------------------------------------------------------
# Iteration (__iter__ / __next__)
# ---------------------------------------------------------------------------

class TestIteration:
    def test_for_loop_returns_all_rows(self):
        """for-loop over cursor raises TypeError before fix: next(list) is invalid."""
        cursor = _cursor([(1,), (2,), (3,)])
        rows = list(cursor)  # TypeError before fix
        assert rows == [(1,), (2,), (3,)]

    def test_iteration_after_partial_fetch(self):
        """Iteration must continue from current_item_index, not from 0."""
        cursor = _cursor([(1,), (2,), (3,), (4,)])
        cursor.fetchone()  # advance index to 1
        rows = list(cursor)
        assert rows == [(2,), (3,), (4,)]

    def test_exhausted_cursor_yields_empty(self):
        cursor = _cursor([(1,)])
        cursor.fetchone()
        assert list(cursor) == []

    def test_stop_iteration_raised_when_exhausted(self):
        cursor = _cursor([(1,)])
        next(cursor)  # consume the only row
        with pytest.raises(StopIteration):
            next(cursor)


# ---------------------------------------------------------------------------
# SQL injection in get_columns
# ---------------------------------------------------------------------------

class TestGetColumnsEscaping:
    """table_name must be sanitised before being embedded in KQL queries."""

    @staticmethod
    def _dialect():
        from sqlalchemy_kusto.dialect_sql import KustoSqlHttpsDialect
        return KustoSqlHttpsDialect()

    def test_double_quote_in_table_name_is_escaped(self):
        dialect = self._dialect()

        # table found → rowcount == 1; schema returns empty columns
        table_result = MagicMock()
        table_result.rowcount = 1

        schema_row = MagicMock()
        schema_row.Schema = json.dumps({"OrderedColumns": []})
        schema_result = MagicMock()
        schema_result.__iter__ = MagicMock(return_value=iter([schema_row]))

        conn = MagicMock()
        conn.execute.side_effect = [table_result, schema_result]

        dialect.get_columns(conn, 'my"table')

        first_query: str = conn.execute.call_args_list[0][0][0]

        # Before fix: raw quote is present → KQL string boundary broken
        assert 'my"table' not in first_query, (
            "Unescaped double-quote found in KQL query — table_name is not sanitised"
        )
        # After fix: quote is escaped
        assert 'my\\"table' in first_query
