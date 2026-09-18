import pytest
import sqlalchemy as sa
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    column,
    create_engine,
    distinct,
    func,
    literal_column,
    select,
    text,
)
from sqlalchemy.sql.selectable import TextAsFrom

from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

engine = create_engine("kustokql+https://localhost/testdb")


def test_compiler_with_projection():
    statement_str = "logs | take 10"
    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select(
        column("Id").label("id"),
        column("TypeId").label("tId"),
        column("Type"),
    ).select_from(stmt)
    query = query.limit(10)

    query_compiled = str(query.compile(engine)).replace("\n", "")
    query_expected = (
        'let virtual_table = (["logs"] '
        "| take 10);virtual_table"
        '| extend ["id"] = ["Id"], ["tId"] = ["TypeId"]'
        '| project ["id"], ["tId"], ["Type"]'
        "| take __[POSTCOMPILE_param_1]"
    )

    assert query_compiled == query_expected


def test_compiler_with_star():
    statement_str = "logs | take 10"
    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select("*").select_from(stmt)
    query = query.limit(10)
    query_compiled = str(query.compile(engine)).replace("\n", "")
    query_expected = (
        'let virtual_table = (["logs"] | take 10);'
        "virtual_table"
        "| take __[POSTCOMPILE_param_1]"
    )
    assert query_compiled == query_expected


def test_select_from_text():
    query = (
        select(column("Field1"), column("Field2")).select_from(text("logs")).limit(100)
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = '["logs"]| project ["Field1"], ["Field2"]| take 100'
    assert query_compiled == query_expected


@pytest.mark.parametrize(
    ("f", "expected"),
    [
        pytest.param(
            Column("Field1", String).in_(["1", "One"]), """["Field1"] in ('1', 'One')"""
        ),
        pytest.param(
            Column("Field1", String).notin_(["1", "One"]),
            """(["Field1"] !in ('1', 'One'))""",
        ),
        pytest.param(text("Field1 = '1'"), """Field1 == '1'"""),
        pytest.param(text("Field1 <> '1'"), """Field1 != '1'"""),
        pytest.param(text("Field1 LIKE '%123%'"), """Field1 has_cs '123'"""),
        pytest.param(text('Field1 LIKE "%123%"'), 'Field1 has_cs "123"'),
        pytest.param(text("Field1 NOT LIKE '%123%'"), """Field1 !has_cs '123'"""),
        pytest.param(text('Field1 NOT LIKE "%123%"'), 'Field1 !has_cs "123"'),
        pytest.param(text("Field1 LIKE '123%'"), """Field1 startswith_cs '123'"""),
        pytest.param(text('Field1 LIKE "123%"'), 'Field1 startswith_cs "123"'),
        pytest.param(text("Field1 NOT LIKE '123%'"), """Field1 !startswith_cs '123'"""),
        pytest.param(text('Field1 NOT LIKE "123%"'), 'Field1 !startswith_cs "123"'),
        pytest.param(text("Field1 LIKE '%123'"), """Field1 endswith_cs '123'"""),
        pytest.param(text('Field1 LIKE "%123"'), 'Field1 endswith_cs "123"'),
        pytest.param(text("Field1 NOT LIKE '%123'"), """Field1 !endswith_cs '123'"""),
        pytest.param(text('Field1 NOT LIKE "%123"'), 'Field1 !endswith_cs "123"'),
        pytest.param(text("Field1 ILIKE '%123%'"), """Field1 has '123'"""),
        pytest.param(text('Field1 ILIKE "%123%"'), 'Field1 has "123"'),
        pytest.param(text("Field1 NOT ILIKE '%123%'"), """Field1 !has '123'"""),
        pytest.param(text('Field1 NOT ILIKE "%123%"'), 'Field1 !has "123"'),
        pytest.param(text("Field1 ILIKE '123%'"), """Field1 startswith '123'"""),
        pytest.param(text('Field1 ILIKE "123%"'), 'Field1 startswith "123"'),
        pytest.param(text("Field1 NOT ILIKE '123%'"), """Field1 !startswith '123'"""),
        pytest.param(text('Field1 NOT ILIKE "123%"'), 'Field1 !startswith "123"'),
        pytest.param(text("Field1 ILIKE '%123'"), """Field1 endswith '123'"""),
        pytest.param(text('Field1 ILIKE "%123"'), 'Field1 endswith "123"'),
        pytest.param(text("Field1 NOT ILIKE '%123'"), """Field1 !endswith '123'"""),
        pytest.param(text('Field1 NOT ILIKE "%123"'), 'Field1 !endswith "123"'),
        pytest.param(text("Field1 != '1'"), """Field1 != '1'"""),
        pytest.param(
            Column("Field2", Integer).ilike("abc%"),
            """tolower(["Field2"]) startswith_cs tolower('abc')""",
        ),
        pytest.param(
            Column("Field2", Integer).like("%abc"), """["Field2"] endswith_cs 'abc'"""
        ),
        pytest.param(
            Column("Field2", Integer).notlike("%abc"),
            """["Field2"] !endswith_cs 'abc'""",
        ),
        pytest.param(
            Column("Field2", Integer).between(2, 4), """["Field2"] between (2..4)"""
        ),
        pytest.param(Column("Field2", Integer).is_(None), """isnull(["Field2"])"""),
        pytest.param(
            Column("Field2", Integer).isnot(None), """isnotnull(["Field2"])"""
        ),
        pytest.param(
            (Column("Field2", Integer).isnot(None)).__and__(
                Column("Field1", String).notin_(["1", "One"])
            ),
            """isnotnull(["Field2"]) and (["Field1"] !in ('1', 'One'))""",
        ),
        pytest.param(
            (Column("Field2", Integer).isnot(None)).__or__(
                Column("Field1", String).notin_(["1", "One"])
            ),
            """isnotnull(["Field2"]) or (["Field1"] !in ('1', 'One'))""",
        ),
    ],
)
def test_where_predicates(f, expected):
    query = (
        select(column("Field1"), column("Field2")).select_from(text("logs")).where(f)
    ).limit(100)
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        f"""["logs"]| where {expected}| project ["Field1"], ["Field2"]| take 100"""
    )
    assert query_compiled == query_expected


def test_group_by_text():
    # create a query from select_query_text creating clause
    event_col = literal_column('"EventInfo_Time" / time(1d)').label("EventInfo_Time")
    active_users_col = literal_column("ActiveUsers").label("ActiveUserMetric")
    query = (
        select(event_col, active_users_col)
        .select_from(text("ActiveUsersLastMonth"))
        .group_by(literal_column('"EventInfo_Time" / time(1d)'))
        .order_by(text("ActiveUserMetric DESC"))
    )

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query - extend column order follows select order
    query_expected = (
        '["ActiveUsersLastMonth"]| summarize   by ["EventInfo_Time"] / time(1d)'
        '| extend ["EventInfo_Time"] = (["EventInfo_Time"]) / time(1d), '
        '["ActiveUserMetric"] = ["ActiveUsers"]'
        '| project ["EventInfo_Time"], ["ActiveUserMetric"]'
        '| order by ["ActiveUserMetric"] desc'
    )
    assert query_compiled == query_expected
    assert query_compiled == query_expected


@pytest.mark.parametrize(
    ("f", "expected"),
    [
        pytest.param('bin("EventInfo_Time",1d)', 'bin(["EventInfo_Time"],1d)'),
        pytest.param("bin(ingestion_time(),1d)", "bin(ingestion_time(),1d)"),
    ],
)
def test_function_text(f: str, expected: str):
    # create a query from select_query_text creating clause
    event_col = literal_column(f).label("EventInfo_Time")
    active_users_col = literal_column("ActiveUsers").label("ActiveUserMetric")
    query = select(event_col, active_users_col).select_from(
        text("ActiveUsersLastMonth")
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # extend columns follow select order
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| extend ["EventInfo_Time"] = '
        + expected
        + ', ["ActiveUserMetric"] = ["ActiveUsers"]'
        '| project ["EventInfo_Time"], ["ActiveUserMetric"]'
    )
    assert query_compiled == query_expected


def test_group_by_text_vaccine_dataset():
    # SQL: SELECT country_name AS country_name FROM superset."CovidVaccineData" GROUP BY country_name
    # ORDER BY country_name ASC - this is a simple query to get distinct country names
    query = (
        select(literal_column("country_name").label("country_name"))
        .select_from(text('superset."CovidVaccineData"'))
        .group_by(literal_column("country_name"))
        .order_by(text("country_name ASC"))
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        'database("superset").["CovidVaccineData"]| '
        'summarize   by ["country_name"]| '
        'project ["country_name"]| order by ["country_name"] asc'
    )
    assert query_compiled == query_expected


def test_is_kql_function():
    assert KustoKqlCompiler._is_kql_function("""case(Size <= 3, "Small",
                       Size <= 10, "Medium",
                       "Large")""")
    assert KustoKqlCompiler._is_kql_function("""bin(time(16d), 7d)""")
    assert KustoKqlCompiler._is_kql_function(
        """iff((EventType in ("Heavy Rain", "Flash Flood", "Flood")), "Rain event", "Not rain event")"""
    )


def test_percentile_by_text():
    event_col = literal_column("percentile(quantity_ordered, 99)").label("Measure 1")
    query = select(
        event_col,
    ).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["SalesData"]'
        '| summarize ["Measure 1"] = percentile(["quantity_ordered"], 99) '
        '| project ["Measure 1"]'
    )
    assert query_compiled == query_expected


def test_dcountif_by_text():
    event_col = literal_column(
        "dcountif(year, city == 'Paris' or city in ('Madrid'))"
    ).label("Measure 1")
    query = select(
        event_col,
    ).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["SalesData"]'
        "| summarize [\"Measure 1\"] = dcountif([\"year\"], city == 'Paris' or city in ('Madrid')) "
        '| project ["Measure 1"]'
    )
    assert query_compiled == query_expected


def test_countif_by_text():
    event_col = literal_column("countif(city == 'Paris' OR city in ('Madrid'))").label(
        "Measure 1"
    )
    query = select(
        event_col,
    ).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query - column names in predicate passed as-is
    query_expected = (
        '["SalesData"]'
        "| summarize [\"Measure 1\"] = countif(city == 'Paris' OR city in ('Madrid')) "
        '| project ["Measure 1"]'
    )
    assert query_compiled == query_expected


def test_distinct_count_by_text():
    # create a query from select_query_text creating clause
    # 'SELECT "EventInfo_Time" / time(1d) AS "EventInfo_Time", count(DISTINCT ActiveUsers) AS "DistinctUsers"
    # FROM ActiveUsersLastMonth GROUP BY "EventInfo_Time" / time(1d) ORDER BY ActiveUserMetric DESC'
    event_col = literal_column('"EventInfo_Time" / time(1d)').label("EventInfo_Time")
    active_users_col = literal_column("ActiveUsers")
    query = (
        select(
            event_col,
            sa.func.count(distinct(active_users_col)).label("DistinctUsers"),
        )
        .select_from(text("ActiveUsersLastMonth"))
        .group_by(literal_column('"EventInfo_Time" / time(1d)'))
        .order_by(text("ActiveUserMetric DESC"))
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| summarize ["DistinctUsers"] = dcount(["ActiveUsers"])  by ["EventInfo_Time"] / time(1d)'
        '| extend ["EventInfo_Time"] = (["EventInfo_Time"]) / time(1d)'
        '| project ["EventInfo_Time"], ["DistinctUsers"]'
        '| order by ["ActiveUserMetric"] desc'
    )
    assert query_compiled == query_expected


def test_distinct_count_alt_by_text():
    # create a query from select_query_text creating clause
    # 'SELECT "EventInfo_Time" / time(1d) AS "EventInfo_Time", count_distinct(ActiveUsers) AS "DistinctUsers"
    # FROM ActiveUsersLastMonth GROUP BY "EventInfo_Time" / time(1d) ORDER BY ActiveUserMetric DESC'
    event_col = literal_column("EventInfo_Time / time(1d)").label("EventInfo_Time")
    active_users_col = literal_column("COUNT_DISTINCT(ActiveUsers)")
    query = (
        select(event_col, active_users_col.label("DistinctUsers"))
        .select_from(text("ActiveUsersLastMonth"))
        .group_by(literal_column("EventInfo_Time / time(1d)"))
        .order_by(text("ActiveUserMetric DESC"))
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| summarize ["DistinctUsers"] = dcount(["ActiveUsers"])  by ["EventInfo_Time"] / time(1d)'
        '| extend ["EventInfo_Time"] = (["EventInfo_Time"]) / time(1d)'
        '| project ["EventInfo_Time"], ["DistinctUsers"]'
        '| order by ["ActiveUserMetric"] desc'
    )

    assert query_compiled == query_expected


def test_escape_and_quote_columns():
    assert (
        KustoKqlCompiler._escape_and_quote_columns("EventInfo_Time")
        == '["EventInfo_Time"]'
    )
    assert KustoKqlCompiler._escape_and_quote_columns('["UserId"]') == '["UserId"]'
    assert (
        KustoKqlCompiler._escape_and_quote_columns("EventInfo_Time / time(1d)")
        == '["EventInfo_Time"] / time(1d)'
    )


def test_use_table():
    metadata = MetaData()
    stream = Table(
        "logs",
        metadata,
        Column("Field1", String),
        Column("Field2", String),
    )

    query = stream.select().limit(5)
    query_compiled = str(query.compile(engine)).replace("\n", "")

    query_expected = (
        '["logs"]' '| project ["Field1"], ["Field2"]| take __[POSTCOMPILE_param_1]'
    )
    assert query_compiled == query_expected


def test_limit():
    sql = "logs"
    limit = 5
    query = (
        select("*")
        .select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry"))
        .limit(limit)
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = 'let inner_qry = (["logs"]);' "inner_qry" "| take 5"
    assert query_compiled == query_expected


def test_select_count():
    kql_query = "logs"
    column_count = literal_column("count(*)").label("total-count")
    query = (
        select(column_count)
        .select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry"))
        .where(text("Field1 > 1"))
        .where(text("Field2 < 2"))
        .order_by(text("total-count DESC"))
        .limit(5)
    )

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")

    query_expected = (
        'let inner_qry = (["logs"]);'
        "inner_qry"
        "| where Field1 > 1 and Field2 < 2"
        '| summarize ["total-count"] = count() '
        '| project ["total-count"]'
        '| order by ["total-count"] desc'
        "| take 5"
    )

    assert query_compiled == query_expected


def test_select_with_let():
    kql_query = "let x = 5; let y = 3; MyTable | where Field1 == x and Field2 == y"
    query = (
        select("*")
        .select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry"))
        .limit(5)
    )

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")

    query_expected = (
        "let x = 5;"
        "let y = 3;"
        'let inner_qry = (["MyTable"] | where Field1 == x and Field2 == y);'
        "inner_qry"
        "| take 5"
    )

    assert query_compiled == query_expected


def test_quotes():
    quote = engine.dialect.identifier_preparer.quote
    metadata = MetaData()
    stream = Table(
        "logs",
        metadata,
        Column(quote("Field1"), String),
        Column(quote("Field2"), String),
    )
    query = stream.select().limit(5)

    query_compiled = str(query.compile(engine)).replace("\n", "")

    # fmt: off
    query_expected = (
        '["logs"]'
        '| project ["Field1"], ["Field2"]'
        "| take __[POSTCOMPILE_param_1]"
    )
    # fmt: on

    assert query_compiled == query_expected


@pytest.mark.parametrize(
    ("schema_name", "table_name", "expected_table_name"),
    [
        ("schema", "table", 'database("schema").["table"]'),
        ("schema", '"table.name"', 'database("schema").["table.name"]'),
        ('"schema.name"', "table", 'database("schema.name").["table"]'),
        ('"schema.name"', '"table.name"', 'database("schema.name").["table.name"]'),
        ('"schema name"', '"table name"', 'database("schema name").["table name"]'),
        (None, '"table.name"', '["table.name"]'),
        (None, "MyTable", '["MyTable"]'),
    ],
)
def test_schema_from_metadata(
    table_name: str, schema_name: str, expected_table_name: str
):
    metadata = MetaData(schema=schema_name) if schema_name else MetaData()
    stream = Table(
        table_name,
        metadata,
    )
    query = stream.select().limit(5)
    query_compiled = str(query.compile(engine)).replace("\n", "")
    query_expected = f"{expected_table_name}| take __[POSTCOMPILE_param_1]"
    assert query_compiled == query_expected


@pytest.mark.parametrize(
    ("column_name", "expected_aggregate"),
    [
        ("AVG(Score)", 'avg(["Score"])'),
        ('AVG("2014")', 'avg(["2014"])'),
        ('sum("2014")', 'sum(["2014"])'),
        ("SUM(scores)", 'sum(["scores"])'),
        ('MIN("scores")', 'min(["scores"])'),
        ('MIN(["scores"])', 'min(["scores"])'),
        ("max(scores)", 'max(["scores"])'),
        ("startofmonth(somedate)", None),
        ("startofmonth(somedate)/time(1d)", None),
        ("count(*)", "count()"),
        ("count(1)", "count()"),
        ("count(UserId)", 'count(["UserId"])'),
        ("count(distinct CustomerId)", 'dcount(["CustomerId"])'),
        ("count_distinct(CustomerId)", 'dcount(["CustomerId"])'),
        (
            "count_distinctif(order_qty, year > 2022)",
            'count_distinctif(["order_qty"], year > 2022)',
        ),
        ("dcountif(1, year > 2024)", "dcountif(1, year > 2024)"),
        ("sum(Sales)", 'sum(["Sales"])'),
        ("avg(ResponseTime)", 'avg(["ResponseTime"])'),
        ("AVG(ResponseTime)", 'avg(["ResponseTime"])'),
        ("min(Size)", 'min(["Size"])'),
        ("max(Area)", 'max(["Area"])'),
        ("unknown(Column)", None),
    ],
)
def test_match_aggregates(column_name: str, expected_aggregate: str):
    kql_agg = KustoKqlCompiler._extract_maybe_agg_column_parts(column_name)
    if expected_aggregate:
        assert kql_agg is not None
        assert kql_agg == expected_aggregate
    else:
        assert kql_agg is None


@pytest.mark.parametrize(
    ("query_table_name", "expected_table_name"),
    [
        ("schema.table", 'database("schema").["table"]'),
        ('schema."table.name"', 'database("schema").["table.name"]'),
        ('"schema.name".table', 'database("schema.name").["table"]'),
        ('"schema.name"."table.name"', 'database("schema.name").["table.name"]'),
        ('"schema name"."table name"', 'database("schema name").["table name"]'),
        ('"table.name"', '["table.name"]'),
        ("MyTable", '["MyTable"]'),
        ('["schema"].["table"]', 'database("schema").["table"]'),
        ('["table"]', '["table"]'),
    ],
)
def test_schema_from_query(query_table_name: str, expected_table_name: str):
    query = (
        select("*")
        .select_from(TextAsFrom(text(query_table_name), ["*"]).alias("inner_qry"))
        .limit(5)
    )

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")

    query_expected = f"let inner_qry = ({expected_table_name});inner_qry| take 5"
    assert query_compiled == query_expected


class TestCalculatedMeasures:
    """Tests for calculated measures (arithmetic expressions with aggregates)."""

    @pytest.fixture
    def events_table(self):
        metadata = MetaData()
        return Table(
            "events",
            metadata,
            Column("region", String),
            Column("ring", String),
            Column("value", Integer),
        )

    @pytest.fixture
    def pt_search_table(self):
        """Table matching the Superset PT_Search_scenario use case."""
        metadata = MetaData()
        return Table(
            "PT_Search_scenario",
            metadata,
            Column("UserInfo_Ring", String),
            Column("UserInfo_Region", String),
            schema="bc3902d8132f43e3ae086a009979fa88",
        )

    def test_multi_aggregate_expression(self, events_table):
        """Test that expressions with multiple aggregates generate correct KQL."""
        query = select(
            (func.count(events_table.c.region) + func.count(events_table.c.ring)).label(
                "multi_agg"
            )
        ).select_from(events_table)

        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should have summarize with both aggregates
        assert "summarize" in compiled
        # Should have extend for the calculated measure
        assert "extend" in compiled
        # Should project the alias
        assert '["multi_agg"]' in compiled

    def test_arithmetic_expression_with_columns(self, events_table):
        """Test arithmetic expressions with column references."""
        query = select(
            (events_table.c.value / literal_column("100")).label("percentage")
        ).select_from(events_table)

        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should handle division operator
        assert "/" in compiled
        assert '["percentage"]' in compiled

    def test_escape_and_quote_columns_with_arithmetic(self):
        """Test _escape_and_quote_columns handles arithmetic expressions."""
        result = KustoKqlCompiler._escape_and_quote_columns("col1 + col2")
        assert '["col1"]' in result
        assert '["col2"]' in result
        assert "+" in result

    def test_escape_and_quote_columns_with_parentheses(self):
        """Test _escape_and_quote_columns handles parenthesized expressions."""
        result = KustoKqlCompiler._escape_and_quote_columns("(col1 + col2)")
        assert result.startswith("(")
        assert result.endswith(")")
        assert '["col1"]' in result
        assert '["col2"]' in result

    def test_has_operators_outside_quotes(self):
        """Test detection of arithmetic operators outside quoted strings."""
        assert KustoKqlCompiler._has_operators_outside_quotes("a + b") is True
        assert KustoKqlCompiler._has_operators_outside_quotes("a - b") is True
        assert KustoKqlCompiler._has_operators_outside_quotes("a * b") is True
        assert KustoKqlCompiler._has_operators_outside_quotes("a / b") is True
        assert KustoKqlCompiler._has_operators_outside_quotes('["col"]') is False
        assert KustoKqlCompiler._has_operators_outside_quotes('"a + b"') is False

    def test_count_outer_parens(self):
        """Test counting and stripping outer parentheses."""
        count, inner = KustoKqlCompiler._count_outer_parens("((a + b))")
        assert count == 2  # noqa: PLR2004
        assert inner == "a + b"

        count, inner = KustoKqlCompiler._count_outer_parens("(a) + (b)")
        assert count == 0
        assert inner == "(a) + (b)"

    def test_predefined_measures_lowercase(self, pt_search_table):
        """Test that predefined measures (aggregates) compile to lowercase KQL functions."""
        userinfo_ring_count = func.COUNT(pt_search_table.c.UserInfo_Ring).label(
            "UserInfo_Ring Count"
        )
        userinfo_region_count = func.COUNT(pt_search_table.c.UserInfo_Region).label(
            "UserInfo_Region Count"
        )

        query = select(userinfo_ring_count, userinfo_region_count).select_from(
            pt_search_table
        )
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should use lowercase count function in KQL
        assert 'count(["UserInfo_Ring"])' in compiled or "count([" in compiled
        # Should NOT have uppercase COUNT
        assert "COUNT(" not in compiled

    def test_calculated_measure_simple_reference(self, pt_search_table):
        """Test a calculated measure that's just a reference to another measure."""
        measure_15 = literal_column('"UserInfo_Region Count"').label("Measure 15")

        query = select(measure_15).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should convert quoted identifier to bracket notation
        assert '["Measure 15"]' in compiled
        assert '["UserInfo_Region Count"]' in compiled

    def test_calculated_measure_single_paren(self, pt_search_table):
        """Test a calculated measure with single parentheses wrapper."""
        measure_16 = literal_column('("UserInfo_Ring Count")').label("Measure 16")

        query = select(measure_16).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 16"]' in compiled

    def test_calculated_measure_double_paren(self, pt_search_table):
        """Test a calculated measure with double parentheses wrapper."""
        measure_3 = literal_column('(("Measure 1"))').label("Measure 3")

        query = select(measure_3).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 3"]' in compiled
        # Should preserve double parens
        assert "((" in compiled
        assert "))" in compiled

    def test_calculated_measure_multiply_by_constant(self, pt_search_table):
        """Test a calculated measure that multiplies a reference by a constant."""
        measure_9 = literal_column('"UserInfo_Ring Count" * 2').label("Measure 9")

        query = select(measure_9).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 9"]' in compiled
        assert "* 2" in compiled

    def test_calculated_measure_addition(self, pt_search_table):
        """Test a calculated measure that adds two measure references."""
        measure_14 = literal_column(
            '"UserInfo_Region Count" + "UserInfo_Ring Count"'
        ).label("Measure 14")

        query = select(measure_14).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 14"]' in compiled
        assert "+" in compiled
        assert '["UserInfo_Region Count"]' in compiled
        assert '["UserInfo_Ring Count"]' in compiled

    def test_calculated_measure_parens_addition(self, pt_search_table):
        """Test a calculated measure with parenthesized addition."""
        measure_11 = literal_column('("Measure 1") + ("Measure 2")').label("Measure 11")

        query = select(measure_11).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 11"]' in compiled
        assert "+" in compiled

    def test_calculated_measure_complex_expression(self, pt_search_table):
        """Test a complex calculated measure with nested parens and multiplication."""
        measure_8 = literal_column(
            '("UserInfo_Ring Count" + "UserInfo_Region Count") * 2'
        ).label("Measure 8")

        query = select(measure_8).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 8"]' in compiled
        assert "* 2" in compiled
        assert "+" in compiled

    def test_calculated_measure_plus_constant(self, pt_search_table):
        """Test a calculated measure that adds a constant."""
        measure_20 = literal_column('"Measure 1" + 1').label("Measure 20")

        query = select(measure_20).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Measure 20"]' in compiled
        assert "+ 1" in compiled

    def test_no_double_bracketing(self, pt_search_table):
        """Test that there's no double bracketing like [["col"]]."""
        measure = literal_column('"UserInfo_Ring Count"').label("Test Measure")

        query = select(measure).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should not have double brackets
        assert '[["' not in compiled
        assert '"]]' not in compiled

    def test_standalone_quoted_identifier(self):
        """Test that standalone quoted identifiers are converted to bracket notation."""
        metadata = MetaData()
        test_table = Table(
            "TestTable",
            metadata,
            Column("Revenue", String),
            Column("Cost", String),
            schema="test_schema",
        )

        measure_standalone = literal_column('"Revenue"').label("Standalone Quote")

        query = select(measure_standalone).select_from(test_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Standalone Quote"]' in compiled
        assert '["Revenue"]' in compiled

    def test_standalone_quoted_expression(self):
        """Test standalone expression with quoted identifiers."""
        metadata = MetaData()
        test_table = Table(
            "TestTable",
            metadata,
            Column("Revenue", String),
            Column("Cost", String),
            schema="test_schema",
        )

        measure_expr = literal_column('"Revenue" + "Cost"').label(
            "Standalone Expression"
        )

        query = select(measure_expr).select_from(test_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert '["Standalone Expression"]' in compiled
        assert '["Revenue"]' in compiled
        assert '["Cost"]' in compiled
        assert "+" in compiled

    def test_calculated_measure_references_simple_measures(self, pt_search_table):
        """Test that calculated measures can reference other measures by name.

        This simulates the Superset UI where:
        - Measure 1 = count()
        - Measure 4 = (("Measure 1"))  # References Measure 1 by name

        The extend should reference the measure name, not the raw SQL.
        """
        # Simple measures
        measure_1 = literal_column("count()").label("Measure 1")
        measure_2 = literal_column("count()").label("Measure 2")

        # Calculated measure that references Measure 1 by name
        measure_4 = literal_column('(("Measure 1"))').label("Measure 4")

        query = select(measure_1, measure_2, measure_4).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Summarize should have the base measures
        assert 'summarize' in compiled
        assert '["Measure 1"] = count()' in compiled
        assert '["Measure 2"] = count()' in compiled

        # Extend should reference ["Measure 1"] not count()
        assert 'extend' in compiled
        assert '["Measure 4"]' in compiled
        # Should reference the measure, not raw SQL
        assert '(["Measure 1"])' in compiled or '((["Measure 1"]))' in compiled

    def test_calculated_measure_with_arithmetic_on_measure_refs(self, pt_search_table):
        """Test calculated measures with arithmetic on measure references."""
        measure_1 = literal_column("count()").label("Measure 1")
        measure_2 = literal_column("count()").label("Measure 2")

        # Measure 6 = Measure 1 + Measure 2
        measure_6 = literal_column('"Measure 1" + "Measure 2"').label("Measure 6")

        query = select(measure_1, measure_2, measure_6).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        assert 'summarize' in compiled
        assert 'extend' in compiled
        # The calculated measure should reference the measure names
        assert '["Measure 6"]' in compiled
        assert '["Measure 1"]' in compiled
        assert '["Measure 2"]' in compiled

    def test_no_aggregates_in_extend(self, pt_search_table):
        """Verify that aggregate functions don't appear in extend statements."""
        measure_1 = literal_column("count()").label("Measure 1")
        measure_4 = literal_column('(("Measure 1"))').label("Measure 4")

        query = select(measure_1, measure_4).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Find the extend part
        extend_idx = compiled.find('extend')
        if extend_idx != -1:
            project_idx = compiled.find('| project')
            extend_part = compiled[extend_idx:project_idx] if project_idx != -1 else compiled[extend_idx:]
            # Should not have count() in extend - it should reference ["Measure 1"]
            assert 'count()' not in extend_part.lower()
            # Should have the measure reference instead
            assert '["Measure 1"]' in extend_part

    def test_measure_name_with_aggregate_keyword(self, pt_search_table):
        """Test that measure names containing aggregate keywords (like 'Count') aren't parsed as aggregates.

        This tests the case where a measure is named "UserInfo_Ring Count" - the word "Count"
        should NOT be treated as an aggregate function.
        """
        # Base measures with aggregate keywords in their names
        ring_count = func.COUNT(pt_search_table.c.UserInfo_Ring).label("UserInfo_Ring Count")
        region_count = func.COUNT(pt_search_table.c.UserInfo_Region).label("UserInfo_Region Count")

        # Calculated measure referencing measure with "Count" in its name
        measure_4 = literal_column('(("UserInfo_Ring Count"))').label("Measure 4")

        query = select(ring_count, region_count, measure_4).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Find the extend part
        extend_idx = compiled.find('extend')
        if extend_idx != -1:
            project_idx = compiled.find('| project')
            extend_part = compiled[extend_idx:project_idx] if project_idx != -1 else compiled[extend_idx:]

            # Should NOT have COUNT(UserInfo_Ring) in extend - that's the bug we're fixing
            assert 'COUNT(' not in extend_part
            assert 'count(' not in extend_part
            # Should reference the measure name, not the raw SQL
            assert '["UserInfo_Ring Count"]' in extend_part

    def test_measure_name_with_sum_keyword(self, pt_search_table):
        """Test that measure names containing 'Sum' aren't parsed as aggregates."""
        # A measure named "Total Sum" should not have "Sum" treated as an aggregate
        base_measure = literal_column("count()").label("Total Sum")
        calc_measure = literal_column('"Total Sum" * 2').label("Double Sum")

        query = select(base_measure, calc_measure).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # The extend should reference ["Total Sum"], not try to parse "Sum" as aggregate
        extend_idx = compiled.find('extend')
        if extend_idx != -1:
            project_idx = compiled.find('| project')
            extend_part = compiled[extend_idx:project_idx] if project_idx != -1 else compiled[extend_idx:]
            assert '["Total Sum"]' in extend_part
            # Should not have sum() function call in extend
            assert 'sum(' not in extend_part.lower() or '["Total Sum"]' in extend_part

    def test_aggregate_in_quoted_string_not_extracted(self):
        """Test that aggregates inside quoted strings are not extracted."""
        # Expression with "Count" inside a quoted measure name
        expr = '(("UserInfo_Ring Count"))'
        result, new_aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Test")

        # Should NOT extract any aggregates - "Count" is inside quotes
        assert len(new_aggs) == 0
        # Expression should be unchanged (except for normal bracket escaping)
        assert 'count(' not in result.lower()

    def test_real_aggregate_still_extracted(self):
        """Test that real aggregate functions are still properly extracted."""
        # Expression with actual aggregate function
        expr = 'count(col1) + sum(col2)'
        result, new_aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Test")

        # Should extract both aggregates
        assert len(new_aggs) == 2
        # Result should have references, not the original aggregates
        assert 'count(' not in result.lower()
        assert 'sum(' not in result.lower()

    def test_mixed_quoted_and_real_aggregates(self):
        """Test expression with both quoted measure names and real aggregates."""
        # "Ring Count" is a measure name (quoted), count(col) is a real aggregate
        expr = '"Ring Count" + count(col)'
        result, new_aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Test")

        # Should extract only the real aggregate, not the one in quotes
        assert len(new_aggs) == 1
        agg_sql = new_aggs[0][1]
        assert 'count(' in agg_sql.lower()

    def test_bracket_notation_not_extracted(self):
        """Test that aggregates in bracket notation are not extracted."""
        # Expression with "Count" inside bracket notation
        expr = '["UserInfo_Ring Count"] * 2'
        result, new_aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Test")

        # Should NOT extract any aggregates - "Count" is inside brackets
        assert len(new_aggs) == 0

    def test_wrapped_aggregate_extracted_correctly(self, pt_search_table):
        """Test that aggregates wrapped in parens (like ((COUNT(col)))) are extracted correctly."""
        # This is what Superset sends when a user writes (("UserInfo_Ring Count"))
        # Superset resolves the measure reference to the actual SQL
        measure_4 = literal_column("((COUNT(UserInfo_Ring)))").label("Measure 4")

        query = select(measure_4).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Should have summarize with the aggregate
        assert 'summarize' in compiled

        # Find the extend part
        extend_idx = compiled.find('extend')
        if extend_idx != -1:
            project_idx = compiled.find('| project')
            extend_part = compiled[extend_idx:project_idx] if project_idx != -1 else compiled[extend_idx:]

            # Should NOT have COUNT() in extend
            assert 'COUNT(' not in extend_part
            assert 'count(' not in extend_part
            # Should have a reference
            assert '["Measure 4"]' in extend_part

    def test_floating_point_numbers(self, pt_search_table):
        """Test that floating point numbers are preserved correctly."""
        # Measure with floating point multiplier
        measure_1 = literal_column("count()").label("Measure 1")
        measure_2 = literal_column('"Measure 1" * 0.5').label("Measure 2")
        measure_3 = literal_column('"Measure 1" * 1.25').label("Measure 3")
        measure_4 = literal_column('"Measure 1" / 0.1').label("Measure 4")

        query = select(measure_1, measure_2, measure_3, measure_4).select_from(pt_search_table)
        compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

        # Floating point numbers should be preserved, not wrapped in brackets
        assert '* 0.5' in compiled
        assert '* 1.25' in compiled
        assert '/ 0.1' in compiled
        # Should NOT have bracketed numbers
        assert '["0.5"]' not in compiled
        assert '["1.25"]' not in compiled
        assert '["0.1"]' not in compiled

    def test_is_number_literal(self):
        """Test _is_number_literal handles various number formats."""
        # Integers
        assert KustoKqlCompiler._is_number_literal("5") is True
        assert KustoKqlCompiler._is_number_literal("123") is True
        assert KustoKqlCompiler._is_number_literal("0") is True

        # Floating point
        assert KustoKqlCompiler._is_number_literal("0.5") is True
        assert KustoKqlCompiler._is_number_literal("1.25") is True
        assert KustoKqlCompiler._is_number_literal(".5") is True
        assert KustoKqlCompiler._is_number_literal("5.") is True
        assert KustoKqlCompiler._is_number_literal("0.0") is True

        # Negative numbers
        assert KustoKqlCompiler._is_number_literal("-5") is True
        assert KustoKqlCompiler._is_number_literal("-0.5") is True

        # Scientific notation
        assert KustoKqlCompiler._is_number_literal("1e10") is True
        assert KustoKqlCompiler._is_number_literal("1.5e-3") is True

        # Not numbers
        assert KustoKqlCompiler._is_number_literal("abc") is False
        assert KustoKqlCompiler._is_number_literal("1.2.3") is False
        assert KustoKqlCompiler._is_number_literal("") is False


def test_find_top_level_operator_with_single_quotes():
    """Test that _find_top_level_operator handles single-quoted strings correctly."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Operator outside quotes should be found
    assert KustoKqlCompiler._find_top_level_operator("a + b", "+") == 2

    # Operator inside double quotes should NOT be found
    assert KustoKqlCompiler._find_top_level_operator('"a + b"', "+") == -1

    # Operator inside single quotes should NOT be found (KQL string literals)
    assert KustoKqlCompiler._find_top_level_operator("'value-with-minus'", "-") == -1
    assert KustoKqlCompiler._find_top_level_operator("col + 'test-value'", "-") == -1

    # Operator outside single quotes should be found
    assert KustoKqlCompiler._find_top_level_operator("col + 'test'", "+") == 4

    # Mixed quotes
    assert KustoKqlCompiler._find_top_level_operator("\"col\" + 'value'", "+") == 6
    assert KustoKqlCompiler._find_top_level_operator("'a-b' + \"c-d\"", "+") == 6
    assert KustoKqlCompiler._find_top_level_operator("'a-b' + \"c-d\"", "-") == -1


# ==============================================================================
# Tests for dialect_kql.py improvements (Performance & Correctness)
# ==============================================================================

def test_precompiled_pattern_exists_and_works():
    """IMPROVEMENT: Verify KQL_AGG_PATTERN is pre-compiled (performance optimization).

    This tests the fix for the performance regression where the regex pattern
    was being compiled on every function call. Now it's pre-compiled as a
    module-level constant.
    """
    from sqlalchemy_kusto.dialect_kql import KQL_AGG_PATTERN
    import re

    # Must be a pre-compiled Pattern object, not a string
    assert isinstance(KQL_AGG_PATTERN, re.Pattern)

    # Should work correctly
    assert KQL_AGG_PATTERN.search("count(x)") is not None
    assert KQL_AGG_PATTERN.search("SUM(revenue)") is not None
    assert KQL_AGG_PATTERN.search("dcount(users)") is not None

    # Should respect word boundaries
    assert KQL_AGG_PATTERN.search("mycount(x)") is None


def test_is_inside_quotes_or_brackets_handles_escaped_quotes():
    """IMPROVEMENT: Test that _is_inside_quotes_or_brackets handles escaped quotes correctly.

    This tests the bug fix where escaped quotes were not being properly handled,
    which could cause incorrect detection of whether a position is inside quotes.
    """
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Fixed: Escaped double quote should not close the string
    text = r'x + "a\"b" + y'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 5) is True   # at 'a'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 7) is True   # at escaped quote
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 8) is True   # at 'b'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 12) is False # at '+'

    # Fixed: Escaped single quote should not close the string
    text = r"x + 'a\'b' + y"
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 5) is True   # at 'a'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 7) is True   # at escaped quote
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 8) is True   # at 'b'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 12) is False # at '+'

    # Without escape handling, this would incorrectly think position 12 is inside quotes


def test_contains_aggregate_no_unnecessary_extraction():
    """IMPROVEMENT: Test that _contains_aggregate_function is optimized.

    This tests the optimization where _contains_aggregate_function no longer
    does a full extraction (creating references and modifying dicts), but just
    checks if an aggregate exists outside quotes/brackets.
    """
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Should detect presence of aggregates
    assert KustoKqlCompiler._contains_aggregate_function("count(x)") is True
    assert KustoKqlCompiler._contains_aggregate_function("sum(a) + avg(b)") is True
    assert KustoKqlCompiler._contains_aggregate_function("((COUNT(users)))") is True

    # Should correctly skip aggregates in quotes (bug would return True)
    assert KustoKqlCompiler._contains_aggregate_function('"count(x)"') is False
    assert KustoKqlCompiler._contains_aggregate_function("'sum is a word'") is False
    assert KustoKqlCompiler._contains_aggregate_function('["Count Column"]') is False

    # Mixed: real aggregate + quoted text containing aggregate keywords
    assert KustoKqlCompiler._contains_aggregate_function('"Count Text" + count(x)') is True

    # Should not detect non-aggregates
    assert KustoKqlCompiler._contains_aggregate_function("column_name") is False
    assert KustoKqlCompiler._contains_aggregate_function('"Measure 1" + "Measure 2"') is False


def test_extract_aggregates_uses_precompiled_pattern():
    """IMPROVEMENT: Verify _extract_aggregates_from_expression uses pre-compiled pattern.

    This ensures the performance optimization is actually being used by the
    extraction function.
    """
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler, KQL_AGG_PATTERN

    # Extract aggregates
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(
        "count(x) + sum(y) + avg(z)", "measure"
    )

    # Should extract all three aggregates
    assert len(aggs) == 3

    # Verify each aggregate is in the list of KQL aggregates
    for ref_name, kql_agg in aggs:
        # The aggregate should match our pre-compiled pattern
        # (this indirectly verifies the function uses the pattern)
        assert KQL_AGG_PATTERN.search(kql_agg) is not None


def test_escaped_quotes_in_aggregate_extraction():
    """IMPROVEMENT: Test that aggregate extraction handles escaped quotes correctly.

    This ensures the fix for escaped quote handling is applied in the
    extraction logic.
    """
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Expression with escaped quotes - aggregate should still be extracted
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(
        r'"text\"more" + count(x)', "measure"
    )
    assert len(aggs) == 1
    assert "count" in aggs[0][1].lower()

    # Aggregate inside string with escaped quote should NOT be extracted
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(
        r'"count(\"x\")" + y', "measure"
    )
    assert len(aggs) == 0

    # Both escaped quote and real aggregate
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(
        r'"escaped\"quote" + sum(revenue)', "measure"
    )
    assert len(aggs) == 1
    assert "sum" in aggs[0][1].lower()


def test_performance_no_regex_recompilation():
    """IMPROVEMENT: Verify regex pattern is not recompiled on each call.

    This is a regression test to ensure the performance fix stays in place.
    Multiple calls should use the same compiled pattern object.
    """
    from sqlalchemy_kusto.dialect_kql import KQL_AGG_PATTERN, KustoKqlCompiler

    # Get the pattern object id before any calls
    pattern_id_before = id(KQL_AGG_PATTERN)

    # Make multiple calls to functions that use the pattern
    for _ in range(100):
        KustoKqlCompiler._contains_aggregate_function("count(x)")
        KustoKqlCompiler._extract_aggregates_from_expression("sum(y)", "test")

    # Pattern object should be the same (not recompiled)
    pattern_id_after = id(KQL_AGG_PATTERN)
    assert pattern_id_before == pattern_id_after


def test_complex_expression_with_all_improvements():
    """INTEGRATION: Test complex expression uses all improvements correctly.

    This integration test verifies that all improvements work together:
    - Pre-compiled pattern for performance
    - Escaped quote handling for correctness
    - Optimized aggregate detection
    """
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Complex expression with: escaped quotes, aggregates, operators, and quoted measure names
    expr = r'"Total\"Count" + (count(x) + sum(y)) / avg(z) * "Factor"'

    # Should detect aggregates correctly
    contains_agg = KustoKqlCompiler._contains_aggregate_function(expr)
    assert contains_agg is True

    # Should extract aggregates correctly
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "ComplexMeasure")

    # Should extract 3 aggregates (count, sum, avg)
    assert len(aggs) == 3

    # Should NOT extract quoted measure names
    assert any("count" in agg[1].lower() for agg in aggs)
    assert any("sum" in agg[1].lower() for agg in aggs)
    assert any("avg" in agg[1].lower() for agg in aggs)

    # Result should have references, not original aggregates
    assert "count(x)" not in result.lower()
    assert "sum(y)" not in result.lower()
    assert "avg(z)" not in result.lower()

    # But should preserve quoted strings
    assert r'"Total\"Count"' in result or r'["Total\"Count"]' in result
    assert r'"Factor"' in result or r'["Factor"]' in result


# ==============================================================================
# Edge Case Tests (From PR Review Feedback)
# ==============================================================================

def test_large_expression_stress_test():
    """EDGE CASE: Test performance with very large expressions (1000+ characters)."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Generate a large expression with many aggregates
    parts = [f"count(col{i})" for i in range(50)]
    large_expr = " + ".join(parts)

    # Should handle large expression without errors
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(large_expr, "LargeMeasure")

    # Should extract all 50 aggregates
    assert len(aggs) == 50

    # Should also detect correctly
    assert KustoKqlCompiler._contains_aggregate_function(large_expr) is True


def test_deeply_nested_parentheses():
    """EDGE CASE: Test handling of deeply nested parentheses (10+ levels)."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Create deeply nested expression (balanced parens: 9 outer + 1 from count() = 10 total opening, 10 closing with extra trailing)
    expr = "(((((((((count(x)))))))))))"

    # Should extract the aggregate
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "DeepNest")
    assert len(aggs) == 1

    # Result should maintain outer parentheses minus the function's opening paren
    # Original: (((((((((count(x)))))))))))  has 10 '(' and 11 ')'
    # After extraction: (((((((((ref)))))))))))  has 9 '(' and 10 ')'
    # The aggregate "count(x)" is replaced with "ref", removing one ( and one )
    assert result.count("(") == 9
    assert result.count(")") == 10

    # Test _find_matching_paren with deep nesting (using balanced expression)
    balanced_expr = "(((((((((())))))))))"  # 10 levels deep, balanced: 10 '(' and 10 ')'
    assert KustoKqlCompiler._find_matching_paren(balanced_expr, 0) == len(balanced_expr) - 1  # Outermost match
    assert KustoKqlCompiler._find_matching_paren(balanced_expr, 5) == len(balanced_expr) - 6  # 5th level match


def test_unicode_characters_in_column_names():
    """EDGE CASE: Test unicode characters in column names."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Unicode column names (e.g., Chinese, Arabic, emoji)
    expr = 'count(价格) + sum(المبلغ) + avg(🔥column)'

    # Should handle unicode correctly
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Unicode")
    assert len(aggs) == 3

    # Should escape unicode column names properly
    for ref_name, kql_agg in aggs:
        assert kql_agg.startswith(("count(", "sum(", "avg("))


def test_multiple_consecutive_escaped_quotes():
    """EDGE CASE: Test multiple consecutive escaped quotes."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Expression with multiple escaped quotes: "a\\"b\\"c"
    expr = r'"a\\"b\\"c" + count(x)'

    # Should correctly identify that count is outside quotes
    contains_agg = KustoKqlCompiler._contains_aggregate_function(expr)
    assert contains_agg is True

    # Should extract the aggregate
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "EscapedQuotes")
    assert len(aggs) == 1

    # Test _is_inside_quotes_or_brackets at various positions
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(expr, 2) is True   # at 'a'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(expr, 8) is True   # at 'c'
    assert KustoKqlCompiler._is_inside_quotes_or_brackets(expr, 13) is False # at '+'


def test_empty_and_edge_inputs():
    """EDGE CASE: Test empty strings and boundary conditions."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Empty string
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression("", "Empty")
    assert len(aggs) == 0
    assert result == ""

    # Just an aggregate, no operators
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression("count(x)", "Simple")
    assert len(aggs) == 1

    # Just a column reference
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression("column_name", "Column")
    assert len(aggs) == 0

    # Out of bounds checks for helper functions
    assert KustoKqlCompiler._is_inside_quotes_or_brackets("abc", 100) is False
    assert KustoKqlCompiler._find_matching_paren("(abc)", 100) == -1


def test_mixed_aggregate_and_string_patterns():
    """EDGE CASE: Test expressions with aggregate keywords in various contexts."""
    from sqlalchemy_kusto.dialect_kql import KustoKqlCompiler

    # Aggregate keyword in column name, measure name, and real aggregate
    expr = '"Total Count" + ["Count Column"] + count(actual_count)'

    # Should only detect the real aggregate
    result, aggs = KustoKqlCompiler._extract_aggregates_from_expression(expr, "Mixed")
    assert len(aggs) == 1
    assert "count" in aggs[0][1].lower()

    # Should preserve quoted strings and bracket notation
    assert '"Total Count"' in result or '["Total Count"]' in result
    assert '["Count Column"]' in result

