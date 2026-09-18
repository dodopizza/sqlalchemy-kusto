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
        from_obj=stmt,
        columns=[
            column("Id").label("id"),
            column("TypeId").label("tId"),
            column("Type"),
        ],
    )
    query = query.select_from(stmt)
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
    query = sa.select(
        "*",
        from_obj=stmt,
    )
    query = query.select_from(stmt)
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
        select([column("Field1"), column("Field2")])
        .select_from(text("logs"))
        .limit(100)
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
        select([column("Field1"), column("Field2")]).select_from(text("logs")).where(f)
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
        select([event_col, active_users_col])
        .select_from(text("ActiveUsersLastMonth"))
        .group_by(literal_column('"EventInfo_Time" / time(1d)'))
        .order_by(text("ActiveUserMetric DESC"))
    )

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query - order matches column appearance
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| summarize   by ["EventInfo_Time"] / time(1d)'
        '| extend ["EventInfo_Time"] = ["EventInfo_Time"] / time(1d), '
        '["ActiveUserMetric"] = ["ActiveUsers"]'
        '| project ["EventInfo_Time"], ["ActiveUserMetric"]'
        '| order by ["ActiveUserMetric"] desc'
    )
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
    query = select([event_col, active_users_col]).select_from(
        text("ActiveUsersLastMonth")
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # Order matches column appearance in select
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
    # Note: When alias = column name, no extend is needed
    query = (
        select([literal_column("country_name").label("country_name")])
        .select_from(text('superset."CovidVaccineData"'))
        .group_by(literal_column("country_name"))
        .order_by(text("country_name ASC"))
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        'database("superset").["CovidVaccineData"]'
        '| summarize   by ["country_name"]'
        '| project ["country_name"]'
        '| order by ["country_name"] asc'
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
        [
            event_col,
        ]
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
        [
            event_col,
        ]
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
        [
            event_col,
        ]
    ).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    # raw query text from query
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
            [
                event_col,
                sa.func.count(distinct(active_users_col)).label("DistinctUsers"),
            ]
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
        '| extend ["EventInfo_Time"] = ["EventInfo_Time"] / time(1d)'
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
        select([event_col, active_users_col.label("DistinctUsers")])
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
        '| extend ["EventInfo_Time"] = ["EventInfo_Time"] / time(1d)'
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
        select([column_count])
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
        '| summarize ["__total-count_1"] = count() '
        '| extend ["total-count"] = ["__total-count_1"]'
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


def test_escape_and_quote_columns_with_two_quoted_measures():
    """Test that two quoted measure names with operator are properly escaped.

    e.g. "Measure 1" + "Measure 2" --> ["Measure 1"] + ["Measure 2"]
    """
    result = KustoKqlCompiler._escape_and_quote_columns('"Measure 1" + "Measure 2"')
    assert result == '["Measure 1"] + ["Measure 2"]'


def test_escape_and_quote_columns_preserves_already_bracketed():
    """Test that already-bracketed columns are not double-converted."""
    result = KustoKqlCompiler._escape_and_quote_columns('["Measure 1"]')
    assert result == '["Measure 1"]'


def test_calculated_measure_with_two_adhoc_measures():
    """Test calculated measure referencing two ad hoc measures.

    Measure 3 = "Measure 1" + "Measure 2" should compile to (["Measure 1"]) + (["Measure 2"])
    Parentheses are added for arithmetic precedence clarity.
    """
    measure_3 = literal_column('"Measure 1" + "Measure 2"').label("Measure 3")
    query = select([measure_3]).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        '["SalesData"]'
        '| extend ["Measure 3"] = ["Measure 1"] + ["Measure 2"]'
        '| project ["Measure 3"]'
    )
    assert query_compiled == query_expected


def test_escape_and_quote_columns_measure_with_constant():
    """Test that measure with operator and constant is properly escaped.

    e.g. "Measure 1" * 2 --> ["Measure 1"] * 2
    """
    result = KustoKqlCompiler._escape_and_quote_columns('"Measure 1" * 2')
    assert result == '["Measure 1"] * 2'


def test_escape_and_quote_columns_measure_with_operator_in_name():
    """Test that measure names containing operators are properly escaped.

    e.g. "Measure 1-2" --> ["Measure 1-2"] (not split as ["Measure 1"] - ["2"])
    """
    result = KustoKqlCompiler._escape_and_quote_columns('"Measure 1-2"')
    assert result == '["Measure 1-2"]'


def test_is_number_literal():
    """Test _is_number_literal correctly identifies numeric literals."""
    # Should match: integers and decimals with digits on both sides of decimal
    assert KustoKqlCompiler._is_number_literal("5") is True
    assert KustoKqlCompiler._is_number_literal("123") is True
    assert KustoKqlCompiler._is_number_literal("0") is True
    assert KustoKqlCompiler._is_number_literal("0.5") is True
    assert KustoKqlCompiler._is_number_literal("5.0") is True
    assert KustoKqlCompiler._is_number_literal("123.456") is True

    # Should NOT match: trailing decimal, leading decimal, scientific notation, negatives
    assert KustoKqlCompiler._is_number_literal("5.") is False
    assert KustoKqlCompiler._is_number_literal(".5") is False
    assert KustoKqlCompiler._is_number_literal("-5") is False
    assert KustoKqlCompiler._is_number_literal("-0.5") is False
    assert KustoKqlCompiler._is_number_literal("1e10") is False
    assert KustoKqlCompiler._is_number_literal("1.5e-3") is False

    # Should NOT match: non-numeric strings
    assert KustoKqlCompiler._is_number_literal("abc") is False
    assert KustoKqlCompiler._is_number_literal("Measure 1") is False
    assert KustoKqlCompiler._is_number_literal("") is False


def test_calculated_measure_with_adhoc_measure_and_constant():
    """Test calculated measure with an ad hoc measure and a constant.

    Measure 1 = count(*), Measure 2 = "Measure 1" * 2
    Measure 2 should compile to (["Measure 1"]) * 2 (parentheses for arithmetic precedence)
    """
    measure_1 = literal_column("count(*)").label("Measure 1")
    measure_2 = literal_column('"Measure 1" * 2').label("Measure 2")
    query = select([measure_1, measure_2]).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        '["SalesData"]'
        '| summarize ["__Measure 1_1"] = count() '
        '| extend ["Measure 1"] = ["__Measure 1_1"], ["Measure 2"] = ["Measure 1"] * 2'
        '| project ["Measure 1"], ["Measure 2"]'
    )
    assert query_compiled == query_expected


def test_calculated_measure_with_two_adhoc_measures_and_aggregates():
    """Test calculated measure referencing two ad hoc measures with aggregates.

    Measure 1 = count(*), Measure 2 = count(*)
    Measure 3 = "Measure 1" + "Measure 2" should compile to (["Measure 1"]) + (["Measure 2"])
    """
    measure_1 = literal_column("count(*)").label("Measure 1")
    measure_2 = literal_column("count(*)").label("Measure 2")
    measure_3 = literal_column('"Measure 1" + "Measure 2"').label("Measure 3")
    query = select([measure_1, measure_2, measure_3]).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        '["SalesData"]'
        '| summarize ["__Measure 1_1"] = count() '
        '| extend ["Measure 1"] = ["__Measure 1_1"], ["Measure 2"] = ["__Measure 1_1"], ["Measure 3"] = ["Measure 1"] + ["Measure 2"]'
        '| project ["Measure 1"], ["Measure 2"], ["Measure 3"]'
    )
    assert query_compiled == query_expected


def test_calculated_measure_with_inline_aggregates():
    """Test calculated measure with inline aggregates creating intermediary measures.

    Measure = count("a") + count("b") should create intermediary measures:
    - __Measure_1 = count(["a"])
    - __Measure_2 = count(["b"])
    - Measure = ["__Measure_1"] + ["__Measure_2"]
    """
    measure = literal_column('count("a") + count("b")').label("Measure")
    query = select([measure]).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        '["SalesData"]'
        '| summarize ["__Measure_1"] = count(["a"]), ["__Measure_2"] = count(["b"]) '
        '| extend ["Measure"] = ["__Measure_1"] + ["__Measure_2"]'
        '| project ["Measure"]'
    )
    assert query_compiled == query_expected


def test_calculated_measure_with_mixed_aggregates_and_references():
    """Test calculated measure mixing inline aggregate and predefined measure.

    Predefined 1 = count(*), Calculated = "Predefined 1" + count("b")
    Should create:
    - Predefined 1 = count()
    - __Calculated_1 = count(["b"])
    - Calculated = ["Predefined 1"] + ["__Calculated_1"]
    """
    predefined_1 = literal_column("count(*)").label("Predefined 1")
    calculated = literal_column('"Predefined 1" + count("b")').label("Calculated")
    query = select([predefined_1, calculated]).select_from(text("SalesData"))
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = (
        '["SalesData"]'
        '| summarize ["__Predefined 1_1"] = count(), ["__Calculated_1"] = count(["b"]) '
        '| extend ["Predefined 1"] = ["__Predefined 1_1"], ["Calculated"] = ["Predefined 1"] + ["__Calculated_1"]'
        '| project ["Predefined 1"], ["Calculated"]'
    )
    assert query_compiled == query_expected


# ============================================================================
# Unit tests for helper functions
# ============================================================================


class TestIsInsideQuotesOrBrackets:
    """Tests for _is_inside_quotes_or_brackets helper."""

    def test_position_inside_double_quotes(self):
        text = 'before "inside" after'
        # Positions inside the quotes (i, n, s, i, d, e)
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 8) is True
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 13) is True

    def test_position_outside_double_quotes(self):
        text = 'before "inside" after'
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 0) is False
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 5) is False
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 16) is False

    def test_position_inside_single_quotes(self):
        text = "before 'inside' after"
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 8) is True
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 13) is True

    def test_position_outside_single_quotes(self):
        text = "before 'inside' after"
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 0) is False
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 16) is False

    def test_position_inside_brackets(self):
        text = 'before ["column"] after'
        # Position inside the brackets
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 8) is True
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 15) is True

    def test_position_outside_brackets(self):
        text = 'before ["column"] after'
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 0) is False
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 18) is False

    def test_nested_quotes_in_brackets(self):
        text = '["col with \\"quotes\\""]'
        # Position inside should be True
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 5) is True

    def test_position_at_boundary(self):
        text = '"text"'
        assert (
            KustoKqlCompiler._is_inside_quotes_or_brackets(text, 0) is False
        )  # At opening quote
        assert (
            KustoKqlCompiler._is_inside_quotes_or_brackets(text, 1) is True
        )  # After opening quote

    def test_out_of_bounds_position(self):
        text = "short"
        assert KustoKqlCompiler._is_inside_quotes_or_brackets(text, 100) is False


class TestFindMatchingParen:
    """Tests for _find_matching_paren helper."""

    def test_simple_parentheses(self):
        text = "count(x)"
        assert KustoKqlCompiler._find_matching_paren(text, 5) == 7

    def test_nested_parentheses(self):
        text = "sum(count(x))"
        assert KustoKqlCompiler._find_matching_paren(text, 3) == 12  # Outer paren
        assert KustoKqlCompiler._find_matching_paren(text, 9) == 11  # Inner paren

    def test_parentheses_with_quotes(self):
        # Parens inside quotes should be ignored
        text = 'func("text(with)parens")'
        assert KustoKqlCompiler._find_matching_paren(text, 4) == 23

    def test_parentheses_with_brackets(self):
        # Parens inside brackets should be ignored
        text = 'func(["col(1)"])'
        assert KustoKqlCompiler._find_matching_paren(text, 4) == 15

    def test_no_opening_paren_at_position(self):
        text = "no paren here"
        assert KustoKqlCompiler._find_matching_paren(text, 0) == -1

    def test_unmatched_parenthesis(self):
        text = "func(x"
        assert KustoKqlCompiler._find_matching_paren(text, 4) == -1

    def test_empty_parentheses(self):
        text = "func()"
        assert KustoKqlCompiler._find_matching_paren(text, 4) == 5

    def test_deeply_nested(self):
        text = "a(b(c(d)))"
        assert KustoKqlCompiler._find_matching_paren(text, 1) == 9
        assert KustoKqlCompiler._find_matching_paren(text, 3) == 8
        assert KustoKqlCompiler._find_matching_paren(text, 5) == 7


class TestHasOperatorsOutsideQuotes:
    """Tests for _has_operators_outside_quotes helper."""

    def test_simple_addition(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("a + b") is True

    def test_simple_subtraction(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("a - b") is True

    def test_simple_multiplication(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("a * b") is True

    def test_simple_division(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("a / b") is True

    def test_no_operators(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("count(x)") is False

    def test_operator_inside_quotes(self):
        # Note: _find_operator_outside_quotes only tracks double quotes, not single
        assert KustoKqlCompiler._has_operators_outside_quotes('"a + b"') is False

    def test_operator_inside_brackets(self):
        assert KustoKqlCompiler._has_operators_outside_quotes('["a+b"]') is False

    def test_mixed_operators(self):
        assert KustoKqlCompiler._has_operators_outside_quotes("a + b * c") is True

    def test_aggregate_with_operators(self):
        assert (
            KustoKqlCompiler._has_operators_outside_quotes("count(a) + sum(b)") is True
        )


class TestExtractAndReplaceAggregates:
    """Tests for _extract_and_replace_aggregates helper."""

    def test_single_aggregate(self):
        expr = 'count(["a"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        assert result == '["__Measure_1"]'
        assert len(new_aggs) == 1
        assert new_aggs[0] == ('["__Measure_1"]', 'count(["a"])')

    def test_two_aggregates_with_operator(self):
        expr = 'count(["a"]) + sum(["b"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        assert result == '["__Measure_1"] + ["__Measure_2"]'
        assert len(new_aggs) == 2
        assert new_aggs[0][1] == 'count(["a"])'
        assert new_aggs[1][1] == 'sum(["b"])'

    def test_no_aggregates(self):
        expr = '["a"] + ["b"]'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        assert result == '["a"] + ["b"]'
        assert len(new_aggs) == 0

    def test_reuses_existing_aggregate(self):
        expr = 'count(["a"]) + count(["a"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        # Both should use the same reference
        assert result == '["__Measure_1"] + ["__Measure_1"]'
        assert len(new_aggs) == 1  # Only one unique aggregate

    def test_existing_aggs_parameter(self):
        existing = {'count(["a"])': '["existing_ref"]'}
        expr = 'count(["a"]) + sum(["b"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure", existing
        )
        assert '["existing_ref"]' in result
        assert len(new_aggs) == 1  # Only sum is new, count is reused

    def test_aggregate_in_quotes_ignored(self):
        expr = '"count(a)"'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        assert result == '"count(a)"'
        assert len(new_aggs) == 0

    def test_aggregate_in_brackets_ignored(self):
        expr = '["count(a)"]'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, "Measure"
        )
        assert result == '["count(a)"]'
        assert len(new_aggs) == 0

    def test_measure_name_with_special_chars(self):
        expr = 'count(["a"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(
            expr, '["My Measure"]'
        )
        assert result == '["__My Measure_1"]'

    def test_complex_expression(self):
        expr = 'count(["a"]) * 100 / sum(["b"])'
        result, new_aggs = KustoKqlCompiler._extract_and_replace_aggregates(expr, "Pct")
        assert '["__Pct_1"]' in result
        assert '["__Pct_2"]' in result
        assert "* 100 /" in result
        assert len(new_aggs) == 2


class TestContainsAggregateFunction:
    """Tests for _contains_aggregate_function helper."""

    def test_contains_count(self):
        assert KustoKqlCompiler._contains_aggregate_function("count(x)") is True

    def test_contains_sum(self):
        assert KustoKqlCompiler._contains_aggregate_function("sum(x)") is True

    def test_contains_avg(self):
        assert KustoKqlCompiler._contains_aggregate_function("avg(x)") is True

    def test_contains_min_max(self):
        assert KustoKqlCompiler._contains_aggregate_function("min(x)") is True
        assert KustoKqlCompiler._contains_aggregate_function("max(x)") is True

    def test_contains_dcount(self):
        assert KustoKqlCompiler._contains_aggregate_function("dcount(x)") is True

    def test_no_aggregate(self):
        assert KustoKqlCompiler._contains_aggregate_function("col + 1") is False
        assert KustoKqlCompiler._contains_aggregate_function('["column"]') is False

    def test_aggregate_in_quotes_not_counted(self):
        assert KustoKqlCompiler._contains_aggregate_function('"count(x)"') is False
        assert KustoKqlCompiler._contains_aggregate_function("'sum(x)'") is False

    def test_aggregate_in_brackets_not_counted(self):
        assert KustoKqlCompiler._contains_aggregate_function('["count(x)"]') is False

    def test_aggregate_with_complex_arg(self):
        assert (
            KustoKqlCompiler._contains_aggregate_function('count(["My Col"])') is True
        )

    def test_multiple_aggregates(self):
        assert (
            KustoKqlCompiler._contains_aggregate_function("count(a) + sum(b)") is True
        )


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
