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
        '| extend id = ["Id"], tId = ["TypeId"]'
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
    query_expected = 'let virtual_table = (["logs"] | take 10);' "virtual_table" "| take __[POSTCOMPILE_param_1]"
    assert query_compiled == query_expected


def test_select_from_text():
    query = select([column("Field1"), column("Field2")]).select_from(text("logs")).limit(100)
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    query_expected = '["logs"]| project ["Field1"], ["Field2"]| take 100'
    assert query_compiled == query_expected


@pytest.mark.parametrize(
    "f,expected",
    [
        pytest.param(Column("Field1", String).in_(["1", "One"]), """["Field1"] in ('1', 'One')"""),
        pytest.param(Column("Field1", String).notin_(["1", "One"]), """(["Field1"] not in ('1', 'One'))"""),
        pytest.param(text("Field1 = '1'"), """Field1 == '1'"""),
        pytest.param(Column("Field2", Integer).between(2, 4), """["Field2"] between (2..4)"""),
        pytest.param(Column("Field2", Integer).is_(None), """isnull(["Field2"])"""),
        # pytest.param(Column("Field1", String).contains("FIELD"), """["Field2"] has 'FIELD'"""),
        pytest.param(Column("Field2", Integer).isnot(None), """isnotnull(["Field2"])"""),
        pytest.param(
            (Column("Field2", Integer).isnot(None)).__and__(Column("Field1", String).notin_(["1", "One"])),
            """isnotnull(["Field2"]) and (["Field1"] not in ('1', 'One'))""",
        ),
        pytest.param(
            (Column("Field2", Integer).isnot(None)).__or__(Column("Field1", String).notin_(["1", "One"])),
            """isnotnull(["Field2"]) or (["Field1"] not in ('1', 'One'))""",
        ),
    ],
)
def test_where_predicates(f, expected):
    query = (select([column("Field1"), column("Field2")]).select_from(text("logs")).where(f)).limit(100)
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    query_expected = f"""["logs"]| where {expected}| project ["Field1"], ["Field2"]| take 100"""
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

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["ActiveUsersLastMonth"]| extend ActiveUserMetric = ["ActiveUsers"], '
        'EventInfo_Time = ["EventInfo_Time"] / time(1d)'
        '| summarize   by ["EventInfo_Time"] / time(1d)'
        '| project ["EventInfo_Time"], ["ActiveUserMetric"]'
        "| sort by ActiveUserMetric desc"
    )
    assert query_compiled == query_expected


def test_group_by_text_vaccine_dataset():
    # SELECT country_name AS country_name FROM superset."CovidVaccineData" GROUP BY country_name ORDER BY country_name ASC
    query = (
        select([literal_column("country_name").label("country_name")])
        .select_from(text('superset."CovidVaccineData"'))
        .group_by(literal_column("country_name"))
        .order_by(text("country_name ASC"))
    )
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    query_expected = (
        'database("superset").["CovidVaccineData"]'
        '| summarize   by ["country_name"]'
        '| project ["country_name"]'
        "| sort by country_name asc"
    )
    assert query_compiled == query_expected


def test_is_kql_function():
    assert KustoKqlCompiler._is_kql_function(
        """case(Size <= 3, "Small",
                       Size <= 10, "Medium",
                       "Large")"""
    )
    assert KustoKqlCompiler._is_kql_function("""bin(time(16d), 7d)""")
    assert KustoKqlCompiler._is_kql_function(
        """iff((EventType in ("Heavy Rain", "Flash Flood", "Flood")), "Rain event", "Not rain event")"""
    )


def test_distinct_count_by_text():
    # create a query from select_query_text creating clause
    # 'SELECT "EventInfo_Time" / time(1d) AS "EventInfo_Time", count(DISTINCT ActiveUsers) AS "DistinctUsers"
    # FROM ActiveUsersLastMonth GROUP BY "EventInfo_Time" / time(1d) ORDER BY ActiveUserMetric DESC'
    event_col = literal_column('"EventInfo_Time" / time(1d)').label("EventInfo_Time")
    active_users_col = literal_column("ActiveUsers")
    query = (
        select([event_col, sa.func.count(distinct(active_users_col)).label("DistinctUsers")])
        .select_from(text("ActiveUsersLastMonth"))
        .group_by(literal_column('"EventInfo_Time" / time(1d)'))
        .order_by(text("ActiveUserMetric DESC"))
    )
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| extend EventInfo_Time = ["EventInfo_Time"] / time(1d)'
        '| summarize DistinctUsers = dcount(["ActiveUsers"])  by ["EventInfo_Time"] / time(1d)'
        '| project ["EventInfo_Time"], ["DistinctUsers"]'
        "| sort by ActiveUserMetric desc"
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
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    # raw query text from query
    query_expected = (
        '["ActiveUsersLastMonth"]'
        '| extend EventInfo_Time = ["EventInfo_Time"] / time(1d)'
        '| summarize DistinctUsers = dcount(["ActiveUsers"])  by ["EventInfo_Time"] / time(1d)'
        '| project ["EventInfo_Time"], ["DistinctUsers"]'
        "| sort by ActiveUserMetric desc"
    )

    assert query_compiled == query_expected


def test_escape_and_quote_columns():
    assert KustoKqlCompiler._escape_and_quote_columns("EventInfo_Time") == '["EventInfo_Time"]'
    assert KustoKqlCompiler._escape_and_quote_columns('["UserId"]') == '["UserId"]'
    assert KustoKqlCompiler._escape_and_quote_columns("EventInfo_Time / time(1d)") == '["EventInfo_Time"] / time(1d)'


def test_sql_to_kql_aggregate():
    assert KustoKqlCompiler._sql_to_kql_aggregate("count(*)") == "count()"
    assert KustoKqlCompiler._sql_to_kql_aggregate("count", "UserId") == 'count(["UserId"])'
    assert (
        KustoKqlCompiler._sql_to_kql_aggregate("count(distinct", "CustomerId", is_distinct=True)
        == 'dcount(["CustomerId"])'
    )
    assert (
        KustoKqlCompiler._sql_to_kql_aggregate("count_distinct", "CustomerId", is_distinct=True)
        == 'dcount(["CustomerId"])'
    )
    assert KustoKqlCompiler._sql_to_kql_aggregate("sum", "Sales") == 'sum(["Sales"])'
    assert KustoKqlCompiler._sql_to_kql_aggregate("avg", "ResponseTime") == 'avg(["ResponseTime"])'
    assert KustoKqlCompiler._sql_to_kql_aggregate("min", "Size") == 'min(["Size"])'
    assert KustoKqlCompiler._sql_to_kql_aggregate("max", "Area") == 'max(["Area"])'
    assert KustoKqlCompiler._sql_to_kql_aggregate("unknown", "Column") is None


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

    query_expected = '["logs"]' '| project ["Field1"], ["Field2"]| take __[POSTCOMPILE_param_1]'
    assert query_compiled == query_expected


def test_limit():
    sql = "logs"
    limit = 5
    query = select("*").select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry")).limit(limit)

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

    query_expected = 'let inner_qry = (["logs"]);' "inner_qry" "| take 5"

    assert query_compiled == query_expected


def test_select_count():
    kql_query = "logs"
    column_count = literal_column("count(*)").label("count")
    query = (
        select([column_count])
        .select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry"))
        .where(text("Field1 > 1"))
        .where(text("Field2 < 2"))
        .order_by(text("count DESC"))
        .limit(5)
    )

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

    query_expected = (
        'let inner_qry = (["logs"]);'
        "inner_qry"
        "| where Field1 > 1 and Field2 < 2"
        "| summarize count = count() "
        '| project ["count"]'
        "| sort by count desc"
        "| take 5"
    )

    assert query_compiled == query_expected


def test_select_with_let():
    kql_query = "let x = 5; let y = 3; MyTable | where Field1 == x and Field2 == y"
    query = select("*").select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry")).limit(5)

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

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
    "schema_name,table_name,expected_table_name",
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
def test_schema_from_metadata(table_name: str, schema_name: str, expected_table_name: str):
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
    "query_table_name,expected_table_name",
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
    query = select("*").select_from(TextAsFrom(text(query_table_name), ["*"]).alias("inner_qry")).limit(5)

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

    query_expected = f"let inner_qry = ({expected_table_name});inner_qry| take 5"
    assert query_compiled == query_expected
