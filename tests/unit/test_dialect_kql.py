import pytest
import sqlalchemy as sa
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    column,
    create_engine,
    literal_column,
    select,
    text,
)
from sqlalchemy.sql.selectable import TextAsFrom

engine = create_engine("kustokql+https://localhost/testdb")


def test_compiler_with_projection() -> None:
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
        'let virtual_table = (["logs"] | take 10);'
        "virtual_table"
        "| project id = Id, tId = TypeId, Type"
        "| take __[POSTCOMPILE_param_1]"
    )

    assert query_compiled == query_expected


def test_compiler_with_star() -> None:
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


def test_select_from_text() -> None:
    query = (
        select([column("Field1"), column("Field2")])
        .select_from(text("logs"))
        .limit(100)
    )
    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")
    query_expected = '["logs"]' "| project Field1, Field2" "| take 100"

    assert query_compiled == query_expected


def test_use_table() -> None:
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
        '["logs"]' "| project Field1, Field2" "| take __[POSTCOMPILE_param_1]"
    )
    assert query_compiled == query_expected


def test_limit() -> None:
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


def test_select_count() -> None:
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

    query_compiled = str(
        query.compile(engine, compile_kwargs={"literal_binds": True})
    ).replace("\n", "")

    query_expected = (
        'let inner_qry = (["logs"]);'
        "inner_qry"
        "| where Field1 > 1 and Field2 < 2"
        "| summarize count = count()"
        "| take 5"
    )

    assert query_compiled == query_expected


def test_select_with_let() -> None:
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


def test_quotes() -> None:
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
) -> None:
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
def test_schema_from_query(query_table_name: str, expected_table_name: str) -> None:
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
