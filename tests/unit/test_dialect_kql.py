import sqlalchemy as sa
from sqlalchemy import Column, MetaData, String, Table, column, create_engine, literal_column, select, text
from sqlalchemy.sql.selectable import TextAsFrom

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
        "let virtual_table = (logs | take 10);"
        "virtual_table"
        "| project id = Id, tId = TypeId, Type"
        "| take %(param_1)s"
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
    query_expected = "let virtual_table = (logs | take 10);" "virtual_table" "| take %(param_1)s"

    assert query_compiled == query_expected


def test_select_from_text():
    query = select([column("Field1"), column("Field2")]).select_from(text("logs")).limit(100)
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")
    query_expected = "logs" "| project Field1, Field2" "| take 100"

    assert query_compiled == query_expected


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

    query_expected = "logs" "| project Field1, Field2" "| take %(param_1)s"
    assert query_compiled == query_expected


def test_limit():
    sql = "logs"
    limit = 5
    query = select("*").select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry")).limit(limit)

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

    query_expected = "let inner_qry = (logs);" "inner_qry" "| take 5"

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
        "let inner_qry = (logs);"
        "inner_qry"
        "| where Field1 > 1 and Field2 < 2"
        "| summarize count = count()"
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
        "let inner_qry = (MyTable | where Field1 == x and Field2 == y);"
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
        "logs"
        '| project ["Field1"], ["Field2"]'
        "| take %(param_1)s"
    )
    # fmt: on

    assert query_compiled == query_expected


def test_schema_from_metadata():
    quote = engine.dialect.identifier_preparer.quote
    metadata = MetaData(schema="mydb")
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
        'database("mydb").logs'
        '| project ["Field1"], ["Field2"]'
        "| take %(param_1)s"
    )
    # fmt: on

    assert query_compiled == query_expected


def test_schema_from_query():
    kql_query = "let x = 5; let y = 3; mydb.MyTable | where Field1 == x and Field2 == y"
    query = select("*").select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry")).limit(5)

    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True})).replace("\n", "")

    query_expected = (
        "let x = 5;"
        "let y = 3;"
        'let inner_qry = (database("mydb").MyTable | where Field1 == x and Field2 == y);'
        "inner_qry"
        "| take 5"
    )

    assert query_compiled == query_expected
