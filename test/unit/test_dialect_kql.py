import sqlalchemy as sa
from sqlalchemy import create_engine, column, text, select, MetaData, Table, Column, String, func, literal_column
from sqlalchemy.sql.selectable import TextAsFrom


engine = create_engine("kustokql+http://localhost/testdb")


def test_compiler_with_projection():
    statement_str = "MaterialTransferStream | take 10"
    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select(
        from_obj=stmt,
        columns=[
            column("UnitId").label("uId"),
            column("MaterialTypeId").label("mttId"),
            column("Type"),
        ],
    )
    query = query.select_from(stmt)
    query = query.limit(10)

    query_compiled = str(query.compile(engine))
    query_expected = [
        "let virtual_table = (MaterialTransferStream | take 10);",
        "virtual_table",
        "| project uId = UnitId, mttId = MaterialTypeId, Type",
        "| take %(param_1)s",
    ]

    assert query_compiled == "\n".join(query_expected)


def test_compiler_with_star():
    statement_str = "MaterialTransferStream | take 10"
    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select(
        "*",
        from_obj=stmt,
    )
    query = query.select_from(stmt)
    query = query.limit(10)

    query_compiled = str(query.compile(engine))
    query_expected = [
        "let virtual_table = (MaterialTransferStream | take 10);",
        "virtual_table",
        "| take %(param_1)s",
    ]

    assert query_compiled == "\n".join(query_expected)


def test_select_from_text():
    query = select([column("Field1"), column("Field2")]).select_from(text("MyTable")).limit(100)
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    query_expected = [
        "MyTable",
        "| project Field1, Field2",
        "| take 100",
    ]

    assert query_compiled == "\n".join(query_expected)


def test_use_table():
    metadata = MetaData()
    stream = Table(
        "MyTable",
        metadata,
        Column("Field1", String),
        Column("Field2", String),
    )

    query = stream.select().limit(5)
    query_compiled = str(query.compile(engine))

    query_expected = [
        "MyTable",
        "| project Field1, Field2",
        "| take %(param_1)s",
    ]
    assert query_compiled == "\n".join(query_expected)

def test_quotes():
    metadata = MetaData()
    stream = Table(
        '"MyTable"',
        metadata,
        Column("Field1", String),
        Column("Field2", String),
    )

    query = stream.select().limit(5)
    # engine.dialect.identifier_preparer.initial_quote = '['
    # engine.dialect.identifier_preparer.final_quote = ']'

    quote = engine.dialect.identifier_preparer.quote
    full_table_name = quote("MyTable")
    print(full_table_name)
    # query_compiled = str(query.compile(engine))
    #
    # print(query_compiled)
    # query_expected = [
    #     "MyTable",
    #     "| project Field1, Field2",
    #     "| take %(param_1)s",
    # ]
    # assert query_compiled == "\n".join(query_expected)

def test_limit():
    sql = "MyTable"
    limit = 5
    query = (
        select("*")
        .select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry"))
        .limit(limit)
    )

    query_compiled = query.compile(engine, compile_kwargs={"literal_binds": True})
    print(query_compiled)


def test_select_count():
    sql = "MyTable"
    limit = 5
    query = (
        select("*")
        # select("count(*) AS count")
        .select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry"))
        .where("Field1 > 1")
        .where("Field1 < 2")
        .limit(limit)
    )
    print(query)
    # query_compiled = query.compile(engine, compile_kwargs={"literal_binds": True})
    # print(query_compiled)

# SELECT count(*) AS count
# FROM (MaterialTransferStream) AS expr_qry
# WHERE "EffectiveDateTime" >= datetime(2021-06-02T00:00:00)) AND "EffectiveDateTime" < datetime(2021-06-09T00:00:00)) ORDER BY count DESC
#  LIMIT :param_1


def test_select_count_2():
    kql_query = "MyTable"
    column_count = literal_column("count(*)").label("count")
    query = (
        select([column_count])
        .select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry"))
        .where(text('"Field1" > 1'))
        .where(text('"Field2" < 2'))
        .order_by(text("count DESC"))
        .limit(5)
    )

    print(f"\n\nOriginal query:\n{query}")

    query_compiled = query.compile(engine, compile_kwargs={"literal_binds": True})
    print(f"\n\nCompiled query:\n{query_compiled}")

def test_select_with_let():
    kql_query = "let x = 5; let y = 3; MyTable | where Field1 == x and Field2 == y"
    query = (
        select("*")
        .select_from(TextAsFrom(text(kql_query), ["*"]).alias("inner_qry"))
        .limit(5))

    print(f"\n\nOriginal query:\n{query}")

    query_compiled = query.compile(engine, compile_kwargs={"literal_binds": True})
    print(f"\n\nCompiled query:\n{query_compiled}")
