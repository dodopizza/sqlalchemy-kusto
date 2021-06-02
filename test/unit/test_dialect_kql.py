import sqlalchemy as sa
from sqlalchemy import create_engine, column, text, select
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
        "| take %(param_1)s",
    ]

    assert query_compiled == "\n".join(query_expected)
