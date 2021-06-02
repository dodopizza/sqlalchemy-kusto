from test.conftest import (
    KUSTO_KQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)

import sqlalchemy as sa
from sqlalchemy import create_engine, column, text, select
from sqlalchemy.sql.selectable import TextAsFrom

engine = create_engine(
    f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_ddl():
    engine.connect()
    result = engine.execute(".show tables")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert result is not None


def test_get_columns():
    conn = engine.connect()
    columns_result = engine.dialect.get_columns(conn, "_temp__ordercomposition_extended_with_combo_1620690454")
    print("\n")
    print("\n".join([str(r) for r in columns_result]))
    assert columns_result is not None


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
        "| project uId = UnitId, mttId = MaterialTypeId, Type = Type",
        "| take %(param_1)s",
    ]

    assert query_compiled == "\n".join(query_expected)


def test_compiler_with_asterisk():
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
    query = (
        select([column("Field1"), column("Field2")])
        .select_from(text("MyTable"))
        .limit(100)
    )
    query_compiled = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    query_expected = [
        "MyTable",
        "| project Field1 = Field1, Field2 = Field2",
        "| take %(param_1)s",
    ]

    assert query_compiled == "\n".join(query_expected)
