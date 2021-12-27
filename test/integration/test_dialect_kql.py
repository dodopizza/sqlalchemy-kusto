from select import select

from sqlalchemy.sql.selectable import TextAsFrom

from test.conftest import (
    KUSTO_KQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)
from sqlalchemy import create_engine, column, text, select, MetaData, Table, Column, String, func, literal_column

engine = create_engine(
    f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_limit():
    metadata = MetaData()
    stream = Table(
        'stoplog',
        metadata,
        Column("Name", String),
        Column("UnitId", String),
    )

    query = stream.select().limit(5)
    query_compiled = str(query.compile(engine))
    print("")
    print(query_compiled)

    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5

def test_select_count_2():
    sql = "stoplog"

    column_count = literal_column("count(*)").label("count")
    query = select([column_count]) \
        .select_from(TextAsFrom(text(sql), ["*"]).alias("inner_qry")) \
        .where(text("Field1 > 1")) \
        .where(text("Field2 < 2")) \
        .limit(5)

    print(query)

    query_compiled = query.compile(engine, compile_kwargs={"literal_binds": True})
    print(query_compiled)

    engine.connect()
    result = engine.execute(query)
