from test.conftest import (
    KUSTO_KQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)
from sqlalchemy import create_engine, MetaData, Table, Column, String

engine = create_engine(
    f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_limit():
    metadata = MetaData()
    stream = Table(
        'MaterialTransferStream',
        metadata,
        Column("MaterialTypeId", String),
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
