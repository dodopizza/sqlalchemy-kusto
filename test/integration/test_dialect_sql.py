from test.conftest import (
    KUSTO_SQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)
from sqlalchemy import create_engine, MetaData, Table, Column, String

engine = create_engine(
    f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_fetch_one():
    engine.connect()
    result = engine.execute("select top 2 * from MaterialTransferStream")
    print("\n")
    print(result.fetchone())
    print(result.fetchone())
    print(result.fetchone())
    assert engine is not None


def test_fetch_many():
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    assert engine is not None


def test_fetch_all():
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


def test_limit():
    metadata = MetaData()
    stream = Table(
        "MaterialTransferStream",
        metadata,
        Column("MaterialTypeId", String),
        Column("UnitId", String),
    )

    query = stream.select().limit(5)

    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5
