from sqlalchemy.sql.selectable import TextAsFrom

from test.conftest import KUSTO_ALCHEMY_URL, DATABASE, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
from sqlalchemy import Table, Column, String, MetaData, create_engine
import sqlalchemy as sa

def test_limit():
    metadata = MetaData()
    stream = Table(
        "MaterialTransferStream",
        metadata,
        Column("MaterialTypeId", String),
        Column("UnitId", String),
    )

    query = stream.select().limit(5)
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5

def test_compilator():
    statement_str = "MaterialTransferStream | take 10"
    # statement_str = "Select top 100 * from MaterialTransferStream"

    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()

    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    print(str(stmt))
    query = sa.select(stmt, "*")
    query = query.select_from(stmt)
    query = query.limit(10)
    # query = stmt.select()
    engine.execute(query)
    print("========")
    print(str(query))
