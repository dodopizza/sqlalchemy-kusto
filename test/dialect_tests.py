from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine
import os
from dotenv import load_dotenv

load_dotenv()
AZURE_AD_CLIENT_ID = os.environ["AZURE_AD_CLIENT_ID"]
AZURE_AD_CLIENT_SECRET = os.environ["AZURE_AD_CLIENT_SECRET"]
AZURE_AD_TENANT_ID = os.environ["AZURE_AD_TENANT_ID"]
KUSTO_URL = os.environ["KUSTO_URL"]
KUSTO_ALCHEMY_URL = "kusto+" + os.environ["KUSTO_URL"]
DATABASE = os.environ["DATABASE"]


def test_limit():
    metadata = MetaData()
    stream = Table('MaterialTransferStream', metadata,
                  Column('MaterialTypeId', String),
                  Column('UnitId', String),
                  )

    query = stream.select().limit(5)
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5
