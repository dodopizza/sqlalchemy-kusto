from sqlalchemy import create_engine
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, ClientRequestProperties
from sqlalchemy_kusto import connect
import os
from dotenv import load_dotenv

load_dotenv()
AZURE_AD_CLIENT_ID = os.environ["AZURE_AD_CLIENT_ID"]
AZURE_AD_CLIENT_SECRET = os.environ["AZURE_AD_CLIENT_SECRET"]
AZURE_AD_TENANT_ID = os.environ["AZURE_AD_TENANT_ID"]
KUSTO_URL = os.environ["KUSTO_URL"]
KUSTO_ALCHEMY_URL = "kusto+"+os.environ["KUSTO_URL"]


def test_connect():
    connection = connect("test", "deltalake_serving", True)
    assert connection is not None


def test_execute():
    connection = connect(KUSTO_URL,
                         "deltalake_serving", False, None,
                         azure_ad_client_id=AZURE_AD_CLIENT_ID,
                         azure_ad_client_secret=AZURE_AD_CLIENT_SECRET,
                         azure_ad_tenant_id=AZURE_AD_TENANT_ID)
    result = connection.execute("select top 5 * from MaterialTransferStream").fetchall()
    print(result)
    assert result is not None


def test_kusto_client():
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        KUSTO_URL,
        AZURE_AD_CLIENT_ID,
        AZURE_AD_CLIENT_SECRET,
        AZURE_AD_TENANT_ID)
    client = KustoClient(kcsb)
    response = client.execute("deltalake_serving", ".show database schema as json", ClientRequestProperties())
    print(response.primary_results[0])
    assert response is not None


def test_alchemy():
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/deltalake_serving?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None

def test_alchemy_ping():
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/deltalake_serving?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    engine.dialect.do_ping(engine.raw_connection())



def test_fetch_one():
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/deltalake_serving?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    result = engine.execute("select top 2 * from MaterialTransferStream")
    print("\n")
    print(result.fetchone())
    print(result.fetchone())
    print(result.fetchone())
    assert engine is not None

def test_fetch_many():
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/deltalake_serving?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    assert engine is not None


def test_fetch_many():
    engine = create_engine(f"{KUSTO_ALCHEMY_URL}/deltalake_serving?"
                           f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
                           f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
                           f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}")
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


