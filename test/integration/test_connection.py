from test.conftest import (
    KUSTO_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, ClientRequestProperties
from sqlalchemy_kusto import connect


def test_connect():
    connection = connect("test", DATABASE, True)
    assert connection is not None


def test_execute():
    connection = connect(
        KUSTO_URL,
        DATABASE,
        False,
        None,
        azure_ad_client_id=AZURE_AD_CLIENT_ID,
        azure_ad_client_secret=AZURE_AD_CLIENT_SECRET,
        azure_ad_tenant_id=AZURE_AD_TENANT_ID,
    )
    result = connection.execute("select top 5 * from stoplog").fetchall()
    print(result)
    assert result is not None


def test_kusto_client():
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
    )
    client = KustoClient(kcsb)
    response = client.execute(DATABASE, ".show database schema as json", ClientRequestProperties())
    print(response.primary_results[0])
    assert response is not None
