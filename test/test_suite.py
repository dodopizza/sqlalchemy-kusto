from unittest.mock import patch

from sqlalchemy import create_engine
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, ClientRequestProperties

from sqlalchemy_kusto import connect


def test():
    assert True


def test_connect():
    connection = connect("dododevkusto.westeurope", "deltalake_serving", True)
    assert connection is not None


def test_execute():
    connection = connect("https://dododevkusto.westeurope.kusto.windows.net",
                         "deltalake_serving", False, None,
                         azure_ad_client_id="app_id",
                         azure_ad_client_secret="secret",
                         azure_ad_tenant_id="tenant")
    result = connection.execute("MaterialTransferStream | take 2").fetchall()
    print(result)
    assert result is not None


def test_kusto_client():
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        "https://dododevkusto.westeurope.kusto.windows.net",
        "app_id",
        "secret",
        "tenant")
    client = KustoClient(kcsb)
    response = client.execute("deltalake_serving", ".show database schema as json", ClientRequestProperties())
    print(response.primary_results[0])
    assert response is not None


# @patch("sqlalchemy_kusto.__init__")
def test_alchemy():
    engine = create_engine("kusto://dododevkusto.westeurope.kusto.windows.net")
    engine.connect()
    assert engine is not None
