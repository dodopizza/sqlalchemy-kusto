from tests.conftest import (
    KUSTO_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID
)
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
    result = connection.execute(f"select 1").fetchall()
    assert result is not None
