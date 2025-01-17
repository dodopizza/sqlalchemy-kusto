from sqlalchemy_kusto import connect
from tests.integration.conftest import (
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    DATABASE,
    KUSTO_URL,
)


def test_connect() -> None:
    connection = connect("test", DATABASE, True)
    assert connection is not None


def test_execute() -> None:
    connection = connect(
        KUSTO_URL,
        DATABASE,
        False,
        user_msi=None,
        workload_identity=False,
        azure_ad_client_id=AZURE_AD_CLIENT_ID,
        azure_ad_client_secret=AZURE_AD_CLIENT_SECRET,
        azure_ad_tenant_id=AZURE_AD_TENANT_ID,
    )
    result = connection.execute("select 1").fetchall()
    assert result is not None
