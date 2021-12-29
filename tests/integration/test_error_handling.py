import pytest
import sqlalchemy
from sqlalchemy import create_engine

from tests.conftest import AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, DATABASE, KUSTO_SQL_ALCHEMY_URL


def test_operational_error():
    wrong_tenant_id = "wrong_tenant_id"
    engine = create_engine(
        f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={wrong_tenant_id}"
    )
    engine.connect()

    with pytest.raises(sqlalchemy.exc.OperationalError):
        engine.execute("select top 5 * from logs")
