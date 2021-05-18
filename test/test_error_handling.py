from test.conftest import KUSTO_ALCHEMY_URL, DATABASE, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
import pytest
from sqlalchemy import create_engine

import sqlalchemy_kusto.errors


def test_query_error():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}tt"
    )
    engine.connect()
    with pytest.raises(sqlalchemy_kusto.errors.OperationalError):
        engine.execute("select limit * from MaterialTransferStream")
