import pytest
import sqlalchemy
from sqlalchemy import create_engine

from tests.integration.conftest import (
    DATABASE,
    KUSTO_SQL_ALCHEMY_URL,
    USES_EMULATOR,
)


@pytest.mark.skipif(
    USES_EMULATOR, reason="the emulator has no authentication, so it cannot fail on it"
)
def test_operational_error() -> None:
    wrong_tenant_id = "wrong_tenant_id"
    azure_ad_client_id = "x"
    azure_ad_client_secret = "x"
    engine = create_engine(
        f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={azure_ad_client_id}&"
        f"azure_ad_client_secret={azure_ad_client_secret}&"
        f"azure_ad_tenant_id={wrong_tenant_id}"
    )
    engine.connect()

    with pytest.raises(sqlalchemy.exc.OperationalError):
        engine.execute("select top 5 * from logs")
