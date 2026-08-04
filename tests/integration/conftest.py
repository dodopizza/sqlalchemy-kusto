"""Integration test setup.

By default the tests run against a local Kusto emulator over plain HTTP:

    make emulator      # starts the kustainer-linux container
    make integration

Set KUSTO_URL (and the AZURE_AD_* variables) to target a real cluster instead.
When the endpoint is unreachable the suite is skipped rather than failed.
"""

import os
import urllib.error
import urllib.request

import pytest
from azure.kusto.data import KustoConnectionStringBuilder
from dotenv import load_dotenv
from sqlalchemy.dialects import registry

registry.register(
    "kustosql.https", "sqlalchemy_kusto.dialect_sql", "KustoSqlHttpsDialect"
)
registry.register(
    "kustosql.http", "sqlalchemy_kusto.dialect_sql", "KustoSqlHttpDialect"
)
registry.register(
    "kustokql.https", "sqlalchemy_kusto.dialect_kql", "KustoKqlHttpsDialect"
)
# The KQL dialect needs no HTTP subclass: the scheme comes from the URL, not the class.
registry.register(
    "kustokql.http", "sqlalchemy_kusto.dialect_kql", "KustoKqlHttpsDialect"
)

load_dotenv()

EMULATOR_URL = "http://localhost:8080"
HTTP_OK = 200
EMULATOR_DATABASE = "NetDefaultDB"


def _env(name: str) -> str:
    """Read an environment variable, treating the .env.sample placeholders as unset."""
    value = os.environ.get(name, "").strip()
    return "" if value.startswith("<") else value


AZURE_AD_CLIENT_ID = _env("AZURE_AD_CLIENT_ID")
AZURE_AD_CLIENT_SECRET = _env("AZURE_AD_CLIENT_SECRET")
AZURE_AD_TENANT_ID = _env("AZURE_AD_TENANT_ID")
KUSTO_URL = _env("KUSTO_URL") or EMULATOR_URL
KUSTO_SQL_ALCHEMY_URL = "kustosql+" + KUSTO_URL
KUSTO_KQL_ALCHEMY_URL = "kustokql+" + KUSTO_URL

# Plain HTTP means the emulator, which has no authentication whatsoever.
USES_EMULATOR = KUSTO_URL.startswith("http://")
DATABASE = EMULATOR_DATABASE if USES_EMULATOR else _env("DATABASE")


def get_kcsb() -> KustoConnectionStringBuilder:
    """Connection string for the raw client the fixtures use to set up test data."""
    if USES_EMULATOR:
        return KustoConnectionStringBuilder(KUSTO_URL)
    if AZURE_AD_CLIENT_ID and AZURE_AD_CLIENT_SECRET and AZURE_AD_TENANT_ID:
        return KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
        )
    return KustoConnectionStringBuilder.with_az_cli_authentication(KUSTO_URL)


def _emulator_is_up() -> bool:
    request = urllib.request.Request(
        f"{KUSTO_URL}/v1/rest/mgmt",
        data=b'{"csl":".show cluster"}',
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status == HTTP_OK
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_kusto():
    """Skip the suite instead of failing it when there is nothing to talk to."""
    if USES_EMULATOR and not _emulator_is_up():
        pytest.skip(f"Kusto emulator unreachable at {KUSTO_URL}; run `make emulator`")
