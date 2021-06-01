from test.conftest import (
    KUSTO_KQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
)

import sqlalchemy as sa
from sqlalchemy import create_engine, column
from sqlalchemy.sql.selectable import TextAsFrom


def test_compiler():
    statement_str = "MaterialTransferStream | take 10"
    # statement_str = "Select top 100 * from MaterialTransferStream"

    engine = create_engine(
        f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()

    stmt = TextAsFrom(sa.text(statement_str), []).alias("virtual_table")
    query = sa.select(from_obj=stmt, columns=[column("UnitId").label("uId")])
    query = query.select_from(stmt)
    query = query.limit(10)
    print("========")
    print(str(query))
    print("========")
