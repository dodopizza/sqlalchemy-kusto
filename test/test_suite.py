from test.conftest import KUSTO_URL, KUSTO_ALCHEMY_URL, DATABASE, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, \
    AZURE_AD_TENANT_ID
from sqlalchemy import create_engine
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
    result = connection.execute("select top 5 * from MaterialTransferStream").fetchall()
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


def test_alchemy():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


def test_alchemy_ping():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    engine.dialect.do_ping(engine.raw_connection())


def test_fetch_one():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute("select top 2 * from MaterialTransferStream")
    print("\n")
    print(result.fetchone())
    print(result.fetchone())
    print(result.fetchone())
    assert engine is not None


def test_fetch_many():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    assert engine is not None


def test_fetch_all():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute("select top 5 * from MaterialTransferStream")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


def test_ddl():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    engine.connect()
    result = engine.execute(".show tables")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


def test_dialect_columns():
    engine = create_engine(
        f"{KUSTO_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
    conn = engine.connect()
    result = engine.dialect.get_columns(conn, "_temp__ordercomposition_extended_with_combo_1620690454")
    print("\n")
    print("\n".join([str(r) for r in result]))
    assert engine is not None
