from test.conftest import (
    KUSTO_URL,
    KUSTO_SQL_ALCHEMY_URL,
    DATABASE,
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    TABLE_NAME
)
from sqlalchemy import create_engine, MetaData, Table, Column, String
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, ClientRequestProperties
import uuid

engine = create_engine(
    f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_ping():
    conn = engine.connect()
    result = engine.dialect.do_ping(conn)
    assert result is True


def test_get_table_names():
    conn = engine.connect()
    result = engine.dialect.get_table_names(conn)
    assert TABLE_NAME in result


def test_get_columns():
    conn = engine.connect()
    columns_result = engine.dialect.get_columns(conn, TABLE_NAME)
    assert len(columns_result) > 0


# TODO: generate test data
def test_fetch_one():
    engine.connect()
    result = engine.execute("select top 2 * from stoplog")
    print("\n")
    print(result.fetchone())
    print(result.fetchone())
    print(result.fetchone())
    assert engine is not None


# TODO: generate test data
def test_fetch_many():
    engine.connect()
    result = engine.execute("select top 5 * from stoplog")
    print("\n")
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    print("\n".join([str(r) for r in result.fetchmany(3)]))
    assert engine is not None


# TODO: generate test data
def test_fetch_all():
    engine.connect()
    result = engine.execute("select top 5 * from stoplog")
    print("\n")
    print("\n".join([str(r) for r in result.fetchall()]))
    assert engine is not None


# TODO: generate test data
def test_limit():
    stream = Table(
        TABLE_NAME,
        MetaData(),
        Column("Name", String),
        Column("UnitId", String),
    )

    query = stream.select().limit(5)

    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == 5


def test_create_temp_table():
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
    )
    client = KustoClient(kcsb)
    table_name = "_temp_" + uuid.uuid4().hex
    response = client.execute(
        DATABASE, f".create table {table_name}(['id']: int, ['text']: string)", ClientRequestProperties())
    print(response.primary_results[0])
    assert response is not None
