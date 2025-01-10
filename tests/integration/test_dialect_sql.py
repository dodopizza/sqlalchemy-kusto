from collections.abc import Generator
from typing import Any
import uuid

import pytest
from azure.kusto.data import (
    ClientRequestProperties,
    KustoClient,
    KustoConnectionStringBuilder,
)
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

from tests.integration.conftest import (
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    DATABASE,
    KUSTO_SQL_ALCHEMY_URL,
    KUSTO_URL,
)

engine = create_engine(
    f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)


def test_ping() -> None:
    conn = engine.connect()
    result = engine.dialect.do_ping(conn)
    assert result is True


def test_get_table_names(temp_table_name: str) -> None:
    conn = engine.connect()
    result = engine.dialect.get_table_names(conn)
    assert temp_table_name in result


def test_get_view_names(temp_table_name: str) -> None:
    conn = engine.connect()
    result = engine.dialect.get_view_names(conn)
    assert f"{temp_table_name}_fn" in result


def test_get_columns(temp_table_name: str) -> None:
    conn = engine.connect()
    columns_result = engine.dialect.get_columns(conn, temp_table_name)
    assert {"Id", "Text"} == {c["name"] for c in columns_result}


def test_fetch_one(temp_table_name: str) -> None:
    engine.connect()
    result = engine.execute(f"select top 2 * from {temp_table_name} order by Id")
    assert result.fetchone() == (1, "value_1")
    assert result.fetchone() == (2, "value_2")
    assert result.fetchone() is None


def test_fetch_many(temp_table_name: str) -> None:
    engine.connect()
    result = engine.execute(f"select top 5 * from {temp_table_name} order by Id")

    assert {(x[0], x[1]) for x in result.fetchmany(3)} == {
        (1, "value_1"),
        (2, "value_2"),
        (3, "value_3"),
    }
    assert {(x[0], x[1]) for x in result.fetchmany(3)} == {
        (4, "value_4"),
        (5, "value_5"),
    }


def test_fetch_all(temp_table_name: str) -> None:
    engine.connect()
    result = engine.execute(f"select top 3 * from {temp_table_name} order by Id")
    assert {(x[0], x[1]) for x in result.fetchall()} == {
        (1, "value_1"),
        (2, "value_2"),
        (3, "value_3"),
    }


def test_limit(temp_table_name: str) -> None:
    limit = 5
    stream = Table(
        temp_table_name,
        MetaData(),
        Column("Id", Integer),
        Column("Text", String),
    )

    query = stream.select().limit(limit)

    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == limit


def get_kcsb() -> Any:
    return (
        KustoConnectionStringBuilder.with_az_cli_authentication(KUSTO_URL)
        if not AZURE_AD_CLIENT_ID
        and not AZURE_AD_CLIENT_SECRET
        and not AZURE_AD_TENANT_ID
        else KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
        )
    )


def _create_temp_table(table_name: str) -> None:
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create table {table_name}(Id: int, Text: string)",
        ClientRequestProperties(),
    )


def _create_temp_fn(fn_name: str) -> None:
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create function {fn_name}() {{ print now()}}",
        ClientRequestProperties(),
    )


def _ingest_data_to_table(table_name: str) -> None:
    client = KustoClient(get_kcsb())
    data_to_ingest = {i: "value_" + str(i) for i in range(1, 10)}
    str_data = "\n".join("{},{}".format(*p) for p in data_to_ingest.items())
    ingest_query = f""".ingest inline into table {table_name} <|
            {str_data}"""
    client.execute(DATABASE, ingest_query, ClientRequestProperties())


def _drop_table(table_name: str) -> None:
    client = KustoClient(get_kcsb())

    _ = client.execute(DATABASE, f".drop table {table_name}", ClientRequestProperties())
    _ = client.execute(
        DATABASE, f".drop function {table_name}_fn", ClientRequestProperties()
    )


@pytest.fixture
def temp_table_name() -> str:
    return "_temp_" + uuid.uuid4().hex


@pytest.fixture(autouse=True)
def run_around_tests(temp_table_name: str) -> Generator[str, None, None]:
    _create_temp_table(temp_table_name)
    _create_temp_fn(f"{temp_table_name}_fn")
    _ingest_data_to_table(temp_table_name)
    # A test function will be run at this point
    yield temp_table_name
    _drop_table(temp_table_name)
