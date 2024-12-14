import logging
import uuid

import pytest
from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from sqlalchemy import Column, Integer, MetaData, String, Table, case, create_engine, func, literal_column, select, text
from sqlalchemy.orm import sessionmaker

from tests.integration.conftest import (
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    DATABASE,
    KUSTO_KQL_ALCHEMY_URL,
    KUSTO_URL,
)

logger = logging.getLogger(__name__)

kql_engine = create_engine(
    f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
    f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
    f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
    f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
)

Session = sessionmaker(bind=kql_engine)
session = Session()
metadata = MetaData()


def test_group_by(temp_table_name):
    table = Table(
        temp_table_name,
        metadata,
    )
    query = (
        session.query(func.count(text("Id")).label("tag_count")).add_columns(Column("Text", String))
        .select_from(table)
        .group_by(text("Text"))
        .order_by("tag_count")
    )
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        # f"SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}"
        # convert the above query to using alchemy
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert set([(x[0], x[1]) for x in result.fetchall()]) == set([(5, "value_1"), (4, "value_0")])


# Test without group
def test_count_by(temp_table_name):
    # f"SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}"
    # convert the above query to using alchemy
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(func.count(text("Id")).label("tag_count")).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert set([(x[0]) for x in result.fetchall()]) == set([9])


def test_distinct_counts_by(temp_table_name):
    # f"SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}"
    # convert the above query to using alchemy
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(func.count(func.distinct(text("Text"))).label("tag_count")).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert set([(x[0]) for x in result.fetchall()]) == set([2])


@pytest.mark.parametrize(
    "f,label,expected",
    [
        pytest.param(func.min(text("Id")), "Min", 1),
        pytest.param(func.max(text("Id")), "Max", 9),
        pytest.param(func.sum(text("Id")), "Sum", 45),
    ],
)
def test_all_group_ops(f, label, expected, temp_table_name):
    # f"SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}"
    # convert the above query to using alchemy
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(f.label(label)).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert set([(x[0]) for x in result.fetchall()]) == set([expected])


def get_kcsb():
    return (
        KustoConnectionStringBuilder.with_az_cli_authentication(KUSTO_URL)
        if not AZURE_AD_CLIENT_ID and not AZURE_AD_CLIENT_SECRET and not AZURE_AD_TENANT_ID
        else KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
        )
    )


def _create_temp_table(table_name: str):
    client = KustoClient(get_kcsb())
    response = client.execute(DATABASE, f".create table {table_name}(Id: int, Text: string)", ClientRequestProperties())


def _create_temp_fn(fn_name: str):
    client = KustoClient(get_kcsb())
    response = client.execute(DATABASE, f".create function {fn_name}() {{ print now()}}", ClientRequestProperties())


def _ingest_data_to_table(table_name: str):
    client = KustoClient(get_kcsb())
    data_to_ingest = {i: "value_" + str(i % 2) for i in range(1, 10)}
    str_data = "\n".join("{},{}".format(*p) for p in data_to_ingest.items())
    ingest_query = f""".ingest inline into table {table_name} <|
            {str_data}"""
    response = client.execute(DATABASE, ingest_query, ClientRequestProperties())


def _drop_table(table_name: str):
    client = KustoClient(get_kcsb())

    _ = client.execute(DATABASE, f".drop table {table_name}", ClientRequestProperties())
    _ = client.execute(DATABASE, f".drop function {table_name}_fn", ClientRequestProperties())


@pytest.fixture()
def temp_table_name():
    return "_temp_" + uuid.uuid4().hex + "_kql"


@pytest.fixture(autouse=True)
def run_around_tests(temp_table_name):
    _create_temp_table(temp_table_name)
    _create_temp_fn(f"{temp_table_name}_fn")
    _ingest_data_to_table(temp_table_name)
    # A test function will be run at this point
    yield temp_table_name
    _drop_table(temp_table_name)
    # assert files_before == files_after
