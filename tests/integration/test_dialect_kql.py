import logging
import uuid
from datetime import datetime, timedelta
import csv
import io

import pytest
from azure.kusto.data import (
    ClientRequestProperties,
    KustoClient,
    KustoConnectionStringBuilder,
)
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    text,
    literal_column,
)
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
        session.query(func.count(text("Id")).label("tag_count"))
        .add_columns(Column("Text", String))
        .select_from(table)
        .group_by(text("Text"))
        .order_by("tag_count")
    )
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        # SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}
        # convert the above query to using alchemy
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert {(x[0], x[1]) for x in result.fetchall()} == {
            (5, "value_1"),
            (4, "value_0"),
        }


# Test without group
def test_count_by(temp_table_name):
    # Convert the query: SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(func.count(text("Id")).label("tag_count")).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert {(x[0]) for x in result.fetchall()} == {9}


def test_distinct_counts_by(temp_table_name):
    # Convert to : SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(
        func.count(func.distinct(text("Text"))).label("tag_count")
    ).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert {(x[0]) for x in result.fetchall()} == {2}


@pytest.mark.parametrize(
    ("f", "label", "expected"),
    [
        pytest.param(func.min(text("Id")), "Min", 1),
        pytest.param(func.max(text("Id")), "Max", 9),
        pytest.param(func.sum(text("Id")), "Sum", 45),
    ],
)
def test_all_group_ops(f, label, expected, temp_table_name):
    # Convert : SELECT count(distinct (case when Id%2=0 THEN 'Even' end)) as tag_count FROM {temp_table_name}
    table = Table(
        temp_table_name,
        metadata,
    )
    query = session.query(f.label(label)).select_from(table)
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_compiled))
        # There is Even and Empty only for this test, 2 distinct values
        assert {(x[0]) for x in result.fetchall()} == {expected}


@pytest.mark.parametrize(
    ("test_label", "group_fn", "expected", "compare_dates"),
    [
        pytest.param("StartOfDay", 'startofday("DateF")', 1, True),
        pytest.param("Bin", 'bin("DateF",1d)', 1, True),
        pytest.param("StartOfDayWithBin", 'bin(startofday("DateF"),1d)', 1, True),
        pytest.param("StartOfDayWithBinUnquoted", "bin(startofday(DateF),1d)", 1, True),
        pytest.param("StartOfDayUnquoted", "startofday(DateF)", 1, True),
        pytest.param("BinUnquoted", "bin(DateF,1d)", 1, True),
        pytest.param("IngestionTimeFunction", "bin(ingestion_time(),1d)", 9, False),
        pytest.param("IngestionTimeFunction", "startofday(ingestion_time())", 9, False),
    ],
)
# There is a minor issue in ho ingestion_time() is handled.
# The following is an issue as in the project operator, the ingestion_time() is returned as null, probably because the
# grouped records from the summarize line drop that. Nonetheless, the test is still valid as the ingestion_time() can
# be used for grouping and also verifying if functions get quoted
# | summarize ["tag_count"] = count(["Host"])  by bin(ingestion_time(), 1d)
# | project ["tag_count"], bin(ingestion_time(), 1d)
def test_date_bin_ops(test_label, group_fn, temp_table_name, expected, compare_dates):
    table = Table(
        temp_table_name,
        metadata,
    )
    date_col = literal_column(group_fn)
    query = (
        session.query(func.count(text("Id")).label("tag_count"))
        .add_columns(date_col)
        .select_from(table)
        .group_by(date_col)
        .order_by(date_col)
    )
    query_compiled = str(query.statement.compile(kql_engine)).replace("\n", "")
    query_with_comment = f"""
        //{test_label}
        {query_compiled}
    """
    with kql_engine.connect() as connection:
        result = connection.execute(text(query_with_comment))
        actual_result = (
            {(x[0], x[1].strftime("%Y-%m-%dT%H:%M:%SZ")) for x in result.fetchall()}
            if compare_dates
            else {(y[0]) for y in result.fetchall()}
        )
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        expected_records = (
            {
                (expected, (now - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                for i in range(9, 0, -1)
            }
            if compare_dates
            else {expected}
        )
        assert actual_result == expected_records


def get_kcsb():
    return (
        KustoConnectionStringBuilder.with_az_cli_authentication(KUSTO_URL)
        if not AZURE_AD_CLIENT_ID
        and not AZURE_AD_CLIENT_SECRET
        and not AZURE_AD_TENANT_ID
        else KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_URL, AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID
        )
    )


def _create_temp_table(table_name: str):
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create table {table_name}(Id: int, Text: string, DateF: datetime)",
        ClientRequestProperties(),
    )


def _create_temp_fn(fn_name: str):
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create function {fn_name}() {{ print now()}}",
        ClientRequestProperties(),
    )


def _ingest_data_to_table(table_name: str):
    client = KustoClient(get_kcsb())
    now = datetime.now()
    data_to_ingest = [
        (
            i,
            "value_" + str(i % 2),
            (now - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        for i in range(1, 10)
    ]
    output = io.StringIO()
    csv_writer = csv.writer(output, delimiter=",")
    csv_writer.writerows(data_to_ingest)
    # Get the CSV string content
    str_data = output.getvalue().rstrip("\n").rstrip("\r")
    ingest_query = f""".ingest inline into table {table_name} <|
            {str_data} with (policy_ingestiontime=true)"""
    client.execute(DATABASE, ingest_query, ClientRequestProperties())


def _drop_table(table_name: str):
    client = KustoClient(get_kcsb())

    _ = client.execute(DATABASE, f".drop table {table_name}", ClientRequestProperties())
    _ = client.execute(
        DATABASE, f".drop function {table_name}_fn", ClientRequestProperties()
    )


@pytest.fixture
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
