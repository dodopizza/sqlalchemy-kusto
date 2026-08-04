import uuid
from datetime import datetime

import pytest
from azure.kusto.data import ClientRequestProperties, KustoClient
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    exc,
    func,
    select,
)
from tests.integration.conftest import (
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    DATABASE,
    KUSTO_KQL_ALCHEMY_URL,
    KUSTO_SQL_ALCHEMY_URL,
    USES_EMULATOR,
    get_kcsb,
)

engine = create_engine(
    f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}"
    if USES_EMULATOR
    else (
        f"{KUSTO_SQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
)


ROW_COUNT = 9  # rows the fixture ingests into the temp table
NESTED_LIMIT = 5  # outer limit used by the wrapped-query tests
EVENTS_ROW_COUNT = 3  # rows in the datetime fixture


def test_ping():
    conn = engine.connect()
    result = engine.dialect.do_ping(conn)
    assert result is True


def test_get_table_names(temp_table_name):
    conn = engine.connect()
    result = engine.dialect.get_table_names(conn)
    assert temp_table_name in result


def test_get_view_names(temp_table_name):
    conn = engine.connect()
    result = engine.dialect.get_view_names(conn)
    assert f"{temp_table_name}_fn" in result


def test_get_columns(temp_table_name):
    conn = engine.connect()
    columns_result = engine.dialect.get_columns(conn, temp_table_name)
    assert {"Id", "Text"} == {c["name"] for c in columns_result}


def test_fetch_one(temp_table_name):
    engine.connect()
    result = engine.execute(f"select top 2 * from {temp_table_name} order by Id")
    assert result.fetchone() == (1, "value_1")
    assert result.fetchone() == (2, "value_2")
    assert result.fetchone() is None


def test_fetch_many(temp_table_name):
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


def test_fetch_all(temp_table_name):
    engine.connect()
    result = engine.execute(f"select top 3 * from {temp_table_name} order by Id")
    assert {(x[0], x[1]) for x in result.fetchall()} == {
        (1, "value_1"),
        (2, "value_2"),
        (3, "value_3"),
    }


def test_limit(temp_table_name):
    limit_rec_count = 5
    stream = Table(
        temp_table_name,
        MetaData(),
        Column("Id", Integer),
        Column("Text", String),
    )

    query = stream.select().limit(limit_rec_count)

    engine.connect()
    result = engine.execute(query)
    result_length = len(result.fetchall())
    assert result_length == limit_rec_count


def _table(name: str) -> Table:
    return Table(
        name,
        MetaData(),
        Column("Id", Integer),
        Column("Text", String),
    )


def test_top_level_order_by_is_sorted(temp_table_name):
    """A top-level ORDER BY needs no TOP, so the dialect must not add one."""
    table = _table(temp_table_name)

    engine.connect()
    result = engine.execute(table.select().order_by(table.c.Id.desc()))
    ids = [row[0] for row in result.fetchall()]
    assert ids == sorted(ids, reverse=True)
    assert len(ids) == ROW_COUNT  # nothing was capped away


def test_nested_order_by_is_sorted(temp_table_name):
    """Superset's WRAP_SQL shape: outer limit over an inner ordered SELECT."""
    table = _table(temp_table_name)
    inner = table.select().order_by(table.c.Id.desc()).alias("virtual_table")

    engine.connect()
    result = engine.execute(inner.select().limit(NESTED_LIMIT))
    ids = [row[0] for row in result.fetchall()]
    assert ids == sorted(ids, reverse=True)
    assert len(ids) == NESTED_LIMIT


def test_nested_order_by_without_top_is_rejected_by_kusto(temp_table_name):
    """Why the TOP fallback exists: Kusto refuses this shape outright.

    If this ever starts passing, Kusto changed and the fallback can be reconsidered.
    """
    engine.connect()
    with pytest.raises(exc.DatabaseError):
        engine.execute(
            f"select top 5 Id from (select Id from {temp_table_name} order by Id desc) as v"
        ).fetchall()


@pytest.mark.parametrize(
    ("grain", "expected"),
    [
        ("second", datetime(2024, 5, 17, 10, 30, 45)),
        ("minute", datetime(2024, 5, 17, 10, 30)),
        ("hour", datetime(2024, 5, 17, 10, 0)),
        ("day", datetime(2024, 5, 17)),
        ("week", datetime(2024, 5, 13)),  # Monday of that week
        ("week_sun", datetime(2024, 5, 12)),  # Sunday of that week
        ("month", datetime(2024, 5, 1)),
        ("quarter", datetime(2024, 4, 1)),
        ("year", datetime(2024, 1, 1)),
    ],
)
def test_raw_date_trunc(temp_events_table, grain: str, expected: datetime):
    """date_trunc() typed by hand in SQLLab must reach Kusto as DATEADD/DATEDIFF."""
    engine.connect()
    result = engine.execute(
        f"select top 1 date_trunc('{grain}', Ts) as truncated "
        f"from {temp_events_table} order by Ts"
    )
    assert result.fetchone()[0].replace(tzinfo=None) == expected


def test_compiled_date_trunc(temp_events_table):
    """The same grain, this time going through func.date_trunc and the dialect hook."""
    events = Table(
        temp_events_table,
        MetaData(),
        Column("Id", Integer),
        Column("Ts", DateTime),
    )

    engine.connect()
    query = (
        select([func.date_trunc("week", events.c.Ts)]).order_by(events.c.Ts).limit(1)
    )
    truncated = engine.execute(query).fetchone()[0]
    assert truncated.replace(tzinfo=None) == datetime(2024, 5, 13)


@pytest.mark.parametrize("grain", ["milliseconds", "decade"])
def test_unsupported_date_trunc_grain_is_rejected(temp_events_table, grain: str):
    """Unsupported grains reach Kusto untouched and fail loudly instead of silently."""
    engine.connect()
    with pytest.raises(exc.DatabaseError):
        engine.execute(
            f"select top 1 date_trunc('{grain}', Ts) from {temp_events_table}"
        ).fetchall()


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        # spellings analysts actually used in production SQLLab queries
        ("DATETRUNC('hour', Ts)", datetime(2024, 5, 17, 10, 0)),
        ("DATETRUNC(month, Ts)", datetime(2024, 5, 1)),
        ("startofday(Ts)", datetime(2024, 5, 17)),
        ("startofmonth(Ts)", datetime(2024, 5, 1)),
        ("startofweek(Ts)", datetime(2024, 5, 12)),  # KQL weeks start on Sunday
        ("DATE(Ts)", datetime(2024, 5, 17)),
        ("to_date(Ts)", datetime(2024, 5, 17)),
    ],
)
def test_translated_date_functions(
    temp_events_table, expression: str, expected: datetime
):
    """Functions Kusto rejects verbatim must work through the translation."""
    engine.connect()
    result = engine.execute(
        f"select top 1 {expression} as value from {temp_events_table} order by Ts"
    )
    assert result.fetchone()[0].replace(tzinfo=None) == expected


def test_translated_scalar_functions(temp_events_table):
    """date_part and ifnull have exact T-SQL equivalents; check both on real data."""
    engine.connect()
    row = engine.execute(
        f"select top 1 DATE_PART(hour, Ts) as h, IFNULL(Id, -1) as i "
        f"from {temp_events_table} order by Ts"
    ).fetchone()
    assert (row[0], row[1]) == (10, 1)


def test_cte_is_sent_as_tsql(temp_events_table):
    """A query starting with WITH used to be sent as KQL and fail before reaching SQL."""
    engine.connect()
    result = engine.execute(
        f"with daily as (select date_trunc('day', Ts) as d from {temp_events_table}) "
        f"select top 1 d from daily order by d"
    )
    assert result.fetchone()[0].replace(tzinfo=None) == datetime(2024, 5, 17)


def test_kql_query_still_runs_on_the_kql_dialect(temp_events_table):
    """The SQL/KQL split must not misroute a KQL query that starts with a table name."""
    kql_engine = create_engine(
        f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}"
        if USES_EMULATOR
        else (
            f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
            f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
            f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
            f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
        )
    )
    kql_engine.connect()
    rows = kql_engine.execute(f"{temp_events_table} | count").fetchall()
    assert rows[0][0] == EVENTS_ROW_COUNT


def _create_temp_table(table_name: str):
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create table {table_name}(Id: int, Text: string)",
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
    data_to_ingest = {i: "value_" + str(i) for i in range(1, ROW_COUNT + 1)}
    str_data = "\n".join("{},{}".format(*p) for p in data_to_ingest.items())
    ingest_query = f""".ingest inline into table {table_name} <|
            {str_data}"""
    client.execute(DATABASE, ingest_query, ClientRequestProperties())


def _drop_table(table_name: str):
    client = KustoClient(get_kcsb())

    _ = client.execute(DATABASE, f".drop table {table_name}", ClientRequestProperties())
    _ = client.execute(
        DATABASE, f".drop function {table_name}_fn", ClientRequestProperties()
    )


@pytest.fixture
def temp_table_name():
    return "_temp_" + uuid.uuid4().hex


@pytest.fixture(autouse=True)
def run_around_tests(temp_table_name):
    _create_temp_table(temp_table_name)
    _create_temp_fn(f"{temp_table_name}_fn")
    _ingest_data_to_table(temp_table_name)
    # A test function will be run at this point
    yield temp_table_name
    _drop_table(temp_table_name)


@pytest.fixture
def temp_events_table():
    """Table with a datetime column, for the date_trunc tests.

    Timestamps straddle a week boundary so Monday-based and Sunday-based truncation
    give different answers: 2024-05-17 is a Friday, 05-13 its Monday, 05-12 its Sunday.
    """
    table_name = "_events_" + uuid.uuid4().hex
    client = KustoClient(get_kcsb())
    client.execute(
        DATABASE,
        f".create table {table_name}(Id: int, Ts: datetime)",
        ClientRequestProperties(),
    )
    client.execute(
        DATABASE,
        f".ingest inline into table {table_name} <|\n"
        "1,2024-05-17T10:30:45\n"
        "2,2024-05-18T23:59:59\n"
        "3,2024-05-20T00:00:01",
        ClientRequestProperties(),
    )
    yield table_name
    client.execute(DATABASE, f".drop table {table_name}", ClientRequestProperties())
