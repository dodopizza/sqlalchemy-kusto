"""KQL dialect against a live Kusto: every statement shape Superset builds, checked by numbers.

The dataset mirrors a Superset chart source: a physical table plus a virtual table
(KQL text) over it. Each test compiles a SQLAlchemy Select the way Superset does
(``literal_binds=True``) and executes the KQL through the dialect.
"""

import uuid
from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, column, create_engine, literal_column, select, text
from sqlalchemy.sql.selectable import TextAsFrom

from tests.integration.conftest import (
    AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID,
    DATABASE,
    KUSTO_KQL_ALCHEMY_URL,
    USES_EMULATOR,
)

engine = create_engine(
    f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}"
    if USES_EMULATOR
    else (
        f"{KUSTO_KQL_ALCHEMY_URL}/{DATABASE}?"
        f"msi=False&azure_ad_client_id={AZURE_AD_CLIENT_ID}&"
        f"azure_ad_client_secret={AZURE_AD_CLIENT_SECRET}&"
        f"azure_ad_tenant_id={AZURE_AD_TENANT_ID}"
    )
)

TABLE = "KqlEvents_" + uuid.uuid4().hex
QUOTED_EVENT = 'It\'s "quoted"'
# Id, Timestamp, EventName, Country, CustomerId, Platform, Amount
ROWS = [
    (1, "2026-01-05T10:00:00", "open_app", "ru", "c1", "iOS", "10"),
    (2, "2026-01-05T11:00:00", "sign_in", "kz", "c2", "iOS", "20"),
    (3, "2026-01-06T12:00:00", "open_app", "ru", "c1", "Android", "5"),
    (4, "2026-01-06T13:00:00", "open_app", "kz", "c3", "iOS", "7"),
    (5, "2026-01-07T09:00:00", "purchase", "ru", "c4", "iOS", "100"),
    (6, "2026-01-07T10:00:00", "open_app", "kz", "c2", "Android", "real(null)"),
    (7, "2026-02-01T00:00:00", "open_app", "ru", "c5", "iOS", "3"),
    (
        8,
        "2026-01-08T10:00:00",
        QUOTED_EVENT.replace('"', '\\"'),
        "ru",
        "c1",
        "iOS",
        "1",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def events_table():
    rows = ",\n".join(
        f'{id_}, datetime({ts}), "{event}", "{country}", "{customer}", "{platform}", {amount}'
        for id_, ts, event, country, customer, platform, amount in ROWS
    )
    with engine.connect() as connection:
        connection.execute(
            text(
                f".set-or-replace {TABLE} <| datatable(Id:long, Timestamp:datetime, "
                f"EventName:string, Country:string, CustomerId:string, Platform:string, "
                f"Amount:real) [\n{rows}\n]"
            )
        )
    yield
    with engine.connect() as connection:
        connection.execute(text(f".drop table {TABLE}"))


def run(query) -> list[tuple]:
    kql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(text(kql)).fetchall()]


def physical():
    return sa.table(TABLE)


def virtual(kql: str | None = None):
    kql = kql or f"{TABLE}\n| where Platform == 'iOS' or Platform == 'Android'"
    return TextAsFrom(text(kql), []).alias("virtual_table")


def label(name: str):
    return column(name).label(name)


def january():
    return and_(
        column("Timestamp") >= text("datetime(2026-01-01T00:00:00.000000)"),
        column("Timestamp") < text("datetime(2026-02-01T00:00:00.000000)"),
    )


count = literal_column("COUNT(*)").label("count")


@pytest.mark.parametrize("source", [physical, virtual], ids=["physical", "virtual"])
def test_timeseries_group_by(source):
    day = literal_column("startofday(Timestamp)").label("__timestamp")
    query = (
        select(
            [
                day,
                label("EventName"),
                count,
                literal_column("SUM(Amount)").label("sum_amount"),
            ]
        )
        .select_from(source())
        .where(january())
        .group_by(day, label("EventName"))
        .order_by(sa.asc(day), sa.desc(count), sa.asc(label("EventName")))
    )
    assert run(query) == [
        (datetime.fromisoformat("2026-01-05T00:00:00+00:00"), "open_app", 1, 10.0),
        (datetime.fromisoformat("2026-01-05T00:00:00+00:00"), "sign_in", 1, 20.0),
        (datetime.fromisoformat("2026-01-06T00:00:00+00:00"), "open_app", 2, 12.0),
        (
            datetime.fromisoformat("2026-01-07T00:00:00+00:00"),
            "open_app",
            1,
            0.0,
        ),  # Kusto sum() of nulls is 0
        (datetime.fromisoformat("2026-01-07T00:00:00+00:00"), "purchase", 1, 100.0),
        (datetime.fromisoformat("2026-01-08T00:00:00+00:00"), QUOTED_EVENT, 1, 1.0),
    ]


def test_having_filters_after_summarize():
    query = (
        select([label("Country"), count])
        .select_from(virtual())
        .where(january())
        .group_by(label("Country"))
        .having(text('["count"] > 3'))
    )
    assert run(query) == [("ru", 4)]


def test_having_written_the_sql_way():
    query = (
        select([label("Country"), count])
        .select_from(virtual())
        .where(january())
        .group_by(label("Country"))
        .having(text("(COUNT(*) > 3)"))  # Superset passes custom HAVING through sqlglot
    )
    assert run(query) == [("ru", 4)]


def test_distinct_values_for_column():
    query = (
        select([column("Country").label("column_values")])
        .select_from(virtual())
        .distinct()
        .order_by(sa.asc(column("Country").label("column_values")))
        .limit(100)
    )
    assert run(query) == [("kz",), ("ru",)]


@pytest.mark.parametrize("source", [physical, virtual], ids=["physical", "virtual"])
def test_series_limit_join(source):
    """Superset's top-N series: an inner join on a grouped, ordered, limited subquery."""
    inner_event = column("EventName").label("EventName__")
    inner_metric = literal_column("COUNT(*)").label("mme_inner__")
    subquery = (
        select([inner_event, inner_metric])
        .select_from(source())
        .where(january())
        .group_by(inner_event)
        .order_by(sa.desc(inner_metric))
        .limit(1)
    )
    joined = source().join(
        subquery.alias("series_limit"), label("EventName") == sa.column("EventName__")
    )
    query = (
        select([label("EventName"), count])
        .select_from(joined)
        .where(january())
        .group_by(label("EventName"))
    )
    assert run(query) == [("open_app", 4)]


def test_filters_in_like_isnull_between_or():
    def count_where(*criteria) -> int:
        query = select([count]).select_from(physical()).where(and_(*criteria))
        return run(query)[0][0]

    assert (
        count_where(column("Country").in_(["ru"]), column("EventName").like("open%"))
        == 3
    )
    assert count_where(column("Country").notin_(["ru", "kz"])) == 0
    assert count_where(column("EventName").ilike("%OPEN%")) == 5
    assert count_where(column("EventName").notlike("%app")) == 3
    assert count_where(column("EventName").like("o%_app")) == 5
    assert count_where(column("Amount").is_(None)) == 1
    assert count_where(column("Amount").isnot(None)) == 7
    assert count_where(column("Amount").between(5, 10)) == 3
    assert count_where(column("Id") == 1) == 1
    assert count_where(column("Id") != 1) == 7
    assert (
        count_where(
            sa.or_(column("Id") <= 2, column("Id") >= 7), column("Platform") == "iOS"
        )
        == 4
    )
    assert count_where(sa.not_(column("Country") == "ru")) == 3
    assert count_where(column("EventName") == QUOTED_EVENT) == 1
    assert count_where(column("EventName").like(f"%{QUOTED_EVENT}%")) == 1
    assert count_where(column("Timestamp") >= datetime(2026, 2, 1)) == 1


def test_sql_aggregate_functions():
    query = select(
        [
            sa.func.COUNT(column("Amount")).label("non_null"),
            sa.func.COUNT(sa.distinct(column("CustomerId"))).label("customers"),
            sa.func.count().label("all"),
            sa.func.SUM(column("Amount")).label("sum"),
            sa.func.MIN(column("Id")).label("min"),
            sa.func.MAX(column("Id")).label("max"),
            sa.func.AVG(column("Id")).label("avg"),
            literal_column("count(distinct Country)").label("countries"),
            literal_column("round(avg(Amount), 1)").label("expr_over_agg"),
        ]
    ).select_from(physical())
    assert run(query) == [(7, 5, 8, 146.0, 1, 8, 4.5, 2, 20.9)]


def test_virtual_table_with_lets_and_comments():
    kql = f"""// analyst's comment; with a semicolon
let Start = datetime(2026-01-06);
let Stop = datetime(2026-01-08); // trailing comment
{TABLE}
| where Timestamp between (Start .. Stop) and EventName != "a;b" // tail
"""
    query = select([count]).select_from(virtual(kql))
    assert run(query) == [(4,)]  # between is inclusive: rows 3, 4, 5, 6


def test_raw_rows_order_and_take():
    query = (
        select(
            [
                label("Id"),
                label("Timestamp"),
                literal_column("Amount * 2").label("double"),
            ]
        )
        .select_from(virtual())
        .order_by(sa.desc(label("Timestamp")))
        .limit(3)
    )
    assert [(row[0], row[2]) for row in run(query)] == [(7, 6.0), (8, 2.0), (6, None)]


def test_offset_pagination():
    query = (
        select([label("Id")])
        .select_from(physical())
        .order_by(sa.asc(label("Id")))
        .offset(2)
        .limit(3)
    )
    assert run(query) == [(3,), (4,), (5,)]


def test_schema_renders_as_database():
    query = select([count]).select_from(sa.table(TABLE, schema=DATABASE))
    assert run(query) == [(8,)]


def test_select_star_table_preview():
    # Superset's SQL Lab table preview: select * from text(quote_schema(s) + "." + quote(t))
    preparer = engine.dialect.identifier_preparer
    name = f"{preparer.quote_schema(DATABASE)}.{preparer.quote(TABLE)}"
    query = select([count]).select_from(text(name))
    assert run(query) == [(8,)]


def test_join_two_physical_tables():
    other = "KqlIds_" + uuid.uuid4().hex
    with engine.connect() as connection:
        connection.execute(
            text(f".set-or-replace {other} <| datatable(Id:long) [1, 2, 3]")
        )
    try:
        left = sa.table(TABLE, column("Id"), column("Country"))
        right = sa.table(other, column("Id"))
        query = (
            select([left.c.Id, left.c.Country])
            .select_from(left.join(right, left.c.Id == right.c.Id))
            .where(left.c.Id > 1)
            .order_by(sa.asc(left.c.Id))
        )
        assert run(query) == [(2, "kz"), (3, "ru")]
    finally:
        with engine.connect() as connection:
            connection.execute(text(f".drop table {other}"))


def test_orm_query_with_label_reference_order_by():
    session = sessionmaker(bind=engine)()
    query = (
        session.query(sa.func.count(text("Id")).label("tag_count"))
        .add_columns(sa.Column("Country", sa.String))
        .select_from(physical())
        .group_by(text("Country"))
        .order_by("tag_count")
    )
    assert run(query.statement) == [(3, "kz"), (5, "ru")]
