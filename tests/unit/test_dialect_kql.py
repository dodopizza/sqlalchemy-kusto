"""Exact KQL text for every statement shape Superset builds, plus the L1–L7 regressions.

The forms come from superset/models/helpers.py (get_sqla_query): Superset compiles
with literal_binds=True, so bind parameters never reach the DBAPI.
"""

from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    and_,
    column,
    create_engine,
    literal_column,
    select,
    text,
)
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.selectable import TextAsFrom

from sqlalchemy_kusto.dialect_kql import _normalize_kql_text, split_statements

engine = create_engine("kustokql+https://localhost/testdb")

DATASET = (
    'DrinkitAppEvents\n| where Platform == "iOS"\n| extend Amount = toreal(Data.amount)'
)
LET_VIRTUAL = 'let virtual_table = (DrinkitAppEvents\n| where Platform == "iOS"\n| extend Amount = toreal(Data.amount));\nvirtual_table\n'
TIME_RANGE = (
    '| where ["Timestamp"] >= datetime(2026-01-01T00:00:00.000000) '
    'and ["Timestamp"] < datetime(2026-02-01T00:00:00.000000)\n'
)


def compile_kql(query) -> str:
    return str(query.compile(engine, compile_kwargs={"literal_binds": True}))


def virtual():
    return TextAsFrom(text(DATASET), []).alias("virtual_table")


def label(name: str):
    return column(name).label(name)


def time_range():
    # Superset renders the boundaries itself (KustoKqlEngineSpec.convert_dttm)
    return and_(
        column("Timestamp") >= text("datetime(2026-01-01T00:00:00.000000)"),
        column("Timestamp") < text("datetime(2026-02-01T00:00:00.000000)"),
    )


count = literal_column("COUNT(*)").label("count")  # Superset's default metric
day = literal_column("startofday(Timestamp)").label("__timestamp")  # time grain P1D


def test_timeseries_group_by_virtual_dataset():
    query = (
        select(
            [
                day,
                label("EventName"),
                count,
                literal_column("SUM(Amount)").label("sum_amount"),
            ]
        )
        .select_from(virtual())
        .where(time_range())
        .group_by(day, label("EventName"))
        .order_by(sa.desc(count))
        .limit(1000)
    )
    assert compile_kql(query) == (
        LET_VIRTUAL
        + TIME_RANGE
        + '| summarize ["count"] = count(), ["sum_amount"] = sum(Amount) '
        'by ["__timestamp"] = startofday(Timestamp), ["EventName"]\n'
        '| order by ["count"] desc\n'
        '| project ["__timestamp"], ["EventName"], ["count"], ["sum_amount"]\n'
        "| take 1000"
    )


def test_physical_table_with_filters():
    query = (
        select([label("Country"), literal_column("dcount(CustomerId)").label("uniq")])
        .select_from(sa.table("DrinkitAppEvents"))
        .where(
            and_(
                time_range(),
                column("Country").in_(["ru", "kz"]),
                column("EventName").like("open%"),
                column("Amount") > 10,
                column("Country").isnot(None),
                text('EventName != "x"'),  # custom SQL WHERE passes through
            )
        )
        .group_by(label("Country"))
        .order_by(sa.desc(literal_column("dcount(CustomerId)").label("uniq")))
        .limit(1000)
    )
    assert compile_kql(query) == (
        '["DrinkitAppEvents"]\n'
        + TIME_RANGE.rstrip("\n")
        + ' and ["Country"] in ("ru", "kz") and ["EventName"] startswith_cs "open" '
        'and ["Amount"] > 10 and isnotnull(["Country"]) and EventName != "x"\n'
        '| summarize ["uniq"] = dcount(CustomerId) by ["Country"]\n'
        '| order by ["uniq"] desc\n'
        '| project ["Country"], ["uniq"]\n'
        "| take 1000"
    )


def test_l1_saved_metric_without_extra_arguments():
    query = select([literal_column("dcount(CustomerId)").label("uniq")]).select_from(
        virtual()
    )
    assert (
        compile_kql(query)
        == LET_VIRTUAL + '| summarize ["uniq"] = dcount(CustomerId)\n| project ["uniq"]'
    )


def test_l2_having_becomes_where_after_summarize():
    query = (
        select([label("Country"), count])
        .select_from(virtual())
        .group_by(label("Country"))
        .having(text("count > 5"))
    )
    assert compile_kql(query) == (
        LET_VIRTUAL + '| summarize ["count"] = count() by ["Country"]\n'
        "| where count > 5\n"
        '| project ["Country"], ["count"]'
    )


def test_l3_distinct_for_filter_values():
    query = (
        select([column("Country").label("column_values")])
        .select_from(virtual())
        .distinct()
        .limit(100)
    )
    assert compile_kql(query) == (
        LET_VIRTUAL
        + '| extend ["column_values"] = ["Country"]\n| distinct ["column_values"]\n| take 100'
    )


@pytest.mark.parametrize("virtual_dataset", [True, False], ids=["virtual", "physical"])
def test_l4_series_limit_join(virtual_dataset):
    source = virtual() if virtual_dataset else sa.table("DrinkitAppEvents")
    inner_event = column("EventName").label("EventName__")
    inner_metric = literal_column("COUNT(*)").label("mme_inner__")
    subquery = (
        select([inner_event, inner_metric])
        .select_from(source)
        .where(time_range())
        .group_by(inner_event)
        .order_by(sa.desc(inner_metric))
        .limit(5)
    )
    joined = source.join(
        subquery.alias("series_limit"), label("EventName") == sa.column("EventName__")
    )
    query = (
        select([day, label("EventName"), count])
        .select_from(joined)
        .where(time_range())
        .group_by(day, label("EventName"))
        .order_by(sa.desc(count))
        .limit(1000)
    )
    head = LET_VIRTUAL if virtual_dataset else '["DrinkitAppEvents"]\n'
    inner_source = "virtual_table" if virtual_dataset else '["DrinkitAppEvents"]'
    assert compile_kql(query) == (
        head
        + f"| join kind=inner ({inner_source}\n"
        + TIME_RANGE
        + '| summarize ["mme_inner__"] = count() by ["EventName__"] = ["EventName"]\n'
        '| order by ["mme_inner__"] desc\n'
        '| project ["EventName__"], ["mme_inner__"]\n'
        '| take 5) on $left.["EventName"] == $right.["EventName__"]\n'
        + TIME_RANGE
        + '| summarize ["count"] = count() by ["__timestamp"] = startofday(Timestamp), ["EventName"]\n'
        '| order by ["count"] desc\n'
        '| project ["__timestamp"], ["EventName"], ["count"]\n'
        "| take 1000"
    )


def test_series_limit_over_computed_column_extends_before_join():
    lower = literal_column("tolower(Country)").label("cc")
    subquery = (
        select([literal_column("tolower(Country)").label("cc__"), count])
        .select_from(virtual())
        .group_by(literal_column("tolower(Country)").label("cc__"))
        .limit(5)
    )
    joined = virtual().join(subquery.alias("series_limit"), lower == sa.column("cc__"))
    query = select([lower, count]).select_from(joined).group_by(lower)
    assert compile_kql(query) == (
        LET_VIRTUAL.rstrip("\n") + '\n| extend ["cc"] = tolower(Country)\n'
        "| join kind=inner (virtual_table\n"
        '| summarize ["count"] = count() by ["cc__"] = tolower(Country)\n'
        '| project ["cc__"], ["count"]\n'
        '| take 5) on $left.["cc"] == $right.["cc__"]\n'
        '| summarize ["count"] = count() by ["cc"] = tolower(Country)\n'
        '| project ["cc"], ["count"]'
    )


def test_l5_lets_and_comments_are_hoisted():
    dataset = """// комментарий аналитика; с точкой с запятой

let StartDate = todatetime("2022-09-01");
let EndDate = endofday(todatetime("2029-05-30")); // trailing

DrinkitAppEvents
| where Timestamp between (StartDate..EndDate) and EventName == "a;b" // tail
"""
    query = select([count]).select_from(
        TextAsFrom(text(dataset), []).alias("virtual_table")
    )
    assert compile_kql(query) == (
        'let StartDate = todatetime("2022-09-01");\n'
        'let EndDate = endofday(todatetime("2029-05-30"));\n'
        "let virtual_table = (DrinkitAppEvents\n"
        '| where Timestamp between (StartDate..EndDate) and EventName == "a;b");\n'
        "virtual_table\n"
        '| summarize ["count"] = count()\n'
        '| project ["count"]'
    )


def test_lets_are_emitted_once_when_the_virtual_table_is_joined_to_itself():
    dataset = "let x = 1;\nT | where a == x"
    source = TextAsFrom(text(dataset), []).alias("virtual_table")
    subquery = select([column("k").label("k__")]).select_from(source).limit(1)
    query = select([label("k")]).select_from(
        source.join(subquery.alias("s"), label("k") == column("k__"))
    )
    assert compile_kql(query).count("let x = 1;") == 1
    assert compile_kql(query).startswith(
        "let x = 1;\nlet virtual_table = (T | where a == x);\nvirtual_table\n"
    )


def test_l6_string_literals_keep_operators_and_quotes():
    query = (
        select([label("Id")])
        .select_from(virtual())
        .where(column("Name") == "a=b <> 'c' \"d\" \\e")
    )
    assert compile_kql(query) == (
        LET_VIRTUAL
        + '| where ["Name"] == "a=b <> \'c\' \\"d\\" \\\\e"\n| project ["Id"]'
    )


def test_l7_no_extend_noise_for_group_by_columns():
    query = (
        select([label("EventName"), count])
        .select_from(virtual())
        .group_by(label("EventName"))
    )
    assert '| extend ["' not in compile_kql(query)
    assert '| summarize ["count"] = count() by ["EventName"]' in compile_kql(query)


def test_superset_raw_rows_group_by_every_column():
    # Superset raw mode with a time column: the same expression under two labels, GROUP BY all
    query = (
        select(
            [
                column("Timestamp").label("__timestamp"),
                label("Timestamp"),
                label("EventName"),
            ]
        )
        .select_from(sa.table("T"))
        .group_by(column("Timestamp"), column("EventName"), column("Timestamp"))
        .order_by(sa.desc(label("Timestamp")))
    )
    assert compile_kql(query) == (
        '["T"]\n| summarize by ["__timestamp"] = ["Timestamp"], ["Timestamp"], ["EventName"]\n'
        '| order by ["Timestamp"] desc\n'
        '| project ["__timestamp"], ["Timestamp"], ["EventName"]'
    )


def test_series_limit_subquery_orders_by_the_metric_alias():
    # Superset orders the inner query by a fresh Label("count", count(*)) while the
    # select list carries the same expression as mme_inner__
    inner = literal_column("COUNT(*)").label("mme_inner__")
    query = (
        select([column("EventName").label("EventName__"), inner])
        .select_from(sa.table("T"))
        .group_by(column("EventName").label("EventName__"))
        .order_by(sa.desc(literal_column("count(*)").label("count")))
        .limit(5)
    )
    assert '| order by ["mme_inner__"] desc' in compile_kql(query)


def test_having_repeats_the_aggregate_the_sql_way():
    query = (
        select([label("Country"), count, literal_column("SUM(Amount)").label("total")])
        .select_from(sa.table("T"))
        .group_by(label("Country"))
        .having(text("(COUNT(*) > 5 AND sum(Amount) > 100)"))
    )
    assert '| where (["count"] > 5 AND ["total"] > 100)' in compile_kql(query)


def test_superset_escaped_colons_in_text_are_unescaped():
    query = (
        select([count])
        .select_from(sa.table("T"))
        .where(column("T") >= text("datetime(2026-01-01T00\\:00\\:00)"))
    )
    assert '| where ["T"] >= datetime(2026-01-01T00:00:00)' in compile_kql(query)


def test_empty_conjunction_emits_no_where():
    query = select([count]).select_from(sa.table("T")).where(sa.and_())
    assert (
        compile_kql(query)
        == '["T"]\n| summarize ["count"] = count()\n| project ["count"]'
    )


def test_raw_rows_with_calculated_column():
    conv = literal_column("toreal(a) / toreal(b)").label("conv")
    query = (
        select([label("Timestamp"), label("EventName"), conv])
        .select_from(virtual())
        .where(time_range())
        .order_by(sa.desc(label("Timestamp")))
        .limit(1000)
    )
    assert compile_kql(query) == (
        LET_VIRTUAL + TIME_RANGE + '| extend ["conv"] = toreal(a) / toreal(b)\n'
        '| order by ["Timestamp"] desc\n'
        '| project ["Timestamp"], ["EventName"], ["conv"]\n'
        "| take 1000"
    )


def test_order_by_direction_is_always_explicit():
    query = (
        select([label("a"), label("b")])
        .select_from(sa.table("T"))
        .order_by(label("a"), text("b DESC"), sa.asc(label("c")))
    )
    assert '| order by ["a"] asc, b desc, ["c"] asc' in compile_kql(query)


def test_offset_paginates_after_sorting():
    query = (
        select([label("Id")])
        .select_from(sa.table("T"))
        .order_by(sa.asc(label("Id")))
        .offset(20)
        .limit(10)
    )
    assert compile_kql(query) == (
        '["T"]\n| order by ["Id"] asc\n| project ["Id"]\n| serialize\n| where row_number() > 20\n| take 10'
    )


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (column("F").in_(["1", "One"]), '["F"] in ("1", "One")'),
        (column("F").notin_(["1", "One"]), '["F"] !in ("1", "One")'),
        (column("F").in_([]), "false"),
        (column("F").notin_([]), "true"),
        (column("F") == "1", '["F"] == "1"'),
        (column("F") != "1", '["F"] != "1"'),
        (column("F") == 1, '["F"] == 1'),
        (column("F") == True, '["F"] == true'),  # noqa: E712
        (
            column("F") >= datetime(2026, 1, 2, 3, 4, 5),
            '["F"] >= datetime(2026-01-02T03:04:05)',
        ),
        (column("F").like("%123%"), '["F"] contains_cs "123"'),
        (column("F").notlike("%123%"), '["F"] !contains_cs "123"'),
        (column("F").like("123%"), '["F"] startswith_cs "123"'),
        (column("F").notlike("123%"), '["F"] !startswith_cs "123"'),
        (column("F").like("%123"), '["F"] endswith_cs "123"'),
        (column("F").like("123"), '["F"] == "123"'),
        (column("F").notlike("123"), '["F"] != "123"'),
        (column("F").ilike("%123%"), '["F"] contains "123"'),
        (column("F").notilike("123%"), '["F"] !startswith "123"'),
        (column("F").ilike("%123"), '["F"] endswith "123"'),
        (column("F").ilike("abc"), '["F"] =~ "abc"'),
        (column("F").notilike("abc"), '["F"] !~ "abc"'),
        (column("F").like("a%b.c"), '["F"] matches regex "^a.*b\\\\.c$"'),
        (column("F").ilike("%a%b"), '["F"] matches regex "^(?i).*a.*b$"'),
        (column("F").between(2, 4), '["F"] between (2 .. 4)'),
        (~column("F").between(2, 4), '["F"] !between (2 .. 4)'),
        (column("F").is_(None), 'isnull(["F"])'),
        (column("F").isnot(None), 'isnotnull(["F"])'),
        (column("F") == None, 'isnull(["F"])'),  # noqa: E711
        (
            sa.and_(column("F").isnot(None), column("G").notin_(["1"])),
            'isnotnull(["F"]) and ["G"] !in ("1")',
        ),
        (sa.or_(column("F") == 1, column("G") == 2), '(["F"] == 1 or ["G"] == 2)'),
        (
            sa.and_(column("F") == 1, sa.or_(column("G") == 2, column("G") == 3)),
            '["F"] == 1 and (["G"] == 2 or ["G"] == 3)',
        ),
        (
            sa.not_(sa.or_(column("F") == 1, column("G") == 2)),
            'not(["F"] == 1 or ["G"] == 2)',
        ),
        (text("Field1 == 'x' and Field2 > 3"), "Field1 == 'x' and Field2 > 3"),
        (
            text('Data.x == "a:b"'),
            'Data.x == "a:b"',
        ),  # a ':' in KQL text is not a bind marker
        (column("F") % 2 == 0, '["F"] % 2 == 0'),
    ],
)
def test_where_predicates(predicate, expected):
    query = (
        select([column("F")]).select_from(sa.table("logs")).where(predicate).limit(100)
    )
    assert (
        compile_kql(query)
        == f'["logs"]\n| where {expected}\n| project ["F"]\n| take 100'
    )


def test_where_criteria_are_grouped_before_and():
    query = (
        select([column("F")])
        .select_from(sa.table("logs"))
        .where(sa.or_(column("a") == 1, column("b") == 2))
        .where(column("c") == 3)
    )
    assert '| where (["a"] == 1 or ["b"] == 2) and ["c"] == 3' in compile_kql(query)


def test_like_needs_a_literal_pattern():
    query = (
        select([column("F")])
        .select_from(sa.table("logs"))
        .where(column("F").like(column("G")))
    )
    with pytest.raises(CompileError, match="literal string pattern"):
        compile_kql(query)


def test_sqlalchemy_functions_from_superset_adhoc_metrics():
    query = select(
        [
            sa.func.COUNT(column("Id")).label("c"),
            sa.func.COUNT(sa.distinct(column("Cust"))).label("d"),
            sa.func.count().label("all"),
            sa.func.count(literal_column("*")).label("star"),
            sa.func.count(sa.func.distinct(text("Text"))).label("d2"),
            sa.func.SUM(column("Amount")).label("s"),
            sa.func.AVG(literal_column("toreal(a) / toreal(b)")).label("a"),
            sa.func.round(sa.func.avg(column("x")), 2).label("r"),
        ]
    ).select_from(sa.table("T"))
    assert compile_kql(query) == (
        '["T"]\n| summarize ["c"] = countif(isnotnull(["Id"])), ["d"] = dcount(["Cust"]), ["all"] = count(), '
        '["star"] = count(), ["d2"] = dcount(Text), ["s"] = sum(["Amount"]), ["a"] = avg(toreal(a) / toreal(b)), '
        '["r"] = round(avg(["x"]), 2)\n'
        '| project ["c"], ["d"], ["all"], ["star"], ["d2"], ["s"], ["a"], ["r"]'
    )


def test_unaggregated_column_outside_group_by_is_an_error():
    query = (
        select([label("a"), column("b")])
        .select_from(sa.table("T"))
        .group_by(label("a"))
    )
    with pytest.raises(
        CompileError, match='\\["b"\\]: not an aggregate and not in GROUP BY'
    ):
        compile_kql(query)


def test_group_by_without_aggregates_is_a_bare_summarize():
    query = (
        select([label("country_name")])
        .select_from(sa.table("CovidVaccineData", schema="superset"))
        .group_by(label("country_name"))
        .order_by(text("country_name ASC"))
    )
    assert compile_kql(query) == (
        'database("superset").["CovidVaccineData"]\n'
        '| summarize by ["country_name"]\n'
        "| order by country_name asc\n"
        '| project ["country_name"]'
    )


def test_group_by_text_matches_a_column():
    query = (
        select([sa.func.count(text("Id")).label("tag_count"), Column("Text", String)])
        .select_from(sa.table("T"))
        .group_by(text("Text"))
        .order_by("tag_count")
    )
    assert compile_kql(query) == (
        '["T"]\n| summarize ["tag_count"] = countif(isnotnull(Id)) by ["Text"]\n'
        '| order by ["tag_count"] asc\n'
        '| project ["tag_count"], ["Text"]'
    )


def test_aggregate_without_alias_skips_project():
    query = select([literal_column("count(*)")]).select_from(sa.table("T"))
    assert compile_kql(query) == '["T"]\n| summarize count()'


def test_star_selects_everything():
    query = select("*").select_from(virtual()).limit(10)
    assert compile_kql(query) == LET_VIRTUAL + "| take 10"


def test_table_object_projects_its_columns():
    logs = Table("logs", MetaData(), Column("Field1", String), Column("Field2", String))
    assert (
        compile_kql(logs.select().limit(5))
        == '["logs"]\n| project ["Field1"], ["Field2"]\n| take 5'
    )


def test_already_quoted_identifiers_are_left_alone():
    quote = engine.dialect.identifier_preparer.quote
    query = select(
        [column(quote("Field1")), literal_column(quote("Field2"))]
    ).select_from(sa.table("logs"))
    assert compile_kql(query) == '["logs"]\n| project ["Field1"], ["Field2"]'


def test_identifiers_with_odd_characters():
    query = select([column('say "hi"'), column("имя с пробелом")]).select_from(
        sa.table("a.b c")
    )
    assert (
        compile_kql(query)
        == '["a.b c"]\n| project ["say \\"hi\\""], ["имя с пробелом"]'
    )


def test_text_from_passes_through():
    query = (
        select([column("Field1")]).select_from(text("logs | where x == 1")).limit(100)
    )
    assert compile_kql(query) == 'logs | where x == 1\n| project ["Field1"]\n| take 100'


@pytest.mark.parametrize(
    ("from_text", "source"),
    [
        # Superset's select_star: quote_schema(schema) + "." + quote(table)
        ('["events"].["DrinkitAppEvents"]', 'database("events").["DrinkitAppEvents"]'),
        ('b2b."IotMeasurementsHourly"', 'database("b2b").["IotMeasurementsHourly"]'),
        ("datalake.Orders", 'database("datalake").["Orders"]'),
        ('["a.b c"]', '["a.b c"]'),
        ('["say \\"hi\\""].T', 'database("say \\"hi\\"").["T"]'),
        ("Orders", '["Orders"]'),
        # anything that is not just a name is KQL and stays as written
        ('database("x").T', 'database("x").T'),
        ("T | take 5", "T | take 5"),
    ],
)
def test_text_from_table_reference(from_text, source):
    query = select("*").select_from(text(from_text)).limit(100)
    assert compile_kql(query) == f"{source}\n| take 100"


def test_join_of_two_tables():
    left = sa.table("Events", column("Id"), column("Text"))
    right = sa.table("IdTable", column("Id"))
    query = (
        select([left.c.Text])
        .select_from(left.outerjoin(right, left.c.Id == right.c.Id))
        .where(right.c.Id > 8)
    )
    assert compile_kql(query) == (
        '["Events"]\n| join kind=leftouter (["IdTable"]) on $left.["Id"] == $right.["Id"]\n| where ["Id"] > 8\n| project ["Text"]'
    )


def test_output_never_starts_with_a_tsql_keyword():
    from sqlalchemy_kusto.dbapi import is_tsql_query

    for query in (
        select([count]).select_from(virtual()),
        select([column("select")]).select_from(sa.table("with")),
        select("*").select_from(text("select_events | take 1")),
    ):
        assert not is_tsql_query(compile_kql(query))


def test_bind_parameters_are_always_inlined():
    query = (
        select([column("F")])
        .select_from(sa.table("T"))
        .where(column("F") == "x")
        .limit(5)
    )
    assert (
        str(query.compile(engine))
        == '["T"]\n| where ["F"] == "x"\n| project ["F"]\n| take 5'
    )


def test_multiple_froms_are_rejected():
    query = (
        select([column("a"), column("b")])
        .select_from(sa.table("T"))
        .select_from(sa.table("U"))
    )
    with pytest.raises(CompileError, match="exactly one source"):
        compile_kql(query)


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("T | take 1", ["T | take 1"]),
        (
            "let x = 5; let y = 3; T | where a == x",
            ["let x = 5", "let y = 3", "T | where a == x"],
        ),
        (
            'T | where s == "a;b" // c; d\n| take 1;',
            ['T | where s == "a;b" \n| take 1'],
        ),
        ("T | where s == 'it\\'s; ok'", ["T | where s == 'it\\'s; ok'"]),
        ('T | where s == @"C:\\dir\\"";"" x"', ['T | where s == @"C:\\dir\\"";"" x"']),
        (
            "T | where s == ```multi;\nline // not a comment```",
            ["T | where s == ```multi;\nline // not a comment```"],
        ),
        ("// only a comment\n\n\nT", ["T"]),
    ],
)
def test_split_statements(script, expected):
    assert split_statements(script) == expected


@pytest.mark.parametrize(
    ("text_", "expected"),
    [
        ("COUNT(*)", "count()"),
        ("count(1)", "count()"),
        ("Count( * )", "count()"),
        ("count(distinct CustomerId)", "dcount(CustomerId)"),
        ("COUNT_DISTINCT(CustomerId)", "dcount(CustomerId)"),
        ("SUM(Amount)", "sum(Amount)"),
        ("AVG(Score) / MAX(Score)", "avg(Score) / max(Score)"),
        ("percentile(quantity_ordered, 99)", "percentile(quantity_ordered, 99)"),
        (
            "dcountif(year, city == 'Paris' or city in ('Madrid'))",
            "dcountif(year, city == 'Paris' or city in ('Madrid'))",
        ),
        ("startofmonth(somedate)", "startofmonth(somedate)"),
        (
            'strcat("SUM(", x, ")")',
            'strcat("SUM(", x, ")")',
        ),  # string literals are opaque
        ("my_count(x)", "my_count(x)"),
        ("Data.count(x)", "Data.count(x)"),
        ("tolower(Country)", "tolower(Country)"),
        ("TOLOWER(Country)", "tolower(Country)"),  # Superset's sqlglot sanitizer shouts
        ("PARSE_JSON(Data).x", "parse_json(Data).x"),
        ("MyFunc(x)", "MyFunc(x)"),  # user-defined functions keep their case
    ],
)
def test_normalize_kql_text(text_, expected):
    assert _normalize_kql_text(text_) == expected


@pytest.mark.parametrize(
    ("expression", "aggregate"),
    [
        ("count(*)", True),
        ("round(avg(Amount), 2)", True),
        ("1.0 * sumif(a, b > 1) / count()", True),
        ("toreal(a) / toreal(b)", False),
        ('strcat("sum(", x)', False),
        ("bin(Timestamp, 1d)", False),
        ("ActiveUsers", False),
    ],
)
def test_literal_column_aggregate_detection(expression, aggregate):
    query = select([literal_column(expression).label("m")]).select_from(sa.table("T"))
    assert ("| summarize" in compile_kql(query)) is aggregate
