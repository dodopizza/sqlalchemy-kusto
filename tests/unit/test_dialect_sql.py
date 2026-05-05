from sqlalchemy import Boolean, Column, Integer, MetaData, Table, create_engine, select

engine = create_engine("kustosql+https://localhost/testdb")

metadata = MetaData()
orders = Table(
    "Orders",
    metadata,
    Column("TotalAmount", Integer),
    Column("IsCorporateOrder", Boolean),
    schema="test",
)


def test_boolean_false_renders_as_zero():
    """Kusto T-SQL does not support `false`/`true` literals; they must be 1/0."""
    query = select(orders.c.TotalAmount).where(
        orders.c.IsCorporateOrder == False  # noqa: E712
    )
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    assert "false" not in sql.lower()
    assert "0" in sql


def test_boolean_true_renders_as_one():
    query = select(orders.c.TotalAmount).where(
        orders.c.IsCorporateOrder == True  # noqa: E712
    )
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    assert "true" not in sql.lower()
    assert "1" in sql


def test_boolean_filter_full_query():
    """Mirrors the exact query from the bug report."""
    query = (
        select(orders.c.TotalAmount)
        .where(orders.c.IsCorporateOrder == False)  # noqa: E712
        .group_by(orders.c.TotalAmount)
    )
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
    assert "false" not in sql.lower()
    assert "true" not in sql.lower()
