# sqlalchemy-kusto
Kusto dialect for SQLAlchemy

---
`sqlalchemy-kusto` Implements a DBAPI ([PEP-249](https://www.python.org/dev/peps/pep-0249)) and SQLAlchemy dialect that enables SQL and basic KQL select query only execution.

Notice that KQL and SQL dialects don't support DDL statements and inserts, deletes, updates.

In SQL dialect pay your attention that Kusto implementation of T-SQL has not full coverage; check the [list of known issues](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/tds/sqlknownissues).

## Installation
```shell
$ pip install sqlalchemy-kusto
```

## Library usage 

### Using DBApi
```python
from sqlalchemy_kusto import connect

connection = connect(
        kusto_url,
        database_name,
        False,
        None,
        azure_ad_client_id=kusto_client_id,
        azure_ad_client_secret=kusto_client_secret,
        azure_ad_tenant_id=kusto_tenant_id,
)
result = connection.execute(f"select 1").fetchall()
```

### Using SQLAlchemy raw sql

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    f"{kusto_url}/{database_name}?"
    f"msi=False&azure_ad_client_id={kusto_client_id}&"
    f"azure_ad_client_secret={kusto_client_secret}&"
    f"azure_ad_tenant_id={kusto_tenant_id}"
)
engine.connect()
cursor = engine.execute(f"select top 1")
data_rows = cursor.fetchall()


```

### Using SQLAlchemy 

```python
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer

engine = create_engine(
    f"{kusto_url}/{database_name}?"
    f"msi=False&azure_ad_client_id={kusto_client_id}&"
    f"azure_ad_client_secret={kusto_client_secret}&"
    f"azure_ad_tenant_id={kusto_tenant_id}"
)

my_table = Table(
        "MyTable",
        MetaData(),
        Column("Id", Integer),
        Column("Text", String),
)

query = my_table.select().limit(5)

engine.connect()
cursor = engine.execute(query)
print([row for row in cursor])
```


---
Issue that inspired this solution https://github.com/apache/superset/issues/10646
