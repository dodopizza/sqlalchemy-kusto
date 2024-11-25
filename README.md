# Azure Data Explorer (Kusto) dialect for SQLAlchemy

[![pypi](https://img.shields.io/pypi/v/sqlalchemy-kusto)](https://pypi.org/project/sqlalchemy-kusto/)

`sqlalchemy-kusto` implements a DBAPI ([PEP-249](https://www.python.org/dev/peps/pep-0249)) and [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/) that enables SQL query execution via SQLAlchemy.

Current project includes support for two dialects: SQL dialect and KQL dialect.

## SQL dialect

Current implementation has full support for SQL queries. But pay your attention that Kusto implementation of T-SQL has not full coverage; check the [list of known issues](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/tds/sqlknownissues).

## KQL dialect

KQL dialect still in progress. Please, use it on your own risk for now.

> Notice that implemented Kusto dialects don't support DDL statements and inserts, deletes, updates.

## Installation

```shell
pip install sqlalchemy-kusto
```

## Library usage 

### Using DBAPI

```python
from sqlalchemy_kusto import connect

connection = connect(
        cluster=kusto_url,
        database=database_name,
        msi=False,
        user_msi=None,
        azure_ad_client_id=kusto_client_id,
        azure_ad_client_secret=kusto_client_secret,
        azure_ad_tenant_id=kusto_tenant_id,
        dev_mode=False
)

result = connection.execute(f"select 1").fetchall()
```

### Using SQLAlchemy raw sql

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    f"kustosql+{kusto_url}/{database_name}?"
    f"msi=False&azure_ad_client_id={kusto_client_id}&"
    f"azure_ad_client_secret={kusto_client_secret}&"
    f"azure_ad_tenant_id={kusto_tenant_id}&"
    f"dev_mode=False"
)
engine.connect()
cursor = engine.execute(f"select top 1")
data_rows = cursor.fetchall()
```

### Using SQLAlchemy 

```python
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer

engine = create_engine(
    f"kustosql+{kusto_url}/{database_name}?"
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

### Using with Apache Superset

[Apache Superset](https://github.com/apache/superset) starting from [version 1.5](https://github.com/apache/superset/blob/1c1beb653a52c1fcc67a97e539314f138117c6ba/RELEASING/release-notes-1-5/README.md) also supports Kusto database engine spec. \
When connecting to a new data source you may choose a data source type either KustoSQL or KustoKQL depending on the dialect you want to use.

There are following connection string formats:

```shell
# KustoSQL
kustosql+https://<CLUSTER_URL>/<DATABASE>?azure_ad_client_id=<CLIENT_ID>&azure_ad_client_secret=<CLIENT_SECRET>&azure_ad_tenant_id=<TENANT_ID>&msi=False

# KustoKQL
kustokql+https://<CLUSTER_URL>/<DATABASE>?azure_ad_client_id=<CLIENT_ID>&azure_ad_client_secret=<CLIENT_SECRET>&azure_ad_tenant_id=<TENANT_ID>&msi=False
```
> Important notice on package version compatibility. \
> Apache Superset stable releases 1.5 and 2.0 dependent on `sqlalchemy==1.3.24`. If you want to use `sqlalchemy-kusto` with these versions you need to install version `1.*` of the package.
> 
> Current `master` branch of the `apache/superset` dependent on `sqlalchemy==1.4.36`. If you want to use `sqlalchemy-kusto` with the latest unstable version of `apache/superset`, you need to install version `2.*` of the package.

## Contributing

Please see the [CONTRIBUTING.md](.github/CONTRIBUTING.md) for development setup and contributing process guidelines.

---
[Issue in Apache Superset repository](https://github.com/apache/superset/issues/10646) that inspired current solution.
