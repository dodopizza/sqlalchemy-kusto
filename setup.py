import os
from setuptools import find_packages, setup

VERSION = "0.0.12"
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

setup(
    name="sqlalchemy-kusto",
    description=("Kusto sqlalchemy dialect"),
    version=VERSION,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "kustosql = sqlalchemy_kusto.dialect_sql:KustoSqlHTTPDialect",
            "kustosql.http = sqlalchemy_kusto.dialect_sql:KustoSqlHTTPDialect",
            "kustosql.https = sqlalchemy_kusto.dialect_sql:KustoSqlHTTPSDialect",
            "kustokql = sqlalchemy_kusto.dialect_kql:KustoKqlHTTPDialect",
            "kustokql.http = sqlalchemy_kusto.dialect_kql:KustoKqlHTTPDialect",
            "kustokql.https = sqlalchemy_kusto.dialect_kql:KustoKqlHTTPSDialect",
        ]
    },
    install_requires=["azure-kusto-data==2.1.1", "sqlalchemy"],
)
