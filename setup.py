import os
from setuptools import find_packages, setup

VERSION = "0.0.1"
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
            "kusto = es.elastic.sqlalchemy:ESHTTPDialect",
            "kusto.http = es.elastic.sqlalchemy:ESHTTPDialect",
            "kusto.https = es.elastic.sqlalchemy:ESHTTPSDialect"
        ]
    },
    install_requires=["azure-kusto-data>=2.1.1", "sqlalchemy"]
)
