from setuptools import find_packages, setup

NAME = "sqlalchemy-kusto"
DESCRIPTION = "SQLAlchemy dialect for Azure Data Explorer (Kusto)"
VERSION = "0.0.12"

REQUIREMENTS = [
    "azure-kusto-data==2.1.1",
    "sqlalchemy==1.3.24",
]
EXTRAS = {
    "dev": [
        "black~=21.12b0",
        "pytest>=6.2.5"
        "python-dotenv>=0.19.2"
    ]
}

setup(
    author="Dodo Engineering",
    author_email="devcommunity@dodopizza.com",
    classifiers=["Intended Audience :: Developers"],
    description=DESCRIPTION,
    entry_points={
        "sqlalchemy.dialects": [
            "kustosql.https = sqlalchemy_kusto.dialect_sql:KustoSqlHttpsDialect",
        ]
    },
    extra_require=EXTRAS,
    include_package_data=True,
    install_requires=REQUIREMENTS,
    license="Apache License, Version 2.0",
    name=NAME,
    url="https://github.com/dodopizza/sqlalchemy-kusto",
    packages=find_packages(exclude=["tests", "tests.*"]),
    version=VERSION,
    zip_safe=False,
)
