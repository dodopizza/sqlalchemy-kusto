from setuptools import find_packages, setup

NAME = "sqlalchemy-kusto"
DESCRIPTION = "Azure Data Explorer (Kusto) dialect for SQLAlchemy"
VERSION = "1.0.1"

REQUIREMENTS = [
    "azure-kusto-data==2.1.1",
    "sqlalchemy==1.3.24",
]
EXTRAS = {
    "dev": [
        "black>=21.12b0",
        "isort>=5.10.1",
        "mypy>=0.9.30",
        "pylint>=2.12.2",
        "pytest>=6.2.5",
        "python-dotenv>=0.19.2",
    ]
}

with open("README.md", "r", encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

setup(
    author="Dodo Engineering",
    author_email="devcommunity@dodopizza.com",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    description=DESCRIPTION,
    entry_points={
        "sqlalchemy.dialects": [
            "kustosql.https = sqlalchemy_kusto.dialect_sql:KustoSqlHttpsDialect",
            "kustokql.https = sqlalchemy_kusto.dialect_kql:KustoKqlHttpsDialect",
        ]
    },
    extras_require=EXTRAS,
    include_package_data=True,
    install_requires=REQUIREMENTS,
    license="Apache License, Version 2.0",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    name=NAME,
    url="https://github.com/dodopizza/sqlalchemy-kusto",
    packages=find_packages(exclude=["tests", "tests.*"]),
    project_urls={
        "Bug Tracker": "https://github.com/dodopizza/sqlalchemy-kusto/issues",
    },
    python_requires=">=3.7",
    version=VERSION,
    zip_safe=False,
)
