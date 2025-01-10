import json
from abc import ABC
from types import ModuleType
from typing import Any

from sqlalchemy.engine import Connection, default
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler
from sqlalchemy.types import (
    DATE,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Float,
    Integer,
    String,
)

import sqlalchemy_kusto


def parse_bool_argument(value: str) -> bool:
    if value in ("True", "true"):
        return True
    if value in ("False", "false"):
        return False
    raise ValueError(f"Expected boolean found {value}")


kql_to_sql_types = {
    "bool": Boolean,
    "boolean": Boolean,
    "datetime": TIMESTAMP,
    "date": DATE,
    "dynamic": String,
    "stringbuffer": String,
    "guid": String,
    "int": Integer,
    "i32": Integer,
    "i16": Integer,
    "i8": Integer,
    "r64": Float,
    "r32": Float,
    "long": BigInteger,
    "i64": BigInteger,
    "string": String,
    "timespan": String,
    "decimal": Float,
    "real": Float,
}


class KustoBaseDialect(default.DefaultDialect, ABC):
    driver = "rest"
    type_compiler = compiler.GenericTypeCompiler
    preparer = compiler.IdentifierPreparer
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = True
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = True
    _map_parse_connection_parameters: dict[str, Any] = {
        "msi": parse_bool_argument,
        "azure_ad_client_id": str,
        "azure_ad_client_secret": str,
        "azure_ad_tenant_id": str,
        "user_msi": str,
        "dev_mode": parse_bool_argument,
    }

    @classmethod
    def dbapi(cls) -> ModuleType:
        return sqlalchemy_kusto

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "cluster": "https://" + url.host,
            "database": url.database,
        }

        if url.query:
            kwargs.update(url.query)

        for name, parse_func in self._map_parse_connection_parameters.items():
            if name in kwargs:
                kwargs[name] = parse_func(url.query[name])

        return [], kwargs

    def get_schema_names(self, connection: Connection, **kwargs) -> list[str]:
        result = connection.execute(".show databases | project DatabaseName")
        return [row.DatabaseName for row in result]

    def has_table(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ) -> bool:
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(
        self, connection: Connection, schema: str | None = None, **kwargs
    ) -> list[str]:
        # Schema is not used in Kusto cause database is written in the connection string
        result = connection.execute(".show tables | project TableName")
        return [row.TableName for row in result]

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        table_search_query = f"""
            .show tables
            | where TableName == "{table_name}"
        """
        function_search_query = f"""
            .show functions
            | where Name == "{table_name}"
        """
        table_search_result = connection.execute(table_search_query)

        # Add Functions as View as well. Retrieve the schema of the table
        if table_search_result.rowcount == 0:
            function_search_result = connection.execute(function_search_query)
            if function_search_result.rowcount == 1:
                function_schema = f".show function {table_name} schema as json"
                query_result = connection.execute(function_schema)
                rows = list(query_result)
                entity_schema = json.loads(rows[0].Schema)
                return [
                    self.schema_definition(column)
                    for column in entity_schema["OutputColumns"]
                ]
        entity_type = (
            "table" if table_search_result.rowcount == 1 else "materialized-view"
        )
        query = f".show {entity_type} {table_name} schema as json"
        query_result = connection.execute(query)
        rows = list(query_result)
        entity_schema = json.loads(rows[0].Schema)
        return [
            self.schema_definition(column) for column in entity_schema["OrderedColumns"]
        ]

    @staticmethod
    def schema_definition(column) -> dict:
        return {
            "name": column["Name"],
            "type": kql_to_sql_types[column["CslType"].lower()],
            "nullable": True,
            "default": "",
        }

    def get_view_names(
        self, connection: Connection, schema: str | None = None, **kwargs
    ) -> list[str]:
        materialized_views = connection.execute(
            ".show materialized-views | project Name"
        )
        # Functions are also Views.
        # Filtering no input functions specifically here as there is no way to pass parameters today
        functions = connection.execute(
            ".show functions | where Parameters =='()' | project Name"
        )
        materialized_view = [row.Name for row in materialized_views]
        view = [row.Name for row in functions]
        return materialized_view + view

    def get_pk_constraint(
        self, connection: Connection, table_name: str, schema: str | None = None, **kw
    ):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ):
        return []

    def get_table_comment(
        self, connection: Connection, table_name, schema: str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Not implemented."""
        return {"text": ""}

    def get_indexes(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        return []

    def get_unique_constraints(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ):
        return []

    def _check_unicode_returns(
        self, connection: Connection, additional_tests: list[Any] | None = None
    ) -> bool:
        return True

    def _check_unicode_description(self, connection: Connection) -> bool:
        return True

    def do_ping(self, dbapi_connection: sqlalchemy_kusto.dbapi.Connection):
        try:
            query = ".show tables"
            dbapi_connection.execute(query)
        except sqlalchemy_kusto.OperationalError:
            return False
        else:
            return True

    def do_rollback(self, dbapi_connection: sqlalchemy_kusto.dbapi.Connection):
        pass

    def get_temp_table_names(self, connection, schema=None, **kw):
        pass

    def get_sequence_names(self, connection, schema=None, **kw):
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):
        pass

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        pass

    def _get_server_version_info(self, connection):
        pass

    def _get_default_schema_name(self, connection):
        pass

    def do_set_input_sizes(self, cursor, list_of_tuples, context):
        pass

    def do_begin_twophase(self, connection, xid):
        pass

    def do_prepare_twophase(self, connection, xid):
        pass

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_recover_twophase(self, connection):
        pass

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        pass

    def get_view_definition(
        self,
        connection: Connection,
        view_name: str,
        schema: str | None = None,
        **kwargs,
    ):
        pass

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        pass
