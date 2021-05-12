import logging
from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from typing import List

from sqlalchemy_kusto import dbapi

logger = logging.getLogger(__name__)


def parse_bool_argument(value: str) -> bool:
    if value in ("True", "true"):
        return True
    elif value in ("False", "false"):
        return False
    else:
        raise ValueError(f"Expected boolean found {value}")


type_map = {
    "boolean": types.Boolean,
    "datetime": types.TIMESTAMP,
    "datetime": types.DATE,
    "dynamic": types.String,
    "stringbuffer": types.String,
    "guid": types.String,
    "int": types.BigInteger,
    "long": types.BigInteger,
    "real": types.Float,
    "string": types.String,
    "timespan": types.String,
    "decimal": types.Float,
    "real": types.Float,
}


class UniversalSet(object):
    def __contains__(self, item):
        return True


class KustoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class KustoCompiler(compiler.SQLCompiler):
    pass


class KustoTypeCompiler(compiler.GenericTypeCompiler):
    pass


class KustoDialect(default.DefaultDialect):
    name = "kusto"
    scheme = "http"
    driver = "rest"
    statement_compiler = KustoCompiler
    type_compiler = KustoTypeCompiler
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
    _map_parse_connection_parameters = {
        "msi": parse_bool_argument,
        "azure_ad_client_id": str,
        "azure_ad_client_secret": str,
        "azure_ad_tenant_id": str,
        "user_msi": str,
    }

    @classmethod
    def dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 9200,
            "path": url.database,
            "scheme": self.scheme,
            "user": url.username or None,
            "password": url.password or None,
        }
        if url.query:
            kwargs.update(url.query)

        for name, parse_func in self._map_parse_connection_parameters.items():
            if name in kwargs:
                kwargs[name] = parse_func(url.query[name])

        return [], kwargs

    def get_schema_names(self, connection, **kwargs):
        result = connection.execute(".show databases | project DatabaseName")
        return [row.DatabaseName for row in result]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs) -> List[str]:
        if schema:
            database_subquery = f"| where DatabaseName == \"{schema}\""
        result = connection.execute(f".show tables {database_subquery}  | project TableName")
        return [row.TableName for row in result]

    def get_columns(self, connection, table_name, schema=None, **kw):
        query = f".show table {table_name}"
        result = connection.execute(query)

        return [
            {
                "name": row.AttributeName,
                "type": type_map[row.AttributeType.lower()],
                "nullable": True,
                "default": "",
            }
            for row in result
        ]

    def get_view_names(self, connection, schema=None, **kwargs):
        result = connection.execute(f".show materialized-views  | project Name")
        return [row.Name for row in result]

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass  # pragma: no cover

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


def parse_bool_argument(value: str) -> bool:
    if value in ("True", "true"):
        return True
    elif value in ("False", "false"):
        return False
    else:
        raise ValueError(f"Expected boolean found {value}")
