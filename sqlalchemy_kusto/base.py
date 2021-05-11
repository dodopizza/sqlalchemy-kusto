import logging
from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

import sqlalchemy_kusto

logger = logging.getLogger(__name__)


type_map = {
    "char": types.String,
    "varchar": types.String,
    "float": types.Float,
    "decimal": types.Float,
    "real": types.Float,
    "double": types.Float,
    "boolean": types.Boolean,
    "tinyint": types.BigInteger,
    "smallint": types.BigInteger,
    "integer": types.BigInteger,
    "bigint": types.BigInteger,
    "timestamp": types.TIMESTAMP,
    "date": types.DATE,
    "other": types.BLOB,
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


class BaseKustoDialect(default.DefaultDialect):

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

    # _not_supported_column_types = ["object", "nested"]

    @classmethod
    def dbapi(cls):
        return sqlalchemy_kusto

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

        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        # ES does not have the concept of a schema
        ".show databases | project DatabaseName"

        return [DEFAULT_SCHEMA]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs) -> List[str]:
        pass  # pragma: no cover

    def get_columns(self, connection, table_name, schema=None, **kw):
        pass  # pragma: no cover

    def get_view_names(self, connection, schema=None, **kwargs):
        return []  # pragma: no cover

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


def get_type(data_type: str) -> int:
    type_map = {
        "bytes": types.LargeBinary,
        "boolean": types.Boolean,
        "date": types.DateTime,
        "datetime": types.DateTime,
        "double": types.Numeric,
        "text": types.String,
        "keyword": types.String,
        "integer": types.Integer,
        "half_float": types.Float,
        "geo_point": types.String,
        # TODO get a solution for nested type
        "nested": types.String,
        # TODO get a solution for object
        "object": types.BLOB,
        "long": types.BigInteger,
        "float": types.Float,
        "ip": types.String,
    }
    type_ = type_map.get(data_type)
    if not type_:
        logger.warning(f"Unknown type found {data_type} reverting to string")
        type_ = types.String
    return type_
