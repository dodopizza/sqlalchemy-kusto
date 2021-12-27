import json

from types import ModuleType
from typing import List, Any, Dict, Optional, Tuple

from sqlalchemy.engine.url import URL
from sqlalchemy.types import Boolean, TIMESTAMP, DATE, String, BigInteger, Integer, Float
from sqlalchemy.engine import default, Connection
from sqlalchemy.sql import compiler
import sqlalchemy_kusto
from sqlalchemy_kusto import OperationalError


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


class KustoSqlCompiler(compiler.SQLCompiler):
    def get_select_precolumns(self, select, **kw) -> str:
        """Kusto uses TOP instead of LIMIT"""
        select_precolumns = super(KustoSqlCompiler, self).get_select_precolumns(select, **kw)

        if select._limit_clause is not None:
            kw["literal_execute"] = True
            select_precolumns += "TOP %s " % self.process(select._limit_clause, **kw)

        return select_precolumns

    def fetch_clause(self, select, **kw):
        """Not supported"""
        return ""

    def limit_clause(self, select, **kw):
        """Do not add LIMIT to the end of the query"""
        return ""


class KustoSqlDialect(default.DefaultDialect):
    name = "kustosql"
    scheme = "http"  # TODO: Use https and remove KustoSqlHttpsDialect
    driver = "rest"
    statement_compiler = KustoSqlCompiler
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
    _map_parse_connection_parameters: Dict[str, Any] = {
        "msi": parse_bool_argument,
        "azure_ad_client_id": str,
        "azure_ad_client_secret": str,
        "azure_ad_tenant_id": str,
        "user_msi": str,
    }

    @classmethod
    def dbapi(cls) -> ModuleType:
        return sqlalchemy_kusto

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Any]]:
        kwargs: Dict[str, Any] = {
            "cluster": "https://" + url.host,
            "database": url.database,
        }

        if url.query:
            kwargs.update(url.query)

        for name, parse_func in self._map_parse_connection_parameters.items():
            if name in kwargs:
                kwargs[name] = parse_func(url.query[name])

        return [], kwargs

    def get_schema_names(self, connection: Connection, **kwargs) -> List[str]:
        result = connection.execute(".show databases | project DatabaseName")
        return [row.DatabaseName for row in result]

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs) -> bool:
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kwargs) -> List[str]:
        database_subquery = ""
        if schema:  # TODO: Not used in Kusto, we need to remove this if and do not use schema here
            database_subquery = f'| where DatabaseName == "{schema}"'
        result = connection.execute(f".show tables {database_subquery} " f"| project TableName")
        return [row.TableName for row in result]

    def get_columns(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs
    ) -> List[Dict[str, Any]]:
        table_search_query = f"""
            .show tables
            | where TableName == "{table_name}"
        """
        table_search_result = connection.execute(table_search_query)
        entity_type = "table" if table_search_result.rowcount == 1 else "materialized-view"

        query = f".show {entity_type} {table_name} schema as json"
        query_result = connection.execute(query)
        rows = list(query_result)
        entity_schema = json.loads(rows[0].Schema)

        return [
            {
                "name": column["Name"],
                "type": kql_to_sql_types[column["CslType"].lower()],
                "nullable": True,
                "default": "",
            }
            for column in entity_schema["OrderedColumns"]
        ]

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kwargs) -> List[str]:
        result = connection.execute(".show materialized-views | project Name")
        return [row.Name for row in result]

    def get_table_options(self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs
    ):
        return {}

    def get_pk_constraint(self, conn: Connection, table_name: str, schema: Optional[str] = None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs):
        return []

    def get_table_comment(
        self, connection: Connection, table_name, schema: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Not implemented"""
        return {"text": ""}

    def get_indexes(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs
    ) -> List[Dict[str, Any]]:
        return []

    def get_unique_constraints(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs):
        return []

    def _check_unicode_returns(self, connection: Connection, additional_tests: List[Any] = None) -> bool:
        return True  # TODO: Should we override it here or not?

    def _check_unicode_description(self, connection: Connection) -> bool:
        return True  # TODO: Should we override it here or not?

    def do_ping(self, dbapi_connection: sqlalchemy_kusto.dbapi.Connection):
        try:
            query = ".show tables"
            dbapi_connection.execute(query)
            return True
        except OperationalError:
            return False

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

    def get_view_definition(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs):
        pass


KustoSqlHTTPDialect = KustoSqlDialect   # TODO: Fix dialect naming


class KustoSqlHTTPSDialect(KustoSqlDialect):
    scheme = "https"
