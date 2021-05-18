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


type_map = {
    "boolean": Boolean,
    "datetime": TIMESTAMP,
    "date": DATE,
    "dynamic": String,
    "stringbuffer": String,
    "guid": String,
    "int": Integer,
    "i32": Integer,
    "long": BigInteger,
    "i64": BigInteger,
    "string": String,
    "timespan": String,
    "decimal": Float,
    "real": Float,
}


class UniversalSet:
    def __contains__(self, item):
        return True


class KustoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class KustoCompiler(compiler.SQLCompiler):
    def get_select_precolumns(self, select, **kw) -> str:
        """Kusto puts TOP, it's version of LIMIT here"""
        # sqlalchemy.sql.selectable.Select
        select_precolumns = super(KustoCompiler, self).get_select_precolumns(select, **kw)

        if select._limit_clause is not None:  # pylint: disable=protected-access
            kw["literal_execute"] = True
            select_precolumns += "TOP %s " % self.process(
                select._limit_clause, **kw  # pylint: disable=protected-access
            )

        return select_precolumns

    def fetch_clause(self, select, **kw):   # pylint: disable=no-self-use
        return ""

    def limit_clause(self, select, **kw):
        return ""


class KustoTypeCompiler(compiler.GenericTypeCompiler):
    pass


class KustoDialect(default.DefaultDialect):
    """ See description sqlalchemy/engine/interfaces.py """

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
    _map_parse_connection_parameters: Dict[str, Any] = {
        "msi": parse_bool_argument,
        "azure_ad_client_id": str,
        "azure_ad_client_secret": str,
        "azure_ad_tenant_id": str,
        "user_msi": str,
    }

    @classmethod
    def dbapi(cls) -> ModuleType:  # pylint: disable=method-hidden
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

    def get_schema_names(self, connection: Connection, **kwargs) -> List[str]:  # pylint: disable=no-self-use
        result = connection.execute(".show databases | project DatabaseName")
        return [row.DatabaseName for row in result]

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs) -> bool:
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kwargs) -> List[str]:
        if schema:
            database_subquery = f'| where DatabaseName == "{schema}"'
        result = connection.execute(f".show tables {database_subquery} " f"| project TableName")
        return [row.TableName for row in result]

    def get_columns(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs
    ) -> List[Dict[str, Any]]:
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

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kwargs) -> List[str]:
        result = connection.execute(".show materialized-views  | project Name")
        return [row.Name for row in result]

    def get_table_options(  # pylint: disable=no-self-use
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs
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
        return {"text": ""}

    def get_indexes(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs
    ) -> List[Dict[str, Any]]:
        return []

    def get_unique_constraints(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs):
        return []

    def get_view_definition(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs):
        pass  # pragma: no cover

    def _check_unicode_returns(self, connection: Connection, additional_tests: List[Any] = None) -> bool:
        return True

    def _check_unicode_description(self, connection: Connection) -> bool:
        return True

    def do_rollback(self, dbapi_connection: sqlalchemy_kusto.dbapi.Connection):
        pass

    def do_ping(self, dbapi_connection: sqlalchemy_kusto.dbapi.Connection):
        try:
            query = ".show tables"
            dbapi_connection.execute(query)
            return True
        except OperationalError:
            return False


KustoHTTPDialect = KustoDialect


class KustoHTTPSDialect(KustoDialect):
    scheme = "https"
