import json
import logging
from types import ModuleType
from typing import List, Any, Dict, Optional, Tuple

from sqlalchemy import Column
from sqlalchemy.engine import default, Connection
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler, selectable
from sqlalchemy.types import Boolean, TIMESTAMP, DATE, String, BigInteger, Integer, Float

import sqlalchemy_kusto
from sqlalchemy_kusto import OperationalError, NotSupportedError

logger = logging.getLogger(__name__)

def parse_bool_argument(value: str) -> bool:
    if value in ("True", "true"):
        return True
    if value in ("False", "false"):
        return False
    raise ValueError(f"Expected boolean found {value}")


csl_to_sql_types = {
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

aggregates_sql_to_kql = {
    "count(*)": "count()",
}

class UniversalSet:
    def __contains__(self, item):
        return True


class KustoKqlIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()

    def __init__(self, dialect, **kw):
        super(KustoKqlIdentifierPreparer, self).__init__(
            dialect, initial_quote="", final_quote="", **kw
        )


class KustoKqlCompiler(compiler.SQLCompiler):
    def visit_select(
        self,
        select: selectable.Select,
        asfrom=False,
        parens=True,
        fromhints=None,
        compound_index: int = 0,
        nested_join_translation=False,
        select_wraps_for=None,
        lateral=False,
        **kwargs,
    ):
        logger.debug(f"Incoming query {select}")
        logger.debug(type(select.froms[0]))

        if len(select.froms) != 1:
            raise NotSupportedError("Only 1 from is supported in kql compiler")

        compiled_query_lines = []

        from_object = select.froms[0]
        if hasattr(from_object, "element"):
            query = self._getMostInnerElement(from_object.element)
            (main, lets) = self._extract_let_statements(query.text)
            compiled_query_lines.extend(lets)
            compiled_query_lines.append(f"let {from_object.name} = ({main});")
            compiled_query_lines.append(from_object.name)
        elif hasattr(from_object, "name"):
            compiled_query_lines.append(from_object.name)
        else:
            compiled_query_lines.append(from_object.text)

        # if select._where_criteria:
        #     t = super()._generate_delimited_and_list(select._where_criteria, **kwargs)
        #     if t:
        #         compiled_query_lines.append("| " + t)

        projections = self._get_projection_or_summarize(select)
        if projections:
            compiled_query_lines.append(projections)

        if select._limit_clause is not None:  # pylint: disable=protected-access
            kwargs["literal_execute"] = True
            compiled_query_lines.append(
                f"| take {self.process(select._limit_clause, **kwargs)}"
            )  # pylint: disable=protected-access

        compiled_query_lines = list(filter(None, compiled_query_lines))

        compiled_query = "\n".join(compiled_query_lines)
        logger.debug(f"Compiled query: {compiled_query}")
        return compiled_query

    def _getMostInnerElement(self, clause):
        innerElement = getattr(clause, "element", None)
        if innerElement is not None:
            return self._getMostInnerElement(innerElement)
        else:
            return clause

    def _extract_let_statements(self, clause) -> Tuple[str, List[str]]:
        rows = [s.strip() for s in clause.split(';')]
        main = next(filter(lambda row: not row.startswith("let"), rows), None)
        lets = [row + ";" for row in rows if row.startswith("let")]
        return main, lets


    def fetch_clause(self, select, **kw):  # pylint: disable=no-self-use
        return ""

    def limit_clause(self, select, **kw):
        return ""

    def _get_projection_or_summarize(self, select: selectable.Select) -> str:
        # TODO: Migrate to select.exported_columns
        columns = select.inner_columns
        if columns is not None:
            column_labels = []
            is_summarize = False
            for column in [c for c in columns if c.name != "*"]:
                column_name, column_alias = self._extract_column_name_and_alias(column)

                if column_name in aggregates_sql_to_kql:
                    is_summarize = True
                    column_labels.append(self._build_column_projection(aggregates_sql_to_kql[column_name], column_alias))
                else:
                    column_labels.append(self._build_column_projection(column_name, column_alias))

            if column_labels:
                projection_type = "summarize" if is_summarize else "project"
                return f"| {projection_type} {', '.join(column_labels)}"
        return ""

    def _extract_column_name_and_alias(self, column: Column) -> Tuple[str, Optional[str]]:
        if hasattr(column, "element"):
            return column.element.name, column.name
        else:
            return column.name, None

    def _build_column_projection(self, column_name: str, column_alias: str = None):
        return f"{column_alias} = {column_name}" if column_alias else column_name


class KustoKqlTypeCompiler(compiler.GenericTypeCompiler):
    pass


class KustoKqlDialect(default.DefaultDialect):
    """ See description sqlalchemy/engine/interfaces.py """

    name = "kustokql"
    scheme = "http"
    driver = "rest"
    statement_compiler = KustoKqlCompiler
    type_compiler = KustoKqlTypeCompiler
    preparer = KustoKqlIdentifierPreparer
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
                "type": csl_to_sql_types[column["CslType"].lower()],
                "nullable": True,
                "default": "",
            }
            for column in entity_schema["OrderedColumns"]
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


KustoKqlHTTPDialect = KustoKqlDialect


class KustoKqlHTTPSDialect(KustoKqlDialect):
    scheme = "https"
