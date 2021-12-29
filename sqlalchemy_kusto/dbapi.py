from collections import namedtuple
from typing import Any, List, Optional, Tuple

from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._models import KustoResultColumn
from azure.kusto.data.exceptions import KustoAuthenticationError, KustoServiceError

from sqlalchemy_kusto import errors


def check_closed(func):
    """Decorator that checks if connection/cursor is closed."""

    def decorator(self, *args, **kwargs):
        if self.closed:
            raise Exception("{klass} already closed".format(klass=self.__class__.__name__))
        return func(self, *args, **kwargs)

    return decorator


def check_result(func):
    """Decorator that checks if the cursor has results from `execute`."""

    def decorator(self, *args, **kwargs):
        if self._results is None:  # pylint: disable=protected-access
            raise Exception("Called before `execute`")
        return func(self, *args, **kwargs)

    return decorator


def connect(
    cluster: str,
    database: str,
    msi: bool = False,
    user_msi: str = None,
    azure_ad_client_id: str = None,
    azure_ad_client_secret: str = None,
    azure_ad_tenant_id: str = None,
):
    """Return a connection to the database."""
    return Connection(
        cluster,
        database,
        msi,
        user_msi,
        azure_ad_client_id,
        azure_ad_client_secret,
        azure_ad_tenant_id,
    )


class Connection:
    """Connection to Kusto cluster."""

    def __init__(
        self,
        cluster: str,
        database: str,
        msi: bool = False,
        user_msi: str = None,
        azure_ad_client_id: str = None,
        azure_ad_client_secret: str = None,
        azure_ad_tenant_id: str = None,
    ):
        self.closed = False
        self.cursors: List[Cursor] = []
        kcsb = None

        if msi:
            # Managed Service Identity (MSI)
            kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                cluster, client_id=user_msi
            )
        else:
            # Service Principal auth
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                connection_string=cluster,
                aad_app_id=azure_ad_client_id,
                app_key=azure_ad_client_secret,
                authority_id=azure_ad_tenant_id,
            )

        self.kusto_client = KustoClient(kcsb)
        self.database = database
        self.properties = ClientRequestProperties()

    @check_closed
    def close(self):
        """Close the connection now. Kusto does not require to close the connection."""
        self.closed = True
        for cursor in self.cursors:
            cursor.close()

    @check_closed
    def commit(self):
        """Kusto does not support transactions."""

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(
            self.kusto_client,
            self.database,
            self.properties,
        )

        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        """Execute operation inside cursor. DBAPI Spec does not mention this method but SQLAlchemy requires it."""
        return self.cursor().execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


CursorDescriptionRow = namedtuple(
    "CursorDescriptionRow",
    ["name", "type", "display_size", "internal_size", "precision", "scale", "null_ok"],
)


class Cursor:
    """Connection cursor."""

    def __init__(
        self,
        kusto_client: KustoClient,
        database: str,
        properties: Optional[ClientRequestProperties] = None,
    ):
        self._results: Optional[List[Tuple[Any, ...]]] = None
        self.kusto_client = kusto_client
        self.database = database
        self.closed = False
        self.description: Optional[List[CursorDescriptionRow]] = None
        self.current_item_index = 0
        self.properties = properties if properties is not None else ClientRequestProperties()

    @property  # type: ignore
    @check_result
    @check_closed
    def rowcount(self) -> int:
        """Counts the number of rows on a result."""
        # Consumes the iterator
        results = list(self._results)  # type: ignore # check_result decorator will ensure that value is not None
        return len(results)

    @check_closed
    def close(self):
        """Closes the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None) -> "Cursor":
        """Executes query. Supports only SELECT statements."""
        if operation.lower().startswith("select"):
            self.properties.set_option("query_language", "sql")
        else:
            self.properties.set_option("query_language", "kql")

        query = Cursor._apply_parameters(operation, parameters)
        query = query.rstrip()
        try:
            server_response = self.kusto_client.execute(self.database, query, self.properties)
        except KustoServiceError as kusto_error:
            raise errors.DatabaseError(str(kusto_error))
        except KustoAuthenticationError as context_error:
            raise errors.OperationalError(str(context_error))

        rows = []
        for row in server_response.primary_results[0]:
            rows.append(tuple(row.to_list()))
        self._results = rows
        self.description = self._get_description_from_columns(server_response.primary_results[0].columns)
        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        """Not supported"""
        raise NotImplementedError("`executemany` is not supported, use `execute` instead")

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetches the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        if self.rowcount > self.current_item_index:
            item = self._results[self.current_item_index]  # type: ignore
            self.current_item_index += 1
            return item

        return None

    @check_result
    @check_closed
    def fetchmany(self, size: int = None):
        """
        Fetches the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        if size:
            items = self._results[self.current_item_index : self.current_item_index + size]  # type: ignore
            self.current_item_index += size
            return items

        return self._results

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetches all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self._results)  # type: ignore

    @check_closed
    def setinputsizes(self, sizes):
        """Not supported"""

    @check_closed
    def setoutputsizes(self, sizes):
        """Not supported"""

    @staticmethod
    def _get_description_from_columns(columns: List[KustoResultColumn]) -> List[CursorDescriptionRow]:
        """Gets CursorDescriptionRow for Kusto columns"""
        return [
            CursorDescriptionRow(
                name=column.column_name,
                type=column.column_type,
                display_size=None,
                internal_size=None,
                precision=None,
                scale=None,
                null_ok=True,
            )
            for column in columns
        ]

    @check_closed
    def __iter__(self):
        return self

    @check_result
    @check_closed
    def __next__(self):
        return next(self._results)  # type: ignore

    next = __next__

    @staticmethod
    def _apply_parameters(operation, parameters) -> str:
        """Applies parameters to operation string"""
        if not parameters:
            return operation

        escaped_parameters = {key: Cursor._escape(value) for key, value in parameters.items()}
        return operation % escaped_parameters

    @staticmethod
    def _escape(value) -> str:
        """
        Escape the parameter value.

        Note that bool is a subclass of int so order of statements matter.
        """

        if value == "*":
            return value
        if isinstance(value, str):
            return "'{}'".format(value.replace("'", "''"))
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            return ", ".join(Cursor._escape(element) for element in value)

        return value
