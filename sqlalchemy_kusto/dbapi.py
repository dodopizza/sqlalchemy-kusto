from collections import namedtuple
from typing import Any

from azure.kusto.data import (
    ClientRequestProperties,
    KustoClient,
    KustoConnectionStringBuilder,
)
from azure.identity import DefaultAzureCredential
from azure.kusto.data._models import KustoResultColumn
from azure.kusto.data.exceptions import KustoAuthenticationError, KustoServiceError

from sqlalchemy_kusto import errors

# PostgreSQL date_trunc grain → T-SQL datepart name
_GRAIN_TO_TSQL: dict[str, str] = {
    "microseconds": "millisecond",  # T-SQL has no microsecond
    "milliseconds": "millisecond",
    "second": "second",
    "minute": "minute",
    "hour": "hour",
    "day": "day",
    "week": "week",
    "week_sun": "week",  # extension: Sunday-based week (not in PostgreSQL)
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}

# Safe DATEDIFF base epoch per grain.
# For sub-day grains, epoch 0 (1900-01-01) causes integer overflow; use 2000-01-01.
# week_sun uses epoch -1 = 1899-12-31 (Sunday), so DATEADD/DATEDIFF stay in Sunday-aligned weeks.
_GRAIN_EPOCH: dict[str, str] = {
    "microseconds": "'2000-01-01'",
    "milliseconds": "'2000-01-01'",
    "second": "'2000-01-01'",
    "minute": "'2000-01-01'",
    "hour": "'2000-01-01'",
    "day": "0",
    "week": "0",
    "week_sun": "-1",
    "month": "0",
    "quarter": "0",
    "year": "0",
}


def _translate_raw_date_trunc(sql: str) -> str:
    """Translate date_trunc('grain', expr) calls in a raw SQL string to T-SQL DATEADD/DATEDIFF.

    Applied in Cursor.execute() so it covers every execution path including Superset SQLLab,
    which calls the DBAPI cursor directly via engine.raw_connection(), bypassing dialect hooks.
    Grain and function name matching are case-insensitive. Unknown grains are left unchanged.
    """
    marker = "date_trunc("
    result: list[str] = []
    i = 0
    sql_lower = sql.lower()

    while i < len(sql):
        pos = sql_lower.find(marker, i)
        if pos == -1:
            result.append(sql[i:])
            break

        result.append(sql[i:pos])

        # Walk forward tracking depth to find the matching ')'.
        depth = 1
        j = pos + len(marker)
        while j < len(sql) and depth > 0:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
            j += 1

        if depth != 0:
            # Unbalanced parentheses — leave the rest unchanged.
            result.append(sql[pos:])
            i = len(sql)
            break

        interior = sql[pos + len(marker) : j - 1]

        # Split interior at the first top-level comma to get grain and expression.
        comma_pos: int | None = None
        inner_depth = 0
        for k, ch in enumerate(interior):
            if ch == "(":
                inner_depth += 1
            elif ch == ")":
                inner_depth -= 1
            elif ch == "," and inner_depth == 0:
                comma_pos = k
                break

        if comma_pos is None:
            # Malformed call — leave unchanged.
            result.append(sql[pos:j])
            i = j
            continue

        grain_raw = interior[:comma_pos].strip().strip("'\"")
        grain = grain_raw.lower()
        expr = interior[comma_pos + 1 :].strip()

        part = _GRAIN_TO_TSQL.get(grain)
        if part is None:
            # Unknown grain — leave the whole call unchanged.
            result.append(sql[pos:j])
            i = j
            continue

        epoch = _GRAIN_EPOCH.get(grain, "0")
        if grain == "week":
            # T-SQL DATEDIFF(week,...) counts Sunday boundaries, but epoch 0 is Monday.
            # Shift the input back 1 day so Sunday stays inside its Mon-Sat week,
            # producing ISO-standard Monday-based truncation (matches PostgreSQL semantics).
            result.append(
                f"DATEADD({part}, DATEDIFF({part}, {epoch}, DATEADD(day, -1, {expr})), {epoch})"
            )
        else:
            result.append(f"DATEADD({part}, DATEDIFF({part}, {epoch}, {expr}), {epoch})")
        i = j

    return "".join(result)


def check_closed(func):
    """Decorator that checks if connection/cursor is closed."""

    def decorator(self, *args, **kwargs):
        if self.closed:
            raise ValueError(f"{self.__class__.__name__} already closed")
        return func(self, *args, **kwargs)

    return decorator


def check_result(func):
    """Decorator that checks if the cursor has results from `execute`."""

    def decorator(self, *args, **kwargs):
        if self._results is None:
            raise ValueError("Called before `execute`")
        return func(self, *args, **kwargs)

    return decorator


def connect(
    cluster: str,
    database: str,
    msi: bool = False,
    user_msi: str | None = None,
    workload_identity: bool = False,
    azure_ad_client_id: str | None = None,
    azure_ad_client_secret: str | None = None,
    azure_ad_tenant_id: str | None = None,
    app_name: str | None = None,
    app_version: str | None = None,
):  # pylint: disable=too-many-positional-arguments
    """Return a connection to the database."""
    return Connection(
        cluster,
        database,
        msi,
        workload_identity,
        user_msi,
        azure_ad_client_id,
        azure_ad_client_secret,
        azure_ad_tenant_id,
        app_name,
        app_version,
    )


class Connection:
    """Connection to Kusto cluster."""

    def __init__(
        self,
        cluster: str,
        database: str,
        msi: bool = False,
        workload_identity: bool = False,
        user_msi: str | None = None,
        azure_ad_client_id: str | None = None,
        azure_ad_client_secret: str | None = None,
        azure_ad_tenant_id: str | None = None,
        app_name: str | None = None,
        app_version: str | None = None,
    ):
        self.closed = False
        self.cursors: list[Cursor] = []
        kcsb = None

        if azure_ad_client_id and azure_ad_client_secret and azure_ad_tenant_id:
            # Service Principal auth
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                connection_string=cluster,
                aad_app_id=azure_ad_client_id,
                app_key=azure_ad_client_secret,
                authority_id=azure_ad_tenant_id,
            )
        elif workload_identity:
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                cluster, DefaultAzureCredential()
            )
        elif msi:
            # Managed Service Identity (MSI)
            if user_msi is None or user_msi == "":
                # System managed identity
                kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                    cluster
                )
            else:
                # user managed identity
                kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                    cluster, client_id=user_msi
                )
        else:
            # neither SP or MSI
            kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
        kcsb._set_connector_details(
            "sqlalchemy-kusto",
            "3.1.0",
            app_name,
            app_version,
        )
        self.kusto_client = KustoClient(kcsb)
        self.database = database
        self.properties = ClientRequestProperties()

    @check_closed
    def close(self):
        """Close the connection now. Kusto does not require to close the connection."""
        self.closed = True
        for cursor in self.cursors:
            if not cursor.closed:
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
        properties: ClientRequestProperties | None = None,
    ):
        self._results: list[tuple[Any, ...]] | None = None
        self.kusto_client = kusto_client
        self.database = database
        self.closed = False
        self.description: list[CursorDescriptionRow] | None = None
        self.current_item_index = 0
        self.properties = (
            properties if properties is not None else ClientRequestProperties()
        )

    @property
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

        query = Cursor._apply_parameters(_translate_raw_date_trunc(operation), parameters)
        query = query.rstrip()
        try:
            server_response = self.kusto_client.execute(
                self.database, query, self.properties
            )
        except KustoServiceError as kusto_error:
            raise errors.DatabaseError(str(kusto_error)) from kusto_error
        except KustoAuthenticationError as context_error:
            raise errors.OperationalError(str(context_error)) from context_error

        rows = []
        for row in server_response.primary_results[0]:
            rows.append(tuple(row.to_list()))
        self._results = rows
        self.description = self._get_description_from_columns(
            server_response.primary_results[0].columns
        )
        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        """Not supported."""
        raise NotImplementedError(
            "`executemany` is not supported, use `execute` instead"
        )

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
    def fetchmany(self, size: int | None = None):
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
        """Not supported."""

    @check_closed
    def setoutputsizes(self, sizes):
        """Not supported."""

    @staticmethod
    def _get_description_from_columns(
        columns: list[KustoResultColumn],
    ) -> list[CursorDescriptionRow]:
        """Gets CursorDescriptionRow for Kusto columns."""
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
    def _apply_parameters(operation, parameters: dict) -> str:
        """Applies parameters to operation string."""
        if not parameters:
            return operation

        escaped_parameters = {
            key: Cursor._escape(value) for key, value in parameters.items()
        }
        return operation % escaped_parameters

    @staticmethod
    def _escape(value: Any) -> str:
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
        if isinstance(value, int | float):
            return str(value)
        if isinstance(value, list | tuple):
            return ", ".join(Cursor._escape(element) for element in value)

        return value
