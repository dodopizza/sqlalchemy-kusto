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

# PostgreSQL date_trunc grain → (T-SQL datepart, DATEDIFF base epoch).
# Epoch 0 is 1900-01-01, a Monday. Sub-day grains use '2000-01-01' instead because
# DATEDIFF(second, 0, <today>) overflows a 32-bit int.
# week_sun uses epoch -1 = 1899-12-31, a Sunday, keeping weeks Sunday-aligned.
# Grains finer than a second are deliberately absent: DATEDIFF(millisecond, ...)
# overflows after 24 days from any fixed epoch, so there is no usable form.
_GRAIN_TO_TSQL: dict[str, tuple[str, str]] = {
    "second": ("second", "'2000-01-01'"),
    "minute": ("minute", "'2000-01-01'"),
    "hour": ("hour", "'2000-01-01'"),
    "day": ("day", "0"),
    "week": ("week", "0"),
    "week_sun": ("week", "-1"),  # extension: Sunday-based week (not in PostgreSQL)
    "month": ("month", "0"),
    "quarter": ("quarter", "0"),
    "year": ("year", "0"),
}

_DATE_TRUNC_MARKER = "date_trunc("


def _date_trunc_expr(grain: str, expr: str) -> str | None:
    """Build the T-SQL equivalent of date_trunc(grain, expr), or None if the grain is unknown.

    Grain matching is case-insensitive and tolerates surrounding quotes.
    """
    key = grain.strip().strip("'\"").lower()
    part_and_epoch = _GRAIN_TO_TSQL.get(key)
    if part_and_epoch is None:
        return None

    part, epoch = part_and_epoch
    if key == "week":
        # DATEDIFF(week, ...) counts Sunday boundaries while epoch 0 is a Monday.
        # Shifting the input back one day keeps Sunday inside the preceding Mon-Sun
        # week, which is PostgreSQL's ISO (Monday-based) truncation.
        expr = f"DATEADD(day, -1, {expr})"
    return f"DATEADD({part}, DATEDIFF({part}, {epoch}, {expr}), {epoch})"


def _skip_literal(sql: str, start: int) -> int:
    """Return the index just past the string literal opening at `start`."""
    quote = sql[start]
    i = start + 1
    while i < len(sql):
        if sql[i] == quote:
            if sql[i + 1 : i + 2] == quote:  # doubled quote escapes itself
                i += 2
                continue
            return i + 1
        i += 1
    return len(sql)  # unterminated literal: treat the remainder as opaque


def _split_date_trunc_args(sql: str, start: int) -> tuple[str, str, int] | None:
    """Parse `grain, expr)` starting at `start`.

    Returns (grain, expr, index just past the closing paren), or None when the call
    is malformed, so the caller can leave such SQL for Kusto to reject.
    """
    depth = 1
    comma = None
    i = start
    while i < len(sql):
        char = sql[i]
        if char in "'\"":
            i = _skip_literal(sql, i)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return (sql[start:comma], sql[comma + 1 : i], i + 1) if comma else None
        elif char == "," and depth == 1 and comma is None:
            comma = i
        i += 1
    return None


def _translate_raw_date_trunc(sql: str) -> str:
    """Rewrite date_trunc('grain', expr) in a raw SQL string as T-SQL DATEADD/DATEDIFF.

    Applied in Cursor.execute() so it covers every execution path including Superset SQLLab,
    which calls the DBAPI cursor directly via engine.raw_connection(), bypassing dialect hooks.
    Function and grain names match case-insensitively; string literals and unknown grains
    are left untouched.
    """
    result: list[str] = []
    sql_lower = sql.lower()
    i = 0

    while i < len(sql):
        if sql[i] in "'\"":  # never rewrite anything inside a string literal
            end = _skip_literal(sql, i)
            result.append(sql[i:end])
            i = end
            continue

        if not sql_lower.startswith(_DATE_TRUNC_MARKER, i):
            result.append(sql[i])
            i += 1
            continue

        parsed = _split_date_trunc_args(sql, i + len(_DATE_TRUNC_MARKER))
        if parsed is None:
            result.append(sql[i:])
            break

        grain, expr, end = parsed
        translated = _date_trunc_expr(grain, _translate_raw_date_trunc(expr.strip()))
        result.append(translated if translated is not None else sql[i:end])
        i = end

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
            # T-SQL only: a KQL query must never be rewritten.
            operation = _translate_raw_date_trunc(operation)
        else:
            self.properties.set_option("query_language", "kql")

        query = Cursor._apply_parameters(operation, parameters)
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
