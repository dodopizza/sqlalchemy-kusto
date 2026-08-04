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


def _skip_opaque(sql: str, i: int) -> int | None:
    """Index just past the literal or comment starting at `i`, else None.

    Comments must be skipped, not just left in place: an apostrophe inside one
    ("-- don't ask") would otherwise be read as the start of a string literal and
    swallow the rest of the query, so a real call after it never got translated.
    """
    char = sql[i]
    if char in "'\"":
        return _skip_literal(sql, i)
    if sql.startswith("--", i):
        end = sql.find("\n", i)
        return len(sql) if end == -1 else end + 1
    if sql.startswith("/*", i):
        end = sql.find("*/", i)
        return len(sql) if end == -1 else end + 2
    return None


def _split_call_args(sql: str, start: int) -> tuple[list[str], int] | None:
    """Split the argument list of a call whose '(' ends just before `start`.

    Returns (arguments, index just past the closing paren), or None when the call is
    unbalanced, so the caller can leave such SQL for Kusto to reject. Commas inside
    nested calls and inside string literals do not split.
    """
    args: list[str] = []
    depth = 1
    arg_start = i = start
    while i < len(sql):
        char = sql[i]
        skipped = _skip_opaque(sql, i)
        if skipped is not None:
            i = skipped
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                args.append(sql[arg_start:i])
                return args, i + 1
        elif char == "," and depth == 1:
            args.append(sql[arg_start:i])
            arg_start = i + 1
        i += 1
    return None


def _date_part_expr(unit: str, expr: str) -> str | None:
    """date_part('unit', x) → DATEPART(unit, x); T-SQL spells the unit bare."""
    part = unit.strip().strip("'\"").lower()
    if not part.isalpha():
        return None
    return f"DATEPART({part}, {expr})"


# Functions Kusto's T-SQL emulation does not implement, mapped to the form it does.
# Every replacement here was executed against the Kusto emulator; anything without an
# exact equivalent (FORMAT, DATE_FORMAT, TRY_CAST, JSON_VALUE, OPENJSON) is left alone
# on purpose, so Kusto reports it instead of us guessing at the semantics.
_CALL_TRANSLATIONS: dict[str, tuple[int, Any]] = {
    # name: (argument count, builder)
    "date_trunc": (2, _date_trunc_expr),
    "datetrunc": (2, _date_trunc_expr),  # T-SQL 2022 spelling, same arguments
    "date_part": (2, _date_part_expr),
    "startofday": (1, lambda expr: _date_trunc_expr("day", expr)),
    "startofweek": (1, lambda expr: _date_trunc_expr("week_sun", expr)),
    "startofmonth": (1, lambda expr: _date_trunc_expr("month", expr)),
    "startofyear": (1, lambda expr: _date_trunc_expr("year", expr)),
    "ifnull": (2, lambda a, b: f"COALESCE({a}, {b})"),
    # Kusto has a single datetime type, so CAST/CONVERT to DATE keeps the time of day.
    # DATE(x) and to_date(x) are expected to drop it, which is a truncation to the day.
    "to_date": (1, lambda expr: _date_trunc_expr("day", expr)),
    "date": (1, lambda expr: _date_trunc_expr("day", expr)),
}

# Longest first, so "date" cannot shadow "date_trunc".
_TRANSLATION_NAMES_LONGEST_FIRST = sorted(_CALL_TRANSLATIONS, key=len, reverse=True)
# First letters of those names: lets the scanner skip most characters outright.
_TRANSLATION_FIRST_CHARS = frozenset(name[0] for name in _CALL_TRANSLATIONS)
_IDENTIFIER_CHARS = frozenset('_.[]"')


def _translate_raw_functions(sql: str) -> str:
    """Rewrite calls Kusto cannot parse into their T-SQL equivalents.

    Applied in Cursor.execute() so it covers every execution path including Superset
    SQLLab, which talks to the DBAPI cursor directly and bypasses the dialect hooks.
    Names match case-insensitively; string literals, unknown grains and unknown call
    shapes are left untouched.
    """
    result: list[str] = []
    sql_lower = sql.lower()
    i = 0

    while i < len(sql):
        end = _skip_opaque(sql, i)  # never rewrite inside a literal or a comment
        if end is not None:
            result.append(sql[i:end])
            i = end
            continue

        match = _match_call_name(sql, sql_lower, i)
        if match is None:
            result.append(sql[i])
            i += 1
            continue

        name, args_start = match
        arity, builder = _CALL_TRANSLATIONS[name]
        parsed = _split_call_args(sql, args_start)
        if parsed is None:  # unbalanced parentheses: leave the remainder as it is
            result.append(sql[i:])
            break

        args, end = parsed
        if len(args) != arity or any(not arg.strip() for arg in args):
            result.append(sql[i:end])
            i = end
            continue

        translated = builder(*(_translate_raw_functions(arg.strip()) for arg in args))
        result.append(translated if translated is not None else sql[i:end])
        i = end

    return "".join(result)


def _match_call_name(sql: str, sql_lower: str, i: int) -> tuple[str, int] | None:
    """Return (function name, index just past its '(') if a known call starts at `i`."""
    if sql_lower[i] not in _TRANSLATION_FIRST_CHARS:
        return None  # cheap gate: no translated name starts with this character
    previous = sql[i - 1] if i else ""
    if previous.isalnum() or previous in _IDENTIFIER_CHARS:
        return None  # part of a longer identifier, e.g. my_date(
    for name in _TRANSLATION_NAMES_LONGEST_FIRST:
        if not sql_lower.startswith(name, i):
            continue
        after = i + len(name)
        while after < len(sql) and sql[after] in " \t\r\n":
            after += 1  # `date (x)` is the same call as `date(x)`
        if sql[after : after + 1] == "(":
            return name, after + 1
    return None


def is_tsql_query(operation: str) -> bool:
    """Whether to send `operation` as T-SQL rather than KQL.

    Kusto needs the query language up front and the only hint available is the text.
    A leading WITH counts: Kusto executes CTEs, and treating those as KQL used to make
    every CTE query fail before it was even sent. Leading whitespace, semicolons and
    comments are skipped; scanning them by hand keeps this linear, which a regex with
    a repeated alternation would not be.
    """
    i, length = 0, len(operation)
    while i < length:
        char = operation[i]
        if char.isspace() or char == ";":
            i += 1
        elif operation.startswith("--", i):
            end = operation.find("\n", i)
            i = length if end == -1 else end + 1
        elif operation.startswith("/*", i):
            end = operation.find("*/", i)
            i = length if end == -1 else end + 2
        else:
            break
    for keyword in ("select", "with"):
        end = i + len(keyword)
        if operation[i:end].lower() != keyword:
            continue
        # must be a whole word: `withdrawals | count` is a KQL table, not a CTE
        following = operation[end : end + 1]
        if not (following.isalnum() or following == "_"):
            return True
    return False


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

        if cluster.startswith("http://"):
            # Plain HTTP means the Kusto emulator: it has no TLS and no authentication.
            kcsb = KustoConnectionStringBuilder(cluster)
        elif azure_ad_client_id and azure_ad_client_secret and azure_ad_tenant_id:
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
        if is_tsql_query(operation):
            self.properties.set_option("query_language", "sql")
            # T-SQL only: a KQL query must never be rewritten.
            operation = _translate_raw_functions(operation)
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
