import itertools
import json
from collections import namedtuple, OrderedDict
from urllib import parse
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(
    host="localhost",
    port=8082,
    path="/druid/v2/sql/",
    scheme="http",
    user=None,
    password=None,
    context=None,
    header=False,
    ssl_verify_cert=True,
    ssl_client_cert=None,
    proxies=None,
):  # noqa: E125
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    context = context or {}

    return Connection(
        host,
        port,
        path,
        scheme,
        user,
        password,
        context,
        header,
        ssl_verify_cert,
        ssl_client_cert,
        proxies,
    )


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                "{klass} already closed".format(klass=self.__class__.__name__)
            )
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return g


def get_description_from_row(row):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            name,  # name
            get_type(value),  # type_code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            get_type(value) == Type.STRING,  # [null_ok]
        )
        for name, value in row.items()
    ]


def get_type(value):
    """
    Infer type from value.

    Note that bool is a subclass of int so order of statements matter.
    """

    if isinstance(value, str) or value is None:
        return Type.STRING
    elif isinstance(value, bool):
        return Type.BOOLEAN
    elif isinstance(value, (int, float)):
        return Type.NUMBER

    raise exceptions.Error("Value of unknown type: {value}".format(value=value))


class Connection(object):
    """Connection to a Druid database."""

    def __init__(
        self,
        host="localhost",
        port=8082,
        path="/druid/v2/sql/",
        scheme="http",
        user=None,
        password=None,
        context=None,
        header=False,
        ssl_verify_cert=True,
        ssl_client_cert=None,
        proxies=None,
    ):
        netloc = "{host}:{port}".format(host=host, port=port)
        self.url = parse.urlunparse((scheme, netloc, path, None, None, None))
        self.context = context or {}
        self.closed = False
        self.cursors = []
        self.header = header
        self.user = user
        self.password = password
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_client_cert = ssl_client_cert
        self.proxies = proxies

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""

        cursor = Cursor(
            self.url,
            self.user,
            self.password,
            self.context,
            self.header,
            self.ssl_verify_cert,
            self.ssl_client_cert,
            self.proxies,
        )

        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


def rows_from_chunks(chunks):
    """
    A generator that yields rows from JSON chunks.

    Druid will return the data in chunks, but they are not aligned with the
    JSON objects. This function will parse all complete rows inside each chunk,
    yielding them as soon as possible.
    """
    body = ""
    for chunk in chunks:
        if chunk:
            body = "".join((body, chunk))

        # find last complete row
        boundary = 0
        brackets = 0
        in_string = False
        for i, char in enumerate(body):
            if char == '"':
                if not in_string:
                    in_string = True
                elif body[i - 1] != "\\":
                    in_string = False

            if in_string:
                continue

            if char == "{":
                brackets += 1
            elif char == "}":
                brackets -= 1
                if brackets == 0 and i > boundary:
                    boundary = i + 1

        rows = body[:boundary].lstrip("[,")
        body = body[boundary:]

        for row in json.loads(
            "[{rows}]".format(rows=rows), object_pairs_hook=OrderedDict
        ):
            yield row


def apply_parameters(operation, parameters):
    if not parameters:
        return operation

    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value):
    """
    Escape the parameter value.

    Note that bool is a subclass of int so order of statements matter.
    """

    if value == "*":
        return value
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)
