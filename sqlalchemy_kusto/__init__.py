from sqlalchemy_kusto.dbapi import connect
# pylint: disable=redefined-builtin
from sqlalchemy_kusto.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]

apilevel = "2.0"  # pylint: disable=invalid-name
# Threads may share the module and connections
threadsafety = 2  # pylint: disable=invalid-name
paramstyle = "pyformat"  # pylint: disable=invalid-name
