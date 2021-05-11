from sqlalchemy_kusto.dbapi_connection import Connection


def connect(
        cluster: str,
        msi: bool = False,
        user_msi: str = None,
        azure_ad_client_id: str = None,
        azure_ad_client_secret: str = None,
        azure_ad_tenant_id: str = None,
):
    """
    Constructor for creating a connection to the database.

    """
    return Connection(
        cluster,
        msi,
        user_msi,
        azure_ad_client_id,
        azure_ad_client_secret,
        azure_ad_tenant_id,
    )


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Exception(
                "{klass} already closed".format(klass=self.__class__.__name__)
            )
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Exception("Called before `execute`")
        return f(self, *args, **kwargs)

    return g

