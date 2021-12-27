from typing import List

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from sqlalchemy_kusto.dbapi_cursor import Cursor
from sqlalchemy_kusto.utils import check_closed


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
        pass

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
