from sqlalchemy_kusto.dbapi_cursor import Cursor
from sqlalchemy_kusto.utils import check_closed
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties


class Connection(object):
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
            query_language: str = "kql",
    ):
        self.closed = False
        self.cursors = []

        kcsb = self.get_connection_string_builder(
            azure_ad_client_id,
            azure_ad_client_secret,
            azure_ad_tenant_id,
            cluster,
            msi,
            user_msi)

        self.kusto_client = KustoClient(kcsb)
        self.database = database
        self.properties = ClientRequestProperties()
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/request-properties
        self.properties.set_option("query_language", query_language)

    @staticmethod
    def get_connection_string_builder(
            azure_ad_client_id,
            azure_ad_client_secret,
            azure_ad_tenant_id,
            cluster,
            msi,
            user_msi):
        # Managed Service Identity(MSI)
        if msi:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                cluster,
                client_id=user_msi
            )
        # Service Principal auth
        else:
            return KustoConnectionStringBuilder.with_aad_application_key_authentication(
                connection_string=cluster,
                aad_app_id=azure_ad_client_id,
                app_key=azure_ad_client_secret,
                authority_id=azure_ad_tenant_id,
            )

    @check_closed
    def close(self):
        """No need to close connection."""
        self.closed = True
        for cursor in self.cursors:
            cursor.close()

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
            self.kusto_client,
            self.database,
            self.properties,
        )

        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        if operation.startswith("."):
            return self.cursor().execute_ddl(operation, parameters)
        return self.cursor().execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()
