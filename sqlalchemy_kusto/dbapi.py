from sqlalchemy_kusto.dbapi_connection import Connection


def connect(
        cluster: str,
        database: str,
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
        database,
        msi,
        user_msi,
        azure_ad_client_id,
        azure_ad_client_secret,
        azure_ad_tenant_id,
    )


