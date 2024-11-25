import os

from dotenv import load_dotenv
from sqlalchemy.dialects import registry

registry.register("kustosql.https", "sqlalchemy_kusto.dialect_sql", "KustoSqlHttpsDialect")
registry.register("kustokql.https", "sqlalchemy_kusto.dialect_kql", "KustoKqlHttpsDialect")

load_dotenv()
AZURE_AD_CLIENT_ID = os.environ.get("AZURE_AD_CLIENT_ID", "")
AZURE_AD_CLIENT_SECRET = os.environ.get("AZURE_AD_CLIENT_SECRET", "")
AZURE_AD_TENANT_ID = os.environ.get("AZURE_AD_TENANT_ID", "")
KUSTO_URL = os.environ["KUSTO_URL"]
KUSTO_SQL_ALCHEMY_URL = "kustosql+" + KUSTO_URL
KUSTO_KQL_ALCHEMY_URL = "kustokql+" + KUSTO_URL
DATABASE = os.environ["DATABASE"]
