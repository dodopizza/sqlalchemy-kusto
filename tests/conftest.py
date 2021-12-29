import os
from sqlalchemy.dialects import registry
from dotenv import load_dotenv

registry.register("kustosql.https", "sqlalchemy_kusto.dialect_sql", "KustoSqlHttpsDialect")
registry.register("kustokql.https", "sqlalchemy_kusto.dialect_kql", "KustoKqlHttpsDialect")

load_dotenv()
AZURE_AD_CLIENT_ID = os.environ["AZURE_AD_CLIENT_ID"]
AZURE_AD_CLIENT_SECRET = os.environ["AZURE_AD_CLIENT_SECRET"]
AZURE_AD_TENANT_ID = os.environ["AZURE_AD_TENANT_ID"]
KUSTO_URL = os.environ["KUSTO_URL"]
KUSTO_SQL_ALCHEMY_URL = "kustosql+" + os.environ["KUSTO_URL"]
KUSTO_KQL_ALCHEMY_URL = "kustokql+" + os.environ["KUSTO_URL"]
DATABASE = os.environ["DATABASE"]
