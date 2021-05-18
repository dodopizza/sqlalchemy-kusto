import os
from sqlalchemy.dialects import registry
from dotenv import load_dotenv

registry.register("kusto", "sqlalchemy_kusto.dialect", "KustoHTTPDialect")
registry.register("kusto.http", "sqlalchemy_kusto.dialect", "KustoHTTPDialect")
registry.register("kusto.https", "sqlalchemy_kusto.dialect", "KustoHTTPSDialect")


load_dotenv()
AZURE_AD_CLIENT_ID = os.environ["AZURE_AD_CLIENT_ID"]
AZURE_AD_CLIENT_SECRET = os.environ["AZURE_AD_CLIENT_SECRET"]
AZURE_AD_TENANT_ID = os.environ["AZURE_AD_TENANT_ID"]
KUSTO_URL = os.environ["KUSTO_URL"]
KUSTO_ALCHEMY_URL = "kusto+" + os.environ["KUSTO_URL"]
DATABASE = os.environ["DATABASE"]
