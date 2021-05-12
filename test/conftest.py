from sqlalchemy.dialects import registry

registry.register("kusto", "sqlalchemy_kusto.dialect", "KustoHTTPDialect")
registry.register("kusto.http", "sqlalchemy_kusto.dialect", "KustoHTTPDialect")
registry.register("kusto.https", "sqlalchemy_kusto.dialect", "KustoHTTPSDialect")