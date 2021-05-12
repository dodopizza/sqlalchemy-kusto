from sqlalchemy.dialects import registry
import pytest

registry.register("kusto", "kusto.http", "kusto.https")

# pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

# from sqlalchemy.testing.plugin.pytestplugin import *