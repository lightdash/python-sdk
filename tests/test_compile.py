"""
Unit tests for Query.compile() — compile a query to warehouse SQL (issue #5).

Uses a fake client (no network); the live path is covered by the env-gated
acceptance test ``test_compile_query``.
"""

import pytest
from lightdash.models import Model
from lightdash.dimensions import Dimension


class FakeClient:
    project_uuid = "proj-uuid"

    def __init__(self):
        self.requests = []

    def get_query_limits(self):
        return {}

    def _make_request(self, method, path, params=None, json=None):
        self.requests.append({"method": method, "path": path, "json": json})
        return {"query": "SELECT 1 AS x FROM `t`", "parameterReferences": []}


@pytest.fixture
def model():
    m = Model(name="orders", type="default", database_name="db", schema_name="s")
    m._client = FakeClient()
    return m


def dim(name):
    return Dimension(name=name, model_name="orders")


class TestCompile:
    def test_returns_sql_string(self, model):
        sql = model.query(metrics=["orders_revenue"], dimensions=["orders_country"]).compile()
        assert sql == "SELECT 1 AS x FROM `t`"

    def test_posts_to_compile_endpoint(self, model):
        model.query(metrics=["orders_revenue"]).compile()
        req = model._client.requests[-1]
        assert req["method"] == "POST"
        assert req["path"] == "/api/v1/projects/proj-uuid/explores/orders/compileQuery"

    def test_payload_includes_additional_metrics(self, model):
        model.query(metrics=["orders_revenue"]).compile()
        assert model._client.requests[-1]["json"]["additionalMetrics"] == []

    def test_no_filters_sends_empty_object(self, model):
        """With no filters, send {} — the shape the v1 endpoint accepts."""
        model.query(metrics=["orders_revenue"]).compile()
        assert model._client.requests[-1]["json"]["filters"] == {}

    def test_filters_get_ids_on_group_and_rules(self, model):
        """The v1 endpoint requires an id on every filter group and rule."""
        model.query(metrics=["orders_revenue"]).filter(dim("country") == "USA").compile()
        filters = model._client.requests[-1]["json"]["filters"]
        group = filters["dimensions"]
        assert "id" in group
        rule = group["and"][0]
        assert "id" in rule
        assert rule["target"]["fieldId"] == "orders_country"
        assert rule["operator"] == "equals"
        assert rule["values"] == ["USA"]

    def test_limit_included(self, model):
        """The limit flows into the payload (and thus the SQL) unbounded."""
        model.query(metrics=["orders_revenue"]).limit(10_000_000).compile()
        assert model._client.requests[-1]["json"]["limit"] == 10_000_000

    def test_requires_client(self):
        m = Model(name="orders", type="default", database_name="db", schema_name="s")
        with pytest.raises(RuntimeError, match="not properly initialized"):
            m.query(metrics=["orders_revenue"]).compile()
