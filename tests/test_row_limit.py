"""
Tests for fetching results beyond the old 50k cap (issue #19).

Validates that the SDK:
- rejects limits below 1 locally,
- discovers the instance's real ``query.maxLimit`` and rejects larger limits
  with a clear error (no silent truncation),
- pages large fetches at the instance's ``maxPageSize``, and
- paginates with a *uniform* page size so no rows are skipped or duplicated.
"""

import pytest
from lightdash.models import Model
from lightdash.query import Query


class FakeClient:
    """Minimal client stand-in driving the executor without a network."""

    project_uuid = "proj"

    def __init__(self, limits, rows=None):
        self._limits = limits
        self._rows = rows if rows is not None else []
        self.page_requests = []  # (page, pageSize) tuples
        self.submitted = False

    def get_query_limits(self):
        return self._limits

    def _make_request(self, method, path, params=None, json=None):
        if path.endswith("/query/metric-query"):
            self.submitted = True
            return {"queryUuid": "q-1", "fields": {}}

        # GET a page of results
        page = params["page"]
        page_size = params["pageSize"]
        self.page_requests.append((page, page_size))
        start = (page - 1) * page_size
        chunk = self._rows[start:start + page_size]
        total = len(self._rows)
        total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
        return {
            "status": "ready",
            "rows": chunk,
            "totalResults": total,
            "totalPageCount": total_pages,
            "pageSize": page_size,
        }


@pytest.fixture
def model():
    m = Model(name="m", type="default", database_name="db", schema_name="s")
    return m


def _attach(model, client):
    model._client = client
    return model


class TestLimitValidation:
    def test_limit_below_one_rejected(self, model):
        with pytest.raises(ValueError, match="Limit must be at least 1"):
            model.query().limit(0).execute()

    def test_limit_above_max_rejected(self, model):
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2500})
        _attach(model, client)
        with pytest.raises(ValueError, match="exceeds this instance's maximum"):
            model.query().limit(200000).execute()
        # Validation happens before any query is submitted.
        assert client.submitted is False

    def test_limit_above_old_50k_cap_allowed(self, model):
        """Limits between 50k and the server max are now allowed."""
        rows = [{"m_v": {"value": {"raw": i}}} for i in range(60000)]
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2500}, rows=rows)
        _attach(model, client)
        result = model.query().limit(60000).execute()
        assert result.total_results == 60000

    def test_fail_open_when_health_unavailable(self, model):
        """If /health errors, the query still runs (server enforces limits)."""
        class NoHealthClient(FakeClient):
            def get_query_limits(self):
                raise RuntimeError("health down")

        client = NoHealthClient({}, rows=[{"m_v": {"value": {"raw": 1}}}])
        _attach(model, client)
        result = model.query().limit(99999).execute()
        assert result.total_results == 1


class TestPageSizeSelection:
    def test_uses_max_page_size_for_large_pulls(self, model):
        rows = [{"m_v": {"value": {"raw": i}}} for i in range(10)]
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2500}, rows=rows)
        _attach(model, client)
        model.query().limit(100000).execute()
        # First page fetched at the instance maxPageSize, not the legacy 500.
        assert client.page_requests[0] == (1, 2500)

    def test_page_size_bounded_by_limit(self, model):
        rows = [{"m_v": {"value": {"raw": i}}} for i in range(5)]
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2500}, rows=rows)
        _attach(model, client)
        model.query().limit(50).execute()
        # Never request a page larger than the requested limit.
        assert client.page_requests[0] == (1, 50)


class TestPaginationConsistency:
    def test_all_rows_returned_exactly_once(self, model):
        """A multi-page fetch returns every row once, in order (no skips)."""
        rows = [{"m_v": {"value": {"raw": i}}} for i in range(5)]
        # maxPageSize=2 forces 3 pages for 5 rows.
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2}, rows=rows)
        _attach(model, client)
        records = model.query().limit(100).execute().to_records()
        assert [r["m_v"] for r in records] == [0, 1, 2, 3, 4]

    def test_every_page_fetched_at_same_size(self, model):
        rows = [{"m_v": {"value": {"raw": i}}} for i in range(5)]
        client = FakeClient({"maxLimit": 100000, "maxPageSize": 2}, rows=rows)
        _attach(model, client)
        model.query().limit(100).execute().to_records()
        # Pages 2 and 3 are fetched (page 1 is the pre-fetched first page),
        # all at the same page size of 2.
        assert client.page_requests == [(1, 2), (2, 2), (3, 2)]
