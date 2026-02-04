"""
Unit tests for the Query builder pattern.

These tests verify the chainable query builder API works correctly
without requiring API access.
"""

import pytest
from lightdash.models import Model
from lightdash.metrics import Metric
from lightdash.dimensions import Dimension
from lightdash.query import Query
from lightdash.filter import DimensionFilter, CompositeFilter
from lightdash.sorting import Sort


@pytest.fixture
def model():
    """Create a test model without client reference."""
    return Model(
        name="test_model",
        type="default",
        database_name="test_db",
        schema_name="test_schema",
    )


@pytest.fixture
def metric():
    """Create a test metric."""
    return Metric(
        name="revenue",
        model_name="test_model",
        label="Revenue",
        description="Total revenue",
    )


@pytest.fixture
def metric2():
    """Create a second test metric."""
    return Metric(
        name="profit",
        model_name="test_model",
        label="Profit",
        description="Total profit",
    )


@pytest.fixture
def dimension():
    """Create a test dimension."""
    return Dimension(
        name="country",
        model_name="test_model",
        label="Country",
        description="Customer country",
    )


@pytest.fixture
def dimension2():
    """Create a second test dimension."""
    return Dimension(
        name="region",
        model_name="test_model",
        label="Region",
        description="Customer region",
    )


class TestQueryBuilderBasics:
    """Test basic query builder functionality."""

    def test_query_without_args_returns_empty_query(self, model):
        """Query with no args returns an empty query for builder pattern."""
        query = model.query()
        assert isinstance(query, Query)
        assert query._metrics == ()
        assert query._dimensions == ()
        assert query._limit == 500  # Default limit

    def test_metrics_method_adds_metrics(self, model, metric, metric2):
        """The .metrics() method adds metrics to the query."""
        query = model.query().metrics(metric, metric2)
        assert query._metrics == (metric, metric2)

    def test_dimensions_method_adds_dimensions(self, model, dimension, dimension2):
        """The .dimensions() method adds dimensions to the query."""
        query = model.query().dimensions(dimension, dimension2)
        assert query._dimensions == (dimension, dimension2)

    def test_limit_method_sets_limit(self, model):
        """The .limit() method sets the result limit."""
        query = model.query().limit(100)
        assert query._limit == 100

    def test_sort_method_adds_sorts(self, model, metric, dimension):
        """The .sort() method adds sort criteria."""
        sort1 = Sort(metric, descending=True)
        sort2 = Sort(dimension, descending=False)
        query = model.query().sort(sort1, sort2)
        assert query._sort == (sort1, sort2)


class TestQueryBuilderImmutability:
    """Test that query builder returns new immutable objects."""

    def test_metrics_returns_new_query(self, model, metric):
        """Calling .metrics() returns a new Query, not the same one."""
        query1 = model.query()
        query2 = query1.metrics(metric)
        assert query1 is not query2
        assert query1._metrics == ()
        assert query2._metrics == (metric,)

    def test_dimensions_returns_new_query(self, model, dimension):
        """Calling .dimensions() returns a new Query, not the same one."""
        query1 = model.query()
        query2 = query1.dimensions(dimension)
        assert query1 is not query2
        assert query1._dimensions == ()
        assert query2._dimensions == (dimension,)

    def test_filter_returns_new_query(self, model, dimension):
        """Calling .filter() returns a new Query, not the same one."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        query1 = model.query()
        query2 = query1.filter(filter1)
        assert query1 is not query2
        assert query1._filters is None
        assert query2._filters is not None

    def test_limit_returns_new_query(self, model):
        """Calling .limit() returns a new Query, not the same one."""
        query1 = model.query()
        query2 = query1.limit(100)
        assert query1 is not query2
        assert query1._limit == 500
        assert query2._limit == 100

    def test_sort_returns_new_query(self, model, metric):
        """Calling .sort() returns a new Query, not the same one."""
        sort = Sort(metric, descending=True)
        query1 = model.query()
        query2 = query1.sort(sort)
        assert query1 is not query2
        assert query1._sort == ()
        assert query2._sort == (sort,)


class TestQueryBuilderChaining:
    """Test that methods can be chained in any order."""

    def test_full_chain(self, model, metric, dimension):
        """Test chaining all builder methods."""
        sort = Sort(metric, descending=True)
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])

        query = (
            model.query()
            .metrics(metric)
            .dimensions(dimension)
            .filter(filter1)
            .sort(sort)
            .limit(100)
        )

        assert query._metrics == (metric,)
        assert query._dimensions == (dimension,)
        assert query._filters is not None
        assert len(query._filters.filters) == 1
        assert query._sort == (sort,)
        assert query._limit == 100

    def test_chain_order_independent(self, model, metric, dimension):
        """Test that chain order doesn't matter for independent operations."""
        sort = Sort(metric, descending=True)

        # Different order
        query = (
            model.query()
            .limit(100)
            .sort(sort)
            .dimensions(dimension)
            .metrics(metric)
        )

        assert query._metrics == (metric,)
        assert query._dimensions == (dimension,)
        assert query._sort == (sort,)
        assert query._limit == 100

    def test_reuse_base_query(self, model, metric, dimension):
        """Test that base queries can be reused for different variations."""
        base_query = model.query().metrics(metric).dimensions(dimension)

        filter_usa = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        filter_uk = DimensionFilter(field=dimension, operator="equals", values=["UK"])

        usa_query = base_query.filter(filter_usa)
        uk_query = base_query.filter(filter_uk)

        # Base query unchanged
        assert base_query._filters is None

        # Both derived queries have their filters
        assert usa_query._filters is not None
        assert uk_query._filters is not None

        # Filters are different
        assert usa_query._filters.filters[0].values == ["USA"]
        assert uk_query._filters.filters[0].values == ["UK"]


class TestQueryBuilderFilters:
    """Test filter combining behavior."""

    def test_single_filter(self, model, dimension):
        """Test adding a single filter."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        query = model.query().filter(filter1)

        assert query._filters is not None
        assert isinstance(query._filters, CompositeFilter)
        assert len(query._filters.filters) == 1

    def test_multiple_filters_combined_with_and(self, model, dimension, dimension2):
        """Test that multiple filters are combined with AND logic."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        filter2 = DimensionFilter(field=dimension2, operator="equals", values=["West"])

        query = model.query().filter(filter1).filter(filter2)

        assert query._filters is not None
        assert len(query._filters.filters) == 2
        assert query._filters.aggregation == "and"

    def test_composite_filter(self, model, dimension, dimension2):
        """Test adding a CompositeFilter directly."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        filter2 = DimensionFilter(field=dimension2, operator="equals", values=["West"])
        composite = CompositeFilter(filters=[filter1, filter2], aggregation="or")

        query = model.query().filter(composite)

        assert query._filters is not None
        assert len(query._filters.filters) == 2
        assert query._filters.aggregation == "or"


class TestQueryBuilderAccumulation:
    """Test that methods accumulate values correctly."""

    def test_multiple_metrics_calls_accumulate(self, model, metric, metric2):
        """Test that multiple .metrics() calls accumulate."""
        query = model.query().metrics(metric).metrics(metric2)
        assert query._metrics == (metric, metric2)

    def test_multiple_dimensions_calls_accumulate(self, model, dimension, dimension2):
        """Test that multiple .dimensions() calls accumulate."""
        query = model.query().dimensions(dimension).dimensions(dimension2)
        assert query._dimensions == (dimension, dimension2)

    def test_multiple_sort_calls_accumulate(self, model, metric, dimension):
        """Test that multiple .sort() calls accumulate."""
        sort1 = Sort(metric, descending=True)
        sort2 = Sort(dimension, descending=False)
        query = model.query().sort(sort1).sort(sort2)
        assert query._sort == (sort1, sort2)

    def test_limit_replaces_previous(self, model):
        """Test that .limit() replaces (doesn't accumulate)."""
        query = model.query().limit(100).limit(50)
        assert query._limit == 50


class TestBackwardsCompatibility:
    """Test that existing single-call API still works."""

    def test_single_call_with_all_args(self, model, metric, dimension):
        """Test the original single-call pattern still works."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        sort = Sort(metric, descending=True)

        query = model.query(
            metrics=[metric],
            dimensions=[dimension],
            filters=filter1,
            sort=sort,
            limit=100,
        )

        assert query._metrics == (metric,)
        assert query._dimensions == (dimension,)
        assert query._filters is not None
        assert query._sort == (sort,)
        assert query._limit == 100

    def test_single_metric_string(self, model):
        """Test that single metric as string still works."""
        query = model.query(metrics="test_model_revenue")
        assert query._metrics == ("test_model_revenue",)

    def test_single_dimension_string(self, model, metric):
        """Test that single dimension as string still works."""
        query = model.query(metrics=metric, dimensions="test_model_country")
        assert query._dimensions == ("test_model_country",)
