"""
Unit tests for validation and usability improvements.

These tests don't require API access - they test local validation logic.
"""

import pytest
from lightdash.dimensions import Dimension, Dimensions
from lightdash.metrics import Metric, Metrics
from lightdash.models import Model, Models
from lightdash.filter import DimensionFilter, CompositeFilter
from lightdash.sorting import Sort
from lightdash.query import Query
from unittest.mock import MagicMock


# Fixtures for testing

@pytest.fixture
def sample_dimension():
    return Dimension(name="country", model_name="orders")


@pytest.fixture
def sample_metric():
    return Metric(name="revenue", model_name="orders")


@pytest.fixture
def mock_model():
    """Create a mock model for testing."""
    model = Model(
        name="orders",
        type="default",
        database_name="analytics",
        schema_name="public",
    )
    # Mock the client
    model._client = MagicMock()
    return model


# Test 1: Sort parameter accepts single Sort object

class TestSortParameter:
    def test_sort_accepts_single_sort_object(self, mock_model, sample_metric):
        """Sort parameter should accept single Sort object."""
        sort = Sort(sample_metric, descending=True)
        query = mock_model.query(
            metrics=[sample_metric],
            sort=sort,
            limit=10,
        )
        # Sort should be normalized to a tuple internally (for immutability)
        assert query._sort == (sort,)

    def test_sort_accepts_sequence_of_sorts(self, mock_model, sample_metric, sample_dimension):
        """Sort parameter should accept sequence of Sort objects."""
        sorts = [
            Sort(sample_metric, descending=True),
            Sort(sample_dimension, descending=False),
        ]
        query = mock_model.query(
            metrics=[sample_metric],
            dimensions=[sample_dimension],
            sort=sorts,
            limit=10,
        )
        assert query._sort == tuple(sorts)

    def test_sort_accepts_none(self, mock_model, sample_metric):
        """Sort parameter should accept None (default)."""
        query = mock_model.query(
            metrics=[sample_metric],
            sort=None,
            limit=10,
        )
        # None normalizes to empty tuple for immutability
        assert query._sort == ()


# Test 2: Assertions replaced with proper validation

class TestFilterValidation:
    def test_invalid_operator_raises_value_error(self, sample_dimension):
        """Invalid filter operator should raise ValueError, not AssertionError."""
        with pytest.raises(ValueError) as exc_info:
            DimensionFilter(
                field=sample_dimension,
                operator="invalid_operator",
                values=["test"],
            )
        assert "Invalid operator" in str(exc_info.value)
        assert "invalid_operator" in str(exc_info.value)

    def test_valid_operator_works(self, sample_dimension):
        """Valid operators should work without error."""
        f = DimensionFilter(
            field=sample_dimension,
            operator="equals",
            values=["test"],
        )
        assert f.operator == "equals"

    def test_field_must_be_dimension_object(self):
        """Field must be a Dimension object, not a string."""
        with pytest.raises(TypeError) as exc_info:
            DimensionFilter(
                field="not_a_dimension",  # type: ignore
                operator="equals",
                values=["test"],
            )
        assert "must be a Dimension object" in str(exc_info.value)

    def test_invalid_aggregation_raises_value_error(self, sample_dimension):
        """Invalid aggregation should raise ValueError."""
        f = DimensionFilter(
            field=sample_dimension,
            operator="equals",
            values=["test"],
        )
        with pytest.raises(ValueError) as exc_info:
            CompositeFilter(filters=[f], aggregation="invalid")
        assert "Invalid aggregation" in str(exc_info.value)

    def test_valid_aggregations_work(self, sample_dimension):
        """Valid aggregations should work."""
        f = DimensionFilter(
            field=sample_dimension,
            operator="equals",
            values=["test"],
        )
        cf_and = CompositeFilter(filters=[f], aggregation="and")
        assert cf_and.aggregation == "and"

        cf_or = CompositeFilter(filters=[f], aggregation="or")
        assert cf_or.aggregation == "or"

    def test_validation_works_with_python_o(self, sample_dimension):
        """Validation should still work with python -O (assertions disabled)."""
        # This test verifies the behavior is the same regardless of __debug__
        # If assertions were used, they'd be disabled with -O flag
        with pytest.raises(ValueError):
            DimensionFilter(
                field=sample_dimension,
                operator="not_valid",
                values=["test"],
            )


# Test 3: Fuzzy matching suggestions

class TestFuzzyMatchingSuggestions:
    def test_metric_typo_suggests_alternatives(self):
        """Typo in metric name should suggest similar metrics."""
        # Create a mock model with metrics
        mock_model = MagicMock()
        mock_model.list_metrics.return_value = [
            Metric(name="revenue", model_name="orders"),
            Metric(name="total_orders", model_name="orders"),
            Metric(name="average_order_value", model_name="orders"),
        ]

        metrics = Metrics(mock_model)

        # Access a typo of "revenue"
        with pytest.raises(AttributeError) as exc_info:
            _ = metrics.revnue  # typo

        error_msg = str(exc_info.value)
        assert "No metric named 'revnue' found" in error_msg
        assert "Did you mean" in error_msg
        assert "'revenue'" in error_msg

    def test_dimension_typo_suggests_alternatives(self):
        """Typo in dimension name should suggest similar dimensions."""
        mock_model = MagicMock()
        mock_model.list_dimensions.return_value = [
            Dimension(name="country", model_name="orders"),
            Dimension(name="city", model_name="orders"),
            Dimension(name="customer_name", model_name="orders"),
        ]

        dimensions = Dimensions(mock_model)

        with pytest.raises(AttributeError) as exc_info:
            _ = dimensions.contry  # typo

        error_msg = str(exc_info.value)
        assert "No dimension named 'contry' found" in error_msg
        assert "Did you mean" in error_msg
        assert "'country'" in error_msg

    def test_model_typo_suggests_alternatives(self):
        """Typo in model name should suggest similar models."""
        mock_client = MagicMock()

        # Create mock models
        orders = Model(name="orders", type="default", database_name="db", schema_name="schema")
        customers = Model(name="customers", type="default", database_name="db", schema_name="schema")
        products = Model(name="products", type="default", database_name="db", schema_name="schema")

        mock_client._fetch_models.return_value = [orders, customers, products]

        models = Models(mock_client)

        with pytest.raises(AttributeError) as exc_info:
            _ = models.ordrs  # typo

        error_msg = str(exc_info.value)
        assert "No model named 'ordrs' found" in error_msg
        assert "Did you mean" in error_msg
        assert "'orders'" in error_msg

    def test_no_suggestions_for_completely_different_name(self):
        """No suggestions when name is completely different."""
        mock_model = MagicMock()
        mock_model.list_metrics.return_value = [
            Metric(name="revenue", model_name="orders"),
        ]

        metrics = Metrics(mock_model)

        with pytest.raises(AttributeError) as exc_info:
            _ = metrics.xyz123  # completely different

        error_msg = str(exc_info.value)
        assert "No metric named 'xyz123' found" in error_msg
        # Should not have suggestions
        assert "Did you mean" not in error_msg

    def test_multiple_suggestions_shown(self):
        """Multiple similar names should all be suggested."""
        mock_model = MagicMock()
        mock_model.list_metrics.return_value = [
            Metric(name="total_revenue", model_name="orders"),
            Metric(name="total_cost", model_name="orders"),
            Metric(name="total_profit", model_name="orders"),
        ]

        metrics = Metrics(mock_model)

        with pytest.raises(AttributeError) as exc_info:
            _ = metrics.total_revenu  # typo

        error_msg = str(exc_info.value)
        assert "Did you mean" in error_msg
        assert "'total_revenue'" in error_msg
