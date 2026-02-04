"""
Tests for filter operator overloading.

Tests that dimension comparison operators and filter combining work correctly.
"""

import pytest
from lightdash.dimensions import Dimension
from lightdash.filter import DimensionFilter, CompositeFilter


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
        name="revenue",
        model_name="test_model",
        label="Revenue",
        description="Total revenue",
    )


class TestDimensionComparisonOperators:
    """Test comparison operators on Dimension class."""

    def test_equals_operator_returns_filter(self, dimension):
        """Test dim == 'value' creates a DimensionFilter."""
        result = dimension == "USA"
        assert isinstance(result, DimensionFilter)
        assert result.field == dimension
        assert result.operator == "equals"
        assert result.values == ["USA"]

    def test_equals_with_list(self, dimension):
        """Test dim == ['a', 'b'] creates filter with multiple values."""
        result = dimension == ["USA", "UK"]
        assert isinstance(result, DimensionFilter)
        assert result.values == ["USA", "UK"]

    def test_not_equals_operator(self, dimension):
        """Test dim != 'value' creates a not equals filter."""
        result = dimension != "USA"
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is not"
        assert result.values == ["USA"]

    def test_greater_than_operator(self, dimension2):
        """Test dim > value creates a greater than filter."""
        result = dimension2 > 1000
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is greater than"
        assert result.values == [1000]

    def test_less_than_operator(self, dimension2):
        """Test dim < value creates a less than filter."""
        result = dimension2 < 500
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is less than"
        assert result.values == [500]

    def test_dimension_equality_still_works(self, dimension):
        """Test that comparing two dimensions still returns bool."""
        same = Dimension(
            name="country",
            model_name="test_model",
            label="Country",
            description="Customer country",
        )
        different = Dimension(
            name="region",
            model_name="test_model",
            label="Region",
            description="Customer region",
        )
        assert dimension == same
        assert dimension != different


class TestDimensionHelperMethods:
    """Test helper methods on Dimension class."""

    def test_in_method(self, dimension):
        """Test dim.in_(['a', 'b']) creates equals filter with list."""
        result = dimension.in_(["USA", "UK", "Canada"])
        assert isinstance(result, DimensionFilter)
        assert result.operator == "equals"
        assert result.values == ["USA", "UK", "Canada"]

    def test_not_in_method(self, dimension):
        """Test dim.not_in(['a', 'b']) creates not equals filter."""
        result = dimension.not_in(["USA", "UK"])
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is not"
        assert result.values == ["USA", "UK"]

    def test_contains_method(self, dimension):
        """Test dim.contains('substring') creates includes filter."""
        result = dimension.contains("States")
        assert isinstance(result, DimensionFilter)
        assert result.operator == "includes"
        assert result.values == ["States"]

    def test_starts_with_method(self, dimension):
        """Test dim.starts_with('prefix') creates starts with filter."""
        result = dimension.starts_with("United")
        assert isinstance(result, DimensionFilter)
        assert result.operator == "starts with"
        assert result.values == ["United"]

    def test_ends_with_method(self, dimension):
        """Test dim.ends_with('suffix') creates ends with filter."""
        result = dimension.ends_with("Kingdom")
        assert isinstance(result, DimensionFilter)
        assert result.operator == "ends with"
        assert result.values == ["Kingdom"]

    def test_is_null_method(self, dimension):
        """Test dim.is_null() creates is null filter."""
        result = dimension.is_null()
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is null"
        assert result.values == []

    def test_is_not_null_method(self, dimension):
        """Test dim.is_not_null() creates is not null filter."""
        result = dimension.is_not_null()
        assert isinstance(result, DimensionFilter)
        assert result.operator == "is not null"
        assert result.values == []


class TestFilterCombining:
    """Test combining filters with & and | operators."""

    def test_and_two_filters(self, dimension, dimension2):
        """Test filter1 & filter2 creates CompositeFilter with AND."""
        filter1 = dimension == "USA"
        filter2 = dimension2 > 1000
        result = filter1 & filter2
        assert isinstance(result, CompositeFilter)
        assert result.aggregation == "and"
        assert len(result.filters) == 2
        assert result.filters[0] == filter1
        assert result.filters[1] == filter2

    def test_or_two_filters(self, dimension, dimension2):
        """Test filter1 | filter2 creates CompositeFilter with OR."""
        filter1 = dimension == "USA"
        filter2 = dimension == "UK"
        result = filter1 | filter2
        assert isinstance(result, CompositeFilter)
        assert result.aggregation == "or"
        assert len(result.filters) == 2

    def test_parentheses_precedence(self, dimension, dimension2):
        """Test that parentheses work for precedence: (a == 1) | (b == 2)."""
        result = (dimension == "USA") | (dimension == "UK")
        assert isinstance(result, CompositeFilter)
        assert result.aggregation == "or"
        assert len(result.filters) == 2

    def test_chain_and_filters(self, dimension, dimension2):
        """Test chaining multiple & operations."""
        dim3 = Dimension(name="status", model_name="test_model")
        filter1 = dimension == "USA"
        filter2 = dimension2 > 1000
        filter3 = dim3 == "active"
        result = filter1 & filter2 & filter3
        assert isinstance(result, CompositeFilter)
        assert result.aggregation == "and"
        assert len(result.filters) == 3

    def test_chain_or_filters(self, dimension):
        """Test chaining multiple | operations."""
        filter1 = dimension == "USA"
        filter2 = dimension == "UK"
        filter3 = dimension == "Canada"
        result = filter1 | filter2 | filter3
        assert isinstance(result, CompositeFilter)
        assert result.aggregation == "or"
        assert len(result.filters) == 3


class TestBackwardsCompatibility:
    """Test that original filter constructors still work."""

    def test_dimension_filter_constructor(self, dimension):
        """Test DimensionFilter can still be created directly."""
        filter1 = DimensionFilter(
            field=dimension,
            operator="equals",
            values=["USA"]
        )
        assert filter1.field == dimension
        assert filter1.operator == "equals"
        assert filter1.values == ["USA"]

    def test_composite_filter_constructor(self, dimension, dimension2):
        """Test CompositeFilter can still be created directly."""
        filter1 = DimensionFilter(field=dimension, operator="equals", values=["USA"])
        filter2 = DimensionFilter(field=dimension2, operator="is greater than", values=[1000])
        composite = CompositeFilter(filters=[filter1, filter2], aggregation="and")
        assert len(composite.filters) == 2
        assert composite.aggregation == "and"

    def test_to_dict_still_works(self, dimension):
        """Test that to_dict() still works on filters created with operators."""
        result = dimension == "USA"
        dict_result = result.to_dict()
        assert "target" in dict_result
        assert "operator" in dict_result
        assert "values" in dict_result
        assert dict_result["target"]["fieldId"] == "test_model_country"


class TestDimensionHashability:
    """Test that Dimension remains hashable after overriding __eq__."""

    def test_dimension_in_set(self, dimension, dimension2):
        """Test dimensions can be added to sets."""
        dim_set = {dimension, dimension2}
        assert len(dim_set) == 2
        assert dimension in dim_set

    def test_dimension_as_dict_key(self, dimension):
        """Test dimension can be used as dict key."""
        dim_dict = {dimension: "value"}
        assert dim_dict[dimension] == "value"

    def test_equal_dimensions_have_same_hash(self, dimension):
        """Test that equal dimensions have the same hash."""
        same = Dimension(
            name="country",
            model_name="test_model",
            label="Country",
            description="Customer country",
        )
        assert hash(dimension) == hash(same)
