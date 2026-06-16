"""
Tests for table calculations and table calculation filters (issue #21).

Verifies that table calculations can be defined, added to queries, and used in
filters that serialize under ``filters.tableCalculations`` — the shape confirmed
against the Lightdash ``Filters`` / ``SqlTableCalculation`` types.
"""

import pytest
from lightdash.dimensions import Dimension
from lightdash.filter import DimensionFilter, TableCalculationFilter, CompositeFilter
from lightdash.models import Model
from lightdash.table_calculations import TableCalculation


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
def dimension():
    """Create a test dimension."""
    return Dimension(
        name="country",
        model_name="test_model",
        label="Country",
        description="Customer country",
    )


@pytest.fixture
def calc():
    """Create a test table calculation."""
    return TableCalculation(
        name="profit_ratio",
        sql="${test_model.profit} / ${test_model.revenue}",
        display_name="Profit Ratio",
    )


class TestTableCalculation:
    """Test the TableCalculation class."""

    def test_field_id_is_name(self, calc):
        """Table calculations are referenced by name, without a model prefix."""
        assert calc.field_id == "profit_ratio"

    def test_to_dict_matches_sql_table_calculation(self, calc):
        """Serialization matches the SqlTableCalculation shape {name, displayName, sql}."""
        assert calc.to_dict() == {
            "name": "profit_ratio",
            "displayName": "Profit Ratio",
            "sql": "${test_model.profit} / ${test_model.revenue}",
        }

    def test_to_dict_display_name_defaults_to_name(self):
        """displayName falls back to name when not provided."""
        calc = TableCalculation(name="my_calc", sql="1 + 1")
        assert calc.to_dict()["displayName"] == "my_calc"

    def test_equality_between_calculations(self, calc):
        """Comparing two TableCalculations returns bool, not a filter."""
        same = TableCalculation(
            name="profit_ratio",
            sql="${test_model.profit} / ${test_model.revenue}",
            display_name="Profit Ratio",
        )
        different = TableCalculation(name="other", sql="1")
        assert calc == same
        assert calc != different

    def test_hashable(self, calc):
        """TableCalculations can be used in sets and as dict keys."""
        same = TableCalculation(
            name="profit_ratio",
            sql="${test_model.profit} / ${test_model.revenue}",
            display_name="Profit Ratio",
        )
        assert hash(calc) == hash(same)
        assert calc in {calc}


class TestTableCalculationOperators:
    """Test filter creation via operators on TableCalculation."""

    def test_equals_operator(self, calc):
        result = calc == 0.5
        assert isinstance(result, TableCalculationFilter)
        assert result.operator == "equals"
        assert result.values == [0.5]

    def test_not_equals_operator(self, calc):
        result = calc != 0.5
        assert isinstance(result, TableCalculationFilter)
        assert result.operator == "notEquals"

    def test_comparison_operators(self, calc):
        assert (calc > 1).operator == "greaterThan"
        assert (calc >= 1).operator == "greaterThanOrEqual"
        assert (calc < 1).operator == "lessThan"
        assert (calc <= 1).operator == "lessThanOrEqual"

    def test_helper_methods(self, calc):
        assert calc.in_([1, 2]).operator == "equals"
        assert calc.not_in([1, 2]).operator == "notEquals"
        assert calc.contains("a").operator == "include"
        assert calc.starts_with("a").operator == "startsWith"
        assert calc.ends_with("a").operator == "endsWith"
        assert calc.is_null().operator == "isNull"
        assert calc.is_not_null().operator == "notNull"
        assert calc.between(1, 2).operator == "inBetween"
        assert calc.not_between(1, 2).operator == "notInBetween"


class TestTableCalculationFilter:
    """Test the TableCalculationFilter class."""

    def test_to_dict_with_calculation_object(self, calc):
        result = (calc > 0.2).to_dict()
        assert result == {
            "target": {"fieldId": "profit_ratio"},
            "operator": "greaterThan",
            "values": [0.2],
        }

    def test_field_as_string(self):
        """A table calculation can be referenced by name."""
        f = TableCalculationFilter(
            field="profit_ratio", operator="greaterThan", values=[0.2]
        )
        assert f.to_dict()["target"]["fieldId"] == "profit_ratio"

    def test_invalid_operator_raises(self, calc):
        with pytest.raises(ValueError, match="Invalid operator"):
            TableCalculationFilter(field=calc, operator="bogus", values=[1])

    def test_invalid_field_raises(self):
        with pytest.raises(TypeError, match="field must be a TableCalculation"):
            TableCalculationFilter(field=123, operator="equals", values=[1])

    def test_scalar_values_wrapped_in_list(self, calc):
        f = TableCalculationFilter(field=calc, operator="equals", values=0.5)
        assert f.values == [0.5]


class TestTableCalculationFilterSerialization:
    """Test composite serialization under filters.tableCalculations."""

    def test_calc_only_composite(self, calc):
        composite = CompositeFilter(filters=[calc > 0.2])
        result = composite.to_dict()
        assert result["dimensions"] == {"and": []}
        rules = result["tableCalculations"]["and"]
        assert len(rules) == 1
        assert rules[0]["target"]["fieldId"] == "profit_ratio"

    def test_mixed_composite(self, calc, dimension):
        """Dimension and table calc filters serialize under separate keys."""
        composite = (dimension == "USA") & (calc > 0.2)
        result = composite.to_dict()
        dim_rules = result["dimensions"]["and"]
        calc_rules = result["tableCalculations"]["and"]
        assert len(dim_rules) == 1
        assert dim_rules[0]["target"]["fieldId"] == "test_model_country"
        assert len(calc_rules) == 1
        assert calc_rules[0]["target"]["fieldId"] == "profit_ratio"

    def test_or_aggregation(self, calc):
        composite = (calc > 0.8) | (calc < 0.2)
        result = composite.to_dict()
        assert len(result["tableCalculations"]["or"]) == 2

    def test_no_calc_filters_omits_key(self, dimension):
        """tableCalculations key is omitted when no calc filters exist."""
        composite = CompositeFilter(filters=[dimension == "USA"])
        assert "tableCalculations" not in composite.to_dict()


class TestQueryIntegration:
    """Test table calculations in the query builder."""

    def test_table_calculations_method_adds_calcs(self, model, calc):
        query = model.query().table_calculations(calc)
        assert query._table_calculations == (calc,)

    def test_table_calculations_accumulate(self, model, calc):
        other = TableCalculation(name="other", sql="1")
        query = model.query().table_calculations(calc).table_calculations(other)
        assert query._table_calculations == (calc, other)

    def test_table_calculations_returns_new_query(self, model, calc):
        query1 = model.query()
        query2 = query1.table_calculations(calc)
        assert query1 is not query2
        assert query1._table_calculations is None

    def test_query_kwarg(self, model, calc):
        query = model.query(table_calculations=[calc])
        assert tuple(query._table_calculations) == (calc,)

    def test_payload_includes_table_calculations(self, model, calc):
        query = model.query().table_calculations(calc)
        payload = query._build_payload()
        assert payload["tableCalculations"] == [calc.to_dict()]

    def test_payload_accepts_raw_dicts(self, model):
        raw = {"name": "my_calc", "displayName": "My Calc", "sql": "1 + 1"}
        query = model.query().table_calculations(raw)
        payload = query._build_payload()
        assert payload["tableCalculations"] == [raw]

    def test_filter_with_table_calculation(self, model, calc):
        """Table calc filters flow through .filter() into the payload."""
        query = model.query().table_calculations(calc).filter(calc > 0.2)
        payload = query._build_payload()
        rules = payload["filters"]["tableCalculations"]["and"]
        assert len(rules) == 1
        assert rules[0]["target"]["fieldId"] == "profit_ratio"

    def test_filter_combines_with_dimension_filters(self, model, calc, dimension):
        query = (
            model.query()
            .filter(dimension == "USA")
            .filter(calc > 0.2)
        )
        payload = query._build_payload()
        assert len(payload["filters"]["dimensions"]["and"]) == 1
        assert len(payload["filters"]["tableCalculations"]["and"]) == 1

    def test_filter_as_query_kwarg(self, model, calc):
        """A single TableCalculationFilter can be passed as filters=..."""
        query = model.query(filters=calc > 0.2)
        payload = query._build_payload()
        assert len(payload["filters"]["tableCalculations"]["and"]) == 1
