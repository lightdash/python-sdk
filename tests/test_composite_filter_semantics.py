"""
Tests for composite filter boolean semantics (issue #23).

Covers nested AND/OR precedence, the `.filter()`-always-ANDs rule, flattening of
same-operator chains, and the clear error for the one shape the Lightdash API
cannot represent (OR across dimension and table-calculation filters).
"""

import pytest
from lightdash.dimensions import Dimension
from lightdash.filter import CompositeFilter
from lightdash.models import Model
from lightdash.table_calculations import TableCalculation


@pytest.fixture
def model():
    return Model(name="m", type="default", database_name="db", schema_name="s")


def dim(name):
    return Dimension(name=name, model_name="m")


def _ids(rules):
    return [r["target"]["fieldId"] for r in rules]


class TestNestedPrecedence:
    def test_and_of_or_keeps_group_nested(self):
        # a & (b | c)  ->  AND[ a, OR[b, c] ]
        f = (dim("a") == 1) & ((dim("b") == 2) | (dim("c") == 3))
        d = f.to_dict()["dimensions"]
        assert list(d) == ["and"]
        assert d["and"][0]["target"]["fieldId"] == "m_a"
        assert d["and"][1] == {
            "or": [
                {"target": {"fieldId": "m_b"}, "operator": "equals", "values": [2]},
                {"target": {"fieldId": "m_c"}, "operator": "equals", "values": [3]},
            ]
        }

    def test_or_of_and_keeps_group_nested(self):
        # a | (b & c)  ->  OR[ a, AND[b, c] ]
        f = (dim("a") == 1) | ((dim("b") == 2) & (dim("c") == 3))
        d = f.to_dict()["dimensions"]
        assert list(d) == ["or"]
        assert d["or"][0]["target"]["fieldId"] == "m_a"
        assert "and" in d["or"][1]
        assert _ids(d["or"][1]["and"]) == ["m_b", "m_c"]

    def test_documented_complex_example(self):
        # country == "USA" & ((amount > 1000) | (priority == "high"))
        f = (dim("country") == "USA") & (
            (dim("amount") > 1000) | (dim("priority") == "high")
        )
        d = f.to_dict()["dimensions"]
        assert d["and"][0]["target"]["fieldId"] == "m_country"
        nested = d["and"][1]["or"]
        assert _ids(nested) == ["m_amount", "m_priority"]


class TestFlatteningPreserved:
    def test_chained_and_flattens(self):
        f = (dim("a") == 1) & (dim("b") == 2) & (dim("c") == 3)
        rules = f.to_dict()["dimensions"]["and"]
        assert _ids(rules) == ["m_a", "m_b", "m_c"]
        assert all("target" in r for r in rules)  # flat, no nested groups

    def test_chained_or_flattens(self):
        d = dim("a")
        f = (d == 1) | (d == 2) | (d == 3)
        assert len(f.to_dict()["dimensions"]["or"]) == 3

    def test_same_field_range_still_flat(self):
        # The #20 behaviour: (d >= x) & (d <= y) stays a flat AND of two rules.
        d = dim("order_date")
        f = (d >= "2026-01-01") & (d <= "2026-01-31")
        rules = f.to_dict()["dimensions"]["and"]
        assert [r["operator"] for r in rules] == ["greaterThanOrEqual", "lessThanOrEqual"]


class TestQueryFilterAlwaysAnds:
    def test_filter_after_or_ands_as_a_unit(self, model):
        """(a | b).filter(c)  ->  (a OR b) AND c  — the core #23 bug."""
        status = dim("status")
        q = (
            model.query()
            .filter((status == "active") | (status == "pending"))
            .filter(dim("region") == "west")
        )
        d = q._build_payload()["filters"]["dimensions"]
        assert list(d) == ["and"]
        assert "or" in d["and"][0]
        assert len(d["and"][0]["or"]) == 2
        assert d["and"][1]["target"]["fieldId"] == "m_region"

    def test_two_simple_filters_and_flat(self, model):
        q = model.query().filter(dim("a") == 1).filter(dim("b") == 2)
        rules = q._build_payload()["filters"]["dimensions"]["and"]
        assert _ids(rules) == ["m_a", "m_b"]

    def test_filter_composite_then_composite(self, model):
        q = (
            model.query()
            .filter((dim("a") == 1) | (dim("b") == 2))
            .filter((dim("c") == 3) | (dim("d") == 4))
        )
        d = q._build_payload()["filters"]["dimensions"]
        assert list(d) == ["and"]
        assert all("or" in item for item in d["and"])


class TestCrossFieldTypeHandling:
    def test_and_across_dimension_and_calc_splits(self):
        calc = TableCalculation(name="ratio", sql="1")
        f = (dim("country") == "USA") & (calc > 0.2)
        d = f.to_dict()
        assert d["dimensions"]["and"][0]["target"]["fieldId"] == "m_country"
        assert d["tableCalculations"]["and"][0]["target"]["fieldId"] == "ratio"

    def test_and_with_nested_calc_or(self):
        calc_a = TableCalculation(name="ca", sql="1")
        calc_b = TableCalculation(name="cb", sql="2")
        f = (dim("country") == "USA") & ((calc_a > 1) | (calc_b > 2))
        d = f.to_dict()
        assert _ids(d["dimensions"]["and"]) == ["m_country"]
        calc_group = d["tableCalculations"]["and"]
        assert len(calc_group) == 1 and "or" in calc_group[0]
        assert _ids(calc_group[0]["or"]) == ["ca", "cb"]

    def test_or_across_dimension_and_calc_raises(self):
        calc = TableCalculation(name="ratio", sql="1")
        f = (dim("country") == "USA") | (calc > 0.2)
        with pytest.raises(ValueError, match="Cannot combine dimension and table calculation"):
            f.to_dict()
