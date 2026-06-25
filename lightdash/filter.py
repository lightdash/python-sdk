from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Union, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from lightdash.dimensions import Dimension
    from lightdash.table_calculations import TableCalculation

numeric_filters = [
    "isNull",
    "notNull",
    "equals",
    "notEquals",
    "lessThan",
    "lessThanOrEqual",
    "greaterThan",
    "greaterThanOrEqual",
]

string_filters = [
    "isNull",
    "notNull",
    "equals",
    "notEquals",
    "startsWith",
    "endsWith",
    "include",
    "doesNotInclude",
]

boolean_filters = [
    "isNull",
    "notNull",
    "equals",
]

date_filters = [
    "isNull",
    "notNull",
    "equals",
    "notEquals",
    "inThePast",
    "notInThePast",
    "inTheNext",
    "inTheCurrent",
    "notInTheCurrent",
    "lessThan",
    "lessThanOrEqual",
    "greaterThan",
    "greaterThanOrEqual",
    "inBetween",
    "notInBetween",
]

allowed_values = set(
    numeric_filters + string_filters + boolean_filters + date_filters
)


class _FieldFilterMixin:
    """Shared ``&`` / ``|`` combination behavior for single-field filters."""

    def __and__(self, other: Union["FieldFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with AND: filter1 & filter2"""
        if isinstance(other, CompositeFilter):
            return CompositeFilter(filters=[self] + list(other.filters), aggregation="and")
        return CompositeFilter(filters=[self, other], aggregation="and")

    def __or__(self, other: Union["FieldFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with OR: filter1 | filter2"""
        if isinstance(other, CompositeFilter):
            return CompositeFilter(filters=[self] + list(other.filters), aggregation="or")
        return CompositeFilter(filters=[self, other], aggregation="or")


@dataclass
class DimensionFilter(_FieldFilterMixin):
    field: "Dimension"
    operator: str
    values: Union[str, int, float, List[str], List[int], List[float]]

    def __post_init__(self):
        from lightdash.dimensions import Dimension

        if not isinstance(self.values, list):
            self.values = [self.values]

        if self.operator not in allowed_values:
            raise ValueError(
                f"Invalid operator '{self.operator}'. "
                f"Must be one of: {', '.join(sorted(allowed_values))}"
            )

        if not isinstance(self.field, Dimension):
            raise TypeError(
                f"field must be a Dimension object, got {type(self.field).__name__}"
            )

    def to_dict(self) -> Dict[str, Union[str, List[str]]]:
        return {
            "target": {"fieldId": self.field.field_id},
            "operator": self.operator,
            "values": self.values,
        }


@dataclass
class TableCalculationFilter(_FieldFilterMixin):
    """A filter targeting a table calculation.

    Table calculations are referenced by name (no model prefix) and serialize
    under ``filters.tableCalculations`` in the query payload.
    """

    field: Union[str, "TableCalculation"]
    operator: str
    values: Union[str, int, float, List[str], List[int], List[float]]

    def __post_init__(self):
        from lightdash.table_calculations import TableCalculation

        if not isinstance(self.values, list):
            self.values = [self.values]

        if self.operator not in allowed_values:
            raise ValueError(
                f"Invalid operator '{self.operator}'. "
                f"Must be one of: {', '.join(sorted(allowed_values))}"
            )

        if not isinstance(self.field, (str, TableCalculation)):
            raise TypeError(
                "field must be a TableCalculation object or table calculation name, "
                f"got {type(self.field).__name__}"
            )

    @property
    def field_id(self) -> str:
        return self.field if isinstance(self.field, str) else self.field.field_id

    def to_dict(self) -> Dict[str, Union[str, List[str]]]:
        return {
            "target": {"fieldId": self.field_id},
            "operator": self.operator,
            "values": self.values,
        }


FieldFilter = Union[DimensionFilter, TableCalculationFilter]


@dataclass
class CompositeFilter:
    """
    Filters are a list of field filters (on dimensions and table calculations)
    that are applied to a query.
    Later this will also represent complex filters with AND, OR, NOT, etc.
    """

    filters: List[FieldFilter] = field(default_factory=list)
    aggregation: str = "and"

    def __post_init__(self):
        if self.aggregation not in ("and", "or"):
            raise ValueError(
                f"Invalid aggregation '{self.aggregation}'. Must be 'and' or 'or'"
            )

    def to_dict(self):
        dimensions = []
        table_calculations = []
        for f in self.filters:
            # Check that the filter is not a composite filter
            if not hasattr(f, "field"):
                raise TypeError("Multi-level filter composites not supported yet")
            # Multiple filters may target the same field, e.g. a date range
            # expressed as (dim >= start) & (dim <= end).
            if isinstance(f, TableCalculationFilter):
                table_calculations.append(f.to_dict())
            else:
                dimensions.append(f.to_dict())
        out = {"dimensions": {self.aggregation: dimensions}}
        # Only include the tableCalculations group when present, so existing
        # dimension-only payloads are unchanged.
        if table_calculations:
            out["tableCalculations"] = {self.aggregation: table_calculations}
        return out

    def __and__(self, other: Union[FieldFilter, "CompositeFilter"]) -> "CompositeFilter":
        """Combine with another filter using AND: composite & filter"""
        if isinstance(other, CompositeFilter):
            # Flatten if both are AND composites
            if self.aggregation == "and" and other.aggregation == "and":
                return CompositeFilter(
                    filters=list(self.filters) + list(other.filters),
                    aggregation="and"
                )
            # Otherwise wrap as nested (not fully supported yet, but preserve structure)
            return CompositeFilter(filters=list(self.filters) + list(other.filters), aggregation="and")
        # Add single filter to existing composite
        if self.aggregation == "and":
            return CompositeFilter(filters=list(self.filters) + [other], aggregation="and")
        return CompositeFilter(filters=list(self.filters) + [other], aggregation="and")

    def __or__(self, other: Union[FieldFilter, "CompositeFilter"]) -> "CompositeFilter":
        """Combine with another filter using OR: composite | filter"""
        if isinstance(other, CompositeFilter):
            # Flatten if both are OR composites
            if self.aggregation == "or" and other.aggregation == "or":
                return CompositeFilter(
                    filters=list(self.filters) + list(other.filters),
                    aggregation="or"
                )
            return CompositeFilter(filters=list(self.filters) + list(other.filters), aggregation="or")
        if self.aggregation == "or":
            return CompositeFilter(filters=list(self.filters) + [other], aggregation="or")
        return CompositeFilter(filters=list(self.filters) + [other], aggregation="or")
