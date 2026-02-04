from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Union, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from lightdash.dimensions import Dimension

numeric_filters = [
    "is null",
    "is not null",
    "is",
    "equals",
    "is not",
    "is less than",
    "is greater than",
]

string_filters = [
    "is null",
    "is not null",
    "is",
    "equals",
    "is not",
    "starts with",
    "includes",
    "ends with",
]

boolean_filters = [
    "is null",
    "is not null",
    "is",
    "equals",
]

date_filters = [
    "is null",
    "is not null",
    "is",
    "equals",
    "is not",
    "in the last",
    "not in the last",
    "in the next",
    "not in the next",
    "in the current",
    "not in the current",
    "is before",
    "is on or before",
    "is after",
    "is on or after",
    "is between",
]

allowed_values = set(
    numeric_filters + string_filters + boolean_filters + date_filters
)


@dataclass
class DimensionFilter:
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

    def __and__(self, other: Union["DimensionFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with AND: filter1 & filter2"""
        if isinstance(other, CompositeFilter):
            return CompositeFilter(filters=[self] + list(other.filters), aggregation="and")
        return CompositeFilter(filters=[self, other], aggregation="and")

    def __or__(self, other: Union["DimensionFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with OR: filter1 | filter2"""
        if isinstance(other, CompositeFilter):
            return CompositeFilter(filters=[self] + list(other.filters), aggregation="or")
        return CompositeFilter(filters=[self, other], aggregation="or")


@dataclass
class CompositeFilter:
    """
    Filters are a list of dimension filters that are applied to a query.
    Later this will also represent complex filters with AND, OR, NOT, etc.
    """

    filters: List[DimensionFilter] = field(default_factory=list)
    aggregation: str = "and"

    def __post_init__(self):
        if self.aggregation not in ("and", "or"):
            raise ValueError(
                f"Invalid aggregation '{self.aggregation}'. Must be 'and' or 'or'"
            )

    def to_dict(self):
        out = []
        processed_field_ids = set()
        for f in self.filters:
            # Check that the filter is not a composite filter
            if not hasattr(f, "field"):
                raise TypeError("Multi-level filter composites not supported yet")
            # Check that we have at most one filter per field
            if f.field.field_id in processed_field_ids:
                raise NotImplementedError(
                    f"Multiple filters for field {f.field.field_id} not implemented yet"
                )
            processed_field_ids.add(f.field.field_id)
            out.append(f.to_dict())
        return {"dimensions": {self.aggregation: out}}

    def __and__(self, other: Union[DimensionFilter, "CompositeFilter"]) -> "CompositeFilter":
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

    def __or__(self, other: Union[DimensionFilter, "CompositeFilter"]) -> "CompositeFilter":
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
