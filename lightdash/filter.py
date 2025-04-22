from dataclasses import dataclass, field
from typing import List, Union, Dict

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
    field: Dimension
    operator: str
    values: Union[str, int, float, List[str], List[int], List[float]]

    def __post_init__(self):
        if not isinstance(self.values, list):
            self.values = [self.values]

        assert (
            self.operator in allowed_values
        ), f"operator {self.operator} not allowed"

        assert isinstance(
            self.field, Dimension
        ), "field must be a Dimension object, not just the name"

    def to_dict(self) -> Dict[str, Union[str, List[str]]]:
        return {
            "target": {"fieldId": self.field.field_id},
            "operator": self.operator,
            "values": self.values,
        }


@dataclass
class CompositeFilter:
    """
    Filters are a list of dimension filters that are applied to a query.
    Later this will also represent complex filters with AND, OR, NOT, etc.
    """

    filters: List[DimensionFilter] = field(default_factory=list)
    aggregation: str = "and"

    def __post_init__(self):
        assert self.aggregation in ["and", "or"]

    def to_dict(self):
        out = []
        for filter in self.filters:
            # Check that the filter is not a composite filter
            assert hasattr(
                filter, "field"
            ), "Multi-level filter composites not supported yet"
            # Check that we have at most one filter per field
            if filter.field in out:
                raise NotImplementedError(
                    f"Multiple filters for field {filter.field} not implemented yet"
                )
            out.append(filter.to_dict())
        return {"dimensions": {self.aggregation: out}}
