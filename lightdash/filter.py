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


def _combine(left, right, aggregation: str) -> "CompositeFilter":
    """Combine two filters/groups under ``aggregation``.

    A child group is flattened in only when it already uses the *same*
    aggregation; otherwise it is kept as a nested item. This preserves the
    precedence of mixed AND/OR expressions — e.g. ``a & (b | c)`` keeps the
    ``(b | c)`` group nested rather than collapsing to ``a & b & c``.
    """
    items = []
    for operand in (left, right):
        if isinstance(operand, CompositeFilter) and operand.aggregation == aggregation:
            items.extend(operand.filters)
        else:
            items.append(operand)
    return CompositeFilter(filters=items, aggregation=aggregation)


class _FieldFilterMixin:
    """Shared ``&`` / ``|`` combination behavior for single-field filters."""

    def __and__(self, other: Union["FieldFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with AND: filter1 & filter2"""
        return _combine(self, other, "and")

    def __or__(self, other: Union["FieldFilter", "CompositeFilter"]) -> "CompositeFilter":
        """Combine filters with OR: filter1 | filter2"""
        return _combine(self, other, "or")


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


def _target_keys(node) -> set:
    """Payload keys ('dimensions' / 'tableCalculations') used by the leaf rules
    within ``node`` (a filter rule or a CompositeFilter)."""
    if isinstance(node, CompositeFilter):
        keys = set()
        for item in node.filters:
            keys |= _target_keys(item)
        return keys
    if isinstance(node, TableCalculationFilter):
        return {"tableCalculations"}
    return {"dimensions"}


def _serialize_node(node) -> Dict:
    """Serialize a single-field-type node into either a rule dict or a nested
    ``{and|or: [...]}`` group dict."""
    if isinstance(node, CompositeFilter):
        return {node.aggregation: [_serialize_node(item) for item in node.filters]}
    return node.to_dict()


@dataclass
class CompositeFilter:
    """A boolean group of filters applied to a query.

    ``filters`` may contain individual filter rules *and* other
    ``CompositeFilter`` groups, so nested boolean expressions such as
    ``a & (b | c)`` are represented (and serialized) with their precedence
    preserved.
    """

    filters: List[Union[FieldFilter, "CompositeFilter"]] = field(default_factory=list)
    aggregation: str = "and"

    def __post_init__(self):
        if self.aggregation not in ("and", "or"):
            raise ValueError(
                f"Invalid aggregation '{self.aggregation}'. Must be 'and' or 'or'"
            )

    def to_dict(self):
        keys = _target_keys(self)

        # Dimension-only (or empty) — serialize the tree under "dimensions".
        if "tableCalculations" not in keys:
            return {"dimensions": _serialize_node(self)}

        # Table-calculation-only.
        if "dimensions" not in keys:
            return {
                "dimensions": {self.aggregation: []},
                "tableCalculations": _serialize_node(self),
            }

        # Mixed dimension + table-calculation filters. The API keeps these in
        # separate top-level groups that are implicitly AND-ed, so they can only
        # be split when the outer combinator is AND.
        if self.aggregation != "and":
            raise ValueError(
                "Cannot combine dimension and table calculation filters with OR: "
                "the Lightdash API stores them in separate groups that are AND-ed "
                "together. Use AND between the two field types, or run separate "
                "queries."
            )

        dimensions = []
        table_calculations = []
        for item in self.filters:
            item_keys = _target_keys(item)
            if "tableCalculations" not in item_keys:
                dimensions.append(_serialize_node(item))
            elif "dimensions" not in item_keys:
                table_calculations.append(_serialize_node(item))
            else:
                raise ValueError(
                    "A filter group mixes dimension and table calculation filters "
                    "and cannot be represented. Keep each field type in its own "
                    "group, combined with AND."
                )
        return {
            "dimensions": {"and": dimensions},
            "tableCalculations": {"and": table_calculations},
        }

    def __and__(self, other: Union[FieldFilter, "CompositeFilter"]) -> "CompositeFilter":
        """Combine with another filter using AND: composite & filter"""
        return _combine(self, other, "and")

    def __or__(self, other: Union[FieldFilter, "CompositeFilter"]) -> "CompositeFilter":
        """Combine with another filter using OR: composite | filter"""
        return _combine(self, other, "or")
