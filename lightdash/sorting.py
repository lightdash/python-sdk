"""
Sort builders for Lightdash queries.

Example usage:
    from lightdash.sorting import Sort

    # Simple sort
    sort = Sort("orders_revenue", descending=True)

    # With null handling
    sort = Sort("orders_country", descending=False, nulls_first=True)

    # From metric/dimension objects
    sort = Sort(model.metrics.revenue, descending=True)
    sort = model.metrics.revenue.desc()  # Shorthand
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union


@dataclass
class Sort:
    """
    A sort specification for query results.

    Args:
        field: Field ID string or Metric/Dimension object
        descending: If True, sort in descending order (default False)
        nulls_first: If True, null values appear first (default None = database default)
    """
    field: Union[str, Any]  # str or Metric/Dimension
    descending: bool = False
    nulls_first: Optional[bool] = None

    @property
    def field_id(self) -> str:
        """Get the field ID string."""
        if isinstance(self.field, str):
            return self.field
        return self.field.field_id

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "fieldId": self.field_id,
            "descending": self.descending,
        }
        if self.nulls_first is not None:
            result["nullsFirst"] = self.nulls_first
        return result

    def __repr__(self) -> str:
        direction = "DESC" if self.descending else "ASC"
        nulls = f", nulls_first={self.nulls_first}" if self.nulls_first is not None else ""
        return f"Sort({self.field_id!r}, {direction}{nulls})"


def _add_sort_methods_to_field_classes():
    """Add .asc() and .desc() methods to Metric and Dimension classes."""
    from .metrics import Metric
    from .dimensions import Dimension

    def asc(self, nulls_first: Optional[bool] = None) -> Sort:
        """Create ascending sort for this field."""
        return Sort(self, descending=False, nulls_first=nulls_first)

    def desc(self, nulls_first: Optional[bool] = None) -> Sort:
        """Create descending sort for this field."""
        return Sort(self, descending=True, nulls_first=nulls_first)

    Metric.asc = asc
    Metric.desc = desc
    Dimension.asc = asc
    Dimension.desc = desc


# Add methods when module is imported
_add_sort_methods_to_field_classes()
