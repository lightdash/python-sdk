"""
Table calculations for Lightdash queries.

A ``TableCalculation`` is a row-by-row expression evaluated on query results.
It serializes as a ``SqlTableCalculation`` (``{name, displayName, sql}``) in the
query payload, and can be referenced in filters via the comparison operators
below, mirroring the ``Dimension`` filter API.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .filter import TableCalculationFilter


@dataclass
class TableCalculation:
    """A Lightdash table calculation defined by a SQL expression."""
    name: str
    sql: str
    display_name: Optional[str] = None

    def __hash__(self) -> int:
        """Make TableCalculation hashable for use in sets and dict keys."""
        return hash((self.name, self.sql, self.display_name))

    def __str__(self) -> str:
        return f"TableCalculation({self.name})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("TableCalculation(...)")
        else:
            p.text(str(self))

    @property
    def field_id(self) -> str:
        """Table calculations are referenced by their name (no model prefix)."""
        return self.name

    def to_dict(self) -> Dict[str, str]:
        """Serialize as a SqlTableCalculation for the query payload."""
        return {
            "name": self.name,
            "displayName": self.display_name or self.name,
            "sql": self.sql,
        }

    # -------------------------------------------------------------------------
    # Filter operator overloading (mirrors Dimension)
    # -------------------------------------------------------------------------

    def __eq__(self, other: Any) -> Union[bool, "TableCalculationFilter"]:  # type: ignore[override]
        """Create equals filter: calc == value or calc == [a, b]"""
        if isinstance(other, TableCalculation):
            # Allow normal dataclass equality checks
            return (
                self.name == other.name
                and self.sql == other.sql
                and self.display_name == other.display_name
            )
        from .filter import TableCalculationFilter
        values = other if isinstance(other, list) else [other]
        return TableCalculationFilter(field=self, operator="equals", values=values)

    def __ne__(self, other: Any) -> Union[bool, "TableCalculationFilter"]:  # type: ignore[override]
        """Create not equals filter: calc != value"""
        if isinstance(other, TableCalculation):
            return not self.__eq__(other)
        from .filter import TableCalculationFilter
        values = other if isinstance(other, list) else [other]
        return TableCalculationFilter(field=self, operator="notEquals", values=values)

    def __gt__(self, other: Any) -> "TableCalculationFilter":
        """Create greater than filter: calc > value"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="greaterThan", values=[other])

    def __lt__(self, other: Any) -> "TableCalculationFilter":
        """Create less than filter: calc < value"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="lessThan", values=[other])

    def __ge__(self, other: Any) -> "TableCalculationFilter":
        """Create >= filter: calc >= value"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="greaterThanOrEqual", values=[other])

    def __le__(self, other: Any) -> "TableCalculationFilter":
        """Create <= filter: calc <= value"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="lessThanOrEqual", values=[other])

    def in_(self, values: List[Any]) -> "TableCalculationFilter":
        """Create 'in' filter: calc.in_([1, 2])"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="equals", values=values)

    def not_in(self, values: List[Any]) -> "TableCalculationFilter":
        """Create 'not in' filter: calc.not_in([1, 2])"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="notEquals", values=values)

    def contains(self, value: str) -> "TableCalculationFilter":
        """Create contains filter: calc.contains('substring')"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="include", values=[value])

    def starts_with(self, value: str) -> "TableCalculationFilter":
        """Create starts with filter: calc.starts_with('prefix')"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="startsWith", values=[value])

    def ends_with(self, value: str) -> "TableCalculationFilter":
        """Create ends with filter: calc.ends_with('suffix')"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="endsWith", values=[value])

    def is_null(self) -> "TableCalculationFilter":
        """Create is null filter: calc.is_null()"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="isNull", values=[])

    def is_not_null(self) -> "TableCalculationFilter":
        """Create is not null filter: calc.is_not_null()"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="notNull", values=[])

    def between(self, start: Any, end: Any) -> "TableCalculationFilter":
        """Create between filter: calc.between(10, 100)"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="inBetween", values=[start, end])

    def not_between(self, start: Any, end: Any) -> "TableCalculationFilter":
        """Create not between filter: calc.not_between(10, 100)"""
        from .filter import TableCalculationFilter
        return TableCalculationFilter(field=self, operator="notInBetween", values=[start, end])
