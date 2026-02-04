"""
Dimensions for Lightdash models.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from .types import Model, Dimension as DimensionProtocol

if TYPE_CHECKING:
    from .filter import DimensionFilter


@dataclass
class Dimension:
    """A Lightdash dimension."""
    name: str
    model_name: str
    label: Optional[str] = None
    description: Optional[str] = None

    def __hash__(self) -> int:
        """Make Dimension hashable for use in sets and dict keys."""
        return hash((self.name, self.model_name, self.label, self.description))

    def __str__(self) -> str:
        desc_part = f": {self.description}" if self.description else ""
        return f"Dimension({self.name}{desc_part})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("Dimension(...)")
        else:
            p.text(str(self))

    def _repr_html_(self) -> str:
        """Rich HTML display for Jupyter notebooks."""
        display_name = html.escape(self.label or self.name)
        field_id = html.escape(self.field_id)
        model_name = html.escape(self.model_name)

        desc_html = ""
        if self.description:
            escaped_desc = html.escape(self.description)
            desc_html = f'<div style="color: #666; margin-top: 8px;">{escaped_desc}</div>'

        return f"""
        <div style="border: 1px solid #ddd; border-radius: 8px; padding: 16px; font-family: sans-serif; max-width: 400px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 4px;">
                {display_name}
            </div>
            <div style="display: flex; gap: 16px; font-size: 12px; color: #666;">
                <div><span style="font-weight: 500;">Type:</span> Dimension</div>
                <div><span style="font-weight: 500;">Model:</span> {model_name}</div>
            </div>
            <div style="font-size: 12px; color: #888; margin-top: 4px; font-family: monospace;">
                {field_id}
            </div>
            {desc_html}
        </div>
        """

    @property
    def field_id(self) -> str:
        """Get the field ID for this dimension in the format model_name."""
        return f"{self.model_name}_{self.name}"

    @classmethod
    def from_api_response(cls, data: Dict[str, Any], model_name: str) -> "Dimension":
        """Create a Dimension instance from API response data."""
        return cls(
            name=data["name"],
            model_name=model_name,
            label=data.get("label"),
            description=data.get("description"),
        )

    # -------------------------------------------------------------------------
    # Filter operator overloading
    # -------------------------------------------------------------------------

    def __eq__(self, other: Any) -> Union[bool, "DimensionFilter"]:  # type: ignore[override]
        """Create equals filter: dim == 'value' or dim == ['a', 'b']"""
        if isinstance(other, Dimension):
            # Allow normal dataclass equality checks
            return (
                self.name == other.name
                and self.model_name == other.model_name
                and self.label == other.label
                and self.description == other.description
            )
        from .filter import DimensionFilter
        values = other if isinstance(other, list) else [other]
        return DimensionFilter(field=self, operator="equals", values=values)

    def __ne__(self, other: Any) -> Union[bool, "DimensionFilter"]:  # type: ignore[override]
        """Create not equals filter: dim != 'value'"""
        if isinstance(other, Dimension):
            return not self.__eq__(other)
        from .filter import DimensionFilter
        values = other if isinstance(other, list) else [other]
        return DimensionFilter(field=self, operator="notEquals", values=values)

    def __gt__(self, other: Any) -> "DimensionFilter":
        """Create greater than filter: dim > value"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="greaterThan", values=[other])

    def __lt__(self, other: Any) -> "DimensionFilter":
        """Create less than filter: dim < value"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="lessThan", values=[other])

    def __ge__(self, other: Any) -> "DimensionFilter":
        """Create >= filter: dim >= value"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="greaterThanOrEqual", values=[other])

    def __le__(self, other: Any) -> "DimensionFilter":
        """Create <= filter: dim <= value"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="lessThanOrEqual", values=[other])

    def in_(self, values: List[Any]) -> "DimensionFilter":
        """Create 'in' filter: dim.in_(['a', 'b'])"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="equals", values=values)

    def not_in(self, values: List[Any]) -> "DimensionFilter":
        """Create 'not in' filter: dim.not_in(['a', 'b'])"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="notEquals", values=values)

    def contains(self, value: str) -> "DimensionFilter":
        """Create contains filter: dim.contains('substring')"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="include", values=[value])

    def starts_with(self, value: str) -> "DimensionFilter":
        """Create starts with filter: dim.starts_with('prefix')"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="startsWith", values=[value])

    def ends_with(self, value: str) -> "DimensionFilter":
        """Create ends with filter: dim.ends_with('suffix')"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="endsWith", values=[value])

    def is_null(self) -> "DimensionFilter":
        """Create is null filter: dim.is_null()"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="isNull", values=[])

    def is_not_null(self) -> "DimensionFilter":
        """Create is not null filter: dim.is_not_null()"""
        from .filter import DimensionFilter
        return DimensionFilter(field=self, operator="notNull", values=[])


class Dimensions:
    """
    Container for Lightdash dimensions with attribute-based access.
    
    Allows accessing dimensions as attributes, e.g.:
        model.dimensions.my_dimension_name
    
    Will fetch dimensions from API on first access if not already cached.
    """
    def __init__(self, model: Model):
        self._model = model
        self._dimensions: Optional[Dict[str, Dimension]] = None

    def _ensure_loaded(self) -> None:
        """Ensure dimensions are loaded from API if not already cached."""
        if self._dimensions is None:
            dimensions = self._model.list_dimensions()
            self._dimensions = {dim.name: dim for dim in dimensions}

    def __getattr__(self, name: str) -> Dimension:
        """Get a dimension by name, fetching from API if needed."""
        self._ensure_loaded()
        try:
            return self._dimensions[name]
        except KeyError:
            close_matches = get_close_matches(
                name, self._dimensions.keys(), n=3, cutoff=0.6
            )
            msg = f"No dimension named '{name}' found"
            if close_matches:
                suggestions = ", ".join(f"'{d}'" for d in close_matches)
                msg += f". Did you mean: {suggestions}?"
            raise AttributeError(msg)

    def __dir__(self) -> List[str]:
        """Enable tab completion by returning list of dimension names."""
        self._ensure_loaded()
        return list(self._dimensions.keys())

    def list(self) -> List[Dimension]:
        """List all available dimensions."""
        self._ensure_loaded()
        return list(self._dimensions.values())

    def _repr_html_(self) -> str:
        """Rich HTML display for Jupyter notebooks."""
        preview_count = 5
        try:
            self._ensure_loaded()
            dimensions_list = list(self._dimensions.values())
            total = len(dimensions_list)
            preview = dimensions_list[:preview_count]
        except Exception:
            return '<div style="color: #999;">Unable to load dimensions</div>'

        model_name = html.escape(
            getattr(self._model, "label", None) or self._model.name
        )

        rows = []
        for dimension in preview:
            display_name = html.escape(dimension.label or dimension.name)
            desc = ""
            if dimension.description:
                escaped_desc = html.escape(dimension.description)
                if len(escaped_desc) > 80:
                    escaped_desc = escaped_desc[:77] + "..."
                desc = f'<div style="color: #666; font-size: 12px;">{escaped_desc}</div>'
            rows.append(
                f'<div style="padding: 4px 0; border-bottom: 1px solid #eee;">'
                f"<div>{display_name}</div>{desc}</div>"
            )

        more_text = ""
        if total > preview_count:
            more_text = (
                f'<div style="color: #666; font-style: italic; margin-top: 8px;">'
                f"...and {total - preview_count} more</div>"
            )

        return f"""
        <div style="border: 1px solid #ddd; border-radius: 8px; padding: 16px; font-family: sans-serif; max-width: 400px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 12px; border-bottom: 1px solid #ddd; padding-bottom: 8px;">
                Dimensions from {model_name} ({total})
            </div>
            {"".join(rows) if rows else "<div style='color: #999;'>No dimensions available</div>"}
            {more_text}
        </div>
        """