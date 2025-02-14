"""
Dimensions for Lightdash models.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .types import Model, Dimension as DimensionProtocol


@dataclass
class Dimension:
    """A Lightdash dimension."""
    name: str
    model_name: str
    label: Optional[str] = None
    description: Optional[str] = None

    def __str__(self) -> str:
        desc_part = f": {self.description}" if self.description else ""
        return f"Dimension({self.name}{desc_part})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("Dimension(...)")
        else:
            p.text(str(self))

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
            raise AttributeError(f"No dimension named '{name}' found")

    def __dir__(self) -> List[str]:
        """Enable tab completion by returning list of dimension names."""
        self._ensure_loaded()
        return list(self._dimensions.keys())

    def list(self) -> List[Dimension]:
        """List all available dimensions."""
        self._ensure_loaded()
        return list(self._dimensions.values()) 