"""
Metrics for Lightdash models.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .types import Model, Metric as MetricProtocol


@dataclass
class Metric:
    """A Lightdash metric."""
    name: str
    model_name: str
    label: Optional[str] = None
    description: Optional[str] = None

    def __str__(self) -> str:
        desc_part = f": {self.description}" if self.description else ""
        return f"Metric({self.name}{desc_part})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("Metric(...)")
        else:
            p.text(str(self))

    @property
    def field_id(self) -> str:
        """Get the field ID for this metric in the format model_name."""
        return f"{self.model_name}_{self.name}"

    @classmethod
    def from_api_response(cls, data: Dict[str, Any], model_name: str) -> "Metric":
        """Create a Metric instance from API response data."""
        return cls(
            name=data["name"],
            model_name=model_name,
            label=data.get("label"),
            description=data.get("description"),
        )


class Metrics:
    """
    Container for Lightdash metrics with attribute-based access.
    
    Allows accessing metrics as attributes, e.g.:
        model.metrics.my_metric_name
    
    Will fetch metrics from API on first access if not already cached.
    """
    def __init__(self, model: Model):
        self._model = model
        self._metrics: Optional[Dict[str, Metric]] = None

    def _ensure_loaded(self) -> None:
        """Ensure metrics are loaded from API if not already cached."""
        if self._metrics is None:
            metrics = self._model.list_metrics()
            self._metrics = {metric.name: metric for metric in metrics}

    def __getattr__(self, name: str) -> Metric:
        """Get a metric by name, fetching from API if needed."""
        self._ensure_loaded()
        try:
            return self._metrics[name]
        except KeyError:
            raise AttributeError(f"No metric named '{name}' found")

    def __dir__(self) -> List[str]:
        """Enable tab completion by returning list of metric names."""
        self._ensure_loaded()
        return list(self._metrics.keys())

    def list(self) -> List[Metric]:
        """List all available metrics."""
        self._ensure_loaded()
        return list(self._metrics.values()) 