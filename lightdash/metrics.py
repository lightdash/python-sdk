"""
Metrics for Lightdash models.
"""
import html
from dataclasses import dataclass
from difflib import get_close_matches
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
                <div><span style="font-weight: 500;">Type:</span> Metric</div>
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
            close_matches = get_close_matches(
                name, self._metrics.keys(), n=3, cutoff=0.6
            )
            msg = f"No metric named '{name}' found"
            if close_matches:
                suggestions = ", ".join(f"'{m}'" for m in close_matches)
                msg += f". Did you mean: {suggestions}?"
            raise AttributeError(msg)

    def __dir__(self) -> List[str]:
        """Enable tab completion by returning list of metric names."""
        self._ensure_loaded()
        return list(self._metrics.keys())

    def list(self) -> List[Metric]:
        """List all available metrics."""
        self._ensure_loaded()
        return list(self._metrics.values())

    def _repr_html_(self) -> str:
        """Rich HTML display for Jupyter notebooks."""
        preview_count = 5
        try:
            self._ensure_loaded()
            metrics_list = list(self._metrics.values())
            total = len(metrics_list)
            preview = metrics_list[:preview_count]
        except Exception:
            return '<div style="color: #999;">Unable to load metrics</div>'

        model_name = html.escape(
            getattr(self._model, "label", None) or self._model.name
        )

        rows = []
        for metric in preview:
            display_name = html.escape(metric.label or metric.name)
            desc = ""
            if metric.description:
                escaped_desc = html.escape(metric.description)
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
                Metrics from {model_name} ({total})
            </div>
            {"".join(rows) if rows else "<div style='color: #999;'>No metrics available</div>"}
            {more_text}
        </div>
        """