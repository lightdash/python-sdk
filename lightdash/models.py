"""
Models for interacting with Lightdash explores.
"""

import html
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Union, Sequence

from .filter import DimensionFilter, CompositeFilter
from .types import Model as ModelProtocol, Client
from .metrics import Metric, Metrics
from .dimensions import Dimension, Dimensions
from .query import Query
from .sorting import Sort


@dataclass
class Model:
    """A Lightdash model (explore)."""

    name: str
    type: str
    database_name: str
    schema_name: str
    label: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self):
        self._client: Optional[Client] = None
        self.metrics = Metrics(self)
        self.dimensions = Dimensions(self)

    def __str__(self) -> str:
        desc_part = f": {self.description}" if self.description else ""
        return f"Model({self.name}{desc_part})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("Model(...)")
        else:
            p.text(str(self))

    def _render_field_table(
        self, items: List[Any], title: str, preview_count: int = 5
    ) -> str:
        """Render a preview table for metrics or dimensions."""
        total = len(items)
        preview = items[:preview_count]
        rows = []
        for item in preview:
            display_name = html.escape(item.label or item.name)
            rows.append(f"<div>{display_name}</div>")

        if total > preview_count:
            rows.append(
                f'<div style="color: #666; font-style: italic;">'
                f"...and {total - preview_count} more</div>"
            )

        return f"""
        <div style="flex: 1; min-width: 200px;">
            <div style="font-weight: bold; margin-bottom: 8px; border-bottom: 1px solid #ddd; padding-bottom: 4px;">
                {html.escape(title)} ({total})
            </div>
            {"".join(rows) if rows else "<div style='color: #999;'>None</div>"}
        </div>
        """

    def _repr_html_(self) -> str:
        """Rich HTML display for Jupyter notebooks."""
        display_name = html.escape(self.label or self.name)
        description = ""
        if self.description:
            escaped_desc = html.escape(self.description)
            if len(escaped_desc) > 150:
                escaped_desc = escaped_desc[:147] + "..."
            description = f'<div style="color: #666; margin-bottom: 12px;">{escaped_desc}</div>'

        try:
            metrics_list = self.list_metrics()
            metrics_html = self._render_field_table(metrics_list, "Metrics")
        except Exception:
            metrics_html = '<div style="flex: 1; color: #999;">Unable to load metrics</div>'

        try:
            dimensions_list = self.list_dimensions()
            dimensions_html = self._render_field_table(dimensions_list, "Dimensions")
        except Exception:
            dimensions_html = '<div style="flex: 1; color: #999;">Unable to load dimensions</div>'

        return f"""
        <div style="border: 1px solid #ddd; border-radius: 8px; padding: 16px; font-family: sans-serif; max-width: 600px;">
            <div style="font-size: 16px; font-weight: bold; margin-bottom: 4px;">
                Model: {display_name}
            </div>
            {description}
            <div style="display: flex; gap: 24px;">
                {metrics_html}
                {dimensions_html}
            </div>
        </div>
        """

    def _set_client(self, client: Client) -> None:
        """Set the client reference for making API calls."""
        self._client = client

    def _fetch_table_data(self) -> Dict[str, Any]:
        """Fetch the table data from the API."""
        if self._client is None:
            raise RuntimeError(
                "Model not properly initialized with client reference"
            )

        path = (
            f"/api/v1/projects/{self._client.project_uuid}/explores/{self.name}"
        )
        data = self._client._make_request("GET", path)

        base_table = data["baseTable"]
        return data["tables"][base_table]

    def query(
        self,
        metrics: Optional[Union[str, Metric, Sequence[Union[str, Metric]]]] = None,
        dimensions: Union[str, Dimension, Sequence[Union[str, Dimension]]] = (),
        filters: Optional[Union[DimensionFilter, CompositeFilter]] = None,
        sort: Optional[Union[Sort, Sequence[Sort]]] = None,
        limit: int = 500,
    ) -> Query:
        """
        Create a query against this model.

        Can be called with arguments for single-call usage, or without arguments
        to start a chainable builder pattern.

        Args:
            metrics: A single metric or sequence of metrics to query. Each metric
                can be a field ID string or Metric object. Optional for builder pattern.
            dimensions: A single dimension or sequence of dimensions to query. Each
                dimension can be a field ID string or Dimension object.
            filters: Optional filters to apply to the query.
            sort: Optional Sort object or sequence of Sort objects to order results.
            limit: Maximum number of rows to return.

        Returns:
            A Query object that can be used to fetch results or build further.

        Example (single-call):
            query = model.query(
                metrics=[model.metrics.revenue],
                dimensions=[model.dimensions.country],
                limit=100
            )

        Example (chainable builder):
            query = (
                model.query()
                .metrics(model.metrics.revenue)
                .dimensions(model.dimensions.country)
                .limit(100)
            )
        """
        # Handle metrics - can be None for builder pattern
        if metrics is None:
            metrics_seq = ()
        elif isinstance(metrics, (str, Metric)):
            metrics_seq = [metrics]
        else:
            metrics_seq = metrics

        dimensions_seq = (
            [dimensions]
            if isinstance(dimensions, (str, Dimension))
            else dimensions
        )
        # Normalize single Sort to sequence
        sort_seq = None
        if sort is not None:
            sort_seq = [sort] if isinstance(sort, Sort) else sort
        return Query(
            self,
            metrics=metrics_seq,
            dimensions=dimensions_seq,
            filters=filters,
            sort=sort_seq,
            limit=limit,
        )

    def list_metrics(self) -> List["Metric"]:
        """
        List all metrics available in this model.

        Returns:
            A list of Metric objects.

        Raises:
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
        """
        table_data = self._fetch_table_data()
        return [
            Metric.from_api_response(metric_data, self.name)
            for metric_data in table_data.get("metrics", {}).values()
        ]

    def list_dimensions(self) -> List["Dimension"]:
        """
        List all dimensions available in this model.

        Returns:
            A list of Dimension objects.

        Raises:
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
        """
        table_data = self._fetch_table_data()
        return [
            Dimension.from_api_response(dim_data, self.name)
            for dim_data in table_data.get("dimensions", {}).values()
        ]

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "Model":
        """Create a Model instance from API response data."""
        if "errors" in data:
            print(f"Model has errors: {data['name']}")

        return cls(
            name=data["name"],
            type=data.get("type", "error" if "errors" in data else "default"),
            database_name=data.get("databaseName", None),
            schema_name=data.get("schemaName", None),
            label=data.get("label", None),
            description=data.get("description", None),
        )


class Models:
    """
    Container for Lightdash models with attribute-based access.

    Allows accessing models as attributes, e.g.:
        client.models.my_model_name

    Will fetch models from API on first access if not already cached.
    """

    def __init__(self, client: Client):
        self._client = client
        self._models: Optional[Dict[str, Model]] = None

    def _ensure_loaded(self) -> None:
        """Ensure models are loaded from API if not already cached."""
        if self._models is None:
            models = self._client._fetch_models()
            self._models = {model.name: model for model in models}
            # Set client reference on each model for API access
            for model in self._models.values():
                model._set_client(self._client)

    def __getattr__(self, name: str) -> Model:
        """Get a model by name, fetching from API if needed."""
        return self.get(name)

    def __dir__(self) -> List[str]:
        """Enable tab completion by returning list of model names."""
        self._ensure_loaded()
        return list(self._models.keys())

    def list(self) -> List[Model]:
        """List all available models."""
        self._ensure_loaded()
        return list(self._models.values())

    def get(self, name: str) -> Model:
        """Get a model by name, fetching from API if needed."""
        self._ensure_loaded()
        try:
            return self._models[name]
        except KeyError:
            close_matches = get_close_matches(
                name, self._models.keys(), n=3, cutoff=0.6
            )
            msg = f"No model named '{name}' found"
            if close_matches:
                suggestions = ", ".join(f"'{m}'" for m in close_matches)
                msg += f". Did you mean: {suggestions}?"
            raise AttributeError(msg)
