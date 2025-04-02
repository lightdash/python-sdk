"""
Models for interacting with Lightdash explores.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, Sequence

from .types import Model as ModelProtocol, Client
from .metrics import Metric, Metrics
from .dimensions import Dimension, Dimensions
from .query import Query


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

    def _set_client(self, client: Client) -> None:
        """Set the client reference for making API calls."""
        self._client = client

    def _fetch_table_data(self) -> Dict[str, Any]:
        """Fetch the table data from the API."""
        if self._client is None:
            raise RuntimeError("Model not properly initialized with client reference")

        path = f"/api/v1/projects/{self._client.project_uuid}/explores/{self.name}"
        data = self._client._make_request("GET", path)

        base_table = data["baseTable"]
        return data["tables"][base_table]

    def query(
        self,
        metrics: Union[str, Metric, Sequence[Union[str, Metric]]],
        dimensions: Union[str, Dimension, Sequence[Union[str, Dimension]]] = (),
        limit: int = 50,
    ) -> Query:
        """
        Create a query against this model.

        Args:
            metrics: A single metric or sequence of metrics to query. Each metric can be a field ID string or Metric object.
            dimensions: A single dimension or sequence of dimensions to query. Each dimension can be a field ID string or Dimension object.
            limit: Maximum number of rows to return.

        Returns:
            A Query object that can be used to fetch results.
        """
        metrics_seq = [metrics] if isinstance(metrics, (str, Metric)) else metrics
        dimensions_seq = [dimensions] if isinstance(dimensions, (str, Dimension)) else dimensions
        return Query(self, metrics=metrics_seq, dimensions=dimensions_seq, limit=limit)

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
            raise AttributeError(f"No model named '{name}' found")
