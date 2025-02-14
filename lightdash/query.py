"""
Query functionality for Lightdash models.
"""
from typing import Any, Dict, List, Optional, Union, Sequence

from .dimensions import Dimension
from .metrics import Metric
from .types import Model


class Query:
    """
    A Lightdash query builder and executor.
    
    Allows executing queries against a model to fetch data.
    Example:
        # Single metric, no dimensions
        df = model.query(
            metrics=model.metrics.count_of_product_name
        ).to_df()

        # Single metric and dimension
        df = model.query(
            metrics=model.metrics.count_of_product_name,
            dimensions=model.dimensions.partner_name
        ).to_df()

        # Multiple metrics and dimensions
        df = model.query(
            metrics=[model.metrics.count_of_product_name, model.metrics.sum_of_profit],
            dimensions=[model.dimensions.partner_name, model.dimensions.order_date]
        ).to_df()
    """
    def __init__(
        self,
        model: Model,
        metrics: Sequence[Union[str, Metric]],
        dimensions: Sequence[Union[str, Dimension]] = (),
        limit: int = 50,
    ):
        self._model = model
        self._dimensions = dimensions
        self._metrics = metrics
        self._limit = limit
        self._last_results: Optional[List[Dict[str, Any]]] = None
        self._field_labels: Optional[Dict[str, str]] = None

    def _execute(self) -> None:
        """
        Execute the query against the model if not already executed.
        """
        if self._last_results is not None:
            return

        if not 1 <= self._limit <= 5000:
            raise ValueError("Limit must be between 1 and 5000")

        if self._model._client is None:
            raise RuntimeError("Model not properly initialized with client reference")

        # Convert dimensions/metrics to field IDs if they're objects
        dimension_ids = [
            d.field_id if isinstance(d, Dimension) else d
            for d in self._dimensions
        ]
        metric_ids = [
            m.field_id if isinstance(m, Metric) else m
            for m in self._metrics
        ]

        # Construct query payload
        payload = {
            "exploreName": self._model.name,
            "dimensions": dimension_ids,
            "metrics": metric_ids,
            "filters": {},
            "limit": self._limit,
            "tableCalculations": [],
            "sorts": [],
        }

        # Execute query
        path = f"/api/v1/projects/{self._model._client.project_uuid}/explores/{self._model.name}/runQuery"
        response = self._model._client._make_request("POST", path, json=payload)

        # Store field labels mapping
        self._field_labels = {
            field_id: field_data.get("label") or field_data["name"]
            for field_id, field_data in response.get("fields", {}).items()
        }

        # Transform rows to extract raw values and use labels for keys
        rows = response["rows"]
        self._last_results = [
            {
                self._field_labels.get(field_id, field_id): row[field_id]["value"]["raw"]
                for field_id in row.keys()
            }
            for row in rows
        ]

    def to_records(self) -> List[Dict[str, Any]]:
        """
        Get the query results as a list of dictionaries.

        Returns:
            List of dictionaries, where each dictionary represents a row of data
            with column names as keys (using labels where available) and values as the corresponding data.

        Raises:
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
            ValueError: If limit is outside valid range
        """
        self._execute()
        return self._last_results

    def to_json(self) -> List[Dict[str, Any]]:
        """
        Alias for to_records() for backward compatibility.
        
        Returns:
            List of dictionaries, where each dictionary represents a row of data.
            
        See to_records() for more details.
        """
        return self.to_records()

    def to_df(self, backend: str = "pandas") -> Any:
        """
        Convert the query results to a DataFrame.

        Args:
            backend: The DataFrame backend to use ("pandas" or "polars")

        Returns:
            A pandas or polars DataFrame containing the query results.

        Raises:
            ImportError: If the requested backend is not installed
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
            ValueError: If limit is outside valid range
        """
        self._execute()

        if backend == "pandas":
            try:
                import pandas as pd
            except ImportError:
                raise ImportError(
                    "pandas is required for DataFrame support. "
                    "Install it with: pip install pandas"
                )
            return pd.DataFrame(self._last_results)
        elif backend == "polars":
            try:
                import polars as pl
            except ImportError:
                raise ImportError(
                    "polars is required for DataFrame support. "
                    "Install it with: pip install polars"
                )
            return pl.DataFrame(self._last_results)
        else:
            raise ValueError(
                f"Unsupported DataFrame backend: {backend}. "
                "Use 'pandas' or 'polars'"
            ) 