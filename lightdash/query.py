"""Query functionality for Lightdash models."""
import time
import warnings
from typing import Any, Dict, List, Optional, Union, Sequence, Iterator

from .dimensions import Dimension
from .metrics import Metric
from .filter import DimensionFilter, CompositeFilter
from .sorting import Sort
from .types import Model
from .exceptions import QueryError, QueryTimeout, QueryCancelled
from .results import BaseResult


class _QueryExecutor:
    """Internal class that handles V2 async query submission and polling."""

    def __init__(self, client):
        self._client = client
        self._poll_backoff_start_ms = 100
        self._poll_backoff_max_ms = 2000

    def execute(
        self,
        query_payload: Dict[str, Any],
        timeout_seconds: float = 300,
        invalidate_cache: bool = False
    ) -> "QueryResult":
        """Submit query via V2 API and poll until complete."""
        # Step 1: Submit query
        submit_response = self._client._make_request(
            "POST",
            f"/api/v2/projects/{self._client.project_uuid}/query/metric-query",
            json={
                "query": query_payload,
                "context": "api",
                "invalidateCache": invalidate_cache
            }
        )
        query_uuid = submit_response["queryUuid"]
        fields = submit_response.get("fields", {})

        # Step 2: Poll for first page
        first_page = self._poll_until_ready(query_uuid, timeout_seconds)

        return QueryResult(
            query_uuid=query_uuid,
            fields=fields,
            first_page=first_page,
            executor=self
        )

    def _poll_until_ready(
        self,
        query_uuid: str,
        timeout_seconds: float,
        page: int = 1,
        page_size: int = 500
    ) -> Dict[str, Any]:
        """Poll with exponential backoff until query completes."""
        backoff_ms = self._poll_backoff_start_ms
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            response = self._client._make_request(
                "GET",
                f"/api/v2/projects/{self._client.project_uuid}/query/{query_uuid}",
                params={"page": page, "pageSize": page_size}
            )

            status = response.get("status")
            if status == "ready":
                return response
            elif status == "error":
                raise QueryError(response.get("error", "Unknown error"), query_uuid)
            elif status == "cancelled":
                raise QueryCancelled(query_uuid=query_uuid)

            # Status is "pending" - wait and retry
            time.sleep(backoff_ms / 1000)
            backoff_ms = min(backoff_ms * 2, self._poll_backoff_max_ms)

        raise QueryTimeout(
            f"Query {query_uuid} did not complete within {timeout_seconds}s",
            query_uuid
        )

    def get_page(self, query_uuid: str, page: int, page_size: int = 500) -> Dict[str, Any]:
        """Fetch a specific page of results."""
        return self._client._make_request(
            "GET",
            f"/api/v2/projects/{self._client.project_uuid}/query/{query_uuid}",
            params={"page": page, "pageSize": page_size}
        )

    def cancel(self, query_uuid: str) -> None:
        """Cancel a running query."""
        self._client._make_request(
            "POST",
            f"/api/v2/projects/{self._client.project_uuid}/query/{query_uuid}/cancel"
        )


class QueryResult(BaseResult):
    """
    Result of a query execution with pagination support.

    Lazily fetches additional pages from the V2 API as needed.
    Implements the ResultSet protocol for consistent interaction with query results.

    Example:
        result = query.execute()

        # Iterate over rows
        for row in result:
            print(row['Revenue'])

        # Get total count
        print(f"Got {len(result)} rows")

        # Convert to DataFrame
        df = result.to_df()
    """

    def __init__(
        self,
        query_uuid: str,
        fields: Dict[str, Any],
        first_page: Dict[str, Any],
        executor: _QueryExecutor
    ):
        self._query_uuid = query_uuid
        self._fields = fields
        self._first_page = first_page
        self._executor = executor
        self._all_rows: Optional[List[Dict[str, Any]]] = None
        self._field_labels = self._build_field_labels()

    def _build_field_labels(self) -> Dict[str, str]:
        """Build mapping from field IDs to labels."""
        return {
            field_id: field_data.get("label") or field_data.get("name", field_id)
            for field_id, field_data in self._fields.items()
        }

    def _transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a raw API row to use labels and extract raw values."""
        result = {}
        for field_id, field_value in row.items():
            label = self._field_labels.get(field_id, field_id)
            # Handle both V2 format (direct values) and V1 format (nested value.raw)
            if isinstance(field_value, dict) and "value" in field_value:
                result[label] = field_value["value"].get("raw", field_value["value"])
            else:
                result[label] = field_value
        return result

    def _transform_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform multiple rows."""
        return [self._transform_row(row) for row in rows]

    @property
    def query_uuid(self) -> str:
        """The unique identifier for this query."""
        return self._query_uuid

    @property
    def total_results(self) -> int:
        """Total number of rows across all pages."""
        return self._first_page.get("totalResults", len(self._first_page.get("rows", [])))

    @property
    def total_pages(self) -> int:
        """Total number of pages available."""
        return self._first_page.get("totalPageCount", 1)

    @property
    def fields(self) -> Dict[str, Any]:
        """Field metadata from the query."""
        return self._fields

    def page(self, page_num: int, page_size: int = 500) -> List[Dict[str, Any]]:
        """
        Get a specific page of results.

        Args:
            page_num: Page number (1-indexed)
            page_size: Number of rows per page (max 5000)

        Returns:
            List of row dictionaries for the requested page
        """
        if page_num == 1 and page_size == self._first_page.get("pageSize", 500):
            return self._transform_rows(self._first_page.get("rows", []))

        page_data = self._executor.get_page(self._query_uuid, page_num, page_size)
        return self._transform_rows(page_data.get("rows", []))

    def iter_pages(self, page_size: int = 500) -> Iterator[List[Dict[str, Any]]]:
        """
        Iterate through all pages of results.

        Args:
            page_size: Number of rows per page

        Yields:
            List of row dictionaries for each page
        """
        # Yield first page
        yield self._transform_rows(self._first_page.get("rows", []))

        # Fetch and yield remaining pages
        for page_num in range(2, self.total_pages + 1):
            page_data = self._executor.get_page(self._query_uuid, page_num, page_size)
            yield self._transform_rows(page_data.get("rows", []))

    def to_records(self) -> List[Dict[str, Any]]:
        """
        Get all results as a list of dictionaries.

        Fetches all pages if not already cached.

        Returns:
            List of all row dictionaries
        """
        if self._all_rows is None:
            self._all_rows = []
            for page in self.iter_pages():
                self._all_rows.extend(page)
        return self._all_rows

    def to_json(self) -> List[Dict[str, Any]]:
        """
        Deprecated: Use to_records() instead.

        For JSON string output, use to_json_str().
        """
        warnings.warn(
            "to_json() is deprecated, use to_records() instead. "
            "For JSON string output, use to_json_str().",
            DeprecationWarning,
            stacklevel=2
        )
        return self.to_records()

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over all result rows, fetching pages as needed."""
        for page in self.iter_pages():
            yield from page

    def __len__(self) -> int:
        """Return total number of results."""
        return self.total_results

    def to_df(self, backend: str = "pandas") -> Any:
        """
        Convert all results to a DataFrame.

        Args:
            backend: DataFrame backend ("pandas" or "polars")

        Returns:
            DataFrame containing all query results
        """
        records = self.to_records()

        if backend == "pandas":
            try:
                import pandas as pd
            except ImportError:
                raise ImportError(
                    "pandas is required for DataFrame support. "
                    "Install it with: pip install pandas"
                )
            return pd.DataFrame(records)
        elif backend == "polars":
            try:
                import polars as pl
            except ImportError:
                raise ImportError(
                    "polars is required for DataFrame support. "
                    "Install it with: pip install polars"
                )
            return pl.DataFrame(records)
        else:
            raise ValueError(
                f"Unsupported DataFrame backend: {backend}. "
                "Use 'pandas' or 'polars'"
            )

    def to_df_lazy(self, backend: str = "pandas", page_size: int = 500) -> Any:
        """
        Stream results into DataFrame page by page (memory efficient for large results).

        Args:
            backend: DataFrame backend ("pandas" or "polars")
            page_size: Number of rows per page

        Returns:
            DataFrame containing all query results
        """
        if backend == "pandas":
            import pandas as pd
            dfs = [pd.DataFrame(page) for page in self.iter_pages(page_size)]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        elif backend == "polars":
            import polars as pl
            dfs = [pl.DataFrame(page) for page in self.iter_pages(page_size)]
            return pl.concat(dfs) if dfs else pl.DataFrame()
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def __repr__(self) -> str:
        return f"QueryResult(query_uuid={self._query_uuid!r}, total_results={self.total_results}, total_pages={self.total_pages})"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("QueryResult(...)")
        else:
            p.text(repr(self))


class Query:
    """
    A Lightdash query builder and executor.

    Supports lazy evaluation - API calls only happen when results are requested.
    Supports both single-call and chainable builder patterns.

    Example (single-call pattern):
        # Build query (no API call yet)
        query = model.query(
            metrics=[model.metrics.revenue],
            dimensions=[model.dimensions.country],
            limit=100
        )

        # Execute and get results
        df = query.to_df()  # API call happens here

    Example (chainable builder pattern):
        # Build incrementally
        query = (
            model.query()
            .metrics(model.metrics.revenue, model.metrics.profit)
            .dimensions(model.dimensions.country)
            .filter(model.dimensions.status == 'active')
            .sort(model.metrics.revenue.desc())
            .limit(100)
        )

        # Reuse base queries
        base_query = model.query().metrics(model.metrics.revenue)
        usa_query = base_query.filter(model.dimensions.country == 'USA')
        uk_query = base_query.filter(model.dimensions.country == 'UK')
    """

    def __init__(
        self,
        model: Model,
        metrics: Sequence[Union[str, Metric]] = (),
        dimensions: Sequence[Union[str, Dimension]] = (),
        filters: Optional[Union[DimensionFilter, CompositeFilter, Any]] = None,
        limit: int = 500,
        # Phase 1 additions (initially None)
        sort: Optional[Union[Sort, Sequence[Sort]]] = None,
        table_calculations: Optional[Sequence[Any]] = None,
        # Phase 3 additions (initially None)
        custom_dimensions: Optional[Sequence[Any]] = None,
        additional_metrics: Optional[Sequence[Any]] = None,
        timezone: Optional[str] = None,
    ):
        self._model = model
        # Store as tuples for immutability
        self._metrics = tuple(metrics) if metrics else ()
        self._dimensions = tuple(dimensions) if dimensions else ()
        self._limit = limit

        # Handle filters - normalize to CompositeFilter if DimensionFilter is passed
        if filters is None:
            self._filters = None
        elif isinstance(filters, DimensionFilter):
            self._filters = CompositeFilter(filters=[filters])
        else:
            self._filters = filters

        # Normalize single Sort to tuple
        if sort is None:
            self._sort = ()
        elif isinstance(sort, Sort):
            self._sort = (sort,)
        else:
            self._sort = tuple(sort)
        self._table_calculations = table_calculations
        self._custom_dimensions = custom_dimensions
        self._additional_metrics = additional_metrics
        self._timezone = timezone

        # Cached result
        self._result: Optional[QueryResult] = None

    def _clone(self, **kwargs) -> "Query":
        """Create a new Query with some attributes replaced."""
        return Query(
            model=kwargs.get("model", self._model),
            metrics=kwargs.get("metrics", self._metrics),
            dimensions=kwargs.get("dimensions", self._dimensions),
            filters=kwargs.get("filters", self._filters),
            limit=kwargs.get("limit", self._limit),
            sort=kwargs.get("sort", self._sort),
            table_calculations=kwargs.get("table_calculations", self._table_calculations),
            custom_dimensions=kwargs.get("custom_dimensions", self._custom_dimensions),
            additional_metrics=kwargs.get("additional_metrics", self._additional_metrics),
            timezone=kwargs.get("timezone", self._timezone),
        )

    def metrics(self, *metrics: Union[str, Metric]) -> "Query":
        """
        Add metrics to the query.

        Returns a new Query with the specified metrics added.

        Args:
            *metrics: Metric objects or field ID strings to add

        Returns:
            A new Query with the metrics added

        Example:
            query = model.query().metrics(model.metrics.revenue, model.metrics.profit)
        """
        return self._clone(metrics=self._metrics + metrics)

    def dimensions(self, *dimensions: Union[str, Dimension]) -> "Query":
        """
        Add dimensions to the query.

        Returns a new Query with the specified dimensions added.

        Args:
            *dimensions: Dimension objects or field ID strings to add

        Returns:
            A new Query with the dimensions added

        Example:
            query = model.query().dimensions(model.dimensions.country, model.dimensions.region)
        """
        return self._clone(dimensions=self._dimensions + dimensions)

    def filter(self, filter: Union[DimensionFilter, CompositeFilter]) -> "Query":
        """
        Add a filter to the query.

        Multiple calls to filter() combine filters with AND logic.
        Returns a new Query with the filter added.

        Args:
            filter: A DimensionFilter or CompositeFilter to apply

        Returns:
            A new Query with the filter added

        Example:
            query = (
                model.query()
                .filter(model.dimensions.country == 'USA')
                .filter(model.dimensions.status == 'active')
            )
        """
        if self._filters is None:
            # First filter
            if isinstance(filter, DimensionFilter):
                new_filters = CompositeFilter(filters=[filter])
            else:
                new_filters = filter
        else:
            # Combine with existing filters using AND
            if isinstance(filter, DimensionFilter):
                # Add to existing CompositeFilter's list
                new_filters = CompositeFilter(
                    filters=list(self._filters.filters) + [filter],
                    aggregation=self._filters.aggregation
                )
            else:
                # Both are CompositeFilters - combine them
                new_filters = CompositeFilter(
                    filters=list(self._filters.filters) + list(filter.filters),
                    aggregation="and"
                )
        return self._clone(filters=new_filters)

    def sort(self, *sorts: Sort) -> "Query":
        """
        Add sort criteria to the query.

        Returns a new Query with the specified sorts added.

        Args:
            *sorts: Sort objects specifying order

        Returns:
            A new Query with the sorts added

        Example:
            query = model.query().sort(model.metrics.revenue.desc(), model.dimensions.country.asc())
        """
        return self._clone(sort=self._sort + sorts)

    def limit(self, n: int) -> "Query":
        """
        Set the result limit for the query.

        Returns a new Query with the specified limit.

        Args:
            n: Maximum number of rows to return (1-50000)

        Returns:
            A new Query with the limit set

        Example:
            query = model.query().limit(100)
        """
        return self._clone(limit=n)

    def _build_payload(self) -> Dict[str, Any]:
        """Build the query payload for the V2 API."""
        # Convert dimensions/metrics to field IDs
        dimension_ids = [
            d.field_id if isinstance(d, Dimension) else d
            for d in self._dimensions
        ]
        metric_ids = [
            m.field_id if isinstance(m, Metric) else m
            for m in self._metrics
        ]

        # Build filters dict - use empty CompositeFilter if no filters
        if self._filters is None:
            filters_dict = CompositeFilter().to_dict()
        elif hasattr(self._filters, 'to_dict'):
            filters_dict = self._filters.to_dict()
        else:
            filters_dict = self._filters

        payload = {
            "exploreName": self._model.name,
            "dimensions": dimension_ids,
            "metrics": metric_ids,
            "filters": filters_dict,
            "limit": self._limit,
            "tableCalculations": [],
            "sorts": [],
        }

        # Add sorts if provided (Phase 1)
        if self._sort:
            payload["sorts"] = [
                s.to_dict() if hasattr(s, 'to_dict') else s
                for s in self._sort
            ]

        # Add table calculations if provided (Phase 1)
        if self._table_calculations is not None:
            payload["tableCalculations"] = [
                tc.to_dict() if hasattr(tc, 'to_dict') else tc
                for tc in self._table_calculations
            ]

        # Add custom dimensions if provided (Phase 3)
        if self._custom_dimensions is not None:
            payload["customDimensions"] = [
                cd.to_dict() if hasattr(cd, 'to_dict') else cd
                for cd in self._custom_dimensions
            ]

        # Add additional metrics if provided (Phase 3)
        if self._additional_metrics is not None:
            payload["additionalMetrics"] = [
                am.to_dict() if hasattr(am, 'to_dict') else am
                for am in self._additional_metrics
            ]

        # Add timezone if provided
        if self._timezone is not None:
            payload["timezone"] = self._timezone

        return payload

    def execute(
        self,
        timeout_seconds: float = 300,
        invalidate_cache: bool = False
    ) -> QueryResult:
        """
        Execute the query and return a QueryResult object.

        Args:
            timeout_seconds: Maximum time to wait for query completion
            invalidate_cache: If True, bypass server-side cache

        Returns:
            QueryResult object with pagination support
        """
        if self._result is not None and not invalidate_cache:
            return self._result

        if not 1 <= self._limit <= 50000:
            raise ValueError("Limit must be between 1 and 50000")

        if self._model._client is None:
            raise RuntimeError("Model not properly initialized with client reference")

        executor = _QueryExecutor(self._model._client)
        payload = self._build_payload()

        self._result = executor.execute(
            payload,
            timeout_seconds=timeout_seconds,
            invalidate_cache=invalidate_cache
        )
        return self._result

    def to_records(self) -> List[Dict[str, Any]]:
        """Get all query results as a list of dictionaries."""
        return self.execute().to_records()

    def to_json(self) -> List[Dict[str, Any]]:
        """
        Deprecated: Use to_records() instead.

        For JSON string output, use to_json_str().
        """
        warnings.warn(
            "to_json() is deprecated, use to_records() instead. "
            "For JSON string output, use to_json_str().",
            DeprecationWarning,
            stacklevel=2
        )
        return self.to_records()

    def to_json_str(self) -> str:
        """
        Convert results to a JSON string.

        Returns:
            JSON string representation of the results
        """
        return self.execute().to_json_str()

    def to_df(self, backend: str = "pandas") -> Any:
        """Convert query results to a DataFrame."""
        return self.execute().to_df(backend)

    def __repr__(self) -> str:
        return f"Query(model={self._model.name!r}, metrics={len(self._metrics)}, dimensions={len(self._dimensions)}, limit={self._limit})"
