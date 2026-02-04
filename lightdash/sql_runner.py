"""
Raw SQL execution for Lightdash.

Example usage:
    # Execute SQL
    result = client.sql("SELECT * FROM orders LIMIT 100")
    df = result.to_df()

    # Get available tables
    tables = client.sql_runner.tables()

    # Get table fields
    fields = client.sql_runner.fields("orders")

    # Iterate over results
    for row in result:
        print(row)
"""
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from .exceptions import QueryError, QueryTimeout
from .results import BaseResult


@dataclass
class SqlResult(BaseResult):
    """
    Result of a SQL query execution.

    Implements the ResultSet protocol for consistent interaction with query results.

    Example:
        result = client.sql("SELECT * FROM orders LIMIT 10")

        # Iterate over rows
        for row in result:
            print(row['order_id'])

        # Get total count
        print(f"Got {len(result)} rows")

        # Convert to DataFrame
        df = result.to_df()
    """
    rows: List[Dict[str, Any]]
    columns: List[str]

    def to_records(self) -> List[Dict[str, Any]]:
        """Get results as list of dictionaries."""
        return self.rows

    def to_df(self, backend: str = "pandas") -> Any:
        """
        Convert to DataFrame.

        Args:
            backend: DataFrame backend ("pandas" or "polars")

        Returns:
            DataFrame containing the results
        """
        if backend == "pandas":
            try:
                import pandas as pd
            except ImportError:
                raise ImportError(
                    "pandas is required for DataFrame support. "
                    "Install it with: pip install pandas"
                )
            return pd.DataFrame(self.rows, columns=self.columns)
        elif backend == "polars":
            try:
                import polars as pl
            except ImportError:
                raise ImportError(
                    "polars is required for DataFrame support. "
                    "Install it with: pip install polars"
                )
            return pl.DataFrame(self.rows)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over result rows."""
        yield from self.rows

    def __len__(self) -> int:
        """Return total number of results."""
        return len(self.rows)

    def __repr__(self) -> str:
        return f"SqlResult(rows={len(self.rows)}, columns={self.columns})"


class SqlRunner:
    """
    SQL runner for executing raw SQL against the data warehouse.
    """

    def __init__(self, client):
        self._client = client
        self._poll_interval_ms = 500
        self._max_poll_time_s = 300

    def execute(self, sql: str, limit: int = 500) -> SqlResult:
        """
        Execute a raw SQL query.

        Args:
            sql: SQL query to execute
            limit: Maximum rows to return

        Returns:
            SqlResult with query results
        """
        # Submit SQL query
        response = self._client._make_request(
            "POST",
            f"/api/v1/projects/{self._client.project_uuid}/sqlRunner/run",
            json={"sql": sql, "limit": limit}
        )

        job_id = response.get("jobId")
        if not job_id:
            # Synchronous response (if results are returned directly)
            return self._parse_result(response)

        # Poll for async result
        return self._poll_for_result(job_id)

    def _fetch_raw_results(self, url: str) -> List[Dict[str, Any]]:
        """Fetch raw results from a URL (JSONL format - one JSON object per line)."""
        import httpx
        import json
        from urllib.parse import urljoin

        full_url = urljoin(self._client.instance_url, url)
        with httpx.Client(
            headers={
                "Authorization": f"ApiKey {self._client.access_token}",
                "Accept": "application/json",
            },
            timeout=self._client.timeout
        ) as client:
            response = client.get(full_url)
            response.raise_for_status()

            # Parse JSONL format (one JSON object per line)
            rows = []
            for line in response.text.strip().split('\n'):
                if line:
                    rows.append(json.loads(line))
            return rows

    def _poll_for_result(self, job_id: str) -> SqlResult:
        """Poll for async query result."""
        start_time = time.time()

        while time.time() - start_time < self._max_poll_time_s:
            # Use the scheduler job status endpoint
            response = self._client._make_request(
                "GET",
                f"/api/v1/schedulers/job/{job_id}/status"
            )

            status = response.get("status")
            if status == "completed":
                # Get results from the fileUrl
                details = response.get("details", {})
                file_url = details.get("fileUrl")
                columns = [col.get("reference", col.get("name", ""))
                          for col in details.get("columns", [])]

                if file_url:
                    # Fetch raw results (JSONL format)
                    import re
                    match = re.search(r'/api/v1/projects/.+/sqlRunner/results/.+', file_url)
                    if match:
                        results_path = match.group(0)
                        rows = self._fetch_raw_results(results_path)
                        return SqlResult(rows=rows, columns=columns)

                # Fallback if no fileUrl
                return SqlResult(rows=[], columns=columns)

            elif status == "error":
                error_msg = response.get("details", {}).get("error", "SQL query failed")
                raise QueryError(error_msg)

            time.sleep(self._poll_interval_ms / 1000)

        raise QueryTimeout(f"SQL query {job_id} timed out")

    def _parse_result(self, data: Dict[str, Any]) -> SqlResult:
        """Parse SQL result from API response."""
        rows = data.get("rows", [])
        columns = list(rows[0].keys()) if rows else []
        return SqlResult(rows=rows, columns=columns)

    def tables(self) -> List[Dict[str, Any]]:
        """
        Get list of available tables in the warehouse.

        Returns:
            List of table metadata dictionaries
        """
        response = self._client._make_request(
            "GET",
            f"/api/v1/projects/{self._client.project_uuid}/sqlRunner/tables"
        )
        return response

    def fields(self, table: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get fields/columns for a table.

        Args:
            table: Table name
            schema: Optional schema name

        Returns:
            List of field metadata dictionaries
        """
        params = {"table": table}
        if schema:
            params["schema"] = schema

        response = self._client._make_request(
            "GET",
            f"/api/v1/projects/{self._client.project_uuid}/sqlRunner/fields",
            params=params
        )
        return response
