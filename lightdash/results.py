"""
Common result interfaces for Lightdash queries.

Provides a unified protocol and base class for all query results.
"""
import json
import warnings
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Protocol, runtime_checkable


@runtime_checkable
class ResultSet(Protocol):
    """
    Common interface for all query results.

    This protocol defines the standard methods that all result types must implement,
    enabling consistent interaction regardless of the query source.
    """

    def to_df(self, backend: str = "pandas") -> Any:
        """Convert results to DataFrame."""
        ...

    def to_records(self) -> List[Dict[str, Any]]:
        """Convert results to list of dictionaries."""
        ...

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over result rows."""
        ...

    def __len__(self) -> int:
        """Return total number of results."""
        ...


class BaseResult(ABC):
    """
    Abstract base class for result sets.

    Provides default implementations for common functionality while requiring
    subclasses to implement the core conversion methods.
    """

    @abstractmethod
    def to_df(self, backend: str = "pandas") -> Any:
        """
        Convert results to DataFrame.

        Args:
            backend: DataFrame backend ("pandas" or "polars")

        Returns:
            DataFrame containing the results
        """
        pass

    @abstractmethod
    def to_records(self) -> List[Dict[str, Any]]:
        """
        Convert results to list of dictionaries.

        Returns:
            List of row dictionaries
        """
        pass

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over result rows."""
        yield from self.to_records()

    @abstractmethod
    def __len__(self) -> int:
        """Return total number of results."""
        pass

    def to_json_str(self) -> str:
        """
        Convert results to a JSON string.

        Returns:
            JSON string representation of the results
        """
        return json.dumps(self.to_records())

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
