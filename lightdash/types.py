"""
Type definitions for Lightdash.
"""
from typing import Protocol, Dict, List, Optional, Any


class Client(Protocol):
    """Type protocol for the Lightdash client."""
    instance_url: str
    access_token: str
    project_uuid: str

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]: ...


class Model(Protocol):
    """Type protocol for a Lightdash model."""
    name: str
    type: str
    database_name: Optional[str]
    schema_name: Optional[str]
    label: Optional[str]
    description: Optional[str]

    def list_metrics(self) -> List["Metric"]: ...
    def list_dimensions(self) -> List["Dimension"]: ...


class Metric(Protocol):
    """Type protocol for a Lightdash metric."""
    name: str
    model_name: str
    label: Optional[str]
    description: Optional[str]

    @property
    def field_id(self) -> str: ...


class Dimension(Protocol):
    """Type protocol for a Lightdash dimension."""
    name: str
    model_name: str
    label: Optional[str]
    description: Optional[str]

    @property
    def field_id(self) -> str: ... 