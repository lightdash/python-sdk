"""Custom exceptions for Lightdash SDK."""


class LightdashError(Exception):
    """Base exception for Lightdash API errors."""
    def __init__(self, message: str, name: str = "LightdashError", status_code: int = 0):
        self.message = message
        self.name = name
        self.status_code = status_code
        super().__init__(f"{name} ({status_code}): {message}")


class QueryError(LightdashError):
    """Raised when a query fails."""
    def __init__(self, message: str, query_uuid: str = None):
        self.query_uuid = query_uuid
        super().__init__(message, "QueryError", 400)


class QueryTimeout(LightdashError):
    """Raised when a query times out."""
    def __init__(self, message: str, query_uuid: str = None):
        self.query_uuid = query_uuid
        super().__init__(message, "QueryTimeout", 408)


class QueryCancelled(LightdashError):
    """Raised when a query is cancelled."""
    def __init__(self, message: str = "Query was cancelled", query_uuid: str = None):
        self.query_uuid = query_uuid
        super().__init__(message, "QueryCancelled", 499)
