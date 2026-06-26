"""Custom exceptions for Lightdash SDK."""


class LightdashError(Exception):
    """Base exception for Lightdash API errors."""
    def __init__(self, message: str, name: str = "LightdashError", status_code: int = 0):
        self.message = message
        self.name = name
        self.status_code = status_code
        super().__init__(f"{name} ({status_code}): {message}")


class LightdashConnectionError(LightdashError):
    """Raised when the SDK cannot reach the Lightdash instance.

    Covers DNS failures, refused connections, and timeouts — typically an
    incorrect ``instance_url`` or an instance that is down or unreachable.
    """
    def __init__(self, message: str):
        super().__init__(message, "LightdashConnectionError", 0)


class LightdashAuthError(LightdashError):
    """Raised when authentication fails (HTTP 401/403).

    Typically an invalid or expired ``access_token``, or a token without
    access to the requested project.
    """
    def __init__(self, message: str, status_code: int = 401):
        super().__init__(message, "LightdashAuthError", status_code)


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
