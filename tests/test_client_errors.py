"""
Tests for clear connection / auth error messages (issue #1).

A misconfigured client used to surface a raw ``httpx`` error that never
mentioned Lightdash. These tests verify the SDK now translates the two reported
cases — unreachable instance and bad credentials — into descriptive exceptions,
while leaving the existing API-error handling untouched.
"""

import httpx
import pytest
from lightdash import (
    Client,
    LightdashError,
    LightdashConnectionError,
    LightdashAuthError,
)


def _client() -> Client:
    return Client(
        instance_url="https://demo.lightdash.cloud",
        access_token="ldpat_token",
        project_uuid="proj",
    )


def _response(status: int, json_body=None, text: str = "") -> httpx.Response:
    request = httpx.Request("GET", "https://demo.lightdash.cloud/api/v1/x")
    if json_body is not None:
        return httpx.Response(status, json=json_body, request=request)
    return httpx.Response(status, text=text, request=request)


class TestConnectionErrors:
    def test_connect_error_is_wrapped(self, monkeypatch):
        """A DNS/connection failure names the instance and the likely cause."""
        def boom(*args, **kwargs):
            raise httpx.ConnectError("nodename nor servname provided, or not known")
        monkeypatch.setattr(httpx.Client, "request", boom)

        with pytest.raises(LightdashConnectionError, match="Could not connect to Lightdash") as ei:
            _client()._make_request("GET", "/api/v1/x")
        assert "demo.lightdash.cloud" in str(ei.value)
        assert "instance_url" in str(ei.value)

    def test_timeout_is_wrapped(self, monkeypatch):
        def boom(*args, **kwargs):
            raise httpx.ConnectTimeout("timed out")
        monkeypatch.setattr(httpx.Client, "request", boom)

        with pytest.raises(LightdashConnectionError, match="timed out"):
            _client()._make_request("GET", "/api/v1/x")

    def test_connection_error_subclasses_lightdash_error(self, monkeypatch):
        """Existing `except LightdashError` handlers keep working."""
        def boom(*args, **kwargs):
            raise httpx.ConnectError("x")
        monkeypatch.setattr(httpx.Client, "request", boom)

        with pytest.raises(LightdashError):
            _client()._make_request("GET", "/api/v1/x")

    def test_original_error_is_chained(self, monkeypatch):
        """The underlying httpx error is preserved as the cause for debugging."""
        original = httpx.ConnectError("boom")
        def boom(*args, **kwargs):
            raise original
        monkeypatch.setattr(httpx.Client, "request", boom)

        with pytest.raises(LightdashConnectionError) as ei:
            _client()._make_request("GET", "/api/v1/x")
        assert ei.value.__cause__ is original


class TestAuthErrors:
    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_failure_raises_auth_error(self, monkeypatch, status):
        monkeypatch.setattr(
            httpx.Client, "request", lambda *a, **k: _response(status, text="unauthorized")
        )
        with pytest.raises(LightdashAuthError, match="Authentication failed") as ei:
            _client()._make_request("GET", "/api/v1/x")
        assert ei.value.status_code == status
        assert "access_token" in str(ei.value)


class TestExistingBehaviourPreserved:
    """The structured-error and success paths are unchanged by this fix."""

    def test_api_error_in_ok_response_is_surfaced(self, monkeypatch):
        body = {"status": "error", "error": {"message": "Bad query", "name": "QueryError", "statusCode": 400}}
        monkeypatch.setattr(httpx.Client, "request", lambda *a, **k: _response(200, json_body=body))
        with pytest.raises(LightdashError, match="Bad query") as ei:
            _client()._make_request("GET", "/api/v1/x")
        assert not isinstance(ei.value, LightdashAuthError)

    def test_ok_response_returns_results(self, monkeypatch):
        body = {"status": "ok", "results": [{"a": 1}]}
        monkeypatch.setattr(httpx.Client, "request", lambda *a, **k: _response(200, json_body=body))
        assert _client()._make_request("GET", "/api/v1/x") == [{"a": 1}]
