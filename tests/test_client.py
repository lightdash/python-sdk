"""
Unit tests for Client construction.

Covers the Authorization scheme selection: service account tokens (``ldsvc_``)
authenticate with ``Bearer`` while personal access tokens and anything else use
``ApiKey`` — matching the Lightdash backend auth middleware (issue #17).
"""

import pytest
from lightdash import Client


def _client(token: str) -> Client:
    # Construction makes no network call, so a dummy URL/project is fine.
    return Client(
        instance_url="https://example.lightdash.cloud",
        access_token=token,
        project_uuid="00000000-0000-0000-0000-000000000000",
    )


class TestAuthHeaderScheme:
    def test_service_account_token_uses_bearer(self):
        """ldsvc_ tokens must use the Bearer scheme."""
        assert _client("ldsvc_abc123").auth_header == "Bearer"

    def test_personal_access_token_uses_apikey(self):
        """ldpat_ tokens keep the ApiKey scheme (unchanged behaviour)."""
        assert _client("ldpat_abc123").auth_header == "ApiKey"

    def test_unprefixed_token_defaults_to_apikey(self):
        """Any other token defaults to ApiKey, preserving backwards compatibility."""
        assert _client("legacy-token").auth_header == "ApiKey"

    def test_authorization_header_value(self):
        """The composed Authorization header uses the selected scheme."""
        c = _client("ldsvc_abc123")
        assert f"{c.auth_header} {c.access_token}" == "Bearer ldsvc_abc123"
