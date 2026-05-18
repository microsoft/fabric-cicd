# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Integration tests for token acquisition against live Azure AD.

These tests require an authenticated Azure CLI session (`az login`).
They are skipped by default unless opted in via env var.

Usage:
    FABRIC_CICD_LIVE_TOKEN_TESTS=1 uv run pytest tests/test_integration_token.py -v
    # PowerShell:
    $env:FABRIC_CICD_LIVE_TOKEN_TESTS="1"; uv run pytest tests/test_integration_token.py -v
"""

import os
import statistics
import time

import pytest

from fabric_cicd._common._fabric_endpoint import FabricEndpoint

_SKIP_REASON = "Live token tests require FABRIC_CICD_LIVE_TOKEN_TESTS=1 and `az login`"
_enabled = os.environ.get("FABRIC_CICD_LIVE_TOKEN_TESTS", "0") == "1"

pytestmark = pytest.mark.skipif(not _enabled, reason=_SKIP_REASON)


@pytest.fixture(scope="module")
def credential():
    """Provide a real AzureCliCredential. Skips if az login hasn't been run."""
    try:
        from azure.identity import AzureCliCredential

        cred = AzureCliCredential()
        # Validate it works before yielding
        cred.get_token("https://api.fabric.microsoft.com/.default")
        return cred
    except Exception as e:
        pytest.skip(f"Azure CLI credential unavailable: {e}")


@pytest.fixture(scope="module")
def live_endpoint(credential):
    """Create a FabricEndpoint with real credentials."""
    return FabricEndpoint(token_credential=credential)


class TestTokenAcquisitionPerformance:
    """Validate that token acquisition latency is acceptable with real Azure AD."""

    def test_init_token_acquisition_time(self, credential):
        """FabricEndpoint init (which calls _get_token) should complete quickly."""
        start = time.perf_counter()
        FabricEndpoint(token_credential=credential)
        elapsed = time.perf_counter() - start

        # First call may involve a network round-trip; allow up to 5 seconds
        assert elapsed < 5, f"Init took {elapsed:.2f}s, expected < 5s"

    def test_cached_token_is_fast(self, live_endpoint):
        """Subsequent _get_token calls should return the SDK-cached token quickly."""
        timings = []
        for _ in range(100):
            start = time.perf_counter()
            live_endpoint._get_token()
            timings.append(time.perf_counter() - start)

        avg = statistics.mean(timings)
        p99 = sorted(timings)[98]

        # Cached token should be sub-millisecond on average
        assert avg < 0.01, f"Average _get_token latency {avg*1000:.2f}ms, expected < 10ms"
        assert p99 < 0.05, f"p99 _get_token latency {p99*1000:.2f}ms, expected < 50ms"

    def test_repeated_token_consistency(self, live_endpoint):
        """Token value should remain stable across rapid calls (SDK caching works)."""
        tokens = {live_endpoint._get_token() for _ in range(50)}

        # SDK should return the same cached token for all calls
        assert len(tokens) == 1, f"Expected 1 unique token, got {len(tokens)}"


class TestTokenResilience:
    """Validate token acquisition handles edge cases with real credentials."""

    def test_rapid_sequential_calls(self, credential):
        """Simulate a large deployment with many sequential _get_token calls."""
        endpoint = FabricEndpoint(token_credential=credential)

        start = time.perf_counter()
        for _ in range(500):
            token = endpoint._get_token()
            assert token is not None
            assert len(token) > 0
        elapsed = time.perf_counter() - start

        # 500 cached token calls should be well under 5 seconds
        assert elapsed < 5, f"500 calls took {elapsed:.2f}s, expected < 5s"

    def test_multiple_endpoint_instances(self, credential):
        """Multiple FabricEndpoint instances sharing the same credential should not conflict."""
        endpoints = [FabricEndpoint(token_credential=credential) for _ in range(5)]

        tokens = set()
        for ep in endpoints:
            tokens.add(ep._get_token())

        # All should get the same cached token from the shared credential
        assert len(tokens) == 1, f"Expected 1 unique token across instances, got {len(tokens)}"

    def test_token_is_valid_jwt(self, live_endpoint):
        """Verify the returned token has valid JWT structure."""
        import base64
        import json

        token = live_endpoint._get_token()
        parts = token.split(".")
        assert len(parts) == 3, "Token should be a 3-part JWT"

        # Decode payload (add padding)
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))

        assert "exp" in payload, "Token payload should contain 'exp' claim"
        assert payload["exp"] > time.time(), "Token should not be expired"
        assert "aud" in payload, "Token payload should contain 'aud' claim"
