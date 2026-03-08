# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os

import pytest

from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._validate_env_vars import validate_api_url

ENV_VAR = "TEST_API_URL"


@pytest.fixture(autouse=True)
def _clean_env():
    """Ensure the test env var is cleaned up after each test."""
    yield
    os.environ.pop(ENV_VAR, None)


# --- Valid URLs ---


def test_accepts_default_when_env_not_set():
    """When env var is not set, the default value is returned."""
    result = validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")
    assert result == "https://api.fabric.microsoft.com"


def test_accepts_valid_fabric_url():
    os.environ[ENV_VAR] = "https://api.fabric.microsoft.com"
    result = validate_api_url(ENV_VAR, "https://api.powerbi.com")
    assert result == "https://api.fabric.microsoft.com"


def test_accepts_valid_powerbi_url():
    os.environ[ENV_VAR] = "https://api.powerbi.com"
    result = validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")
    assert result == "https://api.powerbi.com"


def test_accepts_private_link_url():
    """Private Link format: https://{workspaceid}.z{xy}.w.api.fabric.microsoft.com"""
    os.environ[ENV_VAR] = "https://abc12345-def0-1234-5678-abcdef012345.z42.w.api.fabric.microsoft.com"
    result = validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")
    assert result == "https://abc12345-def0-1234-5678-abcdef012345.z42.w.api.fabric.microsoft.com"


def test_strips_trailing_slash():
    os.environ[ENV_VAR] = "https://api.fabric.microsoft.com/"
    result = validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")
    assert result == "https://api.fabric.microsoft.com"


# --- Security: HTTPS enforcement ---


def test_rejects_http_scheme():
    os.environ[ENV_VAR] = "http://api.fabric.microsoft.com"
    with pytest.raises(InputError, match="must use HTTPS scheme"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


def test_rejects_ftp_scheme():
    os.environ[ENV_VAR] = "ftp://api.fabric.microsoft.com"
    with pytest.raises(InputError, match="must use HTTPS scheme"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


# --- Security: Hostname allowlist (SSRF prevention) ---


def test_rejects_attacker_host():
    os.environ[ENV_VAR] = "https://api.evil.com"
    with pytest.raises(InputError, match="invalid hostname"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


def test_rejects_lookalike_host():
    """Reject domains that embed valid names but aren't actually Microsoft."""
    os.environ[ENV_VAR] = "https://fabric.microsoft.com.evil.com"
    with pytest.raises(InputError, match="invalid hostname"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


def test_rejects_empty_hostname():
    os.environ[ENV_VAR] = "https://"
    with pytest.raises(InputError, match="invalid hostname"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


# --- Correctness: Path components ---


def test_rejects_path_components():
    os.environ[ENV_VAR] = "https://api.fabric.microsoft.com/v1/workspaces"
    with pytest.raises(InputError, match="root URL without path components"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


# --- Basic data type check ---


def test_rejects_empty_string():
    os.environ[ENV_VAR] = ""
    with pytest.raises(InputError, match="must resolve to a non-empty string"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")


def test_rejects_whitespace_only():
    os.environ[ENV_VAR] = "   "
    with pytest.raises(InputError, match="must resolve to a non-empty string"):
        validate_api_url(ENV_VAR, "https://api.fabric.microsoft.com")
