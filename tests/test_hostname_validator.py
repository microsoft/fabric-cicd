# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os

import pytest

from fabric_cicd._common._hostname_validator import (
    VALID_HOSTNAME_REGEX,
    validate_and_get_env_variable,
)


class TestValidHostnameRegex:
    """Tests for the VALID_HOSTNAME_REGEX pattern."""

    @pytest.mark.parametrize(
        "hostname",
        [
            "api.fabric.microsoft.com",
            "api.powerbi.com",
            "myapi.fabric.microsoft.com",
            "myapi.powerbi.com",
            "someapi.fabric.microsoft.com",
            "someapi.powerbi.com",
            "some.api.fabric.microsoft.com",
            "some.api.powerbi.com",
            "my-org.api.fabric.microsoft.com",
            "a.b.api.fabric.microsoft.com",
            "abcdef01234567890abcdef012345678.z01.w.api.fabric.microsoft.com",
            "abcdef01234567890abcdef012345678.z42.w.api.powerbi.com",
            # Hyphen and underscore in labels
            "staging-api.fabric.microsoft.com",
            "staging-api.powerbi.com",
            "my_org.api.fabric.microsoft.com",
            # Deeply nested subdomains
            "a.b.c.d.api.fabric.microsoft.com",
            # Numeric subdomain label
            "123.api.powerbi.com",
            # Case insensitive
            "API.fabric.microsoft.com",
            "api.FABRIC.microsoft.com",
            "api.fabric.MICROSOFT.com",
            "Api.PowerBI.Com",
        ],
    )
    def test_valid_hostnames(self, hostname):
        assert VALID_HOSTNAME_REGEX.match(hostname), f"Expected '{hostname}' to be valid"

    @pytest.mark.parametrize(
        "hostname",
        [
            "fabric.microsoft.com",
            "powerbi.com",
            "contoso.com",
            "api.fabric.microsoft.com.contoso.com",
            "api.powerbi.com.contoso.com",
            "",
            "https://api.fabric.microsoft.com",
            "api.fabric.microsoft.com/path",
            "random.hostname.com",
            # Label doesn't end with 'api'
            "dfs.fabric.microsoft.com",
            "management.fabric.microsoft.com",
            "apix.fabric.microsoft.com",
            "my-apix.fabric.microsoft.com",
            # Trailing dot
            "api.fabric.microsoft.com.",
            # Leading dot
            ".api.fabric.microsoft.com",
            # Domain suffix spoofing
            "api.fabric.microsoft.com.br",
            "api.fabric.microsoft.community",
            "api.notfabric.microsoft.com",
            "api.fakepowerbi.com",
        ],
    )
    def test_invalid_hostnames(self, hostname):
        assert not VALID_HOSTNAME_REGEX.match(hostname), f"Expected '{hostname}' to be invalid"


class TestValidateAndGetEnvVariable:
    """Tests for the validate_and_get_env_variable function."""

    def test_returns_default_when_env_not_set(self):
        env_var = "TEST_HOSTNAME_NOT_SET_12345"
        assert env_var not in os.environ
        result = validate_and_get_env_variable(env_var, "https://api.fabric.microsoft.com")
        assert result == "https://api.fabric.microsoft.com"

    def test_returns_env_value_when_set(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://api.powerbi.com")
        result = validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://api.powerbi.com"

    def test_preserves_original_value_with_path(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com/v1/workspaces")
        result = validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://api.fabric.microsoft.com/v1/workspaces"

    def test_raises_on_invalid_hostname(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://evil.com")
        with pytest.raises(ValueError, match="Invalid hostname"):
            validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_raises_on_empty_hostname(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "")
        with pytest.raises(ValueError, match="Invalid or missing scheme"):
            validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_prefixed_hostname_accepted(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://myapi.fabric.microsoft.com")
        result = validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://myapi.fabric.microsoft.com"

    def test_workspace_id_pattern_accepted(self, monkeypatch):
        url = "https://abcdef01234567890abcdef012345678.z01.w.api.fabric.microsoft.com"
        monkeypatch.setenv("TEST_HOSTNAME_VAR", url)
        result = validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == url

    def test_dotted_prefix_hostname_accepted(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://some.api.fabric.microsoft.com")
        result = validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://some.api.fabric.microsoft.com"

    def test_hostname_without_scheme_rejected(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "api.fabric.microsoft.com")
        with pytest.raises(ValueError, match="Invalid or missing scheme"):
            validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_rejects_http_scheme(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "http://api.fabric.microsoft.com")
        with pytest.raises(ValueError, match="Invalid or missing scheme"):
            validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_rejects_ftp_scheme(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "ftp://api.fabric.microsoft.com")
        with pytest.raises(ValueError, match="Invalid or missing scheme"):
            validate_and_get_env_variable("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
