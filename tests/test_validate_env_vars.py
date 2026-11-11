# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os

import pytest
import requests

from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._validate_env_vars import (
    _VALID_HOSTNAME_REGEX,
    validate_api_url,
    validate_env_var_api_url,
    validate_extra_headers,
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
        assert _VALID_HOSTNAME_REGEX.match(hostname), f"Expected '{hostname}' to be valid"

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
        assert not _VALID_HOSTNAME_REGEX.match(hostname), f"Expected '{hostname}' to be invalid"


class TestValidateApiUrl:
    """Tests for the standalone validate_api_url function."""

    def test_accepts_valid_fabric_url(self):
        result = validate_api_url("https://api.fabric.microsoft.com", "test_label")
        assert result == "https://api.fabric.microsoft.com"

    def test_accepts_valid_powerbi_url(self):
        result = validate_api_url("https://api.powerbi.com", "test_label")
        assert result == "https://api.powerbi.com"

    def test_strips_trailing_slash(self):
        result = validate_api_url("https://api.fabric.microsoft.com/", "test_label")
        assert result == "https://api.fabric.microsoft.com"

    def test_label_appears_in_error_message(self):
        with pytest.raises(InputError, match="my_config_key"):
            validate_api_url("http://api.fabric.microsoft.com", "my_config_key")

    def test_rejects_empty_string(self):
        with pytest.raises(InputError, match="must resolve to a non-empty string"):
            validate_api_url("", "test_label")

    def test_rejects_whitespace_only(self):
        with pytest.raises(InputError, match="must resolve to a non-empty string"):
            validate_api_url("   ", "test_label")

    def test_rejects_http_scheme(self):
        with pytest.raises(InputError, match="HTTPS scheme"):
            validate_api_url("http://api.fabric.microsoft.com", "test_label")

    def test_rejects_invalid_hostname(self):
        with pytest.raises(InputError, match="invalid hostname"):
            validate_api_url("https://evil.com", "test_label")

    def test_rejects_path_components(self):
        with pytest.raises(InputError, match="root URL without path components"):
            validate_api_url("https://api.fabric.microsoft.com/v1/workspaces", "test_label")

    def test_accepts_private_link_url(self):
        url = "https://abcdef01234567890abcdef012345678.z01.w.api.fabric.microsoft.com"
        result = validate_api_url(url, "test_label")
        assert result == url


class TestValidateApiUrlHostname:
    """Tests for the validate_env_var_api_url function (env var wrapper)."""

    def test_returns_default_when_env_not_set(self):
        env_var = "TEST_HOSTNAME_NOT_SET_12345"
        assert env_var not in os.environ
        result = validate_env_var_api_url(env_var, "https://api.fabric.microsoft.com")
        assert result == "https://api.fabric.microsoft.com"

    def test_returns_default_powerbi_url_when_env_not_set(self):
        env_var = "TEST_HOSTNAME_NOT_SET_12345"
        assert env_var not in os.environ
        result = validate_env_var_api_url(env_var, "https://api.powerbi.com")
        assert result == "https://api.powerbi.com"

    def test_returns_env_value_when_set(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://api.powerbi.com")
        result = validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://api.powerbi.com"

    def test_rejects_path_components(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com/v1/workspaces")
        with pytest.raises(InputError, match="root URL without path components"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_raises_on_invalid_hostname(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://evil.com")
        with pytest.raises(InputError, match="invalid hostname"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_raises_on_empty_hostname(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "")
        with pytest.raises(InputError, match="must resolve to a non-empty string"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_prefixed_hostname_accepted(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://myapi.fabric.microsoft.com")
        result = validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://myapi.fabric.microsoft.com"

    def test_workspace_id_pattern_accepted(self, monkeypatch):
        url = "https://abcdef01234567890abcdef012345678.z01.w.api.fabric.microsoft.com"
        monkeypatch.setenv("TEST_HOSTNAME_VAR", url)
        result = validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == url

    def test_dotted_prefix_hostname_accepted(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://some.api.fabric.microsoft.com")
        result = validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://some.api.fabric.microsoft.com"

    def test_hostname_without_scheme_rejected(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "api.fabric.microsoft.com")
        with pytest.raises(InputError, match="Invalid or missing scheme"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_rejects_http_scheme(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "http://api.fabric.microsoft.com")
        with pytest.raises(InputError, match="Invalid or missing scheme"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_rejects_ftp_scheme(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "ftp://api.fabric.microsoft.com")
        with pytest.raises(InputError, match="Invalid or missing scheme"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_raises_on_whitespace_only(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "   ")
        with pytest.raises(InputError, match="must resolve to a non-empty string"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")

    def test_strips_trailing_slash(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com/")
        result = validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")
        assert result == "https://api.fabric.microsoft.com"

    def test_rejects_url_with_no_authority(self, monkeypatch):
        monkeypatch.setenv("TEST_HOSTNAME_VAR", "https:///")
        with pytest.raises(InputError, match="invalid hostname"):
            validate_env_var_api_url("TEST_HOSTNAME_VAR", "https://api.fabric.microsoft.com")


class TestValidateExtraHeaders:
    """Tests for validate_extra_headers."""

    def test_accepts_valid_headers(self):
        headers = {"x-correlation-id": "abc-123", "X-Custom_Header.1": "", "x-tab": "a\tb"}
        assert validate_extra_headers(headers, "EXTRA_HEADERS") == headers

    @pytest.mark.parametrize(
        ("headers", "error"),
        [
            (["x-a"], "must be a mapping"),
            ({"bad header": "v"}, "invalid header name"),
            ({"": "v"}, "invalid header name"),
            ({1: "v"}, "invalid header name"),
            ({"AUTHORIZATION": "v"}, "reserved header"),
            ({"user-agent": "v"}, "reserved header"),
            ({"Content-Type": "v"}, "reserved header"),
            ({"Transfer-Encoding": "chunked"}, "reserved header"),
            ({"x-a": "1", "X-A": "2"}, "more than once"),
            ({"x-a": 1}, "must be a string"),
            ({"x-a": "v\nx-b: injected"}, "must be a string without control characters"),
            ({"x-a": "v\x0bv"}, "must be a string without control characters"),
            ({"x-a": "v\x1b[0m"}, "must be a string without control characters"),
            ({"x-a": "v\x7f"}, "must be a string without control characters"),
            ({"x-a": " secret-value"}, "must not start with whitespace"),
            ({"x-a": "\tsecret-value"}, "must not start with whitespace"),
        ],
        ids=[
            "not_mapping",
            "space_in_name",
            "empty_name",
            "non_string_name",
            "authorization_any_case",
            "user_agent",
            "content_type",
            "transfer_encoding",
            "case_insensitive_duplicate",
            "non_string_value",
            "newline_in_value",
            "vertical_tab_in_value",
            "escape_in_value",
            "delete_in_value",
            "leading_space_value",
            "leading_tab_value",
        ],
    )
    def test_rejects_invalid_headers(self, headers, error):
        with pytest.raises(InputError, match=error):
            validate_extra_headers(headers, "EXTRA_HEADERS")

    def test_leading_whitespace_error_names_header_without_value(self):
        with pytest.raises(InputError) as exc_info:
            validate_extra_headers({"x-api-key": " secret-value"}, "EXTRA_HEADERS")

        assert "x-api-key" in str(exc_info.value)
        assert "secret-value" not in str(exc_info.value)

    @pytest.mark.parametrize(
        "value",
        ["secret-value", "", "a b", "trailing ", " secret-value", "\tsecret-value", "\u00a0secret-value"],
        ids=["plain", "empty", "inner_space", "trailing_space", "leading_space", "leading_tab", "leading_nbsp"],
    )
    def test_accepts_only_values_requests_can_prepare(self, value):
        """Test validation accepts a value exactly when real Requests request preparation does (no request is sent)."""
        try:
            requests.Request("GET", "https://api.fabric.microsoft.com", headers={"x-api-key": value}).prepare()
            requests_accepts = True
        except requests.exceptions.InvalidHeader:
            requests_accepts = False

        try:
            validate_extra_headers({"x-api-key": value}, "EXTRA_HEADERS")
            validator_accepts = True
        except InputError:
            validator_accepts = False

        assert validator_accepts == requests_accepts
