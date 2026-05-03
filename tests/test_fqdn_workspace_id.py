# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

import fabric_cicd.constants as constants
from fabric_cicd.constants import _get_fabric_fqdn_url, configure_fabric_fqdn


class TestGetFabricFqdnUrl:
    """Tests for the _get_fabric_fqdn_url helper function."""

    def test_produces_correct_fqdn_url(self):
        url = _get_fabric_fqdn_url("f953f3da-c5f0-4e36-a644-c85933e35e2f")
        assert url == "https://f953f3dac5f04e36a644c85933e35e2f.zf9.w.api.fabric.microsoft.com"

    def test_rejects_workspace_id_without_dashes(self):
        with pytest.raises(ValueError, match="valid GUID with dashes"):
            _get_fabric_fqdn_url("f953f3dac5f04e36a644c85933e35e2f")


class TestConfigureFabricFqdn:
    """Tests for configure_fabric_fqdn."""

    def test_globals_updated(self, monkeypatch):
        monkeypatch.setattr(constants, "FABRIC_API_ROOT_URL", "https://api.fabric.microsoft.com")
        monkeypatch.setattr(constants, "DEFAULT_API_ROOT_URL", "https://api.powerbi.com")

        configure_fabric_fqdn("f953f3da-c5f0-4e36-a644-c85933e35e2f")

        expected = "https://f953f3dac5f04e36a644c85933e35e2f.zf9.w.api.fabric.microsoft.com"
        assert constants.FABRIC_API_ROOT_URL == expected
        assert constants.DEFAULT_API_ROOT_URL == expected

    def test_overwrite_warning_on_second_call(self, monkeypatch, mocker):
        monkeypatch.setattr(constants, "FABRIC_API_ROOT_URL", "https://api.fabric.microsoft.com")
        monkeypatch.setattr(constants, "DEFAULT_API_ROOT_URL", "https://api.powerbi.com")

        mock_logger = mocker.Mock()
        monkeypatch.setattr(constants, "logger", mock_logger)

        configure_fabric_fqdn("f953f3da-c5f0-4e36-a644-c85933e35e2f")
        mock_logger.warning.assert_not_called()

        configure_fabric_fqdn("f953f3da-c5f0-4e36-a644-c85933e35e2f")
        mock_logger.warning.assert_called_once()
