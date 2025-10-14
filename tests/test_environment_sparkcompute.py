# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for Sparkcompute.yml parameter replacement functionality."""

import tempfile
from pathlib import Path
from unittest import mock

from fabric_cicd._items._environment import _update_compute_settings


def test_sparkcompute_find_replace():
    """Test that find_replace works on Sparkcompute.yml."""
    # Create a temporary Sparkcompute.yml file
    with tempfile.TemporaryDirectory() as tmpdir:
        item_path = Path(tmpdir)
        setting_path = item_path / "Setting"
        setting_path.mkdir()
        sparkcompute_path = setting_path / "Sparkcompute.yml"

        # Create sample Sparkcompute.yml content
        sparkcompute_content = """enable_native_execution_engine: false
driver_cores: 4
driver_memory: 28g
executor_cores: 4
executor_memory: 28g
dynamic_executor_allocation:
  enabled: true
  min_executors: 1
  max_executors: 1
runtime_version: 1.3
"""
        sparkcompute_path.write_text(sparkcompute_content)

        # Mock FabricWorkspace object
        mock_workspace = mock.MagicMock()
        mock_workspace.environment = "TEST"
        mock_workspace.environment_parameter = {
            "find_replace": [
                {
                    "find_value": "4",
                    "replace_value": {"TEST": "8"},
                }
            ]
        }
        mock_workspace.repository_directory = Path(tmpdir)

        # Mock the endpoint invoke
        mock_workspace.endpoint.invoke.return_value = None
        mock_workspace.base_api_url = "https://api.fabric.microsoft.com/v1"

        # Call the function
        _update_compute_settings(mock_workspace, item_path, "test-guid", "test-item")

        # Verify the endpoint was called
        assert mock_workspace.endpoint.invoke.called
        call_args = mock_workspace.endpoint.invoke.call_args
        body = call_args[1]["body"]

        # Verify that driver_cores and executor_cores were replaced with 8
        assert body["driverCores"] == 8
        assert body["executorCores"] == 8


def test_sparkcompute_key_value_replace():
    """Test that key_value_replace works on Sparkcompute.yml."""
    # Create a temporary Sparkcompute.yml file
    with tempfile.TemporaryDirectory() as tmpdir:
        item_path = Path(tmpdir)
        setting_path = item_path / "Setting"
        setting_path.mkdir()
        sparkcompute_path = setting_path / "Sparkcompute.yml"

        # Create sample Sparkcompute.yml content
        sparkcompute_content = """enable_native_execution_engine: false
driver_cores: 4
driver_memory: 28g
executor_cores: 4
executor_memory: 28g
dynamic_executor_allocation:
  enabled: true
  min_executors: 1
  max_executors: 1
runtime_version: 1.3
"""
        sparkcompute_path.write_text(sparkcompute_content)

        # Mock FabricWorkspace object
        mock_workspace = mock.MagicMock()
        mock_workspace.environment = "TEST"
        mock_workspace.environment_parameter = {
            "key_value_replace": [
                {
                    "find_key": "$.driver_cores",
                    "replace_value": {"TEST": 8},
                }
            ]
        }
        mock_workspace.repository_directory = Path(tmpdir)

        # Mock the endpoint invoke
        mock_workspace.endpoint.invoke.return_value = None
        mock_workspace.base_api_url = "https://api.fabric.microsoft.com/v1"

        # Call the function
        _update_compute_settings(mock_workspace, item_path, "test-guid", "test-item")

        # Verify the endpoint was called
        assert mock_workspace.endpoint.invoke.called
        call_args = mock_workspace.endpoint.invoke.call_args
        body = call_args[1]["body"]

        # Verify that driver_cores was replaced with 8
        assert body["driverCores"] == 8


def test_sparkcompute_with_file_path_filter():
    """Test that file_path filter works for Sparkcompute.yml."""
    # Create a temporary Sparkcompute.yml file
    with tempfile.TemporaryDirectory() as tmpdir:
        item_path = Path(tmpdir)
        setting_path = item_path / "Setting"
        setting_path.mkdir()
        sparkcompute_path = setting_path / "Sparkcompute.yml"

        # Create sample Sparkcompute.yml content
        sparkcompute_content = """enable_native_execution_engine: false
driver_cores: 4
driver_memory: 28g
executor_cores: 4
executor_memory: 28g
dynamic_executor_allocation:
  enabled: true
  min_executors: 1
  max_executors: 1
runtime_version: 1.3
"""
        sparkcompute_path.write_text(sparkcompute_content)

        # Mock FabricWorkspace object
        mock_workspace = mock.MagicMock()
        mock_workspace.environment = "TEST"
        mock_workspace.environment_parameter = {
            "key_value_replace": [
                {
                    "find_key": "$.driver_cores",
                    "replace_value": {"TEST": 8},
                    "file_path": "**/Setting/Sparkcompute.yml",
                }
            ]
        }
        mock_workspace.repository_directory = Path(tmpdir)

        # Mock the endpoint invoke
        mock_workspace.endpoint.invoke.return_value = None
        mock_workspace.base_api_url = "https://api.fabric.microsoft.com/v1"

        # Call the function
        _update_compute_settings(mock_workspace, item_path, "test-guid", "test-item")

        # Verify the endpoint was called
        assert mock_workspace.endpoint.invoke.called
        call_args = mock_workspace.endpoint.invoke.call_args
        body = call_args[1]["body"]

        # Verify that driver_cores was replaced with 8
        assert body["driverCores"] == 8
