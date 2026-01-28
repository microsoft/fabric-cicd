# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for semantic model deployment with destructive changes and refresh functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd import constants
from fabric_cicd._items._semanticmodel import (
    _is_destructive_change_error,
    _publish_semanticmodel_with_retry,
    _refresh_semanticmodel,
    _refresh_semanticmodels_if_configured,
    publish_semanticmodels,
)
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()

    def mock_invoke(method, url, **_kwargs):
        if method == "GET" and "workspaces" in url and not url.endswith("/items"):
            return {"body": {"value": [], "capacityId": "test-capacity"}}
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}
        if method == "POST" and url.endswith("/folders"):
            return {"body": {"id": "mock-folder-id"}}
        if method == "POST" and url.endswith("/items"):
            return {"body": {"id": "mock-item-id", "workspaceId": "mock-workspace-id"}}
        if method == "POST" and "refreshes" in url:
            return {"body": {}, "status_code": 202}
        return {"body": {"value": [], "capacityId": "test-capacity"}}

    mock.invoke.side_effect = mock_invoke
    mock.upn_auth = True
    return mock


@pytest.fixture(autouse=True)
def clear_feature_flags():
    """Clear feature flags before and after each test."""
    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.clear()
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


def test_is_destructive_change_error_with_error_code():
    """Test detection of destructive change errors by error code."""
    assert _is_destructive_change_error("Some error message", "Alm_InvalidRequest_PurgeRequired")
    assert _is_destructive_change_error("Some error message", "PurgeRequired")
    assert not _is_destructive_change_error("Some error message", "OtherErrorCode")


def test_is_destructive_change_error_with_keywords():
    """Test detection of destructive change errors by keywords in message."""
    assert _is_destructive_change_error("Dataset changes will cause loss of data and purge required")
    assert _is_destructive_change_error("This operation requires data to be dropped")
    assert _is_destructive_change_error("Destructive change detected")
    assert _is_destructive_change_error("Data deletion is required")
    assert not _is_destructive_change_error("Normal deployment error")
    assert not _is_destructive_change_error("Invalid request")


def test_is_destructive_change_error_case_insensitive():
    """Test that destructive change detection is case-insensitive."""
    assert _is_destructive_change_error("PURGE REQUIRED for this update")
    assert _is_destructive_change_error("Will Cause LOSS OF DATA")


def test_is_destructive_change_error_no_message():
    """Test handling of None or empty error messages."""
    assert not _is_destructive_change_error(None)
    assert not _is_destructive_change_error("")
    # Test non-string types
    assert not _is_destructive_change_error(123)  # type: ignore
    assert not _is_destructive_change_error([])  # type: ignore
    assert not _is_destructive_change_error({})  # type: ignore


def test_publish_semanticmodel_with_retry_success(mock_endpoint):
    """Test successful semantic model deployment without retry."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Refresh repository items to populate the workspace
            workspace._refresh_repository_items()

            # Should not raise any exception
            _publish_semanticmodel_with_retry(
                fabric_workspace_obj=workspace,
                item_name="Test Model",
                item_type="SemanticModel",
                exclude_path=r".*\.pbi[/\\].*",
            )


def test_publish_semanticmodel_with_retry_destructive_error():
    """Test semantic model deployment with destructive change error."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        # Mock endpoint to raise destructive change error
        mock_endpoint_with_error = MagicMock()

        def mock_invoke_with_error(method, url, **_kwargs):
            if method == "POST" and "/items" in url and "/updateDefinition" not in url:
                error_msg = "Alm_InvalidRequest_PurgeRequired - Dataset changes will cause loss of data"
                raise Exception(error_msg)
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            return {"body": {"value": []}}

        mock_endpoint_with_error.invoke.side_effect = mock_invoke_with_error
        mock_endpoint_with_error.upn_auth = True

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint_with_error),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Refresh repository items to populate the workspace
            workspace._refresh_repository_items()

            # Should raise the exception after logging warnings
            with pytest.raises(Exception, match="Alm_InvalidRequest_PurgeRequired"):
                _publish_semanticmodel_with_retry(
                    fabric_workspace_obj=workspace,
                    item_name="Test Model",
                    item_type="SemanticModel",
                    exclude_path=r".*\.pbi[/\\].*",
                )


def test_refresh_semanticmodel_default_payload(mock_endpoint):
    """Test semantic model refresh with default payload."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Should not raise any exception
            _refresh_semanticmodel(
                fabric_workspace_obj=workspace,
                model_name="Test Model",
                model_id="test-model-guid",
                custom_payload=None,
            )


def test_refresh_semanticmodel_custom_payload(mock_endpoint):
    """Test semantic model refresh with custom payload."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        custom_payload = {
            "type": "full",
            "objects": [{"table": "Sales"}, {"table": "Products", "partition": "Products-2024"}],
            "commitMode": "transactional",
        }

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Should not raise any exception
            _refresh_semanticmodel(
                fabric_workspace_obj=workspace,
                model_name="Test Model",
                model_id="test-model-guid",
                custom_payload=custom_payload,
            )


def test_refresh_semanticmodels_if_configured_no_config(mock_endpoint):
    """Test that refresh is skipped when not configured."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # No environment_parameter for refresh
            workspace.environment_parameter = {}

            # Should not raise any exception and should not attempt refresh
            _refresh_semanticmodels_if_configured(workspace)


def test_refresh_semanticmodels_if_configured_single_model(mock_endpoint):
    """Test semantic model refresh with single model configuration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Set up repository items with a GUID
            workspace._refresh_repository_items()
            workspace.repository_items["SemanticModel"]["Test Model"].guid = "test-model-guid"

            # Configure refresh
            workspace.environment_parameter = {
                "semantic_model_refresh": {"semantic_model_name": "Test Model", "refresh_payload": {"type": "full"}}
            }

            # Enable the refresh feature flag
            constants.FEATURE_FLAG.add("enable_semantic_model_refresh")

            # Should not raise any exception
            _refresh_semanticmodels_if_configured(workspace)


def test_refresh_semanticmodels_if_configured_multiple_models(mock_endpoint):
    """Test semantic model refresh with multiple models configuration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create two semantic model items
        for model_name in ["Model1", "Model2"]:
            model_dir = temp_path / f"{model_name}.SemanticModel"
            model_dir.mkdir(parents=True, exist_ok=True)

            platform_file = model_dir / ".platform"
            metadata = {
                "metadata": {
                    "type": "SemanticModel",
                    "displayName": model_name,
                    "description": f"Test {model_name}",
                },
                "config": {"logicalId": f"{model_name.lower()}-id"},
            }

            with platform_file.open("w", encoding="utf-8") as f:
                json.dump(metadata, f)

            with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
                f.write(f'{{"name": "{model_name}"}}')

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Set up repository items with GUIDs
            workspace._refresh_repository_items()
            workspace.repository_items["SemanticModel"]["Model1"].guid = "model1-guid"
            workspace.repository_items["SemanticModel"]["Model2"].guid = "model2-guid"

            # Configure refresh for multiple models
            workspace.environment_parameter = {
                "semantic_model_refresh": [
                    {"semantic_model_name": "Model1", "refresh_payload": {"type": "full"}},
                    {"semantic_model_name": ["Model2"], "refresh_payload": None},
                ]
            }

            # Enable the refresh feature flag
            constants.FEATURE_FLAG.add("enable_semantic_model_refresh")

            # Should not raise any exception
            _refresh_semanticmodels_if_configured(workspace)


def test_publish_semanticmodels_with_refresh(mock_endpoint):
    """Test full semantic model publishing with refresh configuration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            # Set up repository items with a GUID
            workspace._refresh_repository_items()
            workspace.repository_items["SemanticModel"]["Test Model"].guid = "test-model-guid"

            # Configure refresh
            workspace.environment_parameter = {
                "semantic_model_refresh": {"semantic_model_name": "Test Model", "refresh_payload": None}
            }

            # Enable the refresh feature flag
            constants.FEATURE_FLAG.add("enable_semantic_model_refresh")

            # Should not raise any exception
            publish_semanticmodels(workspace)


def test_refresh_disabled_without_feature_flag(mock_endpoint):
    """Test that refresh is skipped when feature flag is not enabled."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            workspace._refresh_repository_items()
            workspace.repository_items["SemanticModel"]["Test Model"].guid = "test-model-guid"

            # Configure refresh but DON'T enable the feature flag
            workspace.environment_parameter = {
                "semantic_model_refresh": {"semantic_model_name": "Test Model", "refresh_payload": None}
            }

            # Should not call refresh because feature flag is not enabled
            _refresh_semanticmodels_if_configured(workspace)

            # Verify no refresh was attempted (would have logged if attempted)


def test_destructive_change_detection_with_feature_flag():
    """Test destructive change detection when feature flag is enabled."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        # Mock endpoint to raise destructive change error
        mock_endpoint_with_error = MagicMock()

        def mock_invoke_with_error(method, url, **_kwargs):
            if method == "POST" and "/items" in url and "/updateDefinition" not in url:
                error_msg = "Alm_InvalidRequest_PurgeRequired - Dataset changes will cause loss of data"
                raise Exception(error_msg)
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            return {"body": {"value": []}}

        mock_endpoint_with_error.invoke.side_effect = mock_invoke_with_error
        mock_endpoint_with_error.upn_auth = True

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint_with_error),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            workspace._refresh_repository_items()

            # Enable the destructive change detection feature flag
            constants.FEATURE_FLAG.add("enable_semantic_model_destructive_change_detection")

            # Should raise with guidance when feature flag is enabled
            with pytest.raises(Exception, match="Alm_InvalidRequest_PurgeRequired"):
                publish_semanticmodels(workspace)


def test_destructive_change_detection_without_feature_flag():
    """Test that destructive change detection is skipped when feature flag is not enabled."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a semantic model item
        model_dir = temp_path / "TestModel.SemanticModel"
        model_dir.mkdir(parents=True, exist_ok=True)

        platform_file = model_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Test Model",
                "description": "Test semantic model",
            },
            "config": {"logicalId": "test-model-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (model_dir / "model.bim").open("w", encoding="utf-8") as f:
            f.write('{"name": "TestModel"}')

        # Mock endpoint to raise destructive change error
        mock_endpoint_with_error = MagicMock()

        def mock_invoke_with_error(method, url, **_kwargs):
            if method == "POST" and "/items" in url and "/updateDefinition" not in url:
                error_msg = "Alm_InvalidRequest_PurgeRequired - Dataset changes will cause loss of data"
                raise Exception(error_msg)
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            return {"body": {"value": []}}

        mock_endpoint_with_error.invoke.side_effect = mock_invoke_with_error
        mock_endpoint_with_error.upn_auth = True

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint_with_error),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["SemanticModel"],
            )

            workspace._refresh_repository_items()

            # DON'T enable the feature flag - should use standard deployment
            # Should raise exception but WITHOUT guidance messages
            with pytest.raises(Exception, match="Alm_InvalidRequest_PurgeRequired"):
                publish_semanticmodels(workspace)
