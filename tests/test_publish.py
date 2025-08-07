# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test publishing functionality including selective publishing based on repository content."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.publish as publish
from fabric_cicd._common._exceptions import InputError
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": [], "capacityId": "test-capacity"}}
    mock.upn_auth = True
    return mock


def test_publish_only_existing_item_types(mock_endpoint):
    """Test that publish_all_items only attempts to publish item types that exist in repository."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create only a Notebook item
        notebook_dir = temp_path / "TestNotebook.Notebook"
        notebook_dir.mkdir(parents=True, exist_ok=True)

        platform_file = notebook_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "Test Notebook",
                "description": "Test notebook",
            },
            "config": {"logicalId": "test-notebook-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Create workspace with default item_type_in_scope (None -> all types)
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch("fabric_cicd._items.publish_notebooks") as mock_publish_notebooks,
            patch("fabric_cicd._items.publish_environments") as mock_publish_environments,
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                # item_type_in_scope defaults to None -> all types
            )

            # Call publish_all_items
            publish.publish_all_items(workspace)

            # After publish_all_items, repository_items should be populated
            assert "Notebook" in workspace.repository_items
            assert "Environment" not in workspace.repository_items

            # Verify that only publish_notebooks was called
            mock_publish_notebooks.assert_called_once_with(workspace)
            mock_publish_environments.assert_not_called()


def test_default_none_item_type_in_scope_includes_all_types(mock_endpoint):
    """Test that when item_type_in_scope is None (default), all available item types are included."""

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
                # item_type_in_scope=None by default
            )

            # Should include all available item types
            import fabric_cicd.constants as constants

            expected_types = list(constants.ACCEPTED_ITEM_TYPES_UPN)
            assert set(workspace.item_type_in_scope) == set(expected_types)


def test_empty_item_type_in_scope_list(mock_endpoint):
    """Test that passing an empty item_type_in_scope list works (no items to process)."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=[],
            )
            # Verify that an empty list is accepted and stored correctly
            assert workspace.item_type_in_scope == []


def test_invalid_item_types_in_scope(mock_endpoint):
    """Test that passing invalid item types raises appropriate errors."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Test single invalid item type
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            pytest.raises(InputError, match="Invalid or unsupported item type: 'InvalidItemType'"),
        ):
            FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["InvalidItemType"],
            )


def test_multiple_invalid_item_types_in_scope(mock_endpoint):
    """Test that passing multiple invalid item types raises error for the first invalid one."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Test multiple invalid item types (should fail on first invalid one)
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            pytest.raises(InputError, match="Invalid or unsupported item type: 'FakeType'"),
        ):
            FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["FakeType", "AnotherInvalidType"],
            )


def test_mixed_valid_and_invalid_item_types_in_scope(mock_endpoint):
    """Test that passing a mix of valid and invalid item types raises error for the invalid one."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Test mix of valid and invalid item types (should fail on invalid one)
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            pytest.raises(InputError, match="Invalid or unsupported item type: 'BadType'"),
        ):
            FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook", "BadType", "Environment"],
            )
