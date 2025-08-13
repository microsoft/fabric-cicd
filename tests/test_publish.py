# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test publishing functionality including selective publishing based on repository content."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.publish as publish
from fabric_cicd._common._deployment_log_entry import DeploymentLogEntry
from fabric_cicd._common._exceptions import InputError
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": [], "capacityId": "test-capacity"}, "header": {}}
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

            expected_types = list(constants.ACCEPTED_ITEM_TYPES)
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


def test_publish_all_items_deployment_log_entries(mock_endpoint):
    """Test that publish_all_items returns list of DeploymentLogEntry objects."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a notebook with proper .platform metadata
        notebook_dir = temp_path / "Notebook_1.Notebook"
        notebook_dir.mkdir(parents=True)
        (notebook_dir / "notebook-content.py").write_text("print('test')")

        platform_file = notebook_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "Notebook_1",
                "description": "Test notebook",
            },
            "config": {"logicalId": "notebook-1-id"},
        }
        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        # Mock response with proper id field for successful publish
        mock_endpoint.invoke.return_value = {
            "body": {"value": [], "capacityId": "test-capacity", "id": "test-item-id"},
            "header": {},
        }

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],  # Only test notebooks to avoid complexity
            )

            # Test publish_all_items returns list of DeploymentLogEntry objects
            result = publish.publish_all_items(workspace)

            # Verify return type
            assert isinstance(result, list)
            assert len(result) >= 1  # Should have at least one entry

            # Verify all entries are DeploymentLogEntry objects
            for entry in result:
                assert isinstance(entry, DeploymentLogEntry)
                assert entry.operation_type == "publish"
                assert entry.name == "Notebook_1"
                assert entry.item_type == "Notebook"
                assert isinstance(entry.success, bool)
                assert entry.start_time is not None
                assert entry.end_time is not None
                assert entry.duration_seconds >= 0


def test_unpublish_all_orphan_items_deployment_log_entries(mock_endpoint):
    """Test that unpublish_all_orphan_items returns list of DeploymentLogEntry objects."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a notebook in the repository (so it's not orphaned)
        (temp_path / "Notebook_1.Notebook").mkdir(parents=True)
        (temp_path / "Notebook_1.Notebook" / "notebook-content.py").write_text("print('test')")

        # Mock deployed items - include orphaned items that aren't in repository
        deployed_items = [
            {
                "displayName": "Notebook_1",
                "type": "Notebook",
                "id": "11111111-1111-1111-1111-111111111111",
                "description": "Test notebook 1",
            },
            {
                "displayName": "OrphanedNotebook",
                "type": "Notebook",
                "id": "22222222-2222-2222-2222-222222222222",
                "description": "Orphaned notebook",
            },
            {
                "displayName": "OrphanedEnvironment",
                "type": "Environment",
                "id": "33333333-3333-3333-3333-333333333333",
                "description": "Orphaned environment",
            },
        ]

        # Override the default return value for this test
        mock_endpoint.invoke.return_value = {"body": {"value": deployed_items, "capacityId": "test-capacity"}}

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook", "Environment"],
            )

            # Test unpublish_all_orphan_items returns list of DeploymentLogEntry objects
            result = publish.unpublish_all_orphan_items(workspace)

            # Verify return type - it might include more items due to test setup
            assert isinstance(result, list)
            assert len(result) >= 2  # Should have at least entries for orphaned items

            # Verify all entries are DeploymentLogEntry objects
            for entry in result:
                assert isinstance(entry, DeploymentLogEntry)
                assert entry.operation_type == "unpublish"
                assert entry.name in ["Notebook_1", "OrphanedNotebook", "OrphanedEnvironment"]
                assert entry.item_type in ["Notebook", "Environment"]
                assert isinstance(entry.success, bool)
                assert entry.start_time is not None
                assert entry.end_time is not None
                assert entry.duration_seconds >= 0


def test_unpublish_all_orphan_items_returns_empty_list(mock_endpoint):
    """Test that unpublish_all_orphan_items returns empty list when no orphans exist."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a notebook in the repository
        notebook_dir = temp_path / "Notebook_1.Notebook"
        notebook_dir.mkdir(parents=True)
        (notebook_dir / "notebook-content.py").write_text("print('test')")

        platform_file = notebook_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "Notebook_1",
                "description": "Test notebook",
            },
            "config": {"logicalId": "notebook-1-id"},
        }
        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        # Mock deployed items - only items that exist in repository
        deployed_items = [
            {
                "displayName": "Notebook_1",
                "type": "Notebook",
                "id": "11111111-1111-1111-1111-111111111111",
                "description": "Test notebook",
            }
        ]

        mock_endpoint.invoke.return_value = {
            "body": {"value": deployed_items, "capacityId": "test-capacity"},
            "header": {},
        }

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],
            )

            # Test unpublish_all_orphan_items returns empty list
            result = publish.unpublish_all_orphan_items(workspace)

            # Verify return type
            assert isinstance(result, list)
            assert len(result) == 0
