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


def test_unpublish_feature_flag_warnings(mock_endpoint, caplog):
    """Test that warnings are logged when unpublish feature flags are missing."""
    import json
    import logging
    import tempfile
    from pathlib import Path
    from unittest.mock import MagicMock, patch

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create test items for each type that requires feature flags
        test_items = [
            ("TestLakehouse.Lakehouse", "Lakehouse", "test-lakehouse-id"),
            ("TestWarehouse.Warehouse", "Warehouse", "test-warehouse-id"),
            ("TestSQLDB.SQLDatabase", "SQLDatabase", "test-sqldb-id"),
            ("TestEventhouse.Eventhouse", "Eventhouse", "test-eventhouse-id"),
        ]

        for item_dir_name, item_type, logical_id in test_items:
            item_dir = temp_path / item_dir_name
            item_dir.mkdir(parents=True, exist_ok=True)

            platform_file = item_dir / ".platform"
            metadata = {
                "metadata": {
                    "type": item_type,
                    "displayName": item_dir_name.split(".")[0],
                    "description": f"Test {item_type}",
                },
                "config": {"logicalId": logical_id},
            }

            with platform_file.open("w", encoding="utf-8") as f:
                json.dump(metadata, f)

            with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
                f.write("Dummy file content")

        # Mock deployed items to simulate items exist in workspace
        deployed_items = {
            item_type: {item_dir_name.split(".")[0]: MagicMock()} for item_dir_name, item_type, _ in test_items
        }

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace,
                "_refresh_deployed_items",
                new=lambda self: setattr(self, "deployed_items", deployed_items),
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(
                FabricWorkspace,
                "_unpublish_folders",
                new=lambda _: None,  # Mock to avoid unrelated folder unpublish bug
            ),
            caplog.at_level(logging.WARNING),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Lakehouse", "Warehouse", "SQLDatabase", "Eventhouse"],
            )

            # Call unpublish_all_orphan_items without any feature flags enabled
            publish.unpublish_all_orphan_items(workspace)

            # Check that warnings were logged for each item type
            expected_warnings = [
                "Skipping unpublish for Lakehouse items because the 'enable_lakehouse_unpublish' feature flag is not enabled.",
                "Skipping unpublish for Warehouse items because the 'enable_warehouse_unpublish' feature flag is not enabled.",
                "Skipping unpublish for SQLDatabase items because the 'enable_sqldatabase_unpublish' feature flag is not enabled.",
                "Skipping unpublish for Eventhouse items because the 'enable_eventhouse_unpublish' feature flag is not enabled.",
            ]

            for expected_warning in expected_warnings:
                assert expected_warning in caplog.text


def test_unpublish_with_feature_flags_enabled(mock_endpoint, caplog):
    """Test that no warnings are logged when unpublish feature flags are enabled."""
    import json
    import logging
    import tempfile
    from pathlib import Path
    from unittest.mock import MagicMock, patch

    import fabric_cicd.constants as constants

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a test lakehouse
        lakehouse_dir = temp_path / "TestLakehouse.Lakehouse"
        lakehouse_dir.mkdir(parents=True, exist_ok=True)

        platform_file = lakehouse_dir / ".platform"
        metadata = {
            "metadata": {
                "type": "Lakehouse",
                "displayName": "TestLakehouse",
                "description": "Test Lakehouse",
            },
            "config": {"logicalId": "test-lakehouse-id"},
        }

        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f)

        with (lakehouse_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Mock deployed items
        deployed_items = {"Lakehouse": {"TestLakehouse": MagicMock()}}

        # Enable the lakehouse unpublish feature flag
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add("enable_lakehouse_unpublish")

        try:
            with (
                patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
                patch.object(
                    FabricWorkspace,
                    "_refresh_deployed_items",
                    new=lambda self: setattr(self, "deployed_items", deployed_items),
                ),
                patch.object(
                    FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
                ),
                patch.object(
                    FabricWorkspace,
                    "_unpublish_folders",
                    new=lambda _: None,  # Mock to avoid unrelated folder unpublish bug
                ),
                patch.object(
                    FabricWorkspace,
                    "_unpublish_item",
                    new=lambda _, __, ___: None,  # Mock unpublish to avoid actual API calls
                ),
                caplog.at_level(logging.WARNING),
            ):
                workspace = FabricWorkspace(
                    workspace_id="12345678-1234-5678-abcd-1234567890ab",
                    repository_directory=str(temp_path),
                    item_type_in_scope=["Lakehouse"],
                )

                # Call unpublish_all_orphan_items with feature flag enabled
                publish.unpublish_all_orphan_items(workspace)

                # Check that no feature flag warnings were logged
                assert "enable_lakehouse_unpublish" not in caplog.text
                assert "Skipping unpublish for Lakehouse" not in caplog.text

        finally:
            # Restore original feature flags
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)
