# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test publishing functionality including selective publishing based on repository content."""

import json
import logging
import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.publish as publish
from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._items._notebook import NotebookPublisher
from fabric_cicd.constants import API_FORMAT_MAPPING, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

# =============================================================================
# Shared Fixtures and Helpers
# =============================================================================


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
        return {"body": {"value": [], "capacityId": "test-capacity"}}

    mock.invoke.side_effect = mock_invoke
    mock.upn_auth = True
    return mock


@pytest.fixture
def temp_workspace_dir():
    """Create a temporary directory for test workspaces."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def experimental_feature_flags():
    """Enable experimental feature flags for tests."""
    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.add("enable_experimental_features")
    constants.FEATURE_FLAG.add("enable_exclude_folder")
    constants.FEATURE_FLAG.add("enable_include_folder")
    constants.FEATURE_FLAG.add("enable_items_to_include")
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


def create_test_item(base_path: Path, folder: Optional[str], name: str, item_type: str, logical_id: str) -> Path:
    """Helper to create a test item with .platform file.

    Args:
        base_path: Root directory for the workspace.
        folder: Subfolder path (e.g., "legacy" or "projects/team1") or None for root-level.
        name: Display name of the item.
        item_type: Type of the item (e.g., "Notebook", "SemanticModel").
        logical_id: Logical ID for the item.

    Returns:
        Path to the created item directory.
    """
    item_dir = base_path / folder / f"{name}.{item_type}" if folder else base_path / f"{name}.{item_type}"

    item_dir.mkdir(parents=True, exist_ok=True)

    platform_file = item_dir / ".platform"
    metadata = {
        "metadata": {
            "type": item_type,
            "displayName": name,
            "description": f"Test {item_type}",
        },
        "config": {"logicalId": logical_id},
    }

    with platform_file.open("w", encoding="utf-8") as f:
        json.dump(metadata, f)

    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    return item_dir


# =============================================================================
# Basic Publishing Tests
# =============================================================================


def test_publish_only_existing_item_types(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items only attempts to publish item types that exist in repository."""
    create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "test-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._notebook.NotebookPublisher") as mock_notebook_cls,
        patch("fabric_cicd._items._environment.EnvironmentPublisher") as mock_env_cls,
    ):
        mock_notebook_instance = mock_notebook_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
        )

        publish.publish_all_items(workspace)

        assert "Notebook" in workspace.repository_items
        assert "Environment" not in workspace.repository_items

        mock_notebook_cls.assert_called_once_with(workspace)
        mock_notebook_instance.publish_all.assert_called_once()
        mock_env_cls.assert_not_called()


def test_default_none_item_type_in_scope_includes_all_types(mock_endpoint, temp_workspace_dir):
    """Test that when item_type_in_scope is None (default), all available item types are included."""
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
        )

        expected_types = list(constants.ACCEPTED_ITEM_TYPES)
        assert set(workspace.item_type_in_scope) == set(expected_types)


def test_empty_item_type_in_scope_list(mock_endpoint, temp_workspace_dir):
    """Test that passing an empty item_type_in_scope list works (no items to process)."""
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=[],
        )
        assert workspace.item_type_in_scope == []


# =============================================================================
# Invalid Item Type Tests
# =============================================================================


def test_invalid_item_types_in_scope(mock_endpoint, temp_workspace_dir):
    """Test that passing invalid item types raises appropriate errors."""
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        pytest.raises(InputError, match="Invalid or unsupported item type: 'InvalidItemType'"),
    ):
        FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["InvalidItemType"],
        )


def test_multiple_invalid_item_types_in_scope(mock_endpoint, temp_workspace_dir):
    """Test that passing multiple invalid item types raises error for the first invalid one."""
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        pytest.raises(InputError, match="Invalid or unsupported item type: 'FakeType'"),
    ):
        FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["FakeType", "AnotherInvalidType"],
        )


def test_mixed_valid_and_invalid_item_types_in_scope(mock_endpoint, temp_workspace_dir):
    """Test that passing a mix of valid and invalid item types raises error for the invalid one."""
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        pytest.raises(InputError, match="Invalid or unsupported item type: 'BadType'"),
    ):
        FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook", "BadType", "Environment"],
        )


# =============================================================================
# Unpublish Feature Flag Tests
# =============================================================================


def test_unpublish_feature_flag_warnings(mock_endpoint, temp_workspace_dir, caplog):
    """Test that warnings are logged when unpublish feature flags are missing."""
    test_items = [
        ("legacy", "TestLakehouse", "Lakehouse", "test-lakehouse-id"),
        ("legacy", "TestWarehouse", "Warehouse", "test-warehouse-id"),
        ("legacy", "TestSQLDB", "SQLDatabase", "test-sqldb-id"),
        ("legacy", "TestEventhouse", "Eventhouse", "test-eventhouse-id"),
    ]

    for folder, name, item_type, logical_id in test_items:
        create_test_item(temp_workspace_dir, folder, name, item_type, logical_id)

    deployed_items = {item_type: {name: MagicMock()} for _, name, item_type, _ in test_items}

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
        patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
        caplog.at_level(logging.WARNING),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Lakehouse", "Warehouse", "SQLDatabase", "Eventhouse"],
        )

        publish.unpublish_all_orphan_items(workspace)

        expected_warnings = [
            "Skipping unpublish for Lakehouse items because the 'enable_lakehouse_unpublish' feature flag is not enabled.",
            "Skipping unpublish for Warehouse items because the 'enable_warehouse_unpublish' feature flag is not enabled.",
            "Skipping unpublish for SQLDatabase items because the 'enable_sqldatabase_unpublish' feature flag is not enabled.",
            "Skipping unpublish for Eventhouse items because the 'enable_eventhouse_unpublish' feature flag is not enabled.",
        ]

        for expected_warning in expected_warnings:
            assert expected_warning in caplog.text


def test_unpublish_with_feature_flags_enabled(mock_endpoint, temp_workspace_dir, caplog):
    """Test that no warnings are logged when unpublish feature flags are enabled."""
    create_test_item(temp_workspace_dir, None, "TestLakehouse", "Lakehouse", "test-lakehouse-id")

    deployed_items = {"Lakehouse": {"TestLakehouse": MagicMock()}}

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
            patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
            patch.object(FabricWorkspace, "_unpublish_item", new=lambda _, __, ___: None),
            caplog.at_level(logging.WARNING),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Lakehouse"],
            )

            publish.unpublish_all_orphan_items(workspace)

            assert "enable_lakehouse_unpublish" not in caplog.text
            assert "Skipping unpublish for Lakehouse" not in caplog.text

    finally:
        constants.FEATURE_FLAG.clear()
        constants.FEATURE_FLAG.update(original_flags)


def test_unpublish_orphan_item_is_deleted(mock_endpoint, temp_workspace_dir):
    """Test that unpublish_all_orphan_items deletes an orphaned item not in the repository."""
    create_test_item(temp_workspace_dir, None, "KeepMe", "Notebook", "keep-me-id")

    orphan_deployed = {
        "Notebook": {
            "KeepMe": MagicMock(guid="keep-guid"),
            "OrphanNotebook": MagicMock(guid="orphan-guid-123"),
        }
    }
    orphan_repo = {"Notebook": {"KeepMe": MagicMock()}}

    unpublish_calls = []

    def track_unpublish(_self, item_name, item_type):
        unpublish_calls.append((item_name, item_type))

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace,
            "_refresh_deployed_items",
            new=lambda self: setattr(self, "deployed_items", orphan_deployed),
        ),
        patch.object(
            FabricWorkspace,
            "_refresh_repository_items",
            new=lambda self: setattr(self, "repository_items", orphan_repo),
        ),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
        patch.object(FabricWorkspace, "_unpublish_item", new=track_unpublish),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.unpublish_all_orphan_items(workspace)

        assert len(unpublish_calls) == 1
        assert unpublish_calls[0] == ("OrphanNotebook", "Notebook")


def test_unpublish_orphan_excluded_by_regex(mock_endpoint, temp_workspace_dir):
    """Test that orphaned items matching the exclude regex are NOT unpublished."""
    create_test_item(temp_workspace_dir, None, "KeepMe", "Notebook", "keep-me-id")

    orphan_deployed = {
        "Notebook": {
            "KeepMe": MagicMock(guid="keep-guid"),
            "ProtectedOrphan": MagicMock(guid="protected-guid"),
            "DeleteMe": MagicMock(guid="delete-guid"),
        }
    }
    orphan_repo = {"Notebook": {"KeepMe": MagicMock()}}

    unpublish_calls = []

    def track_unpublish(_self, item_name, item_type):
        unpublish_calls.append((item_name, item_type))

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace,
            "_refresh_deployed_items",
            new=lambda self: setattr(self, "deployed_items", orphan_deployed),
        ),
        patch.object(
            FabricWorkspace,
            "_refresh_repository_items",
            new=lambda self: setattr(self, "repository_items", orphan_repo),
        ),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
        patch.object(FabricWorkspace, "_unpublish_item", new=track_unpublish),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.unpublish_all_orphan_items(workspace, item_name_exclude_regex=r"^Protected.*")

        assert ("DeleteMe", "Notebook") in unpublish_calls
        assert ("ProtectedOrphan", "Notebook") not in unpublish_calls


@pytest.mark.usefixtures("experimental_feature_flags")
def test_unpublish_orphan_filtered_by_items_to_include(mock_endpoint, temp_workspace_dir):
    """Test that items_to_include limits which orphaned items are unpublished."""
    create_test_item(temp_workspace_dir, None, "KeepMe", "Notebook", "keep-me-id")

    orphan_deployed = {
        "Notebook": {
            "KeepMe": MagicMock(guid="keep-guid"),
            "TargetOrphan": MagicMock(guid="target-guid"),
            "OtherOrphan": MagicMock(guid="other-guid"),
        }
    }
    orphan_repo = {"Notebook": {"KeepMe": MagicMock()}}

    unpublish_calls = []

    def track_unpublish(_self, item_name, item_type):
        unpublish_calls.append((item_name, item_type))

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace,
            "_refresh_deployed_items",
            new=lambda self: setattr(self, "deployed_items", orphan_deployed),
        ),
        patch.object(
            FabricWorkspace,
            "_refresh_repository_items",
            new=lambda self: setattr(self, "repository_items", orphan_repo),
        ),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
        patch.object(FabricWorkspace, "_unpublish_item", new=track_unpublish),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.unpublish_all_orphan_items(workspace, items_to_include=["TargetOrphan.Notebook"])

        assert ("TargetOrphan", "Notebook") in unpublish_calls
        assert ("OtherOrphan", "Notebook") not in unpublish_calls


def test_unpublish_no_orphans_no_deletion(mock_endpoint, temp_workspace_dir):
    """Test that unpublish_all_orphan_items does not call _unpublish_item when there are no orphans."""
    create_test_item(temp_workspace_dir, None, "MyNotebook", "Notebook", "my-notebook-id")

    matching_items = {"Notebook": {"MyNotebook": MagicMock(guid="my-guid")}}

    unpublish_calls = []

    def track_unpublish(_self, item_name, item_type):
        unpublish_calls.append((item_name, item_type))

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace,
            "_refresh_deployed_items",
            new=lambda self: setattr(self, "deployed_items", matching_items),
        ),
        patch.object(
            FabricWorkspace,
            "_refresh_repository_items",
            new=lambda self: setattr(self, "repository_items", matching_items),
        ),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
        patch.object(FabricWorkspace, "_unpublish_item", new=track_unpublish),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.unpublish_all_orphan_items(workspace)

        assert len(unpublish_calls) == 0


# =============================================================================
# Publishing Order Tests
# =============================================================================


def test_mirrored_database_published_before_lakehouse(mock_endpoint, temp_workspace_dir):
    """Test that MirroredDatabase items are published before Lakehouse items to enable shortcuts."""
    call_order = []

    def mock_publish_lakehouses():
        call_order.append("Lakehouse")

    def mock_publish_mirroreddatabase():
        call_order.append("MirroredDatabase")

    create_test_item(temp_workspace_dir, None, "TestLakehouse", "Lakehouse", "test-lakehouse-id")
    create_test_item(temp_workspace_dir, None, "TestMirroredDB", "MirroredDatabase", "test-mirrored-db-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._lakehouse.LakehousePublisher") as mock_lakehouse_cls,
        patch("fabric_cicd._items._mirroreddatabase.MirroredDatabasePublisher") as mock_mirrored_cls,
    ):
        mock_lakehouse_instance = mock_lakehouse_cls.return_value
        mock_lakehouse_instance.publish_all.side_effect = mock_publish_lakehouses
        mock_mirrored_instance = mock_mirrored_cls.return_value
        mock_mirrored_instance.publish_all.side_effect = mock_publish_mirroreddatabase

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Lakehouse", "MirroredDatabase"],
        )

        publish.publish_all_items(workspace)

        assert len(call_order) == 2
        assert "MirroredDatabase" in call_order
        assert "Lakehouse" in call_order

        mirrored_db_index = call_order.index("MirroredDatabase")
        lakehouse_index = call_order.index("Lakehouse")
        assert mirrored_db_index < lakehouse_index, (
            f"MirroredDatabase should be published before Lakehouse, but got order: {call_order}"
        )


# =============================================================================
# Folder Exclusion Tests
# =============================================================================


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_exclusion_with_regex(mock_endpoint, temp_workspace_dir):
    """Test that folder_path_exclude_regex can exclude entire folders of items."""
    create_test_item(temp_workspace_dir, "legacy", "LegacyNotebook", "Notebook", "legacy-notebook-id")
    create_test_item(temp_workspace_dir, "legacy", "LegacyModel", "SemanticModel", "legacy-model-id")
    create_test_item(temp_workspace_dir, "current", "CurrentNotebook", "Notebook", "current-notebook-id")
    create_test_item(temp_workspace_dir, None, "RootNotebook", "Notebook", "root-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook", "SemanticModel"],
        )

        exclude_regex = r".*legacy.*"
        publish.publish_all_items(workspace, folder_path_exclude_regex=exclude_regex)

        assert "Notebook" in workspace.repository_items
        assert "SemanticModel" in workspace.repository_items

        assert workspace.repository_items["Notebook"]["LegacyNotebook"].skip_publish is True
        assert workspace.repository_items["SemanticModel"]["LegacyModel"].skip_publish is True

        assert workspace.repository_items["Notebook"]["CurrentNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["RootNotebook"].skip_publish is False


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_exclusion_with_anchored_regex(mock_endpoint, temp_workspace_dir):
    """Test that excluding a parent folder with an anchored regex also excludes
    items in child folders, preserving consistent hierarchy behavior."""
    create_test_item(temp_workspace_dir, "legacy", "LegacyNotebook", "Notebook", "legacy-notebook-id")
    create_test_item(temp_workspace_dir, "legacy/archived", "ArchivedNotebook", "Notebook", "archived-notebook-id")
    create_test_item(temp_workspace_dir, "current", "CurrentNotebook", "Notebook", "current-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        exclude_regex = r"^/legacy$"
        publish.publish_all_items(workspace, folder_path_exclude_regex=exclude_regex)

        assert workspace.repository_items["Notebook"]["LegacyNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["ArchivedNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["CurrentNotebook"].skip_publish is False


def test_item_name_exclusion_still_works(mock_endpoint, temp_workspace_dir):
    """Test that existing item name exclusion still works with the new folder exclusion feature."""
    create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "test-notebook-id")
    create_test_item(temp_workspace_dir, None, "DoNotPublish", "Notebook", "excluded-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        exclude_regex = r".*DoNotPublish.*"
        publish.publish_all_items(workspace, item_name_exclude_regex=exclude_regex)

        assert workspace.repository_items["Notebook"]["DoNotPublish"].skip_publish is True
        assert workspace.repository_items["Notebook"]["TestNotebook"].skip_publish is False


# =============================================================================
# Folder Inclusion Tests
# =============================================================================


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_inclusion_with_folder_path_to_include(mock_endpoint, temp_workspace_dir):
    """Test that folder_path_to_include only filters items found within a Fabric folder."""
    create_test_item(temp_workspace_dir, "active", "ActiveNotebook", "Notebook", "active-notebook-id")
    create_test_item(temp_workspace_dir, "active", "ActiveModel", "SemanticModel", "active-model-id")
    create_test_item(temp_workspace_dir, "archive", "ArchivedNotebook", "Notebook", "archived-notebook-id")
    create_test_item(temp_workspace_dir, None, "RootNotebook", "Notebook", "root-notebook-id")
    create_test_item(temp_workspace_dir, "projects", "ProjectNotebook", "Notebook", "projects-notebook-id")
    create_test_item(temp_workspace_dir, "projects/team1", "NestedNotebook", "Notebook", "nested-notebook-id")
    create_test_item(temp_workspace_dir, "dept", "DeptNotebook", "Notebook", "dept-notebook-id")
    create_test_item(temp_workspace_dir, "dept/eng", "EngNotebook", "Notebook", "eng-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook", "SemanticModel"],
        )

        publish.publish_all_items(
            workspace,
            folder_path_to_include=["/active", "/projects/team1", "/dept", "/dept/eng"],
        )

        assert "Notebook" in workspace.repository_items
        assert "SemanticModel" in workspace.repository_items

        assert workspace.repository_items["Notebook"]["ActiveNotebook"].skip_publish is False
        assert workspace.repository_items["SemanticModel"]["ActiveModel"].skip_publish is False
        assert workspace.repository_items["Notebook"]["ArchivedNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["RootNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["NestedNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["ProjectNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["DeptNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["EngNotebook"].skip_publish is False


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_inclusion_and_exclusion_together(mock_endpoint, temp_workspace_dir):
    """Test that using both folder_path_to_include and folder_path_exclude_regex raises InputError."""
    create_test_item(temp_workspace_dir, "deploy", "DeployNotebook", "Notebook", "deploy-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        with pytest.raises(
            InputError,
            match="Cannot use both 'folder_path_exclude_regex' and 'folder_path_to_include'",
        ):
            publish.publish_all_items(
                workspace,
                folder_path_to_include=["/deploy"],
                folder_path_exclude_regex=r"^/deploy/legacy",
            )


@pytest.mark.usefixtures("experimental_feature_flags")
def test_empty_folder_path_to_include_raises_error(mock_endpoint, temp_workspace_dir):
    """Test that passing an empty list for folder_path_to_include raises an InputError."""
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        with pytest.raises(InputError, match="folder_path_to_include must not be an empty list"):
            publish.publish_all_items(workspace, folder_path_to_include=[])


# =============================================================================
# Combined Filter Tests
# =============================================================================


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_exclusion_with_items_to_include(mock_endpoint, temp_workspace_dir):
    """Test that folder exclusion takes precedence over items_to_include."""
    create_test_item(temp_workspace_dir, "legacy", "ImportantNotebook", "Notebook", "important-notebook-id")
    create_test_item(temp_workspace_dir, None, "StandaloneNotebook", "Notebook", "standalone-notebook-id")
    create_test_item(temp_workspace_dir, None, "OtherNotebook", "Notebook", "other-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.publish_all_items(
            workspace,
            folder_path_exclude_regex=r"^/legacy",
            items_to_include=["ImportantNotebook.Notebook", "StandaloneNotebook.Notebook"],
        )

        assert workspace.repository_items["Notebook"]["ImportantNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["StandaloneNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["OtherNotebook"].skip_publish is True


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_inclusion_with_item_exclusion(mock_endpoint, temp_workspace_dir):
    """Test that item_name_exclude_regex can exclude specific items within an included folder."""
    create_test_item(temp_workspace_dir, "active", "ActiveNotebook", "Notebook", "active-notebook-id")
    create_test_item(temp_workspace_dir, "active", "DebugNotebook", "Notebook", "debug-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.publish_all_items(
            workspace,
            folder_path_to_include=["/active"],
            item_name_exclude_regex=r"^Debug.*",
        )

        assert workspace.repository_items["Notebook"]["DebugNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["ActiveNotebook"].skip_publish is False


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_inclusion_with_items_to_include(mock_endpoint, temp_workspace_dir):
    """Test that folder_path_to_include and items_to_include work together to narrow the scope."""
    create_test_item(temp_workspace_dir, "active", "Notebook1", "Notebook", "notebook1-id")
    create_test_item(temp_workspace_dir, "active", "Notebook2", "Notebook", "notebook2-id")
    create_test_item(temp_workspace_dir, "archive", "ArchivedNotebook", "Notebook", "archived-notebook-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.publish_all_items(
            workspace,
            folder_path_to_include=["/active"],
            items_to_include=["Notebook1.Notebook"],
        )

        assert workspace.repository_items["Notebook"]["Notebook1"].skip_publish is False
        assert workspace.repository_items["Notebook"]["Notebook2"].skip_publish is True
        assert workspace.repository_items["Notebook"]["ArchivedNotebook"].skip_publish is True


@pytest.mark.usefixtures("experimental_feature_flags")
def test_all_filters_combined(mock_endpoint, temp_workspace_dir):
    """Test the complete filter evaluation order with all filters applied."""
    create_test_item(temp_workspace_dir, "active", "DebugNotebook", "Notebook", "debug-id")
    create_test_item(temp_workspace_dir, "active", "TargetNotebook", "Notebook", "target-id")
    create_test_item(temp_workspace_dir, "active", "OtherNotebook", "Notebook", "other-id")
    create_test_item(temp_workspace_dir, "archive", "ArchivedNotebook", "Notebook", "archive-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

        publish.publish_all_items(
            workspace,
            item_name_exclude_regex=r"^Debug.*",
            folder_path_to_include=["/active"],
            items_to_include=["TargetNotebook.Notebook"],
        )

        assert workspace.repository_items["Notebook"]["DebugNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["TargetNotebook"].skip_publish is False
        assert workspace.repository_items["Notebook"]["OtherNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["ArchivedNotebook"].skip_publish is True


# =============================================================================
# NotebookPublisher Tests
# =============================================================================


class TestNotebookPublisher:
    """Tests for NotebookPublisher.publish_one method."""

    @pytest.fixture
    def mock_workspace(self):
        """Create a mock FabricWorkspace object."""
        workspace = MagicMock()
        workspace._publish_item = MagicMock()
        return workspace

    @pytest.fixture
    def publisher(self, mock_workspace):
        """Create a NotebookPublisher instance."""
        publisher = NotebookPublisher.__new__(NotebookPublisher)
        publisher.fabric_workspace_obj = mock_workspace
        return publisher

    def _create_mock_item(self, file_suffix: str) -> MagicMock:
        """Create a mock Item with a file of the given suffix."""
        mock_file = MagicMock()
        mock_file.file_path = Path(f"notebook{file_suffix}")
        mock_item = MagicMock()
        mock_item.item_files = [mock_file]
        return mock_item

    def test_publish_ipynb_includes_api_format(self, publisher, mock_workspace):
        """Test that .ipynb files include api_format in kwargs."""
        item = self._create_mock_item(".ipynb")

        publisher.publish_one("test_notebook", item)

        expected_api_format = API_FORMAT_MAPPING.get(ItemType.NOTEBOOK.value)
        mock_workspace._publish_item.assert_called_once_with(
            item_name="test_notebook",
            item_type=ItemType.NOTEBOOK.value,
            api_format=expected_api_format,
        )

    def test_publish_non_ipynb_excludes_api_format(self, publisher, mock_workspace):
        """Test that non-.ipynb files do not include api_format."""
        item = self._create_mock_item(".py")

        publisher.publish_one("test_notebook", item)

        mock_workspace._publish_item.assert_called_once_with(
            item_name="test_notebook",
            item_type=ItemType.NOTEBOOK.value,
        )

    def test_publish_mixed_files_with_ipynb(self, publisher, mock_workspace):
        """Test that if any file is .ipynb, api_format is included."""
        mock_file_py = MagicMock()
        mock_file_py.file_path = Path("script.py")
        mock_file_ipynb = MagicMock()
        mock_file_ipynb.file_path = Path("notebook.ipynb")

        mock_item = MagicMock()
        mock_item.item_files = [mock_file_py, mock_file_ipynb]

        publisher.publish_one("test_notebook", mock_item)

        expected_api_format = API_FORMAT_MAPPING.get(ItemType.NOTEBOOK.value)
        mock_workspace._publish_item.assert_called_once_with(
            item_name="test_notebook",
            item_type=ItemType.NOTEBOOK.value,
            api_format=expected_api_format,
        )

    def test_item_type_is_notebook(self, publisher):
        """Test that item_type is correctly set to Notebook."""
        assert publisher.item_type == ItemType.NOTEBOOK.value
