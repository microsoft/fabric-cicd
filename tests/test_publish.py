# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test publishing functionality including selective publishing based on repository content."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.publish as publish
from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError
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
        return {"body": {"value": [], "capacityId": "test-capacity"}}

    mock.invoke.side_effect = mock_invoke
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


def test_mirrored_database_published_before_lakehouse(mock_endpoint):
    """Test that MirroredDatabase items are published before Lakehouse items to enable shortcuts."""
    import json
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    # Track the order of function calls
    call_order = []

    def mock_publish_lakehouses(_workspace):
        call_order.append("Lakehouse")

    def mock_publish_mirroreddatabase(_workspace):
        call_order.append("MirroredDatabase")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a Lakehouse item
        lakehouse_dir = temp_path / "TestLakehouse.Lakehouse"
        lakehouse_dir.mkdir(parents=True, exist_ok=True)

        lakehouse_platform_file = lakehouse_dir / ".platform"
        lakehouse_metadata = {
            "metadata": {
                "type": "Lakehouse",
                "displayName": "Test Lakehouse",
                "description": "Test lakehouse",
            },
            "config": {"logicalId": "test-lakehouse-id"},
        }

        with lakehouse_platform_file.open("w", encoding="utf-8") as f:
            json.dump(lakehouse_metadata, f)

        with (lakehouse_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Create a MirroredDatabase item
        mirrored_db_dir = temp_path / "TestMirroredDB.MirroredDatabase"
        mirrored_db_dir.mkdir(parents=True, exist_ok=True)

        mirrored_db_platform_file = mirrored_db_dir / ".platform"
        mirrored_db_metadata = {
            "metadata": {
                "type": "MirroredDatabase",
                "displayName": "Test Mirrored Database",
                "description": "Test mirrored database",
            },
            "config": {"logicalId": "test-mirrored-db-id"},
        }

        with mirrored_db_platform_file.open("w", encoding="utf-8") as f:
            json.dump(mirrored_db_metadata, f)

        with (mirrored_db_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch("fabric_cicd._items.publish_lakehouses", side_effect=mock_publish_lakehouses),
            patch("fabric_cicd._items.publish_mirroreddatabase", side_effect=mock_publish_mirroreddatabase),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Lakehouse", "MirroredDatabase"],
            )

            # Call publish_all_items
            publish.publish_all_items(workspace)

            # Verify that both item types were processed
            assert len(call_order) == 2
            assert "MirroredDatabase" in call_order
            assert "Lakehouse" in call_order

            # Verify that MirroredDatabase was published before Lakehouse
            mirrored_db_index = call_order.index("MirroredDatabase")
            lakehouse_index = call_order.index("Lakehouse")
            assert mirrored_db_index < lakehouse_index, (
                f"MirroredDatabase should be published before Lakehouse, but got order: {call_order}"
            )


def test_folder_exclusion_with_regex(mock_endpoint):
    """Test that folder_path_exclude_regex can exclude entire folders of items."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create items in 'legacy' folder (should be excluded)
        legacy_notebook_dir = temp_path / "legacy" / "LegacyNotebook.Notebook"
        legacy_notebook_dir.mkdir(parents=True, exist_ok=True)

        legacy_notebook_platform = legacy_notebook_dir / ".platform"
        legacy_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "LegacyNotebook",
                "description": "Legacy notebook to be excluded",
            },
            "config": {"logicalId": "legacy-notebook-id"},
        }

        with legacy_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(legacy_notebook_metadata, f)

        with (legacy_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        legacy_model_dir = temp_path / "legacy" / "LegacyModel.SemanticModel"
        legacy_model_dir.mkdir(parents=True, exist_ok=True)

        legacy_model_platform = legacy_model_dir / ".platform"
        legacy_model_metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "LegacyModel",
                "description": "Legacy semantic model to be excluded",
            },
            "config": {"logicalId": "legacy-model-id"},
        }

        with legacy_model_platform.open("w", encoding="utf-8") as f:
            json.dump(legacy_model_metadata, f)

        with (legacy_model_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Create items in 'current' folder (should be included)
        current_notebook_dir = temp_path / "current" / "CurrentNotebook.Notebook"
        current_notebook_dir.mkdir(parents=True, exist_ok=True)

        current_notebook_platform = current_notebook_dir / ".platform"
        current_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "CurrentNotebook",
                "description": "Current notebook to be included",
            },
            "config": {"logicalId": "current-notebook-id"},
        }

        with current_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(current_notebook_metadata, f)

        with (current_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Create root-level item (should be included)
        root_notebook_dir = temp_path / "RootNotebook.Notebook"
        root_notebook_dir.mkdir(parents=True, exist_ok=True)

        root_notebook_platform = root_notebook_dir / ".platform"
        root_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "RootNotebook",
                "description": "Root level notebook to be included",
            },
            "config": {"logicalId": "root-notebook-id"},
        }

        with root_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(root_notebook_metadata, f)

        with (root_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            # Enable experimental feature flags for folder exclusion
            original_flags = constants.FEATURE_FLAG.copy()
            constants.FEATURE_FLAG.add("enable_experimental_features")
            constants.FEATURE_FLAG.add("enable_exclude_folder")

            try:
                workspace = FabricWorkspace(
                    workspace_id="12345678-1234-5678-abcd-1234567890ab",
                    repository_directory=str(temp_path),
                    item_type_in_scope=["Notebook", "SemanticModel"],
                )

                # Test: Exclude items in 'legacy' folder using folder path regex pattern
                exclude_regex = r".*legacy.*"
                publish.publish_all_items(workspace, folder_path_exclude_regex=exclude_regex)

                # Verify that repository_items are populated correctly
                assert "Notebook" in workspace.repository_items
                assert "SemanticModel" in workspace.repository_items

                # Check that legacy items were marked for exclusion (skip_publish = True)
                assert workspace.repository_items["Notebook"]["LegacyNotebook"].skip_publish is True
                assert workspace.repository_items["SemanticModel"]["LegacyModel"].skip_publish is True

                # Check that current and root items were NOT marked for exclusion (skip_publish = False)
                assert workspace.repository_items["Notebook"]["CurrentNotebook"].skip_publish is False
                assert workspace.repository_items["Notebook"]["RootNotebook"].skip_publish is False

            finally:
                # Restore original feature flags
                constants.FEATURE_FLAG.clear()
                constants.FEATURE_FLAG.update(original_flags)


def test_item_name_exclusion_still_works(mock_endpoint):
    """Test that existing item name exclusion still works with the new folder exclusion feature."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create items with specific names to test name-based exclusion
        test_notebook_dir = temp_path / "TestNotebook.Notebook"
        test_notebook_dir.mkdir(parents=True, exist_ok=True)

        test_notebook_platform = test_notebook_dir / ".platform"
        test_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "TestNotebook",
                "description": "Test notebook to be included",
            },
            "config": {"logicalId": "test-notebook-id"},
        }

        with test_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(test_notebook_metadata, f)

        with (test_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        excluded_notebook_dir = temp_path / "DoNotPublish.Notebook"
        excluded_notebook_dir.mkdir(parents=True, exist_ok=True)

        excluded_notebook_platform = excluded_notebook_dir / ".platform"
        excluded_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "DoNotPublish",
                "description": "Notebook to be excluded by name",
            },
            "config": {"logicalId": "excluded-notebook-id"},
        }

        with excluded_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(excluded_notebook_metadata, f)

        with (excluded_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

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
                item_type_in_scope=["Notebook"],
            )

            # Test: Exclude items with "DoNotPublish" in the name
            exclude_regex = r".*DoNotPublish.*"
            publish.publish_all_items(workspace, item_name_exclude_regex=exclude_regex)

            # Verify that the excluded item was marked for exclusion
            assert workspace.repository_items["Notebook"]["DoNotPublish"].skip_publish is True

            # Verify that the regular item was NOT marked for exclusion
            assert workspace.repository_items["Notebook"]["TestNotebook"].skip_publish is False


def test_legacy_folder_exclusion_example(mock_endpoint):
    """Test the specific use case mentioned in the issue: excluding items in a 'legacy' folder."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create items exactly like in the user's example but using simpler item types
        # /legacy/FabricNotebook.Notebook
        legacy_notebook_dir = temp_path / "legacy" / "FabricNotebook.Notebook"
        legacy_notebook_dir.mkdir(parents=True, exist_ok=True)

        legacy_notebook_platform = legacy_notebook_dir / ".platform"
        legacy_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "FabricNotebook",
                "description": "Legacy notebook item",
            },
            "config": {"logicalId": "legacy-notebook-id"},
        }

        with legacy_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(legacy_notebook_metadata, f)

        with (legacy_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # /legacy/Model.SemanticModel
        legacy_model_dir = temp_path / "legacy" / "Model.SemanticModel"
        legacy_model_dir.mkdir(parents=True, exist_ok=True)

        legacy_model_platform = legacy_model_dir / ".platform"
        legacy_model_metadata = {
            "metadata": {
                "type": "SemanticModel",
                "displayName": "Model",
                "description": "Legacy semantic model",
            },
            "config": {"logicalId": "legacy-model-id"},
        }

        with legacy_model_platform.open("w", encoding="utf-8") as f:
            json.dump(legacy_model_metadata, f)

        with (legacy_model_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        # Create a current item outside legacy folder
        current_notebook_dir = temp_path / "CurrentNotebook.Notebook"
        current_notebook_dir.mkdir(parents=True, exist_ok=True)

        current_notebook_platform = current_notebook_dir / ".platform"
        current_notebook_metadata = {
            "metadata": {
                "type": "Notebook",
                "displayName": "CurrentNotebook",
                "description": "Current notebook item",
            },
            "config": {"logicalId": "current-notebook-id"},
        }

        with current_notebook_platform.open("w", encoding="utf-8") as f:
            json.dump(current_notebook_metadata, f)

        with (current_notebook_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
        ):
            # Enable experimental feature flags for folder exclusion
            original_flags = constants.FEATURE_FLAG.copy()
            constants.FEATURE_FLAG.add("enable_experimental_features")
            constants.FEATURE_FLAG.add("enable_exclude_folder")

            try:
                workspace = FabricWorkspace(
                    workspace_id="12345678-1234-5678-abcd-1234567890ab",
                    repository_directory=str(temp_path),
                    item_type_in_scope=["Notebook", "SemanticModel"],
                )

                # Test: Exclude all items in 'legacy' folder using the folder path regex pattern
                exclude_regex = r"^legacy/"  # Match items that start with 'legacy/'
                publish.publish_all_items(workspace, folder_path_exclude_regex=exclude_regex)

                # Verify that legacy items were excluded
                assert workspace.repository_items["Notebook"]["FabricNotebook"].skip_publish is True
                assert workspace.repository_items["SemanticModel"]["Model"].skip_publish is True

                # Verify that current items were NOT excluded
                assert workspace.repository_items["Notebook"]["CurrentNotebook"].skip_publish is False

            finally:
                # Restore original feature flags
                constants.FEATURE_FLAG.clear()
                constants.FEATURE_FLAG.update(original_flags)
