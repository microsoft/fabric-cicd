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
                item_type_in_scope=["Notebook"],
            )

            # Test publish_all_items returns list of DeploymentLogEntry objects
            result = publish.publish_all_items(workspace)

            # Verify return type
            assert isinstance(result, list)
            assert len(result) >= 1

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


def test_publish_all_items_with_failures_logs_correctly(mock_endpoint):
    """Test that publish_all_items logs failed operations correctly via structured logging."""
    from fabric_cicd._common._exceptions import DeploymentError

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

        # Mock endpoint to fail on specific calls
        mock_endpoint.invoke.side_effect = [
            {"body": {"value": [], "capacityId": "test-capacity"}, "header": {}},  # Initial capacity check
            {"body": {"value": []}, "header": {}},  # Deployed items check
            {"body": {"value": []}, "header": {}},  # Deployed folders check
            Exception("API Error: Failed to publish"),  # Fail on actual publish
        ]

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],
            )

            # Test that DeploymentError is raised for system failures
            with pytest.raises(DeploymentError, match="An error occurred during publishing"):
                publish.publish_all_items(workspace)

            # But structured logs should still be accessible
            log_entries = workspace.publish_log_entries

            # Should have one failed entry
            assert len(log_entries) == 1
            entry = log_entries[0]
            assert isinstance(entry, DeploymentLogEntry)
            assert entry.operation_type == "publish"
            assert entry.name == "Notebook_1"
            assert entry.item_type == "Notebook"
            assert entry.success is False
            assert "API Error: Failed to publish" in entry.error
            assert entry.start_time is not None
            assert entry.end_time is not None
            assert entry.duration_seconds >= 0


def test_unpublish_all_orphan_items_deployment_log_entries(mock_endpoint):
    """Test that unpublish_all_orphan_items returns list of DeploymentLogEntry objects."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a notebook in the repository (so it's not orphaned)
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
        mock_endpoint.invoke.return_value = {
            "body": {"value": deployed_items, "capacityId": "test-capacity"},
            "header": {},
        }

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook", "Environment"],
            )

            # Test unpublish_all_orphan_items returns list of DeploymentLogEntry objects
            result = publish.unpublish_all_orphan_items(workspace)

            # Verify return type
            assert isinstance(result, list)
            assert len(result) >= 2

            # Verify all entries are DeploymentLogEntry objects
            for entry in result:
                assert isinstance(entry, DeploymentLogEntry)
                assert entry.operation_type == "unpublish"
                assert entry.name in ["OrphanedNotebook", "OrphanedEnvironment"]
                assert entry.item_type in ["Notebook", "Environment"]
                assert isinstance(entry.success, bool)
                assert entry.start_time is not None
                assert entry.end_time is not None
                assert entry.duration_seconds >= 0


def test_unpublish_all_orphan_items_with_failures_logs_correctly(mock_endpoint):
    """Test that unpublish_all_orphan_items logs failed operations correctly."""
    from fabric_cicd._common._exceptions import DeploymentError

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Mock deployed items with orphaned item
        deployed_items = [
            {
                "displayName": "OrphanedNotebook",
                "type": "Notebook",
                "id": "22222222-2222-2222-2222-222222222222",
                "description": "Orphaned notebook",
            }
        ]

        # Mock endpoint to fail on DELETE call
        # Need to handle all the API calls made during workspace init and unpublish operations
        def mock_invoke_side_effect(method, url, body=None):  # noqa: ARG001
            if "capacityId" in str(url):
                return {"body": {"value": [], "capacityId": "test-capacity"}, "header": {}}
            if "items" in str(url) and method == "GET":
                return {"body": {"value": deployed_items}, "header": {}}
            if "folders" in str(url):
                return {"body": {"value": []}, "header": {}}
            if method == "DELETE":
                # This is the actual delete operation that should fail
                msg = "API Error: Failed to delete item"
                raise Exception(msg)
            return {"body": {}, "header": {}}

        mock_endpoint.invoke.side_effect = mock_invoke_side_effect

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],
            )

            # Test that DeploymentError is raised for system failures
            with pytest.raises(DeploymentError, match="An error occurred during unpublishing"):
                publish.unpublish_all_orphan_items(workspace)

            # But structured logs should still be accessible
            log_entries = workspace.unpublish_log_entries

            # Should have one failed entry
            assert len(log_entries) == 1
            entry = log_entries[0]
            assert isinstance(entry, DeploymentLogEntry)
            assert entry.operation_type == "unpublish"
            assert entry.name == "OrphanedNotebook"
            assert entry.item_type == "Notebook"
            assert entry.success is False
            assert "API Error: Failed to delete item" in entry.error
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


def test_structured_logging_captures_success_and_failure_mixed(mock_endpoint):
    """Test that structured logging captures both successful and failed operations in mixed scenarios."""
    from fabric_cicd._common._exceptions import DeploymentError

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create two notebooks
        for i in [1, 2]:
            notebook_dir = temp_path / f"Notebook_{i}.Notebook"
            notebook_dir.mkdir(parents=True)
            (notebook_dir / "notebook-content.py").write_text("print('test')")

            platform_file = notebook_dir / ".platform"
            metadata = {
                "metadata": {
                    "type": "Notebook",
                    "displayName": f"Notebook_{i}",
                    "description": f"Test notebook {i}",
                },
                "config": {"logicalId": f"notebook-{i}-id"},
            }
            with platform_file.open("w", encoding="utf-8") as f:
                json.dump(metadata, f)

        # Mock successful response for first notebook, failure for second
        def mock_invoke_side_effect(method, url, body=None):  # noqa: ARG001
            if ("workspaces" in str(url) and method == "GET") and not ("items" in str(url)):
                # Workspace details call for capacity check
                return {"body": {"capacityId": "test-capacity"}, "header": {}}
            if ("items" in str(url) and method == "GET") or ("folders" in str(url)):
                return {"body": {"value": []}, "header": {}}
            if method == "POST" and "items" in str(url):
                # First call succeeds, second fails
                if not hasattr(mock_invoke_side_effect, "call_count"):
                    mock_invoke_side_effect.call_count = 0
                mock_invoke_side_effect.call_count += 1

                if mock_invoke_side_effect.call_count == 1:
                    return {"body": {"id": "test-item-id-1"}, "header": {}}
                msg = "API Error: Second notebook failed"
                raise Exception(msg)
            return {"body": {}, "header": {}}

        mock_endpoint.invoke.side_effect = mock_invoke_side_effect

        with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],
            )

        with pytest.raises(DeploymentError, match="An error occurred during publishing"):
            publish.publish_all_items(workspace)

        # Check structured logs captured both operations
        log_entries = workspace.publish_log_entries
        assert len(log_entries) == 2

        # First notebook should be successful
        successful_entries = [e for e in log_entries if e.success]
        failed_entries = [e for e in log_entries if not e.success]

        assert len(successful_entries) == 1
        assert len(failed_entries) == 1

        assert successful_entries[0].name == "Notebook_1"
        assert failed_entries[0].name == "Notebook_2"
        assert "API Error: Second notebook failed" in failed_entries[0].error
