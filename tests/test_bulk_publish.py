# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk publish feature: flag validation, fallback logic, item preparation, and end-to-end bulk flow."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fixtures.credentials import DummyTokenCredential

import fabric_cicd.publish as publish
from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag
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
        if method == "POST" and "bulkImportDefinitions" in url:
            return {"body": {"importItemDefinitionsDetails": []}}
        if method == "POST" and url.endswith("/folders"):
            return {"body": {"id": "mock-folder-id"}}
        if method == "POST" and url.endswith("/items"):
            return {"body": {"id": "mock-item-id", "workspaceId": "mock-workspace-id"}}
        return {"body": {"value": [], "capacityId": "test-capacity"}}

    mock.invoke.side_effect = mock_invoke
    return mock


@pytest.fixture
def temp_workspace_dir():
    """Create a temporary directory for test workspaces."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def bulk_publish_flags():
    """Enable bulk publish feature flags and restore after test."""
    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EXPERIMENTAL_FEATURES.value)
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_BULK_PUBLISH.value)
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


def create_test_item(base_path: Path, folder, name: str, item_type: str, logical_id: str) -> Path:
    """Helper to create a test item with .platform file."""
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


def create_parameter_file(base_path: Path, content: str) -> Path:
    """Helper to create a parameter.yml file."""
    param_file = base_path / "parameter.yml"
    param_file.write_text(content, encoding="utf-8")
    return param_file


# =============================================================================
# Feature Flag Validation Tests
# =============================================================================


class TestBulkPublishFeatureFlags:
    """Tests for bulk publish feature flag validation."""

    def test_bulk_publish_requires_experimental_flag(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish without enable_experimental_features raises InputError."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_BULK_PUBLISH.value)
        try:
            create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                with pytest.raises(InputError, match="requires 'enable_experimental_features'"):
                    publish.publish_all_items(workspace)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    @pytest.mark.usefixtures("bulk_publish_flags")
    def test_bulk_publish_enabled_with_both_flags(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish is enabled when both feature flags are set with supported item types."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(ItemPublisher, "publish_all_bulk", return_value=[]) as mock_bulk,
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            mock_bulk.assert_called_once_with(workspace)


# =============================================================================
# Fallback Logic Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishFallback:
    """Tests for conditions that cause fallback to standard publishing."""

    def test_fallback_on_unsupported_item_type(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish falls back to standard mode when unsupported item types are in scope."""
        create_test_item(temp_workspace_dir, None, "TestWarehouse", "Warehouse", "wh-id-001")

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch("fabric_cicd._items._warehouse.WarehousePublisher") as mock_wh_cls,
        ):
            mock_wh_cls.return_value = MagicMock()
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Warehouse"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is False

    def test_fallback_on_dynamic_replace_value_variables(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish falls back when replace_value contains dynamic variables."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_parameter_file(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "some-id"
    replace_value:
      PPE: "$workspace.other_ws.$items.some_item.id"
""",
        )

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is False

    def test_fallback_on_dynamic_find_value_variables(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish falls back when find_value contains dynamic variables."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_parameter_file(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "$workspace.source_ws.$items.Notebook.some_lakehouse.id"
    replace_value:
      PPE: "replacement-id"
""",
        )

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch("fabric_cicd._items._notebook.NotebookPublisher") as mock_nb_cls,
        ):
            mock_nb_cls.return_value = MagicMock()
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is False

    def test_no_fallback_without_dynamic_variables(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish remains enabled when parameter file has no dynamic variables."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_parameter_file(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "old-connection-string"
    replace_value:
      PPE: "new-connection-string"
""",
        )

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(ItemPublisher, "publish_all_bulk", return_value=[]),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True

    def test_item_name_exclude_regex_supported_in_bulk(self, mock_endpoint, temp_workspace_dir, caplog):
        """item_name_exclude_regex does not cause fallback — filtering is applied in bulk Phase 1."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE.value)
        try:
            create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(
                    workspace,
                    item_name_exclude_regex="Test.*",
                )

                # Verify bulk mode stayed enabled (no fallback)
                assert workspace.bulk_publish_enabled is True

                # Verify no fallback warning was logged
                fallback_msgs = [
                    r.message for r in caplog.records if "Falling back to standard deployment" in r.message
                ]
                assert len(fallback_msgs) == 0

                # Verify the regex was applied — property is set
                assert workspace.publish_item_name_exclude_regex == "Test.*"
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)


# =============================================================================
# Bulk Item Count Limit Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishItemCountLimit:
    """Tests for the bulk publish item count limit."""

    def test_exceeding_item_count_limit_raises_error(self, mock_endpoint, temp_workspace_dir):
        """Exceeding BULK_ITEM_COUNT_LIMIT raises InputError."""
        # Create more items than the limit
        for i in range(constants.BULK_ITEM_COUNT_LIMIT + 1):
            create_test_item(temp_workspace_dir, None, f"Notebook{i}", "Notebook", f"nb-id-{i:04d}")

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            with pytest.raises(InputError, match="exceeds the API limit"):
                publish.publish_all_items(workspace)


# =============================================================================
# Bulk Publish End-to-End (Integration-Style) Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishEndToEnd:
    """Integration-style tests for the bulk publish flow with mocked API."""

    def test_bulk_publish_calls_bulk_import_api(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish makes a POST to the bulkImportDefinitions endpoint."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "TestPipeline", "DataPipeline", "dp-id-001")

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook", "DataPipeline"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            # Verify bulkImportDefinitions was called
            bulk_calls = [call for call in mock_endpoint.invoke.call_args_list if "bulkImportDefinitions" in str(call)]
            assert len(bulk_calls) == 1

    def test_bulk_publish_assigns_guids_from_response(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish assigns item GUIDs from the API response."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

        def mock_invoke(method, url, **_kwargs):
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                return {
                    "body": {
                        "importItemDefinitionsDetails": [
                            {
                                "itemType": "Notebook",
                                "itemDisplayName": "TestNotebook",
                                "itemId": "returned-guid-001",
                                "operationType": "Create",
                            }
                        ]
                    }
                }
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.repository_items["Notebook"]["TestNotebook"].guid == "returned-guid-001"

    def test_bulk_publish_with_multiple_item_types(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish handles multiple supported item types in a single call."""
        create_test_item(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "NB2", "Notebook", "nb-id-002")
        create_test_item(temp_workspace_dir, None, "DP1", "DataPipeline", "dp-id-001")

        bulk_call_bodies = []

        def mock_invoke(method, url, **kwargs):
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                bulk_call_bodies.append(kwargs.get("body", {}))
                return {"body": {"importItemDefinitionsDetails": []}}
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook", "DataPipeline"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bulk_call_bodies) == 1
            # All items should be in a single bulk call
            parts = bulk_call_bodies[0]["definitionParts"]
            # Each item has at least .platform + dummy.txt = 2 parts
            assert len(parts) >= 6  # 3 items x 2 files each

    def test_bulk_publish_empty_workspace(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish with no items in repository completes without error."""
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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            # No bulkImportDefinitions call should be made for empty workspace
            bulk_calls = [call for call in mock_endpoint.invoke.call_args_list if "bulkImportDefinitions" in str(call)]
            assert len(bulk_calls) == 0


# =============================================================================
# Bulk Publish with Parameterization Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishParameterization:
    """Tests for file content replacement applied during bulk publish."""

    def test_static_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Static find_replace parameters are applied during bulk publish."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

        # Create a file with content that should be parameterized
        content_file = item_dir / "notebook-content.py"
        content_file.write_text("connection = 'old-connection-string'", encoding="utf-8")

        create_parameter_file(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "old-connection-string"
    replace_value:
      PPE: "new-connection-string"
""",
        )

        bulk_call_bodies = []

        def mock_invoke(method, url, **kwargs):
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                bulk_call_bodies.append(kwargs.get("body", {}))
                return {"body": {"importItemDefinitionsDetails": []}}
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bulk_call_bodies) == 1

    def test_spark_pool_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Spark pool instance_pool_id is replaced in Environment files during bulk publish."""
        import base64

        item_dir = create_test_item(temp_workspace_dir, None, "TestEnv", "Environment", "env-id-001")

        # Create Setting/Sparkcompute.yml with an instance_pool_id to replace
        setting_dir = item_dir / "Setting"
        setting_dir.mkdir()
        sparkcompute = setting_dir / "Sparkcompute.yml"
        sparkcompute.write_text("instance_pool_id: old-pool-id\nautopause_enabled: true\n", encoding="utf-8")

        create_parameter_file(
            temp_workspace_dir,
            """
spark_pool:
  - instance_pool_id: "old-pool-id"
    replace_value:
      PPE:
        type: "Workspace"
        name: "my-pool"
""",
        )

        bulk_call_bodies = []

        def mock_invoke(method, url, **kwargs):
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "GET" and "environments" in url:
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                bulk_call_bodies.append(kwargs.get("body", {}))
                return {
                    "body": {
                        "importItemDefinitionsDetails": [
                            {
                                "itemType": "Environment",
                                "itemDisplayName": "TestEnv",
                                "itemId": "env-guid-001",
                                "operationType": "Create",
                            }
                        ]
                    }
                }
            if method == "POST" and "staging/publish" in url:
                return {"body": {}}
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(
                FabricWorkspace,
                "_get_workspace_pools",
                return_value=[{"name": "my-pool", "type": "Workspace", "id": "new-pool-guid"}],
            ),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Environment"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bulk_call_bodies) == 1

            # Verify instance_pool_id was replaced in the bulk payload
            parts = bulk_call_bodies[0]["definitionParts"]
            sparkcompute_parts = [p for p in parts if "Sparkcompute" in p.get("path", "")]
            assert len(sparkcompute_parts) == 1

            content = base64.b64decode(sparkcompute_parts[0]["payload"]).decode("utf-8")
            assert "new-pool-guid" in content
            assert "old-pool-id" not in content

            # Verify environment publish was submitted via post_publish_all hook
            publish_calls = [c for c in mock_endpoint.invoke.call_args_list if "staging/publish" in str(c)]
            assert len(publish_calls) == 1


# =============================================================================
# Bulk Publish Post-Publish Hook Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishPostPublishHooks:
    """Tests for post-publish hooks that fire after bulk upload."""

    def test_semantic_model_binding_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Semantic model binding post-publish hook fires after bulk publish."""
        create_test_item(temp_workspace_dir, None, "TestModel", "SemanticModel", "sm-id-001")

        create_parameter_file(
            temp_workspace_dir,
            """
semantic_model_binding:
  default:
    connection_id:
      PPE: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
""",
        )

        def mock_invoke(method, url, **_kwargs):
            if method == "POST" and "bindConnection" in url:
                return {"body": {}}
            if method == "GET" and "/connections" in url and "items" in url:
                return {
                    "body": {
                        "value": [
                            {
                                "id": "old-conn-id",
                                "connectivityType": "ShareableCloud",
                                "connectionDetails": {"type": "SQL", "path": "old.server"},
                            }
                        ]
                    }
                }
            if method == "GET" and "connections" in url and "items" not in url:
                return {
                    "body": {
                        "value": [
                            {
                                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                "connectivityType": "ShareableCloud",
                                "connectionDetails": {"type": "SQL", "path": "server.database"},
                            }
                        ]
                    }
                }
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                return {
                    "body": {
                        "importItemDefinitionsDetails": [
                            {
                                "itemType": "SemanticModel",
                                "itemDisplayName": "TestModel",
                                "itemId": "sm-guid-001",
                                "operationType": "Create",
                            }
                        ]
                    }
                }
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["SemanticModel"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True

            # Verify bindConnection API was called in post_publish_all hook
            bind_calls = [c for c in mock_endpoint.invoke.call_args_list if "bindConnection" in str(c)]
            assert len(bind_calls) == 1

    def test_variable_library_value_set_activated_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Variable library value set is activated via post_publish_all hook after bulk publish."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestVarLib", "VariableLibrary", "vl-id-001")

        # Create settings.json with value sets including the target environment
        settings = {"valueSetsOrder": ["Default value set", "PPE", "PROD"]}
        settings_file = item_dir / "settings.json"
        settings_file.write_text(json.dumps(settings), encoding="utf-8")

        patch_calls = []

        def mock_invoke(method, url, **kwargs):
            if method == "GET" and "workspaces" in url and not url.endswith("/items"):
                return {"body": {"value": [], "capacityId": "test-capacity"}}
            if method == "GET" and url.endswith("/items"):
                return {"body": {"value": []}}
            if method == "POST" and "bulkImportDefinitions" in url:
                return {
                    "body": {
                        "importItemDefinitionsDetails": [
                            {
                                "itemType": "VariableLibrary",
                                "itemDisplayName": "TestVarLib",
                                "itemId": "vl-guid-001",
                                "operationType": "Create",
                            }
                        ]
                    }
                }
            if method == "PATCH" and "VariableLibraries" in url:
                patch_calls.append(kwargs.get("body", {}))
                return {"body": {}}
            return {"body": {"value": []}}

        mock_endpoint.invoke.side_effect = mock_invoke

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["VariableLibrary"],
                environment="PPE",
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True

            # Verify value set activation was called via post_publish_all hook
            assert len(patch_calls) == 1
            assert patch_calls[0]["properties"]["activeValueSetName"] == "PPE"


# =============================================================================
# Bulk Publish Selective Deployment Filtering Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishSelectiveFiltering:
    """Tests that selective deployment filters actually exclude items from the bulk API call."""

    @staticmethod
    def _capture_bulk_call_bodies(mock_endpoint):
        """Configure mock_endpoint to capture bulkImportDefinitions call bodies and return the list."""
        bulk_call_bodies = []
        original_side_effect = mock_endpoint.invoke.side_effect

        def capturing_invoke(method, url, **kwargs):
            if method == "POST" and "bulkImportDefinitions" in url:
                bulk_call_bodies.append(kwargs.get("body", {}))
                return {"body": {"importItemDefinitionsDetails": []}}
            return original_side_effect(method, url, **kwargs)

        mock_endpoint.invoke.side_effect = capturing_invoke
        return bulk_call_bodies

    def test_item_name_exclude_regex_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Items matching item_name_exclude_regex are excluded from the bulk API call."""
        create_test_item(temp_workspace_dir, None, "KeepNotebook", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "ExcludeMe", "Notebook", "nb-id-002")
        create_test_item(temp_workspace_dir, None, "ExcludeAlso", "Notebook", "nb-id-003")

        bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace, item_name_exclude_regex="^Exclude.*")

            assert workspace.bulk_publish_enabled is True
            assert len(bulk_call_bodies) == 1

            # Only KeepNotebook should appear in the bulk call
            paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
            assert any("KeepNotebook" in p for p in paths)
            assert not any("ExcludeMe" in p for p in paths)
            assert not any("ExcludeAlso" in p for p in paths)

    def test_items_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in the items_to_include list are sent to the bulk API call."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE.value)
        try:
            create_test_item(temp_workspace_dir, None, "IncludedNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, None, "ExcludedNB", "Notebook", "nb-id-002")
            create_test_item(temp_workspace_dir, None, "IncludedDP", "DataPipeline", "dp-id-001")

            bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook", "DataPipeline"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(
                    workspace,
                    items_to_include=["IncludedNB.Notebook", "IncludedDP.DataPipeline"],
                )

                assert workspace.bulk_publish_enabled is True
                assert len(bulk_call_bodies) == 1

                paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
                assert any("IncludedNB" in p for p in paths)
                assert any("IncludedDP" in p for p in paths)
                assert not any("ExcludedNB" in p for p in paths)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    def test_folder_path_exclude_regex_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Items in excluded folders are omitted from the bulk API call."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EXCLUDE_FOLDER.value)
        try:
            create_test_item(temp_workspace_dir, "keep_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "exclude_folder", "DropNB", "Notebook", "nb-id-002")

            bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/exclude_folder")

                assert workspace.bulk_publish_enabled is True
                assert len(bulk_call_bodies) == 1

                paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
                assert any("KeepNB" in p for p in paths)
                assert not any("DropNB" in p for p in paths)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    def test_folder_path_exclude_regex_cascades_to_descendants_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Excluding a parent folder also excludes items in descendant folders."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EXCLUDE_FOLDER.value)
        try:
            create_test_item(temp_workspace_dir, None, "RootNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "parent/child", "ChildNB", "Notebook", "nb-id-002")

            bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/parent")

                assert workspace.bulk_publish_enabled is True
                assert len(bulk_call_bodies) == 1

                paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
                assert any("RootNB" in p for p in paths)
                assert not any("ChildNB" in p for p in paths)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    def test_folder_path_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in included folders are sent to the bulk API call."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_INCLUDE_FOLDER.value)
        try:
            create_test_item(temp_workspace_dir, "included_folder", "IncNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "other_folder", "OtherNB", "Notebook", "nb-id-002")

            bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(workspace, folder_path_to_include=["/included_folder"])

                assert workspace.bulk_publish_enabled is True
                assert len(bulk_call_bodies) == 1

                paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
                assert any("IncNB" in p for p in paths)
                assert not any("OtherNB" in p for p in paths)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    def test_combined_item_exclude_and_folder_exclude_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """item_name_exclude_regex and folder_path_exclude_regex work together in bulk mode."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EXCLUDE_FOLDER.value)
        try:
            create_test_item(temp_workspace_dir, "good_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "good_folder", "DropByName", "Notebook", "nb-id-002")
            create_test_item(temp_workspace_dir, "bad_folder", "DropByFolder", "Notebook", "nb-id-003")

            bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                    repository_directory=str(temp_workspace_dir),
                    item_type_in_scope=["Notebook"],
                    token_credential=DummyTokenCredential(),
                )
                publish.publish_all_items(
                    workspace,
                    item_name_exclude_regex="^DropByName$",
                    folder_path_exclude_regex="^/bad_folder",
                )

                assert workspace.bulk_publish_enabled is True
                assert len(bulk_call_bodies) == 1

                paths = [p["path"] for p in bulk_call_bodies[0]["definitionParts"]]
                assert any("KeepNB" in p for p in paths)
                assert not any("DropByName" in p for p in paths)
                assert not any("DropByFolder" in p for p in paths)
        finally:
            constants.FEATURE_FLAG.clear()
            constants.FEATURE_FLAG.update(original_flags)

    def test_all_items_excluded_skips_bulk_api_call(self, mock_endpoint, temp_workspace_dir):
        """When all items are excluded by filters, no bulk API call is made."""
        create_test_item(temp_workspace_dir, None, "OnlyItem", "Notebook", "nb-id-001")

        bulk_call_bodies = self._capture_bulk_call_bodies(mock_endpoint)

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
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["Notebook"],
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(workspace, item_name_exclude_regex="^OnlyItem$")

            assert workspace.bulk_publish_enabled is True
            # No bulk API call since all items were filtered out
            assert len(bulk_call_bodies) == 0
