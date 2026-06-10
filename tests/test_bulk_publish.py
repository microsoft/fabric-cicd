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
from fabric_cicd._parameter._parameter import Parameter
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


# =============================================================================
# Selective Deployment Ignored in Bulk Mode Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishSelectiveDeploymentIgnored:
    """Tests that selective deployment parameters are ignored in bulk publish mode."""

    def test_selective_params_ignored_in_bulk_mode(self, mock_endpoint, temp_workspace_dir, caplog):
        """Selective deployment parameters are logged as ignored when bulk publish is enabled."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

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
                token_credential=DummyTokenCredential(),
            )
            publish.publish_all_items(
                workspace,
                item_name_exclude_regex="Test.*",
                items_to_include=["TestNotebook.Notebook"],
            )

            assert workspace.bulk_publish_enabled is True
            assert any("ignored in bulk publish mode" in r.message for r in caplog.records)


# =============================================================================
# Dynamic Variable Detection (Unit Tests)
# =============================================================================


class TestSearchDynamicReplacementVariables:
    """Unit tests for _search_dynamic_replacement_variables_in_parameter_file."""

    def _make_parameter(self, temp_workspace_dir, yaml_content):
        """Create a Parameter instance from YAML content."""
        create_parameter_file(temp_workspace_dir, yaml_content)
        return Parameter(
            repository_directory=temp_workspace_dir,
            item_type_in_scope=["Notebook"],
            environment="PPE",
        )

    def test_detects_workspace_variable_in_replace_value(self, temp_workspace_dir):
        """Dynamic variable $workspace.* in replace_value is detected."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "old-id"
    replace_value:
      PPE: "$workspace.my_ws.$items.my_item.id"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is True

    def test_detects_items_variable_in_replace_value(self, temp_workspace_dir):
        """Dynamic variable $items.* in replace_value is detected."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "old-id"
    replace_value:
      PPE: "$items.my_lakehouse.id"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is True

    def test_detects_workspace_variable_in_find_value(self, temp_workspace_dir):
        """Dynamic variable $workspace.* in find_value is detected."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "$workspace.source_ws.$items.my_item.id"
    replace_value:
      PPE: "replacement-id"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is True

    def test_no_detection_for_static_values(self, temp_workspace_dir):
        """Static find/replace values are not flagged as dynamic."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
find_replace:
  - find_value: "static-old-value"
    replace_value:
      PPE: "static-new-value"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is False

    def test_no_detection_in_non_dynamic_params(self, temp_workspace_dir):
        """Dynamic variable patterns in spark_pool are not checked."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
spark_pool:
  - instance_pool_id: "pool-id"
    replace_value:
      PPE:
        type: "Capacity"
        name: "$workspace.something"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is False

    def test_detects_dynamic_variable_in_key_value_replace(self, temp_workspace_dir):
        """Dynamic variable in key_value_replace replace_value is detected."""
        param = self._make_parameter(
            temp_workspace_dir,
            """
key_value_replace:
  - find_key: "$.connectionId"
    replace_value:
      PPE: "$workspace.my_ws.$items.my_item.id"
""",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is True

    def test_empty_parameter_file_returns_false(self, temp_workspace_dir):
        """No parameters means no dynamic variables detected."""
        param = Parameter(
            repository_directory=temp_workspace_dir,
            item_type_in_scope=["Notebook"],
            environment="PPE",
        )
        assert param._search_dynamic_replacement_variables_in_parameter_file() is False


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
    """Tests for parameterization applied during bulk publish."""

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
