# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk publish feature: flag validation, fallback logic, item preparation, and end-to-end bulk flow."""

import base64
import json
import tempfile
from contextlib import contextmanager
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

VALID_WORKSPACE_ID = "12345678-1234-5678-abcd-1234567890ab"


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


@contextmanager
def extra_flags(*flags):
    """Context manager to temporarily add feature flags and restore on exit."""
    original = constants.FEATURE_FLAG.copy()
    for flag in flags:
        constants.FEATURE_FLAG.add(flag.value if hasattr(flag, "value") else flag)
    try:
        yield
    finally:
        constants.FEATURE_FLAG.clear()
        constants.FEATURE_FLAG.update(original)


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


@contextmanager
def patched_workspace(mock_endpoint, temp_workspace_dir, item_type_in_scope=None, environment="N/A"):
    """Create a FabricWorkspace with standard mocks applied.

    Yields:
        The configured FabricWorkspace instance.
    """
    if item_type_in_scope is None:
        item_type_in_scope = ["Notebook"]
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id=VALID_WORKSPACE_ID,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=item_type_in_scope,
            environment=environment,
            token_credential=DummyTokenCredential(),
        )
        yield workspace


def capture_bulk_bodies(mock_endpoint):
    """Wrap mock_endpoint to capture bulkImportDefinitions POST bodies. Returns the capture list."""
    bodies = []
    original = mock_endpoint.invoke.side_effect

    def capturing(method, url, **kwargs):
        if method == "POST" and "bulkImportDefinitions" in url:
            bodies.append(kwargs.get("body", {}))
        return original(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = capturing
    return bodies


def set_bulk_response(mock_endpoint, details, *, header=None, status_code=None):
    """Override mock_endpoint to return a specific bulkImportDefinitions response."""
    original = mock_endpoint.invoke.side_effect

    def custom(method, url, **kwargs):
        if method == "POST" and "bulkImportDefinitions" in url:
            resp = {"body": {"importItemDefinitionsDetails": details}}
            if header is not None:
                resp["header"] = header
            if status_code is not None:
                resp["status_code"] = status_code
            return resp
        return original(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = custom


def get_bulk_part_content(bulk_body, path_substring):
    """Decode and return the text content of the first definition part matching path_substring."""
    parts = bulk_body["definitionParts"]
    matching = [p for p in parts if path_substring in p.get("path", "")]
    assert len(matching) == 1, f"Expected 1 part matching '{path_substring}', found {len(matching)}"
    return base64.b64decode(matching[0]["payload"]).decode("utf-8")


def get_bulk_part_names(bulk_body):
    """Return the set of item names found in the definition parts paths."""
    paths = [p["path"] for p in bulk_body["definitionParts"]]
    names = set()
    for p in paths:
        segments = [s for s in p.split("/") if s]
        if len(segments) >= 2:
            names.add(segments[-2].split(".")[0])
    return names


# =============================================================================
# Feature Flag Validation Tests
# =============================================================================


class TestBulkPublishFeatureFlags:
    """Tests for bulk publish feature flag validation."""

    def test_bulk_publish_requires_experimental_flag(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish without enable_experimental_features raises InputError."""
        with extra_flags(FeatureFlag.ENABLE_BULK_PUBLISH):
            create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
            with (
                patched_workspace(mock_endpoint, temp_workspace_dir) as workspace,
                pytest.raises(InputError, match="requires 'enable_experimental_features'"),
            ):
                publish.publish_all_items(workspace)

    @pytest.mark.usefixtures("bulk_publish_flags")
    def test_bulk_publish_enabled_with_both_flags(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish is enabled when both feature flags are set with supported item types."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

        with (
            patched_workspace(mock_endpoint, temp_workspace_dir) as workspace,
            patch.object(ItemPublisher, "publish_all_bulk", return_value=[]) as mock_bulk,
        ):
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
            patched_workspace(mock_endpoint, temp_workspace_dir, item_type_in_scope=["Warehouse"]) as workspace,
            patch("fabric_cicd._items._warehouse.WarehousePublisher", return_value=MagicMock()),
        ):
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

        with patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace:
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is False
            assert workspace.contains_param_vars is True

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
            patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace,
            patch("fabric_cicd._items._notebook.NotebookPublisher", return_value=MagicMock()),
        ):
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is False
            assert workspace.contains_param_vars is True

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
            patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace,
            patch.object(ItemPublisher, "publish_all_bulk", return_value=[]),
        ):
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is True
            assert workspace.contains_param_vars is False

    def test_item_name_exclude_regex_supported_in_bulk(self, mock_endpoint, temp_workspace_dir, caplog):
        """item_name_exclude_regex does not cause fallback -- filtering is applied in bulk Phase 1."""
        with extra_flags(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE):
            create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, item_name_exclude_regex="Test.*")

                assert workspace.bulk_publish_enabled is True
                assert workspace.publish_item_name_exclude_regex == "Test.*"
                assert not any("Falling back to standard deployment" in r.message for r in caplog.records)


# =============================================================================
# Bulk Item Count Limit Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishItemCountLimit:
    """Tests for the bulk publish item count limit."""

    def test_exceeding_item_count_limit_raises_error(self, mock_endpoint, temp_workspace_dir):
        """Exceeding BULK_ITEM_COUNT_LIMIT raises InputError."""
        for i in range(constants.BULK_ITEM_COUNT_LIMIT + 1):
            create_test_item(temp_workspace_dir, None, f"Notebook{i}", "Notebook", f"nb-id-{i:04d}")

        with (
            patched_workspace(mock_endpoint, temp_workspace_dir) as workspace,
            pytest.raises(InputError, match="exceeds the API limit"),
        ):
            publish.publish_all_items(workspace)


# =============================================================================
# Bulk Publish End-to-End (Integration-Style) Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishEndToEnd:
    """Integration-style tests for the bulk publish flow with mocked API."""

    def test_bulk_publish_single_api_call_with_correct_payload(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish sends all items to bulkImportDefinitions in a single POST with allowPairingByName."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "TestPipeline", "DataPipeline", "dp-id-001")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
        ) as workspace:
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bodies) == 1
            assert bodies[0]["options"]["allowPairingByName"] is True
            names = get_bulk_part_names(bodies[0])
            assert "TestNotebook" in names
            assert "TestPipeline" in names

    def test_bulk_publish_assigns_guids_from_response(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish assigns item GUIDs from the API response."""
        create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        set_bulk_response(
            mock_endpoint,
            [
                {
                    "itemType": "Notebook",
                    "itemDisplayName": "TestNotebook",
                    "itemId": "returned-guid-001",
                    "operationType": "Create",
                },
            ],
        )

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace)
            assert workspace.repository_items["Notebook"]["TestNotebook"].guid == "returned-guid-001"

    def test_bulk_publish_multiple_types_single_call(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish handles multiple supported item types in a single call."""
        create_test_item(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "NB2", "Notebook", "nb-id-002")
        create_test_item(temp_workspace_dir, None, "DP1", "DataPipeline", "dp-id-001")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
        ) as workspace:
            publish.publish_all_items(workspace)

            assert len(bodies) == 1
            names = get_bulk_part_names(bodies[0])
            assert names == {"NB1", "NB2", "DP1"}
            assert len(bodies[0]["definitionParts"]) >= 6

    def test_bulk_publish_empty_workspace_no_api_call(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish with no items in repository completes without calling the bulk API."""
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bodies) == 0


# =============================================================================
# Bulk Publish with Parameterization Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishParameterization:
    """Tests for file content replacement applied during bulk publish."""

    def test_static_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Static find_replace parameters are applied to file content in the bulk payload."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
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
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace:
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bodies) == 1
            content = get_bulk_part_content(bodies[0], "notebook-content")
            assert "new-connection-string" in content
            assert "old-connection-string" not in content

    def test_spark_pool_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Spark pool instance_pool_id is replaced in Environment files during bulk publish."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestEnv", "Environment", "env-id-001")
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

        set_bulk_response(
            mock_endpoint,
            [
                {
                    "itemType": "Environment",
                    "itemDisplayName": "TestEnv",
                    "itemId": "env-guid-001",
                    "operationType": "Create",
                },
            ],
        )
        bodies = capture_bulk_bodies(mock_endpoint)
        publish_calls = []
        original = mock_endpoint.invoke.side_effect

        def with_env_hooks(method, url, **kwargs):
            if method == "POST" and "staging/publish" in url:
                publish_calls.append(url)
                return {"body": {}}
            return original(method, url, **kwargs)

        mock_endpoint.invoke.side_effect = with_env_hooks

        with (
            patched_workspace(
                mock_endpoint, temp_workspace_dir, item_type_in_scope=["Environment"], environment="PPE"
            ) as workspace,
            patch.object(
                FabricWorkspace,
                "_get_workspace_pools",
                return_value=[{"name": "my-pool", "type": "Workspace", "id": "new-pool-guid"}],
            ),
        ):
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            content = get_bulk_part_content(bodies[0], "Sparkcompute")
            assert "new-pool-guid" in content
            assert "old-pool-id" not in content
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

        bind_calls = []
        original = mock_endpoint.invoke.side_effect

        def with_sm_hooks(method, url, **kwargs):
            if method == "POST" and "bindConnection" in url:
                bind_calls.append(url)
                return {"body": {}}
            if method == "GET" and "/connections" in url and "items" in url:
                return {
                    "body": {
                        "value": [
                            {
                                "id": "old-conn-id",
                                "connectivityType": "ShareableCloud",
                                "connectionDetails": {"type": "SQL", "path": "old.server"},
                            },
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
                            },
                        ]
                    }
                }
            return original(method, url, **kwargs)

        set_bulk_response(
            mock_endpoint,
            [
                {
                    "itemType": "SemanticModel",
                    "itemDisplayName": "TestModel",
                    "itemId": "sm-guid-001",
                    "operationType": "Create",
                },
            ],
        )
        bulk_side_effect = mock_endpoint.invoke.side_effect
        mock_endpoint.invoke.side_effect = (
            lambda method, url, **kwargs: with_sm_hooks(method, url, **kwargs)
            if ("bindConnection" in url or "connections" in url)
            else bulk_side_effect(method, url, **kwargs)
        )

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["SemanticModel"], environment="PPE"
        ) as workspace:
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(bind_calls) == 1, "bindConnection API should be called exactly once"

    def test_variable_library_value_set_activated_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Variable library value set is activated via post_publish_all hook after bulk publish."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestVarLib", "VariableLibrary", "vl-id-001")
        settings = {"valueSetsOrder": ["Default value set", "PPE", "PROD"]}
        (item_dir / "settings.json").write_text(json.dumps(settings), encoding="utf-8")

        patch_calls = []

        set_bulk_response(
            mock_endpoint,
            [
                {
                    "itemType": "VariableLibrary",
                    "itemDisplayName": "TestVarLib",
                    "itemId": "vl-guid-001",
                    "operationType": "Create",
                },
            ],
        )
        bulk_side_effect = mock_endpoint.invoke.side_effect

        def with_varlib_hooks(method, url, **kwargs):
            if method == "PATCH" and "VariableLibraries" in url:
                patch_calls.append(kwargs.get("body", {}))
                return {"body": {}}
            return bulk_side_effect(method, url, **kwargs)

        mock_endpoint.invoke.side_effect = with_varlib_hooks

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["VariableLibrary"], environment="PPE"
        ) as workspace:
            publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert len(patch_calls) == 1
            assert patch_calls[0]["properties"]["activeValueSetName"] == "PPE"

    def test_lakehouse_shortcuts_published_after_bulk_upload(self, mock_endpoint, temp_workspace_dir):
        """Lakehouse shortcuts are published via post_publish_all Phase 3 after bulk import."""
        with extra_flags(FeatureFlag.ENABLE_SHORTCUT_PUBLISH):
            item_dir = create_test_item(temp_workspace_dir, None, "TestLH", "Lakehouse", "lh-id-001")
            shortcuts_data = [
                {
                    "name": "my_shortcut",
                    "path": "/Tables",
                    "target": {
                        "type": "OneLake",
                        "oneLake": {
                            "path": "Tables/src",
                            "itemId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                            "workspaceId": VALID_WORKSPACE_ID,
                            "artifactType": "Lakehouse",
                        },
                    },
                }
            ]
            (item_dir / "shortcuts.metadata.json").write_text(json.dumps(shortcuts_data), encoding="utf-8")

            shortcut_calls = []
            original = mock_endpoint.invoke.side_effect

            def lakehouse_invoke(method, url, **kwargs):
                if method == "POST" and "shortcuts" in url:
                    shortcut_calls.append(kwargs.get("body", {}))
                    return {"body": {"name": "shortcut", "path": "/Tables"}}
                if method == "GET" and "shortcuts" in url:
                    return {"body": {"value": []}, "header": {}}
                if method == "POST" and "bulkImportDefinitions" in url:
                    return {
                        "body": {
                            "importItemDefinitionsDetails": [
                                {
                                    "itemType": "Lakehouse",
                                    "itemDisplayName": "TestLH",
                                    "itemId": "lh-guid-001",
                                    "operationType": "Create",
                                },
                            ]
                        }
                    }
                return original(method, url, **kwargs)

            mock_endpoint.invoke.side_effect = lakehouse_invoke

            with patched_workspace(mock_endpoint, temp_workspace_dir, item_type_in_scope=["Lakehouse"]) as workspace:
                publish.publish_all_items(workspace)

                assert workspace.bulk_publish_enabled is True
                assert len(shortcut_calls) == 1
                assert shortcut_calls[0]["name"] == "my_shortcut"


# =============================================================================
# Bulk Publish Selective Deployment Filtering Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishSelectiveFiltering:
    """Tests that selective deployment filters actually exclude items from the bulk API call."""

    def test_item_name_exclude_regex_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Items matching item_name_exclude_regex are excluded from the bulk API call."""
        create_test_item(temp_workspace_dir, None, "KeepNotebook", "Notebook", "nb-id-001")
        create_test_item(temp_workspace_dir, None, "ExcludeMe", "Notebook", "nb-id-002")
        create_test_item(temp_workspace_dir, None, "ExcludeAlso", "Notebook", "nb-id-003")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace, item_name_exclude_regex="^Exclude.*")

            assert workspace.bulk_publish_enabled is True
            names = get_bulk_part_names(bodies[0])
            assert "KeepNotebook" in names
            assert "ExcludeMe" not in names
            assert "ExcludeAlso" not in names

    def test_items_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in the items_to_include list are sent to the bulk API call."""
        with extra_flags(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE):
            create_test_item(temp_workspace_dir, None, "IncludedNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, None, "ExcludedNB", "Notebook", "nb-id-002")
            create_test_item(temp_workspace_dir, None, "IncludedDP", "DataPipeline", "dp-id-001")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(
                mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
            ) as workspace:
                publish.publish_all_items(
                    workspace,
                    items_to_include=["IncludedNB.Notebook", "IncludedDP.DataPipeline"],
                )

                names = get_bulk_part_names(bodies[0])
                assert names == {"IncludedNB", "IncludedDP"}

    def test_folder_path_exclude_regex_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Items in excluded folders are omitted from the bulk API call."""
        with extra_flags(FeatureFlag.ENABLE_EXCLUDE_FOLDER):
            create_test_item(temp_workspace_dir, "keep_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "exclude_folder", "DropNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/exclude_folder")

                names = get_bulk_part_names(bodies[0])
                assert names == {"KeepNB"}

    def test_folder_path_exclude_regex_cascades_to_descendants_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Excluding a parent folder also excludes items in descendant folders."""
        with extra_flags(FeatureFlag.ENABLE_EXCLUDE_FOLDER):
            create_test_item(temp_workspace_dir, None, "RootNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "parent/child", "ChildNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/parent")

                names = get_bulk_part_names(bodies[0])
                assert names == {"RootNB"}

    def test_folder_path_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in included folders are sent to the bulk API call."""
        with extra_flags(FeatureFlag.ENABLE_INCLUDE_FOLDER):
            create_test_item(temp_workspace_dir, "included_folder", "IncNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "other_folder", "OtherNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_to_include=["/included_folder"])

                names = get_bulk_part_names(bodies[0])
                assert names == {"IncNB"}

    def test_combined_item_exclude_and_folder_exclude_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """item_name_exclude_regex and folder_path_exclude_regex work together in bulk mode."""
        with extra_flags(FeatureFlag.ENABLE_EXCLUDE_FOLDER):
            create_test_item(temp_workspace_dir, "good_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, "good_folder", "DropByName", "Notebook", "nb-id-002")
            create_test_item(temp_workspace_dir, "bad_folder", "DropByFolder", "Notebook", "nb-id-003")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(
                    workspace,
                    item_name_exclude_regex="^DropByName$",
                    folder_path_exclude_regex="^/bad_folder",
                )

                names = get_bulk_part_names(bodies[0])
                assert names == {"KeepNB"}

    def test_all_items_excluded_skips_bulk_api_call(self, mock_endpoint, temp_workspace_dir):
        """When all items are excluded by filters, no bulk API call is made."""
        create_test_item(temp_workspace_dir, None, "OnlyItem", "Notebook", "nb-id-001")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace, item_name_exclude_regex="^OnlyItem$")

            assert workspace.bulk_publish_enabled is True
            assert len(bodies) == 0


# =============================================================================
# Bulk Publish Logical ID and Workspace ID Preservation Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishIdPreservation:
    """Tests that logical IDs and workspace IDs are preserved in the bulk payload (API resolves them)."""

    def test_logical_ids_preserved_in_bulk_payload(self, mock_endpoint, temp_workspace_dir):
        """Logical IDs in file content are sent as-is to bulk API (not replaced with GUIDs)."""
        create_test_item(temp_workspace_dir, None, "RefPipeline", "DataPipeline", "dp-logical-id-001")
        item_dir = create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-logical-id-001")
        (item_dir / "notebook-content.py").write_text(
            'pipeline_ref = "dp-logical-id-001"  # reference to RefPipeline', encoding="utf-8"
        )

        set_bulk_response(
            mock_endpoint,
            [
                {
                    "itemType": "Notebook",
                    "itemDisplayName": "TestNotebook",
                    "itemId": "nb-guid-001",
                    "operationType": "Create",
                },
                {
                    "itemType": "DataPipeline",
                    "itemDisplayName": "RefPipeline",
                    "itemId": "dp-guid-001",
                    "operationType": "Create",
                },
            ],
        )
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
        ) as workspace:
            publish.publish_all_items(workspace)

            content = get_bulk_part_content(bodies[0], "notebook-content")
            assert "dp-logical-id-001" in content, "Logical ID should be preserved"
            assert "dp-guid-001" not in content, "Logical ID should NOT be replaced with GUID"

    def test_workspace_id_placeholder_preserved_in_bulk_payload(self, mock_endpoint, temp_workspace_dir):
        """Default workspace ID placeholder (00000000-...) is preserved in bulk payload (API handles it)."""
        item_dir = create_test_item(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-logical-id-001")
        (item_dir / "notebook-content.py").write_text(
            'workspace_ref = "00000000-0000-0000-0000-000000000000"  # placeholder', encoding="utf-8"
        )
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace)

            content = get_bulk_part_content(bodies[0], "notebook-content")
            assert "00000000-0000-0000-0000-000000000000" in content, "Default GUID should be preserved"
            assert VALID_WORKSPACE_ID not in content, "Workspace ID should NOT be substituted"


# =============================================================================
# Bulk Publish Response Collection Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishResponseCollection:
    """Tests for enable_response_collection feature flag with bulk publish mode."""

    def test_response_collection_populates_responses_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """enable_response_collection populates workspace.responses with per-item details from bulk import."""
        with extra_flags(FeatureFlag.ENABLE_RESPONSE_COLLECTION):
            create_test_item(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
            create_test_item(temp_workspace_dir, None, "DP1", "DataPipeline", "dp-id-001")

            set_bulk_response(
                mock_endpoint,
                [
                    {
                        "itemType": "Notebook",
                        "itemDisplayName": "NB1",
                        "itemId": "nb-guid-001",
                        "operationType": "Create",
                    },
                    {
                        "itemType": "DataPipeline",
                        "itemDisplayName": "DP1",
                        "itemId": "dp-guid-001",
                        "operationType": "Update",
                    },
                ],
                header={"x-request-id": "req-123"},
                status_code=200,
            )

            with patched_workspace(
                mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
            ) as workspace:
                result = publish.publish_all_items(workspace)

                assert workspace.bulk_publish_enabled is True
                assert result is not None

                nb_resp = result["Notebook"]["NB1"]
                assert nb_resp["body"]["itemId"] == "nb-guid-001"
                assert nb_resp["body"]["operationType"] == "Create"
                assert nb_resp["header"] == {"x-request-id": "req-123"}
                assert nb_resp["status_code"] == 200

                assert result["DataPipeline"]["DP1"]["body"]["operationType"] == "Update"

    def test_no_response_collection_without_flag_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Without enable_response_collection, publish_all_items returns None and responses stay None."""
        create_test_item(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
        set_bulk_response(
            mock_endpoint,
            [
                {"itemType": "Notebook", "itemDisplayName": "NB1", "itemId": "nb-guid-001", "operationType": "Create"},
            ],
        )

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            result = publish.publish_all_items(workspace)

            assert workspace.bulk_publish_enabled is True
            assert result is None
            assert workspace.responses is None

    def test_response_collection_empty_when_all_items_filtered(self, mock_endpoint, temp_workspace_dir):
        """Response collection returns None when all items are filtered out (empty dict is falsy)."""
        with extra_flags(FeatureFlag.ENABLE_RESPONSE_COLLECTION):
            create_test_item(temp_workspace_dir, None, "FilteredNB", "Notebook", "nb-id-001")

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                result = publish.publish_all_items(workspace, item_name_exclude_regex="^FilteredNB$")

                assert workspace.bulk_publish_enabled is True
                assert result is None
