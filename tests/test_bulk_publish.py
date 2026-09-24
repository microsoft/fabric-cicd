# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk publish feature: flag validation, fallback logic, item preparation, and end-to-end bulk flow."""

import base64
import json
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fixtures.credentials import DummyTokenCredential

import fabric_cicd.publish as publish
from fabric_cicd import constants
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._bulk_publish_dependencies import (
    _get_referencing_item_keys,
    build_dynamic_variable_dependency_graph,
    build_logical_id_links,
    compute_publish_batches,
    get_async_attributes_to_wait_for,
    has_unfiltered_items_variable,
)
from fabric_cicd.constants import FeatureFlag, ItemType
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


def create_test_item_dir(base_path: Path, folder, name: str, item_type: str, logical_id: str) -> Path:
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
            create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
            with (
                patched_workspace(mock_endpoint, temp_workspace_dir) as workspace,
                pytest.raises(InputError, match="requires 'enable_experimental_features'"),
            ):
                publish.publish_all_items(workspace)

    @pytest.mark.usefixtures("bulk_publish_flags")
    def test_bulk_publish_enabled_with_both_flags(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish is enabled when both feature flags are set with supported item types."""
        create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

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
        create_test_item_dir(temp_workspace_dir, None, "TestWarehouse", "Warehouse", "wh-id-001")

        with (
            patched_workspace(mock_endpoint, temp_workspace_dir, item_type_in_scope=["Warehouse"]) as workspace,
            patch("fabric_cicd._items._warehouse.WarehousePublisher", return_value=MagicMock()),
        ):
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is False

    @pytest.mark.parametrize(
        "param_yaml",
        [
            'find_replace:\n  - find_value: "some-id"\n    replace_value:\n      PPE: "$items.Notebook.TestNotebook.$id"\n',
            'key_value_replace:\n  - find_key: "$.some.path"\n    replace_value:\n      PPE: "$items.Notebook.TestNotebook.$id"\n',
        ],
        ids=["find_replace", "key_value_replace"],
    )
    def test_fallback_on_unfiltered_items_variable(self, mock_endpoint, temp_workspace_dir, param_yaml):
        """Bulk falls back only when an $items.* replace_value has no item_type/item_name/file_path filter."""
        create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_parameter_file(temp_workspace_dir, param_yaml)

        with (
            patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace,
            patch("fabric_cicd._items._notebook.NotebookPublisher", return_value=MagicMock()),
        ):
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is False
            assert workspace.contains_param_item_vars is True

    @pytest.mark.parametrize(
        ("param_yaml", "contains_item_var"),
        [
            # Filtered current-workspace $items.* -> supported via staged publishing
            (
                'find_replace:\n  - find_value: "some-id"\n    item_type: "Notebook"\n    replace_value:\n      PPE: "$items.Notebook.TestNotebook.$id"\n',
                True,
            ),
            # $workspace.* -> resolved upfront, no dependency
            ('find_replace:\n  - find_value: "some-id"\n    replace_value:\n      PPE: "$workspace.$id"\n', False),
            # Cross-workspace item variable -> targets another workspace, resolved upfront
            (
                'find_replace:\n  - find_value: "some-id"\n    replace_value:\n      PPE: "$workspace.other_ws.$items.Notebook.some_item.$id"\n',
                False,
            ),
            # Dynamic find_value -> resolved upfront, does not gate bulk
            (
                'find_replace:\n  - find_value: "$workspace.source_ws.$items.Notebook.some_lakehouse.$id"\n    replace_value:\n      PPE: "replacement-id"\n',
                False,
            ),
        ],
        ids=["filtered_items", "workspace_var", "cross_workspace_item", "dynamic_find_value"],
    )
    def test_no_fallback_on_supported_dynamic_variables(
        self, mock_endpoint, temp_workspace_dir, param_yaml, contains_item_var
    ):
        """Bulk stays enabled for $workspace.*, cross-workspace, dynamic find_value, and filtered $items.* vars."""
        create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
        create_parameter_file(temp_workspace_dir, param_yaml)

        with (
            patched_workspace(mock_endpoint, temp_workspace_dir, environment="PPE") as workspace,
            patch.object(ItemPublisher, "publish_all_bulk", return_value=[]),
        ):
            publish.publish_all_items(workspace)
            assert workspace.bulk_publish_enabled is True
            assert workspace.contains_param_item_vars is contains_item_var

    def test_no_fallback_without_dynamic_variables(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish remains enabled when parameter file has no dynamic replacement variables."""
        create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
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
            assert workspace.contains_param_item_vars is False

    def test_item_name_exclude_regex_supported_in_bulk(self, mock_endpoint, temp_workspace_dir, caplog):
        """item_name_exclude_regex does not cause fallback -- filtering is applied in bulk Phase 1."""
        with extra_flags(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE):
            create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")

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
            create_test_item_dir(temp_workspace_dir, None, f"Notebook{i}", "Notebook", f"nb-id-{i:04d}")

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

    def test_bulk_publish_assigns_guids_from_response(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish assigns item GUIDs from the API response."""
        create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
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
        """Bulk publish sends all item types in a single POST with allowPairingByName."""
        create_test_item_dir(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
        create_test_item_dir(temp_workspace_dir, None, "NB2", "Notebook", "nb-id-002")
        create_test_item_dir(temp_workspace_dir, None, "DP1", "DataPipeline", "dp-id-001")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["Notebook", "DataPipeline"]
        ) as workspace:
            publish.publish_all_items(workspace)

            assert len(bodies) == 1
            assert bodies[0]["options"]["allowPairingByName"] is True
            names = get_bulk_part_names(bodies[0])
            assert names == {"NB1", "NB2", "DP1"}
            assert len(bodies[0]["definitionParts"]) >= 6

    def test_bulk_publish_empty_workspace_no_api_call(self, mock_endpoint, temp_workspace_dir):
        """Bulk publish with no items in repository completes without calling the bulk API."""
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace)

            assert len(bodies) == 0


# =============================================================================
# Bulk Publish with Parameterization Tests
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishParameterization:
    """Tests for file content replacement applied during bulk publish."""

    def test_static_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Static find_replace parameters are applied to file content in the bulk payload."""
        item_dir = create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-id-001")
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

            assert len(bodies) == 1
            content = get_bulk_part_content(bodies[0], "notebook-content")
            assert "new-connection-string" in content
            assert "old-connection-string" not in content

    def test_spark_pool_parameters_applied_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Spark pool instance_pool_id is replaced in Environment files during bulk publish."""
        item_dir = create_test_item_dir(temp_workspace_dir, None, "TestEnv", "Environment", "env-id-001")
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
        create_test_item_dir(temp_workspace_dir, None, "TestModel", "SemanticModel", "sm-id-001")

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

        def sm_invoke(method, url, **kwargs):
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
            if method == "POST" and "bulkImportDefinitions" in url:
                return {
                    "body": {
                        "importItemDefinitionsDetails": [
                            {
                                "itemType": "SemanticModel",
                                "itemDisplayName": "TestModel",
                                "itemId": "sm-guid-001",
                                "operationType": "Create",
                            },
                        ]
                    }
                }
            return original(method, url, **kwargs)

        mock_endpoint.invoke.side_effect = sm_invoke

        with patched_workspace(
            mock_endpoint, temp_workspace_dir, item_type_in_scope=["SemanticModel"], environment="PPE"
        ) as workspace:
            publish.publish_all_items(workspace)

            assert len(bind_calls) == 1, "bindConnection API should be called exactly once"

    def test_variable_library_value_set_activated_in_bulk_mode(self, mock_endpoint, temp_workspace_dir):
        """Variable library value set is activated via post_publish_all hook after bulk publish."""
        item_dir = create_test_item_dir(temp_workspace_dir, None, "TestVarLib", "VariableLibrary", "vl-id-001")
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

            assert len(patch_calls) == 1
            assert patch_calls[0]["properties"]["activeValueSetName"] == "PPE"

    def test_lakehouse_shortcuts_published_after_bulk_upload(self, mock_endpoint, temp_workspace_dir):
        """Lakehouse shortcuts are published via post_publish_all Phase 3 after bulk import."""
        with extra_flags(FeatureFlag.ENABLE_SHORTCUT_PUBLISH):
            item_dir = create_test_item_dir(temp_workspace_dir, None, "TestLH", "Lakehouse", "lh-id-001")
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
        create_test_item_dir(temp_workspace_dir, None, "KeepNotebook", "Notebook", "nb-id-001")
        create_test_item_dir(temp_workspace_dir, None, "ExcludeMe", "Notebook", "nb-id-002")
        create_test_item_dir(temp_workspace_dir, None, "ExcludeAlso", "Notebook", "nb-id-003")
        bodies = capture_bulk_bodies(mock_endpoint)

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            publish.publish_all_items(workspace, item_name_exclude_regex="^Exclude.*")

            names = get_bulk_part_names(bodies[0])
            assert "KeepNotebook" in names
            assert "ExcludeMe" not in names
            assert "ExcludeAlso" not in names

    def test_items_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in the items_to_include list are sent to the bulk API call."""
        with extra_flags(FeatureFlag.ENABLE_ITEMS_TO_INCLUDE):
            create_test_item_dir(temp_workspace_dir, None, "IncludedNB", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, None, "ExcludedNB", "Notebook", "nb-id-002")
            create_test_item_dir(temp_workspace_dir, None, "IncludedDP", "DataPipeline", "dp-id-001")
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
            create_test_item_dir(temp_workspace_dir, "keep_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, "exclude_folder", "DropNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/exclude_folder")

                names = get_bulk_part_names(bodies[0])
                assert names == {"KeepNB"}

    def test_folder_path_exclude_regex_cascades_to_descendants_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Excluding a parent folder also excludes items in descendant folders."""
        with extra_flags(FeatureFlag.ENABLE_EXCLUDE_FOLDER):
            create_test_item_dir(temp_workspace_dir, None, "RootNB", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, "parent/child", "ChildNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_exclude_regex="^/parent")

                names = get_bulk_part_names(bodies[0])
                assert names == {"RootNB"}

    def test_folder_path_to_include_filters_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Only items in included folders are sent to the bulk API call."""
        with extra_flags(FeatureFlag.ENABLE_INCLUDE_FOLDER):
            create_test_item_dir(temp_workspace_dir, "included_folder", "IncNB", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, "other_folder", "OtherNB", "Notebook", "nb-id-002")
            bodies = capture_bulk_bodies(mock_endpoint)

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                publish.publish_all_items(workspace, folder_path_to_include=["/included_folder"])

                names = get_bulk_part_names(bodies[0])
                assert names == {"IncNB"}

    def test_combined_item_exclude_and_folder_exclude_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """item_name_exclude_regex and folder_path_exclude_regex work together in bulk mode."""
        with extra_flags(FeatureFlag.ENABLE_EXCLUDE_FOLDER):
            create_test_item_dir(temp_workspace_dir, "good_folder", "KeepNB", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, "good_folder", "DropByName", "Notebook", "nb-id-002")
            create_test_item_dir(temp_workspace_dir, "bad_folder", "DropByFolder", "Notebook", "nb-id-003")
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
        create_test_item_dir(temp_workspace_dir, None, "OnlyItem", "Notebook", "nb-id-001")
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
        create_test_item_dir(temp_workspace_dir, None, "RefPipeline", "DataPipeline", "dp-logical-id-001")
        item_dir = create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-logical-id-001")
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
        item_dir = create_test_item_dir(temp_workspace_dir, None, "TestNotebook", "Notebook", "nb-logical-id-001")
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
            create_test_item_dir(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
            create_test_item_dir(temp_workspace_dir, None, "DP1", "DataPipeline", "dp-id-001")

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

                assert result is not None

                nb_resp = result["Notebook"]["NB1"]
                assert nb_resp["body"]["itemId"] == "nb-guid-001"
                assert nb_resp["body"]["operationType"] == "Create"
                assert nb_resp["header"] == {"x-request-id": "req-123"}
                assert nb_resp["status_code"] == 200

                assert result["DataPipeline"]["DP1"]["body"]["operationType"] == "Update"

    def test_no_response_collection_without_flag_in_bulk(self, mock_endpoint, temp_workspace_dir):
        """Without enable_response_collection, publish_all_items returns None and responses stay None."""
        create_test_item_dir(temp_workspace_dir, None, "NB1", "Notebook", "nb-id-001")
        set_bulk_response(
            mock_endpoint,
            [
                {"itemType": "Notebook", "itemDisplayName": "NB1", "itemId": "nb-guid-001", "operationType": "Create"},
            ],
        )

        with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
            result = publish.publish_all_items(workspace)

            assert result is None
            assert workspace.responses is None

    def test_response_collection_empty_when_all_items_filtered(self, mock_endpoint, temp_workspace_dir):
        """Response collection returns None when all items are filtered out (empty dict is falsy)."""
        with extra_flags(FeatureFlag.ENABLE_RESPONSE_COLLECTION):
            create_test_item_dir(temp_workspace_dir, None, "FilteredNB", "Notebook", "nb-id-001")

            with patched_workspace(mock_endpoint, temp_workspace_dir) as workspace:
                result = publish.publish_all_items(workspace, item_name_exclude_regex="^FilteredNB$")

                assert result is None


# =============================================================================
# Dependency Graph Construction
# =============================================================================


def _graph_workspace(environment_parameter, deployed_items, repository_items):
    """Minimal workspace stub for dependency-graph helpers."""
    return SimpleNamespace(
        environment="PPE",
        environment_parameter=environment_parameter,
        deployed_items=deployed_items,
        repository_items=repository_items,
        repository_directory=Path(),
    )


def _repo_item(path=None):
    return SimpleNamespace(path=path)


class TestGetReferencingItemKeys:
    """Tests for matching repository items against parameter filters."""

    def test_no_filters_returns_all_items(self):
        repo = {
            "Notebook": {"Shared": _repo_item(), "NotebookOnly": _repo_item()},
            "DataPipeline": {"Shared": _repo_item()},
        }

        assert _get_referencing_item_keys({}, repo, Path()) == [
            "Notebook.Shared",
            "Notebook.NotebookOnly",
            "DataPipeline.Shared",
        ]

    def test_list_filters_match_any_value_in_each_filter(self):
        repo = {
            "Notebook": {"Shared": _repo_item(), "NotebookOnly": _repo_item()},
            "DataPipeline": {"Shared": _repo_item(), "PipelineOnly": _repo_item()},
            "Lakehouse": {"Shared": _repo_item()},
        }
        filters = {
            "item_type": ["Notebook", "DataPipeline"],
            "item_name": ["Shared", "PipelineOnly"],
        }

        assert _get_referencing_item_keys(filters, repo, Path()) == [
            "Notebook.Shared",
            "DataPipeline.Shared",
            "DataPipeline.PipelineOnly",
        ]

    def test_name_filter_matches_same_name_across_types(self):
        repo = {
            "Notebook": {"Shared": _repo_item()},
            "DataPipeline": {"Shared": _repo_item()},
        }

        assert _get_referencing_item_keys({"item_name": "Shared"}, repo, Path()) == [
            "Notebook.Shared",
            "DataPipeline.Shared",
        ]

    def test_path_filter_returns_containing_item(self, tmp_path):
        notebook_path = tmp_path / "Shared.Notebook"
        pipeline_path = tmp_path / "Load.DataPipeline"
        notebook_path.mkdir()
        pipeline_path.mkdir()
        (notebook_path / "content.py").write_text("", encoding="utf-8")
        (pipeline_path / "content.json").write_text("{}", encoding="utf-8")
        repo = {
            "Notebook": {"Shared": _repo_item(notebook_path)},
            "DataPipeline": {"Load": _repo_item(pipeline_path)},
        }

        result = _get_referencing_item_keys({"file_path": "Shared.Notebook/content.py"}, repo, tmp_path.resolve(), {})

        assert result == ["Notebook.Shared"]

    def test_all_filters_must_match_same_item(self, tmp_path):
        shared_path = tmp_path / "Shared.Notebook"
        other_path = tmp_path / "Other.Notebook"
        shared_path.mkdir()
        other_path.mkdir()
        (shared_path / "content.py").write_text("", encoding="utf-8")
        repo = {
            "Notebook": {
                "Shared": _repo_item(shared_path),
                "Other": _repo_item(other_path),
            }
        }

        result = _get_referencing_item_keys(
            {
                "item_type": "Notebook",
                "item_name": ["Shared", "Other"],
                "file_path": "Shared.Notebook/content.py",
            },
            repo,
            tmp_path.resolve(),
            {},
        )

        assert result == ["Notebook.Shared"]

    def test_multiple_paths_match_items_of_different_types(self, tmp_path):
        notebook_path = tmp_path / "Shared.Notebook"
        pipeline_path = tmp_path / "Load.DataPipeline"
        notebook_path.mkdir()
        pipeline_path.mkdir()
        (notebook_path / "content.py").write_text("", encoding="utf-8")
        (pipeline_path / "content.json").write_text("{}", encoding="utf-8")
        repo = {
            "Notebook": {"Shared": _repo_item(notebook_path)},
            "DataPipeline": {"Load": _repo_item(pipeline_path)},
        }

        result = _get_referencing_item_keys(
            {
                "file_path": [
                    "Shared.Notebook/content.py",
                    "Load.DataPipeline/content.json",
                ]
            },
            repo,
            tmp_path.resolve(),
            {},
        )

        assert result == ["Notebook.Shared", "DataPipeline.Load"]

    def test_type_and_path_must_match_same_item(self, tmp_path):
        notebook_path = tmp_path / "Shared.Notebook"
        pipeline_path = tmp_path / "Load.DataPipeline"
        notebook_path.mkdir()
        pipeline_path.mkdir()
        (pipeline_path / "content.json").write_text("{}", encoding="utf-8")
        repo = {
            "Notebook": {"Shared": _repo_item(notebook_path)},
            "DataPipeline": {"Load": _repo_item(pipeline_path)},
        }

        result = _get_referencing_item_keys(
            {"item_type": "Notebook", "file_path": "Load.DataPipeline/content.json"},
            repo,
            tmp_path.resolve(),
            {},
        )

        assert result == []

    def test_missing_path_returns_no_items(self, tmp_path):
        notebook_path = tmp_path / "Shared.Notebook"
        notebook_path.mkdir()
        repo = {"Notebook": {"Shared": _repo_item(notebook_path)}}

        result = _get_referencing_item_keys({"file_path": "Shared.Notebook/missing.py"}, repo, tmp_path.resolve(), {})

        assert result == []


class TestBulkPublishDependencyGraph:
    """Tests for build_dynamic_variable_dependency_graph and has_unfiltered_items_variable."""

    def test_edge_created_for_new_referenced_item(self):
        """A filtered $items.* ref to an item new in the batch produces a dependency edge."""
        env = {
            "find_replace": [
                {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        edges = build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"})

        assert edges == [("DataPipeline.PL", "Lakehouse.LH")]

    def test_no_edge_when_referenced_item_already_deployed(self):
        """No edge when the referenced item is already deployed (resolvable in a single batch)."""
        env = {
            "find_replace": [
                {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={"Lakehouse": {"LH": {"id": "guid"}}}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"}) == []

    def test_excluded_undeployed_reference_raises_direct_error(self):
        """A published item cannot reference a prerequisite that is excluded and not deployed."""
        env = {
            "find_replace": [
                {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        with pytest.raises(
            InputError,
            match=re.escape("referenced item 'Lakehouse.LH' is excluded from this deployment and does not exist"),
        ):
            build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL"})

    def test_excluded_deployed_reference_is_resolvable(self):
        """An excluded prerequisite is valid when it already exists in the target workspace."""
        env = {
            "find_replace": [
                {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={"Lakehouse": {"LH": {"id": "guid"}}}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL"}) == []

    def test_missing_reference_ignored_when_rule_matches_no_published_item(self):
        """An unavailable reference is irrelevant when its parameter rule applies only to excluded items."""
        env = {
            "find_replace": [
                {"item_type": "Notebook", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {
            "DataPipeline": {"PL": _repo_item()},
            "Notebook": {"NB": _repo_item()},
            "Lakehouse": {"LH": _repo_item()},
        }
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL"}) == []

    def test_no_edge_for_workspace_variable(self):
        """$workspace.* references never create dependency edges."""
        env = {"find_replace": [{"item_type": "DataPipeline", "replace_value": {"PPE": "$workspace.$id"}}]}
        repo = {"DataPipeline": {"PL": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL"}) == []

    def test_self_reference_does_not_create_dependency_edge(self):
        """An item referencing itself does not create a circular dependency."""
        env = {
            "find_replace": [
                {"item_type": "Lakehouse", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"Lakehouse.LH"}) == []

    def test_broad_filter_excludes_only_self_reference(self):
        """A broad rule drops its self-edge while retaining dependencies from other matching items."""
        env = {
            "find_replace": [
                {
                    "item_name": ["LH", "PL"],
                    "replace_value": {"PPE": "$items.Lakehouse.LH.$id"},
                },
            ]
        }
        repo = {"Lakehouse": {"LH": _repo_item()}, "DataPipeline": {"PL": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"Lakehouse.LH", "DataPipeline.PL"}) == [
            ("DataPipeline.PL", "Lakehouse.LH")
        ]

    def test_key_value_replace_creates_dependency_edge(self):
        """A filtered key_value_replace item variable creates the same dependency as find_replace."""
        env = {
            "key_value_replace": [
                {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"}) == [
            ("DataPipeline.PL", "Lakehouse.LH")
        ]

    def test_duplicate_parameter_rules_create_one_dependency_edge(self):
        """Equivalent rules across parameter sections do not duplicate an item dependency."""
        rule = {"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}}
        env = {"find_replace": [rule], "key_value_replace": [rule]}
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"}) == [
            ("DataPipeline.PL", "Lakehouse.LH")
        ]

    def test_list_valued_item_type_filter_creates_edge(self):
        """A list-valued item_type filter matches any listed item type."""
        env = {
            "find_replace": [
                {"item_type": ["DataPipeline", "Notebook"], "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"}) == [
            ("DataPipeline.PL", "Lakehouse.LH")
        ]

    def test_list_valued_item_name_filter_creates_edge(self):
        """A list-valued item_name filter matches any listed item name."""
        env = {
            "find_replace": [
                {"item_name": ["PL"], "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}},
            ]
        }
        repo = {"DataPipeline": {"PL": _repo_item()}, "Lakehouse": {"LH": _repo_item()}}
        ws = _graph_workspace(env, deployed_items={}, repository_items=repo)

        assert build_dynamic_variable_dependency_graph(ws, {"DataPipeline.PL", "Lakehouse.LH"}) == [
            ("DataPipeline.PL", "Lakehouse.LH")
        ]

    def test_has_unfiltered_items_variable_true(self):
        """An $items.* replace_value with no filter is reported as unfiltered."""
        env = {"find_replace": [{"replace_value": {"PPE": "$items.Lakehouse.LH.$id"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})

        assert has_unfiltered_items_variable(ws) is True

    def test_has_unfiltered_items_variable_false_when_filtered(self):
        """An $items.* replace_value carrying any filter is not reported as unfiltered."""
        env = {"find_replace": [{"item_type": "DataPipeline", "replace_value": {"PPE": "$items.Lakehouse.LH.$id"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})

        assert has_unfiltered_items_variable(ws) is False

    def test_has_unfiltered_items_variable_false_for_workspace_var(self):
        """Unfiltered $workspace.* variables do not gate bulk publish."""
        env = {"find_replace": [{"replace_value": {"PPE": "$workspace.$id"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})

        assert has_unfiltered_items_variable(ws) is False


# =============================================================================
# Publish Batch Computation (topological sort)
# =============================================================================


def _batch_ctx(key):
    """Build an (item_name, item, publisher) context tuple from a 'Type.Name' key."""
    item_type, item_name = key.split(".", 1)
    return (item_name, SimpleNamespace(type=item_type), object())


def _batch_keys(batches):
    """Reduce computed batches to a list of key sets for assertion."""
    return [{f"{item.type}.{name}" for name, item, _ in batch} for batch in batches]


def _ordered_batch_keys(batches):
    """Reduce computed batches to item keys while preserving batch and item order."""
    return [[f"{item.type}.{name}" for name, item, _ in batch] for batch in batches]


class TestComputePublishBatches:
    """Tests for compute_publish_batches."""

    def test_no_edges_single_batch(self):
        items = [_batch_ctx("Notebook.A"), _batch_ctx("Notebook.B")]
        batches = compute_publish_batches(items, [])
        assert batches == [items]

    def test_oversized_single_batch_error_has_no_batch_number(self, monkeypatch):
        monkeypatch.setattr(constants, "BULK_ITEM_COUNT_LIMIT", 1)
        items = [_batch_ctx("Notebook.A"), _batch_ctx("Notebook.B")]

        with pytest.raises(InputError) as exc_info:
            compute_publish_batches(items, [])

        assert "Bulk publish batch item count (2)" in str(exc_info.value)
        assert "batch 1" not in str(exc_info.value)

    def test_oversized_later_batch_error_includes_batch_number(self, monkeypatch):
        monkeypatch.setattr(constants, "BULK_ITEM_COUNT_LIMIT", 1)
        items = [_batch_ctx("Lakehouse.Base"), _batch_ctx("Notebook.X"), _batch_ctx("Notebook.Y")]
        edges = [("Notebook.X", "Lakehouse.Base"), ("Notebook.Y", "Lakehouse.Base")]

        with pytest.raises(InputError, match=r"Bulk publish batch 2 item count \(2\)"):
            compute_publish_batches(items, edges)

    def test_linear_chain_produces_ordered_stages(self):
        items = [_batch_ctx("Notebook.A"), _batch_ctx("Lakehouse.B"), _batch_ctx("DataPipeline.C")]
        # C depends on B, B depends on A
        edges = [("DataPipeline.C", "Lakehouse.B"), ("Lakehouse.B", "Notebook.A")]
        assert _batch_keys(compute_publish_batches(items, edges)) == [
            {"Notebook.A"},
            {"Lakehouse.B"},
            {"DataPipeline.C"},
        ]

    def test_shared_dependency_schedules_dependents_in_later_stage(self):
        items = [_batch_ctx("Lakehouse.Base"), _batch_ctx("Notebook.X"), _batch_ctx("Notebook.Y")]
        edges = [("Notebook.X", "Lakehouse.Base"), ("Notebook.Y", "Lakehouse.Base")]
        result = _batch_keys(compute_publish_batches(items, edges))
        assert result[0] == {"Lakehouse.Base"}
        assert result[1] == {"Notebook.X", "Notebook.Y"}

    def test_circular_dependency_raises(self):
        items = [_batch_ctx("Notebook.A"), _batch_ctx("Lakehouse.B")]
        edges = [("Notebook.A", "Lakehouse.B"), ("Lakehouse.B", "Notebook.A")]
        with pytest.raises(InputError, match="Circular dynamic variable dependency"):
            compute_publish_batches(items, edges)

    def test_linked_items_form_publish_unit_within_stage(self):
        # Lakehouse.LH is referenced by a dynamic variable in Notebook.NB (LH must precede NB).
        # DataPipeline.PL references Notebook.NB by logical ID, so PL must ship with NB.
        items = [_batch_ctx("Lakehouse.LH"), _batch_ctx("Notebook.NB"), _batch_ctx("DataPipeline.PL")]
        dependency_edges = [("Notebook.NB", "Lakehouse.LH")]
        logical_id_links = [("DataPipeline.PL", "Notebook.NB")]
        result = _batch_keys(compute_publish_batches(items, dependency_edges, logical_id_links))
        assert result == [{"Lakehouse.LH"}, {"Notebook.NB", "DataPipeline.PL"}]

    def test_transitively_linked_items_share_stage_in_input_order(self):
        """Connected logical-ID links form one unit whose members retain their input order."""
        items = [
            _batch_ctx("Notebook.C"),
            _batch_ctx("Lakehouse.Base"),
            _batch_ctx("Notebook.A"),
            _batch_ctx("Notebook.B"),
        ]
        dependency_edges = [("Notebook.A", "Lakehouse.Base")]
        logical_id_links = [("Notebook.A", "Notebook.B"), ("Notebook.B", "Notebook.C")]

        assert _ordered_batch_keys(compute_publish_batches(items, dependency_edges, logical_id_links)) == [
            ["Lakehouse.Base"],
            ["Notebook.C", "Notebook.A", "Notebook.B"],
        ]

    def test_publish_unit_without_dependencies_yields_single_batch(self):
        items = [_batch_ctx("Notebook.NB"), _batch_ctx("DataPipeline.PL")]
        logical_id_links = [("DataPipeline.PL", "Notebook.NB")]
        result = _batch_keys(compute_publish_batches(items, [], logical_id_links))
        assert result == [{"Notebook.NB", "DataPipeline.PL"}]

    def test_dependency_within_publish_unit_raises(self):
        # A dynamic-variable ordering edge within one publish unit is unsatisfiable in bulk.
        items = [_batch_ctx("Lakehouse.LH"), _batch_ctx("Notebook.NB")]
        dependency_edges = [("Notebook.NB", "Lakehouse.LH")]
        logical_id_links = [("Notebook.NB", "Lakehouse.LH")]
        with pytest.raises(InputError, match="logical-ID reference"):
            compute_publish_batches(items, dependency_edges, logical_id_links)


def _logical_ctx(key, logical_id, contents="", file_name="definition.json"):
    """Build an (item_name, item, publisher) tuple with a logical ID and one text file."""
    item_type, item_name = key.split(".", 1)
    text_file = SimpleNamespace(name=file_name, type="text", contents=contents)
    item = SimpleNamespace(type=item_type, logical_id=logical_id, item_files=[text_file])
    return (item_name, item, object())


class TestBuildLogicalIdLinks:
    """Tests for build_logical_id_links."""

    def test_no_references_returns_empty(self):
        items = [
            _logical_ctx("Notebook.A", "11111111-1111-1111-1111-111111111111", contents="no refs here"),
            _logical_ctx("Lakehouse.B", "22222222-2222-2222-2222-222222222222", contents="also nothing"),
        ]
        assert build_logical_id_links(items) == []

    def test_reference_detected_as_undirected_edge(self):
        pipeline_content = "referenced by 22222222-2222-2222-2222-222222222222 here"
        items = [
            _logical_ctx("Notebook.A", "11111111-1111-1111-1111-111111111111", contents="standalone"),
            _logical_ctx("Lakehouse.B", "22222222-2222-2222-2222-222222222222", contents="lakehouse body"),
            _logical_ctx("DataPipeline.C", "33333333-3333-3333-3333-333333333333", contents=pipeline_content),
        ]
        edges = build_logical_id_links(items)
        assert edges == [("DataPipeline.C", "Lakehouse.B")]

    def test_platform_file_is_not_scanned_for_references(self):
        items = [
            _logical_ctx("Notebook.A", "11111111-1111-1111-1111-111111111111"),
            _logical_ctx(
                "Lakehouse.B",
                "22222222-2222-2222-2222-222222222222",
                contents="11111111-1111-1111-1111-111111111111",
                file_name=".platform",
            ),
        ]

        assert build_logical_id_links(items) == []

    def test_multiple_references_are_detected_once(self):
        pipeline_content = (
            "refs 11111111-1111-1111-1111-111111111111 and 22222222-2222-2222-2222-222222222222 "
            "and duplicate 22222222-2222-2222-2222-222222222222"
        )
        items = [
            _logical_ctx("Notebook.A", "11111111-1111-1111-1111-111111111111", contents="standalone"),
            _logical_ctx("Lakehouse.B", "22222222-2222-2222-2222-222222222222", contents="standalone"),
            _logical_ctx("DataPipeline.C", "33333333-3333-3333-3333-333333333333", contents=pipeline_content),
        ]

        assert set(build_logical_id_links(items)) == {
            ("DataPipeline.C", "Lakehouse.B"),
            ("DataPipeline.C", "Notebook.A"),
        }


# =============================================================================
# Staged Execution Loop (publish_all_bulk)
# =============================================================================


@pytest.mark.usefixtures("bulk_publish_flags")
class TestBulkPublishStagedExecution:
    """Tests that publish_all_bulk issues one bulk call per batch and refreshes between stages."""

    def _workspace_with_two_items(self):
        """MagicMock workspace whose Phase-1 collection yields two Notebook items."""
        base = SimpleNamespace(type="Notebook")
        dep = SimpleNamespace(type="Notebook")

        publisher = MagicMock()
        publisher.get_items_to_publish.return_value = {"base": base, "dep": dep}
        publisher.pre_publish_all.return_value = None
        publisher.post_publish_all.return_value = None
        publisher.has_async_publish_check = False

        ws = MagicMock()
        ws.contains_param_item_vars = False  # skip graph build; batches are injected below
        ws._apply_publish_filters.return_value = False
        ws._dynamic_var_cache = {"$workspace.$id": "wsid", "$items.Notebook.base.$id": "stale"}
        return ws, publisher, base, dep

    def test_one_bulk_call_per_batch_with_refresh_between(self):
        ws, publisher, base, dep = self._workspace_with_two_items()
        two_batches = [[("base", base, publisher)], [("dep", dep, publisher)]]

        with (
            patch.object(ItemPublisher, "get_item_types_to_publish", return_value=[(1, ItemType.NOTEBOOK)]),
            patch.object(ItemPublisher, "create", return_value=publisher),
            patch.object(ItemPublisher, "_mark_skipped_items", return_value=[]),
            patch(
                "fabric_cicd._items._bulk_publish_dependencies.compute_publish_batches",
                return_value=two_batches,
            ),
        ):
            ItemPublisher.publish_all_bulk(ws)

        # One bulk API call per batch
        assert ws._publish_items.call_count == 2
        # Deployed items refreshed once, between the two batches
        assert ws._refresh_deployed_items.call_count == 1
        # Stale $items.* cache entry dropped between stages; $workspace.* entry retained
        assert ws._dynamic_var_cache == {"$workspace.$id": "wsid"}

    def test_single_batch_makes_no_refresh(self):
        ws, publisher, base, dep = self._workspace_with_two_items()
        single_batch = [[("base", base, publisher), ("dep", dep, publisher)]]

        with (
            patch.object(ItemPublisher, "get_item_types_to_publish", return_value=[(1, ItemType.NOTEBOOK)]),
            patch.object(ItemPublisher, "create", return_value=publisher),
            patch.object(ItemPublisher, "_mark_skipped_items", return_value=[]),
            patch(
                "fabric_cicd._items._bulk_publish_dependencies.compute_publish_batches",
                return_value=single_batch,
            ),
        ):
            ItemPublisher.publish_all_bulk(ws)

        assert ws._publish_items.call_count == 1
        assert ws._refresh_deployed_items.call_count == 0
        # Cache untouched for a single batch
        assert ws._dynamic_var_cache == {"$workspace.$id": "wsid", "$items.Notebook.base.$id": "stale"}

    def test_waits_for_async_attribute_before_refresh(self):
        """A source item referenced downstream via an async attribute is waited on between stages."""
        base = SimpleNamespace(type="Lakehouse", guid="lh-guid")
        dep = SimpleNamespace(type="Notebook", guid="nb-guid")

        publisher = MagicMock()
        publisher.get_items_to_publish.return_value = {"base": base, "dep": dep}
        publisher.has_async_publish_check = False

        ws = MagicMock()
        ws.contains_param_item_vars = True
        ws._apply_publish_filters.return_value = False
        ws._dynamic_var_cache = {}

        two_batches = [[("base", base, publisher)], [("dep", dep, publisher)]]

        with (
            patch.object(ItemPublisher, "get_item_types_to_publish", return_value=[(1, ItemType.LAKEHOUSE)]),
            patch.object(ItemPublisher, "create", return_value=publisher),
            patch.object(ItemPublisher, "_mark_skipped_items", return_value=[]),
            patch(
                "fabric_cicd._items._bulk_publish_dependencies.build_dynamic_variable_dependency_graph",
                return_value=[],
            ),
            patch(
                "fabric_cicd._items._bulk_publish_dependencies.get_async_attributes_to_wait_for",
                return_value={"Lakehouse.base": {"sqlendpoint"}},
            ),
            patch(
                "fabric_cicd._items._bulk_publish_dependencies.compute_publish_batches",
                return_value=two_batches,
            ),
        ):
            ItemPublisher.publish_all_bulk(ws)

        # The source item's async attribute is awaited before the refresh feeds the next stage
        ws._wait_for_item_attribute_provisioning.assert_called_once_with("Lakehouse", "lh-guid", "base", "sqlendpoint")
        assert ws._refresh_deployed_items.call_count == 1
        staged_calls = [
            call[0]
            for call in ws.mock_calls
            if call[0] in {"_publish_items", "_wait_for_item_attribute_provisioning", "_refresh_deployed_items"}
        ]
        assert staged_calls == [
            "_publish_items",
            "_wait_for_item_attribute_provisioning",
            "_refresh_deployed_items",
            "_publish_items",
        ]


# =============================================================================
# Async Attributes to Wait For (SQL endpoint / query URI)
# =============================================================================


class TestAsyncAttributesToWaitFor:
    """Map published items to asynchronously provisioned attributes that must be awaited."""

    def test_sqlendpoint_reference_mapped(self):
        env = {"find_replace": [{"replace_value": {"PPE": "$items.Lakehouse.LH.$sqlendpoint"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})
        assert get_async_attributes_to_wait_for(ws, {"Lakehouse.LH"}) == {"Lakehouse.LH": {"sqlendpoint"}}

    def test_id_attribute_is_not_async(self):
        """$id resolves immediately from the items list and needs no provisioning wait."""
        env = {"find_replace": [{"replace_value": {"PPE": "$items.Lakehouse.LH.$id"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})
        assert get_async_attributes_to_wait_for(ws, {"Lakehouse.LH"}) == {}

    def test_reference_outside_publish_set_ignored(self):
        env = {"find_replace": [{"replace_value": {"PPE": "$items.Lakehouse.LH.$sqlendpoint"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})
        assert get_async_attributes_to_wait_for(ws, {"Notebook.NB"}) == {}

    def test_multiple_attributes_aggregated_per_source(self):
        env = {
            "find_replace": [
                {"replace_value": {"PPE": "$items.Lakehouse.LH.$sqlendpoint"}},
                {"replace_value": {"PPE": "$items.Lakehouse.LH.$sqlendpointid"}},
                {"replace_value": {"PPE": "$items.Eventhouse.EH.$queryserviceuri"}},
            ]
        }
        ws = _graph_workspace(env, deployed_items={}, repository_items={})
        assert get_async_attributes_to_wait_for(ws, {"Lakehouse.LH", "Eventhouse.EH"}) == {
            "Lakehouse.LH": {"sqlendpoint", "sqlendpointid"},
            "Eventhouse.EH": {"queryserviceuri"},
        }

    def test_cross_workspace_reference_ignored(self):
        """Cross-workspace items live in another workspace and impose no in-batch provisioning wait."""
        env = {"find_replace": [{"replace_value": {"PPE": "$workspace.other.$items.Lakehouse.LH.$sqlendpoint"}}]}
        ws = _graph_workspace(env, deployed_items={}, repository_items={})
        assert get_async_attributes_to_wait_for(ws, {"Lakehouse.LH"}) == {}


# =============================================================================
# Async Attribute Provisioning Wait (_wait_for_item_attribute_provisioning)
# =============================================================================


def _lakehouse_response(connection_string=None, provisioning_status=None):
    """Build a Lakehouse GET response with optional SQL endpoint connection string / status."""
    sql_props = {}
    if connection_string is not None:
        sql_props["connectionString"] = connection_string
    if provisioning_status is not None:
        sql_props["provisioningStatus"] = provisioning_status
    properties = {"sqlEndpointProperties": sql_props} if sql_props else {}
    return {"body": {"properties": properties}}


class TestWaitForItemAttributeProvisioning:
    """FabricWorkspace._wait_for_item_attribute_provisioning polls until an async attribute is ready."""

    def _fake_ws(self, endpoint):
        return SimpleNamespace(base_api_url="https://api/v1/workspaces/ws", endpoint=endpoint)

    def test_returns_immediately_when_attribute_present(self):
        endpoint = MagicMock()
        endpoint.invoke.return_value = _lakehouse_response(connection_string="sql.endpoint.fabric")
        ws = self._fake_ws(endpoint)

        FabricWorkspace._wait_for_item_attribute_provisioning(ws, "Lakehouse", "guid-1", "LH", "sqlendpoint")

        assert endpoint.invoke.call_count == 1

    def test_polls_until_attribute_provisioned(self):
        endpoint = MagicMock()
        endpoint.invoke.side_effect = [
            _lakehouse_response(provisioning_status="InProgress"),
            _lakehouse_response(connection_string="sql.endpoint.fabric"),
        ]
        ws = self._fake_ws(endpoint)

        with patch("fabric_cicd.fabric_workspace.handle_retry") as mock_retry:
            FabricWorkspace._wait_for_item_attribute_provisioning(ws, "Lakehouse", "guid-1", "LH", "sqlendpoint")

        assert endpoint.invoke.call_count == 2
        assert mock_retry.call_count == 1

    def test_raises_on_failed_provisioning(self):
        endpoint = MagicMock()
        endpoint.invoke.return_value = _lakehouse_response(provisioning_status="Failed")
        ws = self._fake_ws(endpoint)

        with pytest.raises(FailedPublishedItemStatusError):
            FabricWorkspace._wait_for_item_attribute_provisioning(ws, "Lakehouse", "guid-1", "LH", "sqlendpoint")

    def test_noop_for_empty_guid(self):
        endpoint = MagicMock()
        ws = self._fake_ws(endpoint)

        FabricWorkspace._wait_for_item_attribute_provisioning(ws, "Lakehouse", "", "LH", "sqlendpoint")

        endpoint.invoke.assert_not_called()

    def test_noop_for_unmapped_attribute(self):
        endpoint = MagicMock()
        ws = self._fake_ws(endpoint)

        # Notebook has no async-provisioned attribute mapping
        FabricWorkspace._wait_for_item_attribute_provisioning(ws, "Notebook", "guid-1", "NB", "sqlendpoint")

        endpoint.invoke.assert_not_called()
