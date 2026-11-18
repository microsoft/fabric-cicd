# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test publishing functionality including selective publishing based on repository content."""

import base64
import json
import logging
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml
from fixtures.credentials import DummyTokenCredential

import fabric_cicd.publish as publish
from fabric_cicd import append_feature_flag, constants, get_supported_feature_flags
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError, PublishError
from fabric_cicd._common._item import Item
from fabric_cicd._common._validate_input import validate_workspace_capacity
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._notebook import NotebookPublisher
from fabric_cicd._items._orgapp import OrgAppPublisher
from fabric_cicd._items._orgappaudience import OrgAppAudiencePublisher
from fabric_cicd._items._paginatedreport import PaginatedReportPublisher
from fabric_cicd._items._warehouse import WarehousePublisher
from fabric_cicd.constants import API_FORMAT_MAPPING, FeatureFlag, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

# =============================================================================
# Shared Fixtures and Helpers
# =============================================================================


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()

    def mock_invoke(method, url, **_kwargs):
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}
        if method == "GET" and "/mirroredazuredatabrickscatalogs/" in url:
            return {
                "body": {
                    "properties": {
                        "sqlEndpointProperties": {
                            "id": "sql-endpoint-id",
                            "connectionString": "test.datawarehouse.fabric.microsoft.com",
                        }
                    }
                }
            }
        if method == "GET" and "workspaces" in url and not url.endswith("/items"):
            return {"body": {"value": [], "capacityId": "test-capacity"}}
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
# Workspace Capacity Validation
# =============================================================================


@pytest.fixture
def capacity_workspace():
    """Provide only the workspace fields needed by capacity validation."""
    return SimpleNamespace(
        workspace_id="12345678-1234-5678-abcd-1234567890ab",
        item_type_in_scope=["Warehouse"],
        endpoint=MagicMock(),
    )


@pytest.mark.parametrize(
    ("item_types", "response", "should_fail"),
    [
        pytest.param(["Warehouse"], {"body": {"capacityId": "capacity-id"}}, False, id="assigned-capacity"),
        pytest.param(["Warehouse"], {"body": {"capacityId": None}}, True, id="unassigned-capacity"),
        pytest.param(["Warehouse"], {"body": {"capacityId": ""}}, True, id="empty-capacity"),
        pytest.param(["Warehouse"], {"body": {}}, True, id="missing-capacity"),
        pytest.param(["Warehouse"], {}, True, id="missing-response-body"),
        pytest.param(["Report", "SemanticModel", "PaginatedReport"], {"body": {}}, False, id="all-types-exempt"),
        pytest.param(["Report", "Warehouse"], {"body": {}}, True, id="mixed-scope-requires-capacity"),
    ],
)
def test_validate_workspace_capacity(capacity_workspace, item_types, response, should_fail):
    """Capacity is required unless every scoped type is exempt, including when metadata is missing."""
    capacity_workspace.item_type_in_scope = item_types
    capacity_workspace.endpoint.invoke.return_value = response

    if should_fail:
        with pytest.raises(
            FailedPublishedItemStatusError,
            match=f"Workspace {capacity_workspace.workspace_id} does not have an assigned capacity",
        ):
            validate_workspace_capacity(capacity_workspace)
    else:
        assert validate_workspace_capacity(capacity_workspace) is None

    capacity_workspace.endpoint.invoke.assert_called_once_with(
        method="GET",
        url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{capacity_workspace.workspace_id}",
    )


def test_validate_workspace_capacity_propagates_api_errors(capacity_workspace):
    """An API failure must not be interpreted as missing or sufficient capacity."""
    api_error = RuntimeError("Capacity lookup failed")
    capacity_workspace.endpoint.invoke.side_effect = api_error

    with pytest.raises(RuntimeError, match="Capacity lookup failed") as exc_info:
        validate_workspace_capacity(capacity_workspace)

    assert exc_info.value is api_error
    capacity_workspace.endpoint.invoke.assert_called_once()


# =============================================================================
# Basic Publishing Tests
# =============================================================================


@pytest.fixture
def mirrored_databricks_workspace(mock_endpoint, temp_workspace_dir):
    """Load a deterministic Git-style catalog definition with mocked Fabric APIs."""
    temp_workspace_dir = temp_workspace_dir.resolve()
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    item_dir = create_test_item(
        temp_workspace_dir, None, "TestCatalog", item_type, "11111111-1111-1111-1111-111111111111"
    )
    (item_dir / "dummy.txt").unlink()
    platform_path = item_dir / ".platform"
    platform = json.loads(platform_path.read_text(encoding="utf-8"))
    platform["$schema"] = (
        "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    )
    platform["config"]["version"] = "2.0"
    platform_path.write_text(json.dumps(platform), encoding="utf-8")
    definition = {
        "$schema": (
            "https://developer.microsoft.com/json-schemas/fabric/item/mirroredAzureDatabricksCatalog/"
            "definition/mirroredAzureDatabricksCatalogDefinition/1.0.0/schema.json"
        ),
        "autoSync": "Enabled",
        "catalogName": "test_catalog",
        "databricksWorkspaceConnectionId": "22222222-2222-2222-2222-222222222222",
        "storageConnectionId": "33333333-3333-3333-3333-333333333333",
        "mirroringMode": "Partial",
        "mirrorConfiguration": {
            "schemas": [{"mirroringMode": "Full", "name": name} for name in ["schema_1", "schema_2", "schema_3"]]
        },
    }
    (item_dir / "definition.json").write_text(json.dumps(definition), encoding="utf-8")
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=[item_type],
            token_credential=DummyTokenCredential(),
        )
        workspace._refresh_repository_items()
        mock_endpoint.invoke.reset_mock()
        yield workspace


@pytest.mark.parametrize("deployed", [False, True], ids=["create", "update-definition"])
def test_mirrored_azure_databricks_catalog_publishes_definition(mirrored_databricks_workspace, mock_endpoint, deployed):
    """The standard publisher creates and updates the complete catalog definition."""
    workspace = mirrored_databricks_workspace
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    item = workspace.repository_items[item_type]["TestCatalog"]
    original_definition = json.loads((item.path / "definition.json").read_text(encoding="utf-8"))
    original_platform = json.loads((item.path / ".platform").read_text(encoding="utf-8"))
    if deployed:
        original_invoke = mock_endpoint.invoke.side_effect

        def invoke_with_existing_catalog(method, url, **kwargs):
            if method == "GET" and url == f"{workspace.base_api_url}/items":
                return {
                    "body": {
                        "value": [
                            {
                                "id": "44444444-4444-4444-4444-444444444444",
                                "type": item_type,
                                "displayName": item.name,
                                "description": item.description,
                            }
                        ]
                    }
                }
            return original_invoke(method, url, **kwargs)

        mock_endpoint.invoke.side_effect = invoke_with_existing_catalog

    publish.publish_all_items(workspace)
    item = workspace.repository_items[item_type]["TestCatalog"]

    api_calls = [call for call in mock_endpoint.invoke.call_args_list if call.kwargs.get("method") in {"POST", "PATCH"}]
    assert len(api_calls) == 1
    request = api_calls[0].kwargs
    assert request["method"] == "POST"
    if deployed:
        assert request["url"] == f"{workspace.base_api_url}/items/{item.guid}/updateDefinition?updateMetadata=True"
        assert set(request["body"]) == {"definition"}
        assert item.guid == "44444444-4444-4444-4444-444444444444"
    else:
        assert request["url"] == f"{workspace.base_api_url}/items"
        assert request["body"]["type"] == item_type
        assert request["body"]["displayName"] == item.name
        assert request["body"]["description"] == item.description
        assert item.guid == "mock-item-id"
    assert "creationPayload" not in request["body"]
    definition = request["body"]["definition"]
    assert "format" not in definition
    parts = {part["path"]: json.loads(base64.b64decode(part["payload"])) for part in definition["parts"]}
    assert parts == {".platform": original_platform, "definition.json": original_definition}
    assert all(part["payloadType"] == "InlineBase64" for part in definition["parts"])


@pytest.mark.parametrize("contains_param_item_vars", [False, True])
def test_mirrored_azure_databricks_catalog_waits_for_sql_endpoint(
    mirrored_databricks_workspace, mock_endpoint, contains_param_item_vars, monkeypatch, caplog
):
    """Publishing waits for the endpoint even without a dynamic SQL attribute reference."""
    workspace = mirrored_databricks_workspace
    workspace.contains_param_item_vars = contains_param_item_vars
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    original_invoke = mock_endpoint.invoke.side_effect
    endpoint_properties = [
        {},
        {"sqlEndpointProperties": {"id": "sql-endpoint-id"}},
        {"sqlEndpointProperties": {"id": "sql-endpoint-id", "connectionString": ""}},
        {
            "sqlEndpointProperties": {
                "id": "sql-endpoint-id",
                "connectionString": "test.datawarehouse.fabric.microsoft.com",
            }
        },
    ]
    responses = iter(endpoint_properties)
    observed_properties = []
    retry = MagicMock()
    monkeypatch.setattr("fabric_cicd.fabric_workspace.handle_retry", retry)

    def invoke_with_provisioning_endpoint(method, url, **kwargs):
        if method == "GET" and url == f"{workspace.base_api_url}/{item_type.lower()}s/mock-item-id":
            assert f"Published {item_type} 'TestCatalog'" not in caplog.text
            properties = next(responses)
            observed_properties.append(properties)
            return {"body": {"properties": properties}}
        return original_invoke(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = invoke_with_provisioning_endpoint
    with caplog.at_level(logging.INFO):
        publish.publish_all_items(workspace)

    assert observed_properties == endpoint_properties
    assert retry.call_count == 2
    assert f"Published {item_type} 'TestCatalog'" in caplog.text


def test_mirrored_azure_databricks_catalog_skips_sql_endpoint_wait(mirrored_databricks_workspace, mock_endpoint):
    """Excluded catalogs do not wait for an endpoint or report a successful publish."""
    workspace = mirrored_databricks_workspace

    with patch.object(workspace, "_wait_for_item_attribute_provisioning") as wait:
        publish.publish_all_items(workspace, item_name_exclude_regex="TestCatalog")

    wait.assert_not_called()
    assert workspace.repository_items[ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value]["TestCatalog"].skip_publish
    assert not any(call.kwargs.get("method") == "POST" for call in mock_endpoint.invoke.call_args_list)


def test_mirrored_azure_databricks_catalog_propagates_sql_endpoint_failure(
    mirrored_databricks_workspace, mock_endpoint, caplog
):
    """A failed endpoint lookup fails the catalog publish without reporting success."""
    workspace = mirrored_databricks_workspace
    original_invoke = mock_endpoint.invoke.side_effect

    def invoke_with_endpoint_failure(method, url, **kwargs):
        if method == "GET" and "/mirroredazuredatabrickscatalogs/" in url:
            message = "SQL endpoint lookup failed"
            raise RuntimeError(message)
        return original_invoke(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = invoke_with_endpoint_failure
    with caplog.at_level(logging.INFO), pytest.raises(PublishError, match="TestCatalog") as exc_info:
        publish.publish_all_items(workspace)

    assert len(exc_info.value.errors) == 1
    assert str(exc_info.value.errors[0][1]) == "SQL endpoint lookup failed"
    assert "Published MirroredAzureDatabricksCatalog 'TestCatalog'" not in caplog.text


@pytest.mark.parametrize("parameter_type", ["find_replace", "key_value_replace"])
@pytest.mark.parametrize("environment", ["DEV", "PROD"])
def test_mirrored_azure_databricks_catalog_parameterization(
    mirrored_databricks_workspace, mock_endpoint, parameter_type, environment
):
    """Real YAML parameterization replaces connection IDs and applies item/file filters."""
    workspace = mirrored_databricks_workspace
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    item = workspace.repository_items[item_type]["TestCatalog"]
    original_definition = json.loads((item.path / "definition.json").read_text(encoding="utf-8"))
    other_dir = create_test_item(
        workspace.repository_directory, None, "OtherCatalog", item_type, "99999999-9999-9999-9999-999999999999"
    )
    (other_dir / "dummy.txt").unlink()
    (other_dir / "definition.json").write_text(json.dumps(original_definition), encoding="utf-8")
    target_values = {
        "catalogName": {"DEV": "dev_catalog", "PROD": "prod_catalog"},
        "databricksWorkspaceConnectionId": {
            "DEV": "55555555-5555-5555-5555-555555555555",
            "PROD": "77777777-7777-7777-7777-777777777777",
        },
        "storageConnectionId": {
            "DEV": "66666666-6666-6666-6666-666666666666",
            "PROD": "88888888-8888-8888-8888-888888888888",
        },
    }
    parameters = []
    for field, replacements in target_values.items():
        find_entry = (
            {"find_key": f"$.{field}"}
            if parameter_type == "key_value_replace"
            else {"find_value": original_definition[field]}
        )
        parameters.append({
            **find_entry,
            "replace_value": replacements,
            "item_type": item_type,
            "item_name": "TestCatalog",
            "file_path": f"{item.path.name}/definition.json",
        })
    (workspace.repository_directory / "parameter.yml").write_text(
        yaml.safe_dump({parameter_type: parameters}), encoding="utf-8"
    )
    workspace = FabricWorkspace(
        workspace_id=workspace.workspace_id,
        repository_directory=str(workspace.repository_directory),
        environment=environment,
        item_type_in_scope=[item_type],
        token_credential=DummyTokenCredential(),
    )
    mock_endpoint.invoke.reset_mock()

    publish.publish_all_items(workspace)

    requests = {
        call.kwargs["body"]["displayName"]: call.kwargs["body"]
        for call in mock_endpoint.invoke.call_args_list
        if call.kwargs.get("method") == "POST" and call.kwargs["url"].endswith("/items")
    }
    assert set(requests) == {"TestCatalog", "OtherCatalog"}
    for name, body in requests.items():
        parts = {part["path"]: json.loads(base64.b64decode(part["payload"])) for part in body["definition"]["parts"]}
        expected = original_definition.copy()
        if name == "TestCatalog":
            expected.update({field: replacements[environment] for field, replacements in target_values.items()})
        assert parts["definition.json"] == expected
        assert parts[".platform"]["metadata"]["displayName"] == name
    assert json.loads((item.path / "definition.json").read_text(encoding="utf-8")) == original_definition


def test_mirrored_azure_databricks_catalog_publish_and_unpublish_order(monkeypatch):
    """Catalogs publish beside mirrored databases and unpublish after dependent items."""
    catalog_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG
    expected = [ItemType.MIRRORED_DATABASE, catalog_type, ItemType.NOTEBOOK]
    workspace = SimpleNamespace(
        item_type_in_scope=[item_type.value for item_type in reversed(expected)],
        repository_items={item_type.value: {"TestItem": object()} for item_type in expected},
        deployed_items={item_type.value: {"TestItem": object()} for item_type in expected},
    )
    monkeypatch.setattr(constants, "FEATURE_FLAG", set())

    assert [item_type for _, item_type in ItemPublisher.get_item_types_to_publish(workspace)] == expected
    assert ItemPublisher.get_item_types_to_unpublish(workspace) == [item_type.value for item_type in reversed(expected)]
    order = list(constants.SERIAL_ITEM_PUBLISH_ORDER.values())
    assert order.index(catalog_type) == order.index(ItemType.MIRRORED_DATABASE) + 1
    assert order.index(catalog_type) < order.index(ItemType.LAKEHOUSE)
    assert len(order) == len(set(order)) == len(ItemType)
    assert set(order) == set(ItemType)
    assert catalog_type.value not in constants.UNPUBLISH_FLAG_MAPPING
    assert ItemType.MIRRORED_DATABASE.value not in constants.UNPUBLISH_FLAG_MAPPING


def test_mirrored_azure_databricks_catalog_unpublishes_only_orphans(
    mirrored_databricks_workspace, mock_endpoint, monkeypatch
):
    """Like mirrored databases, in-scope orphaned catalogs can be deleted without a flag."""
    workspace = mirrored_databricks_workspace
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    monkeypatch.setattr(constants, "FEATURE_FLAG", set())
    original_invoke = mock_endpoint.invoke.side_effect

    def invoke_with_catalogs(method, url, **kwargs):
        if method == "GET" and url == f"{workspace.base_api_url}/items":
            return {
                "body": {
                    "value": [
                        {
                            "id": "44444444-4444-4444-4444-444444444444",
                            "type": item_type,
                            "displayName": "TestCatalog",
                            "description": "Retained catalog",
                        },
                        {
                            "id": "55555555-5555-5555-5555-555555555555",
                            "type": item_type,
                            "displayName": "OrphanCatalog",
                            "description": "Orphaned catalog",
                        },
                        {
                            "id": "66666666-6666-6666-6666-666666666666",
                            "type": ItemType.NOTEBOOK.value,
                            "displayName": "OutOfScope",
                            "description": "Out-of-scope notebook",
                        },
                    ]
                }
            }
        return original_invoke(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = invoke_with_catalogs
    with patch.object(workspace, "_unpublish_folders"):
        publish.unpublish_all_orphan_items(workspace)

    deletes = [call.kwargs for call in mock_endpoint.invoke.call_args_list if call.kwargs.get("method") == "DELETE"]
    assert len(deletes) == 1
    assert deletes[0]["url"] == f"{workspace.base_api_url}/items/55555555-5555-5555-5555-555555555555"


def test_mirrored_azure_databricks_catalog_sql_endpoint_attributes(mirrored_databricks_workspace, mock_endpoint):
    """Catalog SQL endpoint values are available to downstream dynamic parameterization."""
    workspace = mirrored_databricks_workspace
    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    catalog_id = "44444444-4444-4444-4444-444444444444"
    endpoint_id = "55555555-5555-5555-5555-555555555555"
    workspace.contains_param_item_vars = True

    def invoke_with_sql_endpoint(method, url, **_kwargs):
        assert method == "GET"
        if url == f"{workspace.base_api_url}/items":
            return {
                "body": {
                    "value": [{"id": catalog_id, "type": item_type, "displayName": "TestCatalog", "description": ""}]
                }
            }
        assert url == f"{workspace.base_api_url}/{item_type.lower()}s/{catalog_id}"
        return {
            "body": {"properties": {"sqlEndpointProperties": {"id": endpoint_id, "connectionString": "test.example"}}}
        }

    mock_endpoint.invoke.side_effect = invoke_with_sql_endpoint
    workspace._refresh_deployed_items()

    attributes = workspace.workspace_items[item_type]["TestCatalog"]
    assert attributes["id"] == catalog_id
    assert attributes["sqlendpointid"] == endpoint_id
    assert attributes["sqlendpoint"] == "test.example"


@pytest.mark.parametrize("parameter_type", ["find_replace", "key_value_replace"])
@pytest.mark.parametrize(
    ("publish_mode", "attribute"),
    [
        ("serial", "id"),
        ("serial", "sqlendpoint"),
        ("serial", "sqlendpointid"),
        ("bulk", "id"),
        ("bulk", "sqlendpoint"),
        ("bulk", "sqlendpointid"),
        ("serial-delayed", "sqlendpoint"),
        ("serial-delayed", "sqlendpointid"),
    ],
)
def test_mirrored_azure_databricks_catalog_downstream_dynamic_replacement(
    mirrored_databricks_workspace, mock_endpoint, monkeypatch, parameter_type, attribute, publish_mode
):
    """Resolve catalog attributes downstream only after serial or bulk endpoint waits complete."""
    workspace = mirrored_databricks_workspace
    catalog_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
    catalog_id = "44444444-4444-4444-4444-444444444444"
    endpoint_id = "55555555-5555-5555-5555-555555555555"
    consumer_id = "77777777-7777-7777-7777-777777777777"
    endpoint_connection = "test.example"
    expected = {"id": catalog_id, "sqlendpoint": endpoint_connection, "sqlendpointid": endpoint_id}[attribute]
    bulk_enabled = publish_mode == "bulk"
    delayed_endpoint = (bulk_enabled or publish_mode == "serial-delayed") and attribute != "id"
    monkeypatch.setattr(
        constants,
        "FEATURE_FLAG",
        {FeatureFlag.ENABLE_EXPERIMENTAL_FEATURES.value, FeatureFlag.ENABLE_BULK_PUBLISH.value}
        if bulk_enabled
        else set(),
    )
    consumer_dir = create_test_item(
        workspace.repository_directory,
        None,
        "Consumer",
        ItemType.NOTEBOOK.value,
        "66666666-6666-6666-6666-666666666666",
    )
    (consumer_dir / "dummy.txt").unlink()
    content_path = consumer_dir / "source.json"
    original_content = {"source": "source-placeholder"}
    content_path.write_text(json.dumps(original_content), encoding="utf-8")
    find_entry = (
        {"find_key": "$.source"} if parameter_type == "key_value_replace" else {"find_value": "source-placeholder"}
    )
    (workspace.repository_directory / "parameter.yml").write_text(
        yaml.safe_dump({
            parameter_type: [
                {
                    **find_entry,
                    "replace_value": {"TEST": f"$items.{catalog_type}.TestCatalog.{attribute}"},
                    "item_type": ItemType.NOTEBOOK.value,
                    "item_name": "Consumer",
                    "file_path": f"{consumer_dir.name}/source.json",
                }
            ]
        }),
        encoding="utf-8",
    )
    catalog_created = False
    endpoint_reads = 0
    events = []
    original_invoke = mock_endpoint.invoke.side_effect

    def invoke_with_catalog(method, url, **kwargs):
        nonlocal catalog_created, endpoint_reads
        if method == "GET" and url == f"{workspace.base_api_url}/items":
            deployed = (
                [{"id": catalog_id, "type": catalog_type, "displayName": "TestCatalog", "description": ""}]
                if catalog_created
                else []
            )
            return {"body": {"value": deployed}}
        if method == "GET" and url == f"{workspace.base_api_url}/{catalog_type.lower()}s/{catalog_id}":
            endpoint_reads += 1
            if delayed_endpoint and endpoint_reads == 1:
                events.append("endpoint-missing")
                return {"body": {"properties": {}}}
            if delayed_endpoint and endpoint_reads == 2:
                events.append("endpoint-ready")
            return {
                "body": {
                    "properties": {
                        "sqlEndpointProperties": {"id": endpoint_id, "connectionString": endpoint_connection}
                    }
                }
            }
        if method == "POST" and url == f"{workspace.base_api_url}/items/bulkImportDefinitions":
            platforms = [
                json.loads(base64.b64decode(part["payload"]))
                for part in kwargs["body"]["definitionParts"]
                if part["path"].endswith("/.platform")
            ]
            assert len(platforms) == 1
            metadata = platforms[0]["metadata"]
            if metadata["type"] == catalog_type:
                assert not catalog_created
                catalog_created = True
                deployed_id = catalog_id
            else:
                assert catalog_created
                assert not delayed_endpoint or endpoint_reads >= 2
                deployed_id = consumer_id
            events.append(f"publish-{metadata['displayName']}")
            return {
                "body": {
                    "importItemDefinitionsDetails": [
                        {
                            "itemType": metadata["type"],
                            "itemDisplayName": metadata["displayName"],
                            "itemId": deployed_id,
                            "operationType": "Create",
                        }
                    ]
                }
            }
        if method == "POST" and url == f"{workspace.base_api_url}/items":
            events.append(f"publish-{kwargs['body']['displayName']}")
            if kwargs["body"]["type"] == catalog_type:
                catalog_created = True
                return {"body": {"id": catalog_id}}
            assert catalog_created
            assert not delayed_endpoint or endpoint_reads >= 2
            return {"body": {"id": consumer_id}}
        return original_invoke(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = invoke_with_catalog
    workspace = FabricWorkspace(
        workspace_id=workspace.workspace_id,
        repository_directory=str(workspace.repository_directory),
        environment="TEST",
        item_type_in_scope=[ItemType.NOTEBOOK.value, catalog_type],
        token_credential=DummyTokenCredential(),
    )
    assert workspace.contains_param_item_vars
    mock_endpoint.invoke.reset_mock()

    with (
        patch(
            "fabric_cicd.fabric_workspace.handle_retry", side_effect=lambda **_kwargs: events.append("retry")
        ) as retry,
        patch.object(
            workspace, "_wait_for_item_attribute_provisioning", wraps=workspace._wait_for_item_attribute_provisioning
        ) as wait,
    ):
        publish.publish_all_items(workspace)

    assert workspace.bulk_publish_enabled == bulk_enabled
    if bulk_enabled:
        batches = [
            call.kwargs["body"]["definitionParts"]
            for call in mock_endpoint.invoke.call_args_list
            if call.kwargs.get("method") == "POST"
            and call.kwargs["url"] == f"{workspace.base_api_url}/items/bulkImportDefinitions"
        ]
        assert len(batches) == 2
        batch_names = [
            [
                json.loads(base64.b64decode(part["payload"]))["metadata"]["displayName"]
                for part in batch
                if part["path"].endswith("/.platform")
            ]
            for batch in batches
        ]
        assert batch_names == [["TestCatalog"], ["Consumer"]]
        consumer_payload = [{**part, "path": Path(part["path"]).name} for part in batches[1]]
        if delayed_endpoint:
            wait.assert_called_once_with(catalog_type, catalog_id, "TestCatalog", attribute)
        else:
            wait.assert_not_called()
    else:
        creates = [
            call.kwargs["body"]
            for call in mock_endpoint.invoke.call_args_list
            if call.kwargs.get("method") == "POST" and call.kwargs["url"] == f"{workspace.base_api_url}/items"
        ]
        assert [body["displayName"] for body in creates] == ["TestCatalog", "Consumer"]
        consumer_payload = creates[1]["definition"]["parts"]
        assert [call.args for call in wait.call_args_list] == [
            (catalog_type, catalog_id, "TestCatalog", "sqlendpointid"),
            (catalog_type, catalog_id, "TestCatalog", "sqlendpoint"),
        ]
    consumer_parts = {part["path"]: json.loads(base64.b64decode(part["payload"])) for part in consumer_payload}
    assert consumer_parts["source.json"] == {"source": expected}
    assert workspace.repository_items[catalog_type]["TestCatalog"].guid == catalog_id
    assert workspace.repository_items[ItemType.NOTEBOOK.value]["Consumer"].guid == consumer_id
    if delayed_endpoint:
        retry.assert_called_once()
        assert events == ["publish-TestCatalog", "endpoint-missing", "retry", "endpoint-ready", "publish-Consumer"]
    else:
        retry.assert_not_called()
    assert json.loads(content_path.read_text(encoding="utf-8")) == original_content


def test_mirrored_azure_databricks_catalog_propagates_api_failure(mirrored_databricks_workspace, mock_endpoint):
    """A failed catalog create fails deployment without recording a successful item ID."""
    workspace = mirrored_databricks_workspace
    original_invoke = mock_endpoint.invoke.side_effect

    def invoke_with_failure(method, url, **kwargs):
        if method == "POST" and url == f"{workspace.base_api_url}/items":
            message = "Catalog creation failed"
            raise RuntimeError(message)
        return original_invoke(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = invoke_with_failure
    with pytest.raises(PublishError, match="TestCatalog") as exc_info:
        publish.publish_all_items(workspace)
    assert len(exc_info.value.errors) == 1
    assert exc_info.value.errors[0][0] == "TestCatalog"
    assert str(exc_info.value.errors[0][1]) == "Catalog creation failed"
    assert workspace.repository_items[ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value]["TestCatalog"].guid == ""


@pytest.fixture
def warehouse_workspace(mock_endpoint, temp_workspace_dir, monkeypatch):
    """Create deterministic SQL project files and a workspace with mocked APIs."""
    monkeypatch.setattr(
        constants,
        "FEATURE_FLAG",
        constants.FEATURE_FLAG - {FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value},
    )
    item_dir = create_test_item(
        temp_workspace_dir, None, "TestWarehouse", "Warehouse", "11111111-1111-1111-1111-111111111111"
    )
    (item_dir / "sqldw.sqlproj").write_text(
        '<Project><Sdk Name="Microsoft.Build.Sql" Version="2.3.0" /></Project>', encoding="utf-8"
    )
    tables_dir = item_dir / "dbo" / "Tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "Table1.sql").write_text("CREATE TABLE dbo.Table1 (Id int);", encoding="utf-8")
    queries_dir = item_dir / ".sharedqueries"
    queries_dir.mkdir()
    (queries_dir / "query.sql").write_text("SELECT * FROM dbo.Table1;", encoding="utf-8")
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
            item_type_in_scope=["Warehouse"],
            token_credential=DummyTokenCredential(),
        )
        workspace._refresh_repository_items()
        mock_endpoint.invoke.reset_mock()
        yield workspace


@pytest.fixture
def warehouse_definition_workspace(warehouse_workspace):
    """Enable SQL project definition publishing for an isolated Warehouse workspace."""
    append_feature_flag(FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value)
    return warehouse_workspace


def test_warehouse_definition_publish_flag_is_supported():
    """Expose the opt-in flag through the public supported-feature list."""
    assert FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value in get_supported_feature_flags()


@pytest.mark.parametrize("deployed", [False, True])
@pytest.mark.parametrize(
    ("definition_enabled", "definition_type", "creation_payload"),
    [
        pytest.param(False, "sqlproj", None, id="definition-disabled"),
        pytest.param(
            False, "sqlproj", {"defaultCollation": "Latin1_General_100_BIN2_UTF8"}, id="disabled-with-payload"
        ),
        pytest.param(True, "sqlproj", {}, id="empty-creation-payload"),
        pytest.param(True, "sqlproj", {"defaultCollation": "Latin1_General_100_BIN2_UTF8"}, id="creation-payload"),
        pytest.param(True, "platform_only", None, id="platform-only"),
        pytest.param(True, "dacpac", None, id="dacpac-only"),
        pytest.param(True, "other_files", None, id="no-project"),
    ],
)
def test_warehouse_shell_only_publish(
    warehouse_workspace, mock_endpoint, caplog, deployed, definition_enabled, definition_type, creation_payload
):
    """Shell-only creates and updates preserve metadata, skip replacements, and warn only when opted in."""
    if definition_enabled:
        append_feature_flag(FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value)
    caplog.set_level(logging.WARNING, logger="fabric_cicd._items._warehouse")
    item = warehouse_workspace.repository_items["Warehouse"]["TestWarehouse"]
    platform = next(file for file in item.item_files if file.relative_path == ".platform")
    if creation_payload is not None:
        contents = json.loads(platform.contents)
        contents["metadata"]["creationPayload"] = creation_payload
        platform.contents = json.dumps(contents)
    if definition_type == "other_files":
        item.item_files = [file for file in item.item_files if file.file_path.suffix.lower() != ".sqlproj"]
    elif definition_type != "sqlproj":
        item.item_files = [platform]
        if definition_type == "dacpac":
            item.item_files.append(MagicMock(file_path=Path("sqldw.dacpac")))
    if deployed:
        item.guid = "22222222-2222-2222-2222-222222222222"

    with (
        patch.object(warehouse_workspace, "_replace_parameters") as parameters,
        patch.object(warehouse_workspace, "_replace_logical_ids") as logical_ids,
        patch.object(warehouse_workspace, "_replace_workspace_ids") as workspace_ids,
    ):
        WarehousePublisher(warehouse_workspace).publish_one(item.name, item)

    mock_endpoint.invoke.assert_called_once()
    request = mock_endpoint.invoke.call_args.kwargs
    assert request["method"] == ("PATCH" if deployed else "POST")
    assert request["url"].endswith(f"/items/{item.guid}" if deployed else "/items")
    assert request["body"]["displayName"] == item.name
    assert request["body"]["description"] == item.description
    assert "definition" not in request["body"]
    if creation_payload and not deployed:
        assert request["body"]["creationPayload"] == creation_payload
    else:
        assert "creationPayload" not in request["body"]
    parameters.assert_not_called()
    logical_ids.assert_not_called()
    workspace_ids.assert_not_called()
    warehouse_logs = [record for record in caplog.record_tuples if record[0] == "fabric_cicd._items._warehouse"]
    expected_logs = (
        [("fabric_cicd._items._warehouse", logging.WARNING, f"Warehouse '{item.name}' will be published as shell-only")]
        if definition_enabled
        else []
    )
    assert warehouse_logs == expected_logs


@pytest.mark.parametrize("deployed", [False, True])
def test_warehouse_publishes_sql_project_definition(warehouse_definition_workspace, mock_endpoint, deployed):
    """Opting in creates or updates the complete SQL project through the generic pipeline."""
    workspace = warehouse_definition_workspace
    item = workspace.repository_items["Warehouse"]["TestWarehouse"]
    for relative_path in [".gitignore", "dbo/.gitignore", "dbo/Tables/.gitignore"]:
        (item.path / relative_path).write_text("bin/\nobj/\n", encoding="utf-8")
    (item.path / "dbo" / "Tables" / ".gitignore.sql").write_text("-- SQL asset", encoding="utf-8")
    item.collect_item_files()
    if deployed:
        item.guid = "22222222-2222-2222-2222-222222222222"

    with (
        patch.object(workspace, "_refresh_repository_items"),
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda contents: contents) as logical_ids,
        patch.object(
            workspace, "_replace_parameters", side_effect=lambda file, _item: file.contents.replace("Table1", "Table2")
        ) as parameters,
        patch.object(
            workspace, "_replace_workspace_ids", side_effect=lambda contents, _item: contents
        ) as workspace_ids,
    ):
        publish.publish_all_items(workspace)

    api_calls = [call for call in mock_endpoint.invoke.call_args_list if call.kwargs.get("method") in {"POST", "PATCH"}]
    assert len(api_calls) == 1
    request = api_calls[0].kwargs
    assert request["method"] == "POST"
    if deployed:
        assert request["url"].endswith(f"/items/{item.guid}/updateDefinition?updateMetadata=True")
        assert set(request["body"]) == {"definition"}
    else:
        assert request["url"].endswith("/items")
        assert request["body"]["displayName"] == item.name
        assert request["body"]["description"] == item.description
        assert item.guid == "mock-item-id"
    assert "creationPayload" not in request["body"]
    definition = request["body"]["definition"]
    assert definition["format"] == "sqlproj"
    parts = {part["path"]: base64.b64decode(part["payload"]).decode() for part in definition["parts"]}
    assert parts["dbo/Tables/Table1.sql"] == "CREATE TABLE dbo.Table2 (Id int);"
    assert parts[".sharedqueries/query.sql"] == "SELECT * FROM dbo.Table2;"
    assert set(parts) == {
        ".platform",
        "dummy.txt",
        "sqldw.sqlproj",
        "dbo/Tables/Table1.sql",
        "dbo/Tables/.gitignore.sql",
        ".sharedqueries/query.sql",
    }
    assert parts["dbo/Tables/.gitignore.sql"] == "-- SQL asset"
    assert all(part["payloadType"] == "InlineBase64" for part in definition["parts"])
    assert parameters.call_count == logical_ids.call_count == workspace_ids.call_count == len(parts) - 1
    assert ItemType.WAREHOUSE.value not in constants.SHELL_ONLY_PUBLISH


@pytest.mark.parametrize(
    ("project_count", "project_contents"),
    [
        pytest.param(1, None, id="valid-project"),
        pytest.param(1, "<invalid", id="malformed-project"),
        pytest.param(2, "<invalid", id="duplicate-projects"),
    ],
)
def test_warehouse_definition_api_failure_is_propagated(
    warehouse_definition_workspace, mock_endpoint, project_count, project_contents
):
    """API failures propagate for valid or invalid SQL projects without retrying as shell-only."""
    item = warehouse_definition_workspace.repository_items["Warehouse"]["TestWarehouse"]
    project = next(file for file in item.item_files if file.name == "sqldw.sqlproj")
    if project_contents is not None:
        project.contents = project_contents
    item.item_files = [file for file in item.item_files if file is not project] + [project] * project_count

    api_error = RuntimeError("Definition deployment failed")
    mock_endpoint.invoke.side_effect = api_error

    with pytest.raises(RuntimeError, match="Definition deployment failed") as exc_info:
        WarehousePublisher(warehouse_definition_workspace).publish_one(item.name, item)

    assert exc_info.value is api_error
    mock_endpoint.invoke.assert_called_once()
    definition = mock_endpoint.invoke.call_args.kwargs["body"]["definition"]
    assert definition["format"] == "sqlproj"
    assert sum(part["path"] == "sqldw.sqlproj" for part in definition["parts"]) == project_count


@pytest.mark.parametrize(
    ("definition_enabled", "project_name", "metadata", "expected_shell_only"),
    [
        pytest.param(False, "Warehouse.sqlproj", {}, True, id="definition-disabled"),
        pytest.param(True, "Warehouse.sqlproj", {}, False, id="full-definition"),
        pytest.param(True, "Warehouse.SQLPROJ", {}, False, id="uppercase-project"),
        pytest.param(True, None, {}, True, id="no-project"),
        pytest.param(True, "Warehouse.sqlproj", {"creationPayload": None}, False, id="null-creation-payload"),
        pytest.param(True, "Warehouse.sqlproj", {"creationPayload": {}}, True, id="empty-creation-payload"),
        pytest.param(
            True,
            "Warehouse.sqlproj",
            {"creationPayload": {"defaultCollation": "Latin1_General_100_BIN2_UTF8"}},
            True,
            id="creation-payload",
        ),
    ],
)
def test_get_warehouse_publish_settings(monkeypatch, definition_enabled, project_name, metadata, expected_shell_only):
    """The helper returns the mode, unchanged payload, and definition flag without publishing."""
    monkeypatch.setattr(
        constants,
        "FEATURE_FLAG",
        {FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value} if definition_enabled else set(),
    )
    item = Item("Warehouse", "TestWarehouse", "", "")
    item.item_files = [
        SimpleNamespace(
            relative_path=".platform",
            file_path=Path(".platform"),
            contents=json.dumps({"metadata": metadata}),
        )
    ]
    if project_name is not None:
        item.item_files.append(SimpleNamespace(relative_path=project_name, file_path=Path(project_name)))

    assert WarehousePublisher._get_warehouse_publish_settings(item) == (
        expected_shell_only,
        metadata.get("creationPayload"),
        definition_enabled,
    )


@pytest.mark.parametrize("has_project", [False, True])
def test_warehouse_excluded_items_skip_validation_and_warning(
    warehouse_definition_workspace, mock_endpoint, caplog, has_project
):
    """Exclusion filters skip both native SQL projects and legacy folders without warnings."""
    caplog.set_level(logging.DEBUG, logger="fabric_cicd._items._warehouse")
    workspace = warehouse_definition_workspace
    workspace.publish_item_name_exclude_regex = "TestWarehouse"
    item = workspace.repository_items["Warehouse"]["TestWarehouse"]
    if not has_project:
        item.item_files = []

    WarehousePublisher(workspace).publish_all()

    assert item.skip_publish
    mock_endpoint.invoke.assert_not_called()
    assert "shell-only publishing" not in caplog.text


@pytest.mark.parametrize(
    ("item_type", "publisher_class"),
    [(ItemType.ORG_APP, OrgAppPublisher), (ItemType.ORG_APP_AUDIENCE, OrgAppAudiencePublisher)],
)
def test_org_app_publisher_uses_default_definition(item_type, publisher_class):
    """Both publishers use the generic definition pipeline, excluding app children."""
    workspace = MagicMock()
    publisher = ItemPublisher.create(item_type, workspace)

    assert isinstance(publisher, publisher_class)
    publisher.publish_one("TestItem", MagicMock())

    expected_kwargs = {"item_name": "TestItem", "item_type": item_type.value}
    if item_type == ItemType.ORG_APP:
        expected_kwargs["exclude_path"] = constants.EXCLUDE_PATH_REGEX_MAPPING[item_type.value]
    else:
        assert item_type.value not in constants.EXCLUDE_PATH_REGEX_MAPPING
    workspace._publish_item.assert_called_once_with(**expected_kwargs)
    assert item_type.value not in constants.SHELL_ONLY_PUBLISH
    assert item_type.value not in constants.API_FORMAT_MAPPING
    assert not publisher.has_dependency_tracking


def test_publish_org_app_with_nested_audience(mock_endpoint, temp_workspace_dir):
    """Child definitions stay out of the app payload and resolve the deployed parent ID."""
    app_logical_id = "11111111-1111-1111-1111-111111111111"
    app_dir = create_test_item(temp_workspace_dir, None, "TestApp", "OrgApp", app_logical_id)
    audience_dir = create_test_item(
        app_dir, ".children", "TestAudience", "OrgAppAudience", "22222222-2222-2222-2222-222222222222"
    )
    (app_dir / "definition.json").write_text(json.dumps({"elements": []}), encoding="utf-8")
    (audience_dir / "definition.json").write_text(
        json.dumps({"parentAppId": app_logical_id, "elementReferences": []}), encoding="utf-8"
    )

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
            item_type_in_scope=["OrgApp", "OrgAppAudience"],
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        bodies = [
            call.kwargs["body"]
            for call in mock_endpoint.invoke.call_args_list
            if call.kwargs.get("method") == "POST" and call.kwargs.get("url", "").endswith("/items")
        ]
        assert [body["type"] for body in bodies] == ["OrgApp", "OrgAppAudience"]
        app_parts = bodies[0]["definition"]["parts"]
        assert all(".children" not in part["path"] for part in app_parts)
        assert "definition.json" in {part["path"] for part in app_parts}
        audience_definition = next(
            part for part in bodies[1]["definition"]["parts"] if part["path"] == "definition.json"
        )
        assert json.loads(base64.b64decode(audience_definition["payload"]))["parentAppId"] == "mock-item-id"


def test_publish_org_app_before_audience(mock_endpoint, temp_workspace_dir):
    """Repository discovery and publishing place apps before audiences at the end."""
    create_test_item(temp_workspace_dir, None, "TestApp", "OrgApp", "11111111-1111-1111-1111-111111111111")
    create_test_item(temp_workspace_dir, None, "TestAudience", "OrgAppAudience", "22222222-2222-2222-2222-222222222222")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_publish_item") as mock_publish,
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["OrgAppAudience", "OrgApp"],
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert set(workspace.repository_items) == {"OrgApp", "OrgAppAudience"}
        assert [call.kwargs for call in mock_publish.call_args_list] == [
            {
                "item_name": "TestApp",
                "item_type": "OrgApp",
                "exclude_path": constants.EXCLUDE_PATH_REGEX_MAPPING["OrgApp"],
            },
            {"item_name": "TestAudience", "item_type": "OrgAppAudience"},
        ]
        assert list(constants.SERIAL_ITEM_PUBLISH_ORDER.values())[-2:] == [
            ItemType.ORG_APP,
            ItemType.ORG_APP_AUDIENCE,
        ]


def test_unpublish_org_app_audience_before_app(mock_endpoint, temp_workspace_dir):
    """Orphan audiences are deleted before their apps without feature flags."""
    deployed_items = {
        "OrgApp": {"TestApp": MagicMock(guid="11111111-1111-1111-1111-111111111111")},
        "OrgAppAudience": {"TestAudience": MagicMock(guid="22222222-2222-2222-2222-222222222222")},
    }

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(
            FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", deployed_items)
        ),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch.object(FabricWorkspace, "_unpublish_folders"),
        patch.object(FabricWorkspace, "_unpublish_item") as mock_unpublish,
        patch.object(constants, "FEATURE_FLAG", set()),
    ):
        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["OrgApp", "OrgAppAudience"],
            token_credential=DummyTokenCredential(),
        )

        publish.unpublish_all_orphan_items(workspace)

        assert [call.kwargs for call in mock_unpublish.call_args_list] == [
            {"item_name": "TestAudience", "item_type": "OrgAppAudience"},
            {"item_name": "TestApp", "item_type": "OrgApp"},
        ]


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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "Notebook" in workspace.repository_items
        assert "Environment" not in workspace.repository_items

        mock_notebook_cls.assert_called_once_with(workspace)
        mock_notebook_instance.publish_all.assert_called_once()
        mock_env_cls.assert_not_called()


@pytest.mark.usefixtures("experimental_feature_flags")
@pytest.mark.parametrize(
    "first_filters",
    [
        {"item_name_exclude_regex": r"^Private"},
        {"items_to_include": ["Public.Notebook"]},
    ],
)
def test_publish_filters_do_not_leak_between_calls(mock_endpoint, temp_workspace_dir, first_filters):
    """A selective publish must not silently constrain a later full publish."""
    create_test_item(temp_workspace_dir, None, "Public", "Notebook", "public-id")
    create_test_item(temp_workspace_dir, None, "Private", "Notebook", "private-id")

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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace, **first_filters)
        assert workspace.repository_items["Notebook"]["Private"].skip_publish is True

        publish.publish_all_items(workspace)
        assert workspace.repository_items["Notebook"]["Private"].skip_publish is False
        assert workspace.items_to_include is None
        assert workspace.publish_item_name_exclude_regex is None

        # Verify the previously excluded item actually reaches the API on the second call.
        created_names = [
            call.kwargs["body"]["displayName"]
            for call in mock_endpoint.invoke.call_args_list
            if call.kwargs.get("method") == "POST" and call.kwargs.get("url", "").endswith("/items")
        ]
        assert created_names.count("Private") == 1


@pytest.mark.usefixtures("experimental_feature_flags")
def test_folder_filters_do_not_leak_when_switching_selection(mock_endpoint, temp_workspace_dir):
    """Switching from exclusion to inclusion must not leave both filters active."""
    create_test_item(temp_workspace_dir, "archive", "Archived", "Notebook", "archived-id")
    create_test_item(temp_workspace_dir, "active", "Active", "Notebook", "active-id")

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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace, folder_path_exclude_regex=r"^/archive$")
        assert workspace.repository_items["Notebook"]["Archived"].skip_publish is True

        publish.publish_all_items(workspace, folder_path_to_include=["/archive"])
        assert workspace.repository_items["Notebook"]["Archived"].skip_publish is False
        assert workspace.repository_items["Notebook"]["Active"].skip_publish is True


def test_publish_ontology_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes Ontology items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestOntology", "Ontology", "test-ontology-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._ontology.OntologyPublisher") as mock_ontology_cls,
    ):
        mock_ontology_instance = mock_ontology_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "Ontology" in workspace.repository_items
        mock_ontology_cls.assert_called_once_with(workspace)
        mock_ontology_instance.publish_all.assert_called_once()


def test_publish_cosmosdbdatabase_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes Cosmos DB Database items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestCosmosDB", "CosmosDBDatabase", "test-cosmosdb-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._cosmosdbdatabase.CosmosDBDatabasePublisher") as mock_cosmos_cls,
    ):
        mock_cosmos_instance = mock_cosmos_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "CosmosDBDatabase" in workspace.repository_items
        mock_cosmos_cls.assert_called_once_with(workspace)
        mock_cosmos_instance.publish_all.assert_called_once()


def test_publish_map_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes Map items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestMap", "Map", "test-map-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._map.MapPublisher") as mock_map_cls,
    ):
        mock_map_instance = mock_map_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "Map" in workspace.repository_items
        mock_map_cls.assert_called_once_with(workspace)
        mock_map_instance.publish_all.assert_called_once()


def test_publish_graph_model_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes GraphModel items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestGraphModel", "GraphModel", "test-graph-model-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._graphmodel.GraphModelPublisher") as mock_graph_model_cls,
    ):
        mock_graph_model_instance = mock_graph_model_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "GraphModel" in workspace.repository_items
        mock_graph_model_cls.assert_called_once_with(workspace)
        mock_graph_model_instance.publish_all.assert_called_once()


def test_publish_data_build_tool_job_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes DataBuildToolJob items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestDbtJob", "DataBuildToolJob", "test-dbt-job-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._databuildtooljob.DataBuildToolJobPublisher") as mock_dbt_job_cls,
    ):
        mock_dbt_job_instance = mock_dbt_job_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "DataBuildToolJob" in workspace.repository_items
        mock_dbt_job_cls.assert_called_once_with(workspace)
        mock_dbt_job_instance.publish_all.assert_called_once()


def test_publish_paginated_report_item(mock_endpoint, temp_workspace_dir):
    """Test that publish_all_items publishes PaginatedReport items when present in repository."""
    create_test_item(temp_workspace_dir, None, "TestPaginatedReport", "PaginatedReport", "test-paginated-report-id")

    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
        patch("fabric_cicd._items._paginatedreport.PaginatedReportPublisher") as mock_paginated_report_cls,
    ):
        mock_paginated_report_instance = mock_paginated_report_cls.return_value

        workspace = FabricWorkspace(
            workspace_id="12345678-1234-5678-abcd-1234567890ab",
            repository_directory=str(temp_workspace_dir),
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace)

        assert "PaginatedReport" in workspace.repository_items
        mock_paginated_report_cls.assert_called_once_with(workspace)
        mock_paginated_report_instance.publish_all.assert_called_once()


def test_paginated_report_publisher_uses_sequential_mode():
    """Paginated reports should publish sequentially to avoid intermittent updateDefinition failures."""
    assert PaginatedReportPublisher.parallel_config.enabled is False


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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
        ("legacy", "TestCosmosDB", "CosmosDBDatabase", "test-cosmosdb-id"),
        ("legacy", "TestEventhouse", "Eventhouse", "test-eventhouse-id"),
        ("legacy", "TestGraphModel", "GraphModel", "test-graph-model-id"),
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
            item_type_in_scope=[
                "Lakehouse",
                "Warehouse",
                "SQLDatabase",
                "CosmosDBDatabase",
                "Eventhouse",
                "GraphModel",
            ],
            token_credential=DummyTokenCredential(),
        )

        publish.unpublish_all_orphan_items(workspace)

        expected_warnings = [
            "Skipping unpublish for Lakehouse items because the 'enable_lakehouse_unpublish' feature flag is not enabled.",
            "Skipping unpublish for Warehouse items because the 'enable_warehouse_unpublish' feature flag is not enabled.",
            "Skipping unpublish for SQLDatabase items because the 'enable_sqldatabase_unpublish' feature flag is not enabled.",
            "Skipping unpublish for CosmosDBDatabase items because the 'enable_cosmosdbdatabase_unpublish' feature flag is not enabled.",
            "Skipping unpublish for Eventhouse items because the 'enable_eventhouse_unpublish' feature flag is not enabled.",
            "Skipping unpublish for GraphModel items because the 'enable_graphmodel_unpublish' feature flag is not enabled.",
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
                token_credential=DummyTokenCredential(),
            )

            publish.unpublish_all_orphan_items(workspace)

            assert "enable_lakehouse_unpublish" not in caplog.text
            assert "Skipping unpublish for Lakehouse" not in caplog.text

    finally:
        constants.FEATURE_FLAG.clear()
        constants.FEATURE_FLAG.update(original_flags)


def test_unpublish_graph_model_with_feature_flag_enabled(mock_endpoint, temp_workspace_dir, caplog):
    """Test that orphaned Graph Models are unpublished only when their feature flag is enabled."""
    deployed_items = {"GraphModel": {"OrphanGraph": MagicMock(guid="orphan-graph-id")}}
    unpublish_calls = []

    def track_unpublish(_self, item_name, item_type):
        unpublish_calls.append((item_name, item_type))

    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.add("enable_graphmodel_unpublish")

    try:
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace,
                "_refresh_deployed_items",
                new=lambda self: setattr(self, "deployed_items", deployed_items),
            ),
            patch.object(
                FabricWorkspace,
                "_refresh_repository_items",
                new=lambda self: setattr(self, "repository_items", {}),
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(FabricWorkspace, "_unpublish_folders", new=lambda _: None),
            patch.object(FabricWorkspace, "_unpublish_item", new=track_unpublish),
            caplog.at_level(logging.WARNING),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=["GraphModel"],
                token_credential=DummyTokenCredential(),
            )

            publish.unpublish_all_orphan_items(workspace)

            assert unpublish_calls == [("OrphanGraph", "GraphModel")]
            assert "enable_graphmodel_unpublish" not in caplog.text

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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(
            workspace,
            folder_path_exclude_regex=r"^/legacy",
            items_to_include=["ImportantNotebook.Notebook", "StandaloneNotebook.Notebook"],
        )

        assert workspace.repository_items["Notebook"]["ImportantNotebook"].skip_publish is True
        assert workspace.repository_items["Notebook"]["StandaloneNotebook"].skip_publish is False
        # OtherNotebook is excluded by get_items_to_publish() because it is not in
        # items_to_include, so publish_all() marks it skip_publish=True.
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
            token_credential=DummyTokenCredential(),
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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(
            workspace,
            folder_path_to_include=["/active"],
            items_to_include=["Notebook1.Notebook"],
        )

        assert workspace.repository_items["Notebook"]["Notebook1"].skip_publish is False
        # Notebook2 and ArchivedNotebook are excluded by get_items_to_publish()
        # because they are not in items_to_include, so publish_all() marks them skip_publish=True.
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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(
            workspace,
            item_name_exclude_regex=r"^Debug.*",
            folder_path_to_include=["/active"],
            items_to_include=["TargetNotebook.Notebook"],
        )

        # DebugNotebook, OtherNotebook, and ArchivedNotebook are excluded by
        # get_items_to_publish() because they are not in items_to_include, so
        # publish_all() marks them skip_publish=True.
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

    def test_files_sorted_same_stem_content_before_settings(self, publisher):
        """Test content file precedes settings even when filenames share the same stem."""
        mock_platform = MagicMock()
        mock_platform.file_path = Path(".platform")
        mock_settings = MagicMock()
        mock_settings.file_path = Path("notebook-settings.json")
        mock_content = MagicMock()
        mock_content.file_path = Path("notebook-content.py")

        mock_item = MagicMock()
        mock_item.item_files = [mock_settings, mock_content, mock_platform]

        publisher.publish_one("test_notebook", mock_item)

        assert mock_item.item_files[0].file_path.name == ".platform"
        assert mock_item.item_files[1].file_path.name == "notebook-content.py"
        assert mock_item.item_files[2].file_path.name == "notebook-settings.json"

    def test_files_sorted_with_unknown_extension(self, publisher):
        """Test that unknown file extensions get default priority (2) between content and settings."""
        mock_platform = MagicMock()
        mock_platform.file_path = Path(".platform")
        mock_settings = MagicMock()
        mock_settings.file_path = Path("notebook.json")
        mock_content = MagicMock()
        mock_content.file_path = Path("notebook.py")
        mock_other = MagicMock()
        mock_other.file_path = Path("readme.md")  # Unknown extension

        mock_item = MagicMock()
        mock_item.item_files = [mock_settings, mock_other, mock_content, mock_platform]

        publisher.publish_one("test_notebook", mock_item)

        # Expected order: .platform (0), notebook.py (1), readme.md (2), notebook.json (3)
        assert mock_item.item_files[0].file_path.name == ".platform"
        assert mock_item.item_files[1].file_path.name == "notebook.py"
        assert mock_item.item_files[2].file_path.name == "readme.md"
        assert mock_item.item_files[3].file_path.name == "notebook.json"


# =============================================================================
# Empty items_to_include Tests
# =============================================================================


@pytest.mark.usefixtures("experimental_feature_flags")
def test_empty_items_to_include_skips_all_items(mock_endpoint, temp_workspace_dir):
    """Test that passing an empty list to items_to_include deploys nothing."""
    create_test_item(temp_workspace_dir, None, "NotebookA", "Notebook", "notebook-a-id")
    create_test_item(temp_workspace_dir, None, "NotebookB", "Notebook", "notebook-b-id")

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
            token_credential=DummyTokenCredential(),
        )

        publish.publish_all_items(workspace, items_to_include=[])

        # All items should be marked as skip_publish since none match the empty include list
        assert workspace.repository_items["Notebook"]["NotebookA"].skip_publish is True
        assert workspace.repository_items["Notebook"]["NotebookB"].skip_publish is True


@pytest.mark.usefixtures("experimental_feature_flags")
def test_empty_items_to_include_skips_environment_publish_state_checks(mock_endpoint, temp_workspace_dir):
    """Test that items_to_include=[] does not trigger environment publish-state checks for all environments."""
    create_test_item(temp_workspace_dir, None, "EnvA", "Environment", "env-a-id")
    create_test_item(temp_workspace_dir, None, "EnvB", "Environment", "env-b-id")

    # Configure the mock to return environments with a "running" state.
    # If filtering is broken (empty list treated as None), the publish-state
    # checker would see these as in-progress and loop/retry indefinitely.
    original_side_effect = mock_endpoint.invoke.side_effect

    def env_aware_invoke(method, url, **kwargs):
        if method == "GET" and url.endswith("/environments/"):
            return {
                "body": {
                    "value": [
                        {
                            "displayName": "EnvA",
                            "properties": {"publishDetails": {"state": "Running"}},
                        },
                        {
                            "displayName": "EnvB",
                            "properties": {"publishDetails": {"state": "Running"}},
                        },
                    ]
                }
            }
        return original_side_effect(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = env_aware_invoke

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
            item_type_in_scope=["Environment"],
            token_credential=DummyTokenCredential(),
        )

        # This must complete without hanging — if filtering is broken,
        # _check_environment_publish_state would see "Running" environments
        # and retry indefinitely.
        publish.publish_all_items(workspace, items_to_include=[])

        # All environment items should be marked as skip_publish
        assert workspace.repository_items["Environment"]["EnvA"].skip_publish is True
        assert workspace.repository_items["Environment"]["EnvB"].skip_publish is True
