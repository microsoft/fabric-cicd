# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Regression tests: items_to_include + semantic model connection binding.

Same root cause as issue #948: when items_to_include scopes to a subset of
semantic models, excluded models have skip_publish=True and guid="" after
publish_all(). bind_semanticmodel_to_connection() must not attempt to bind
them — doing so would produce URLs like
  GET  items//connections
  POST semanticModels//bindConnection
which return HTTP 400.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from fabric_cicd._common._item import Item
from fabric_cicd._items._semanticmodel import SemanticModelPublisher, bind_semanticmodel_to_connection
from fabric_cicd.fabric_workspace import FabricWorkspace


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sm(name: str, guid: str = "", skip_publish: bool = False) -> Item:
    """Create a real SemanticModel Item for testing."""
    item = Item(type="SemanticModel", name=name, description="", guid=guid)
    item.skip_publish = skip_publish
    return item


def _make_connections(*conn_ids: str) -> dict:
    """Build a minimal connections dict keyed by connection ID."""
    return {
        cid: {"id": cid, "connectivityType": "ShareableCloud", "connectionDetails": {"type": "SQL", "path": "srv"}}
        for cid in conn_ids
    }


# ---------------------------------------------------------------------------
# Unit tests for bind_semanticmodel_to_connection()
# ---------------------------------------------------------------------------


def test_bind_skips_model_with_skip_publish_true():
    """
    bind_semanticmodel_to_connection() must skip models whose skip_publish=True.
    No HTTP call should be made for the excluded model.
    """
    included = _make_sm("SalesModel", guid="sales-guid", skip_publish=False)
    excluded = _make_sm("DevModel", guid="", skip_publish=True)  # excluded via items_to_include

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-123"
    workspace.repository_items = {
        "SemanticModel": {"SalesModel": included, "DevModel": excluded}
    }
    workspace.endpoint.invoke.return_value = {
        "body": {"value": [{"id": "old-conn", "connectivityType": "ShareableCloud", "connectionDetails": {}}]},
        "status_code": 200,
    }

    connections = _make_connections("conn-001")
    connection_details = {"SalesModel": "conn-001", "DevModel": "conn-001"}

    bind_semanticmodel_to_connection(workspace, connections, connection_details)

    # Only SalesModel should trigger API calls; DevModel must be entirely skipped.
    called_urls = [c[1]["url"] for c in workspace.endpoint.invoke.call_args_list]
    assert all("sales-guid" in url or "SalesModel" in url for url in called_urls), (
        f"Unexpected URL in calls: {called_urls}"
    )
    assert not any("DevModel" in url or "//" in url for url in called_urls), (
        f"Empty-GUID or DevModel URL must not be called, got: {called_urls}"
    )


def test_bind_skips_model_without_guid():
    """
    bind_semanticmodel_to_connection() must skip models whose guid is empty,
    even when skip_publish is False. This is a defensive safety net against
    any future code path that leaves skip_publish unset.
    """
    deployed = _make_sm("SalesModel", guid="sales-guid", skip_publish=False)
    not_deployed = _make_sm("DevModel", guid="", skip_publish=False)  # guid guard must catch this

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-123"
    workspace.repository_items = {
        "SemanticModel": {"SalesModel": deployed, "DevModel": not_deployed}
    }
    workspace.endpoint.invoke.return_value = {
        "body": {"value": [{"id": "old-conn", "connectivityType": "ShareableCloud", "connectionDetails": {}}]},
        "status_code": 200,
    }

    connections = _make_connections("conn-001")
    connection_details = {"SalesModel": "conn-001", "DevModel": "conn-001"}

    bind_semanticmodel_to_connection(workspace, connections, connection_details)

    called_urls = [c[1]["url"] for c in workspace.endpoint.invoke.call_args_list]
    assert not any("//" in url for url in called_urls), (
        f"Empty-GUID URL must not appear, got: {called_urls}"
    )
    assert not any("DevModel" in url for url in called_urls), (
        f"DevModel must be skipped, got: {called_urls}"
    )


def test_bind_processes_included_models_normally():
    """
    Models with skip_publish=False and a valid guid must still be bound.
    """
    model_a = _make_sm("ModelA", guid="guid-a", skip_publish=False)
    model_b = _make_sm("ModelB", guid="guid-b", skip_publish=False)

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-123"
    workspace.repository_items = {
        "SemanticModel": {"ModelA": model_a, "ModelB": model_b}
    }
    workspace.endpoint.invoke.return_value = {
        "body": {"value": [{"id": "old-conn", "connectivityType": "ShareableCloud", "connectionDetails": {}}]},
        "status_code": 200,
    }

    connections = _make_connections("conn-001")
    connection_details = {"ModelA": "conn-001", "ModelB": "conn-001"}

    bind_semanticmodel_to_connection(workspace, connections, connection_details)

    called_urls = [c[1]["url"] for c in workspace.endpoint.invoke.call_args_list]
    assert any("guid-a" in url for url in called_urls), "ModelA should have been processed"
    assert any("guid-b" in url for url in called_urls), "ModelB should have been processed"


# ---------------------------------------------------------------------------
# Integration test through SemanticModelPublisher.post_publish_all()
# ---------------------------------------------------------------------------


def test_post_publish_all_skips_excluded_semantic_models():
    """
    End-to-end regression: SemanticModelPublisher.post_publish_all() must not
    attempt connection binding for models excluded by items_to_include.

    publish_all() marks excluded models with skip_publish=True; the guard in
    bind_semanticmodel_to_connection() must then prevent any API call for them.
    """
    included_model = _make_sm("SalesModel", guid="sales-guid", skip_publish=False)
    excluded_model = _make_sm("DevModel", guid="", skip_publish=True)  # set by publish_all()

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-123"
    workspace.environment = "UAT"
    workspace.repository_items = {
        "SemanticModel": {"SalesModel": included_model, "DevModel": excluded_model}
    }
    workspace.environment_parameter = {
        "semantic_model_binding": {
            "default": {
                "connection_id": {"_ALL_": "conn-001"}  # applies to ALL models by default
            }
        }
    }
    workspace.items_to_include = ["SalesModel.SemanticModel"]

    # Simulate the connections API and bind call responses
    def fake_invoke(method, url, **kwargs):
        if method == "GET" and "/v1/connections" == url.split("?")[0].split("api.fabric.microsoft.com")[-1]:
            return {
                "body": {
                    "value": [
                        {"id": "conn-001", "connectivityType": "ShareableCloud", "connectionDetails": {"type": "SQL", "path": "srv"}}
                    ]
                }
            }
        if method == "GET" and "/connections" in url:
            return {
                "body": {
                    "value": [{"id": "old-conn", "connectivityType": "ShareableCloud", "connectionDetails": {"type": "SQL", "path": "srv"}}]
                }
            }
        if method == "POST" and "bindConnection" in url:
            return {"status_code": 200}
        return {"body": {}}

    workspace.endpoint.invoke.side_effect = fake_invoke

    publisher = SemanticModelPublisher.__new__(SemanticModelPublisher)
    publisher.fabric_workspace_obj = workspace
    publisher.item_type = "SemanticModel"

    publisher.post_publish_all()

    called_urls = [c[1]["url"] for c in workspace.endpoint.invoke.call_args_list]

    # No call should reference an empty GUID segment
    assert not any("//" in url for url in called_urls), (
        f"Empty-GUID URL produced: {called_urls}"
    )
    # DevModel (excluded, guid='') must never appear in any URL
    assert not any("DevModel" in url for url in called_urls), (
        f"DevModel must be skipped entirely: {called_urls}"
    )
    # SalesModel should be bound (bindConnection called with sales-guid)
    assert any("sales-guid" in url and "bindConnection" in url for url in called_urls), (
        f"SalesModel bindConnection not called: {called_urls}"
    )
