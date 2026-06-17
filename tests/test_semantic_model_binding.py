# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Unit tests for semantic model connection binding with single and multiple connections.

Covers:
- build_binding_mapping() with single string connection_id (backward compat)
- build_binding_mapping() with list connection_id (new multi-binding feature)
- bind_semanticmodel_to_connection() with single connection ID
- bind_semanticmodel_to_connection() with multiple connection IDs
- _normalize_connection_ids() helper
"""

from unittest.mock import MagicMock

from fabric_cicd._common._item import Item
from fabric_cicd._items._semanticmodel import (
    _normalize_connection_ids,
    bind_semanticmodel_to_connection,
    build_binding_mapping,
)
from fabric_cicd.fabric_workspace import FabricWorkspace

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sm(name: str, guid: str = "sm-guid", skip_publish: bool = False) -> Item:
    item = Item(type="SemanticModel", name=name, description="", guid=guid)
    item.skip_publish = skip_publish
    return item


def _make_workspace(*model_names: str) -> MagicMock:
    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-id"
    workspace.endpoint = MagicMock()
    workspace.repository_items = {"SemanticModel": {name: _make_sm(name, guid=f"guid-{name}") for name in model_names}}
    return workspace


def _make_connections(*conn_ids: str) -> dict:
    return {
        cid: {"id": cid, "connectivityType": "ShareableCloud", "connectionDetails": {"type": "SQL", "path": "srv"}}
        for cid in conn_ids
    }


def _invoke_side_effect(method: str, url: str, **_kwargs):
    if method == "GET" and url.endswith("/connections"):
        return {"body": {"value": [{"id": "old-conn", "connectivityType": "ShareableCloud", "connectionDetails": {}}]}}
    if method == "POST" and "bindConnection" in url:
        return {"status_code": 200}
    return {"body": {}}


# ---------------------------------------------------------------------------
# _normalize_connection_ids
# ---------------------------------------------------------------------------


def test_normalize_single_string():
    assert _normalize_connection_ids("12345678-1234-1234-1234-123456789012") == ["12345678-1234-1234-1234-123456789012"]


def test_normalize_list_passthrough():
    ids = ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    result = _normalize_connection_ids(ids)
    assert result == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    assert isinstance(result, list)


def test_normalize_list_filters_non_strings(caplog):
    """Non-string elements in a list are skipped with a warning."""
    with caplog.at_level("WARNING"):
        result = _normalize_connection_ids(["11111111-1111-1111-1111-111111111111", {"id": "bad"}, 42, "22222222-2222-2222-2222-222222222222"])
    assert result == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    assert "non-string" in caplog.text.lower() or "skipping" in caplog.text.lower()


def test_normalize_invalid_type_returns_empty():
    result = _normalize_connection_ids(12345)
    assert result == []


# ---------------------------------------------------------------------------
# build_binding_mapping — single string connection_id (backward compat)
# ---------------------------------------------------------------------------


def test_build_binding_mapping_default_single_string():
    """Default section with a single string connection_id applies to all models."""
    workspace = _make_workspace("ModelA", "ModelB")
    binding = {
        "default": {"connection_id": {"PPE": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}},
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result == {"ModelA": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"], "ModelB": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]}


def test_build_binding_mapping_models_single_string():
    """Models section with a single string connection_id."""
    workspace = _make_workspace("ModelA", "ModelB")
    binding = {
        "models": [
            {"semantic_model_name": "ModelA", "connection_id": {"PPE": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}},
        ]
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result == {"ModelA": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]}
    assert "ModelB" not in result  # no default set


def test_build_binding_mapping_all_key_single_string():
    """_ALL_ key maps to the requested environment."""
    workspace = _make_workspace("ModelA")
    binding = {"default": {"connection_id": {"_ALL_": "ffffffff-ffff-ffff-ffff-ffffffffffff"}}}
    result = build_binding_mapping(workspace, binding, "PROD")
    assert result == {"ModelA": ["ffffffff-ffff-ffff-ffff-ffffffffffff"]}


# ---------------------------------------------------------------------------
# build_binding_mapping — list connection_id (new feature)
# ---------------------------------------------------------------------------


def test_build_binding_mapping_default_list():
    """Default section with a list of connection IDs assigns all IDs to each model."""
    workspace = _make_workspace("ModelA", "ModelB")
    binding = {
        "default": {"connection_id": {"PPE": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]}},
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result["ModelA"] == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    assert result["ModelB"] == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]


def test_build_binding_mapping_models_list():
    """Models section with a list of connection IDs."""
    workspace = _make_workspace("ModelA", "ModelB")
    binding = {
        "models": [
            {"semantic_model_name": "ModelA", "connection_id": {"PPE": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]}},
        ]
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result["ModelA"] == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    assert "ModelB" not in result


def test_build_binding_mapping_explicit_overrides_default_with_list():
    """Explicit model binding (list) overrides default; other models still get default."""
    workspace = _make_workspace("ModelA", "ModelB")
    binding = {
        "default": {"connection_id": {"PPE": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}},
        "models": [
            {"semantic_model_name": "ModelA", "connection_id": {"PPE": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]}},
        ],
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result["ModelA"] == ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
    assert result["ModelB"] == ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]


def test_build_binding_mapping_unknown_model_warns(caplog):
    """Models referenced in parameter.yml that don't exist in the repo produce a warning."""
    workspace = _make_workspace("ModelA")
    binding = {
        "models": [
            {"semantic_model_name": "NoSuchModel", "connection_id": {"PPE": ["11111111-1111-1111-1111-111111111111"]}},
        ]
    }
    with caplog.at_level("WARNING"):
        result = build_binding_mapping(workspace, binding, "PPE")
    assert "NoSuchModel" in caplog.text
    assert "NoSuchModel" not in result


def test_build_binding_mapping_missing_env_skips():
    """Missing environment key in connection_id is skipped gracefully."""
    workspace = _make_workspace("ModelA")
    binding = {
        "models": [
            {"semantic_model_name": "ModelA", "connection_id": {"PROD": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}},
        ]
    }
    result = build_binding_mapping(workspace, binding, "PPE")
    assert result == {}


# ---------------------------------------------------------------------------
# bind_semanticmodel_to_connection — single connection ID (backward compat)
# ---------------------------------------------------------------------------


def test_bind_single_connection_id_as_string():
    """Passing a plain string connection_id still works."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": "11111111-1111-1111-1111-111111111111"})

    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    assert len(post_calls) == 1
    assert "bindConnection" in post_calls[0][1]["url"]


def test_bind_single_connection_id_as_list():
    """Passing a single-element list calls bindConnection once."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111"]})

    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    assert len(post_calls) == 1


# ---------------------------------------------------------------------------
# bind_semanticmodel_to_connection — multiple connection IDs (new feature)
# ---------------------------------------------------------------------------


def test_bind_multiple_connection_ids_calls_bind_n_times():
    """Passing a list of N connection IDs results in N bindConnection POST calls."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333"]})

    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    assert len(post_calls) == 3


def test_bind_multiple_models_multiple_connections():
    """Two models each with two connections → 4 total bindConnection calls."""
    workspace = _make_workspace("ModelA", "ModelB")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222")
    bind_semanticmodel_to_connection(
        workspace,
        connections,
        {"ModelA": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"], "ModelB": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]},
    )

    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    assert len(post_calls) == 4


def test_bind_item_connections_fetched_once_per_model():
    """GET /items/{id}/connections is called exactly once per model, even for multiple bindings."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]})

    get_item_conn_calls = [
        c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "GET" and "/connections" in c[1]["url"]
    ]
    assert len(get_item_conn_calls) == 1


def test_bind_invalid_connection_id_warns_and_skips(caplog):
    """An unknown connection ID produces a warning and is skipped; valid IDs still proceed."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111")
    with caplog.at_level("WARNING"):
        bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111", "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"]})

    assert "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" in caplog.text
    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    # Only the valid 11111111-1111-1111-1111-111111111111 should produce a POST
    assert len(post_calls) == 1


def test_bind_all_connection_ids_invalid_skips_model():
    """If all connection IDs are invalid, no API calls are made for that model."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"]})

    # No GET or POST calls for ModelA because all IDs are invalid
    assert workspace.endpoint.invoke.call_count == 0


def test_bind_correct_connection_id_in_request_body():
    """Each bindConnection call must use the correct connection ID in the request body."""
    workspace = _make_workspace("ModelA")
    captured_bodies = []

    def capture_invoke(method, **kwargs):
        if method == "POST":
            captured_bodies.append(kwargs.get("body", {}))
            return {"status_code": 200}
        return {"body": {"value": [{"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "connectivityType": "ShareableCloud", "connectionDetails": {}}]}}

    workspace.endpoint.invoke.side_effect = capture_invoke

    connections = _make_connections("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222")
    bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]})

    assert len(captured_bodies) == 2
    bound_ids = {body["connectionBinding"]["id"] for body in captured_bodies}
    assert bound_ids == {"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"}


def test_bind_list_with_non_string_elements_does_not_raise(caplog):
    """A list containing non-string elements (e.g. dicts) must not raise TypeError.
    Non-string items are filtered by _normalize_connection_ids; valid strings still bind."""
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _invoke_side_effect

    connections = _make_connections("11111111-1111-1111-1111-111111111111")
    with caplog.at_level("WARNING"):
        # {"id": "bad"} is an unhashable dict — must not cause TypeError
        bind_semanticmodel_to_connection(workspace, connections, {"ModelA": ["11111111-1111-1111-1111-111111111111", {"id": "bad"}, 42]})

    post_calls = [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]
    assert len(post_calls) == 1  # only the valid "11111111-1111-1111-1111-111111111111" is bound
