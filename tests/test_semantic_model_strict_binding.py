# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the enable_strict_semantic_model_binding feature flag.

Covers:
- bind_semanticmodel_to_connection() returning one failure per binding that did not happen
- SemanticModelPublisher.post_publish_all() raising on those failures only when the flag is set
- the intentional skips (no existing connections, excluded model) not counting as failures
"""

from typing import Optional
from unittest.mock import MagicMock

import pytest

from fabric_cicd import constants, get_supported_feature_flags
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError
from fabric_cicd._common._item import Item
from fabric_cicd._items._semanticmodel import SemanticModelPublisher, bind_semanticmodel_to_connection
from fabric_cicd.constants import FeatureFlag
from fabric_cicd.fabric_workspace import FabricWorkspace

STRICT_FLAG = FeatureFlag.ENABLE_STRICT_SEMANTIC_MODEL_BINDING.value
GOOD_ID = "11111111-1111-1111-1111-111111111111"
MISSING_ID = "00000000-0000-0000-0000-00000000dead"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_strict_flag():
    """Clear the strict binding feature flag before and after each test."""
    constants.FEATURE_FLAG.discard(STRICT_FLAG)
    yield
    constants.FEATURE_FLAG.discard(STRICT_FLAG)


def _make_sm(name: str, skip_publish: bool = False) -> Item:
    item = Item(type="SemanticModel", name=name, description="", guid=f"guid-{name}")
    item.skip_publish = skip_publish
    return item


def _make_workspace(*model_names: str, parameter: Optional[dict] = None) -> MagicMock:
    workspace = MagicMock(spec=FabricWorkspace)
    workspace.workspace_id = "ws-id"
    workspace.environment = "PROD"
    workspace.environment_parameter = {"semantic_model_binding": parameter} if parameter else {}
    workspace.endpoint = MagicMock()
    workspace.repository_items = {"SemanticModel": {name: _make_sm(name) for name in model_names}}
    return workspace


def _make_connections(*conn_ids: str) -> dict:
    return {
        cid: {"id": cid, "connectivityType": "ShareableCloud", "connectionDetails": {"type": "SQL", "path": "srv"}}
        for cid in conn_ids
    }


def _endpoint(
    *,
    list_fails: bool = False,
    item_connections: Optional[list] = None,
    item_connections_fail: bool = False,
    bind_status: int = 200,
    bind_fails: bool = False,
):
    """Build an endpoint.invoke side effect for the connection, item-connection and bind calls."""
    if item_connections is None:
        item_connections = [{"id": "existing", "connectivityType": "ShareableCloud", "connectionDetails": {}}]

    def invoke(method, url, **_kwargs):
        if method == "GET" and url.endswith("/v1/connections"):
            if list_fails:
                msg = "list connections failed"
                raise RuntimeError(msg)
            return {"body": {"value": list(_make_connections(GOOD_ID).values())}}
        if method == "GET" and url.endswith("/connections"):
            if item_connections_fail:
                msg = "item connections failed"
                raise RuntimeError(msg)
            return {"body": {"value": item_connections}}
        if method == "POST" and "bindConnection" in url:
            if bind_fails:
                msg = "You cannot configure the data connection bindings because you are not the owner"
                raise RuntimeError(msg)
            return {"status_code": bind_status}
        return {"body": {}}

    return invoke


def _post_calls(workspace: MagicMock) -> list:
    return [c for c in workspace.endpoint.invoke.call_args_list if c[1]["method"] == "POST"]


# ---------------------------------------------------------------------------
# bind_semanticmodel_to_connection() return value
# ---------------------------------------------------------------------------


def test_bind_returns_no_failures_when_all_bindings_succeed():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert failures == []
    assert len(_post_calls(workspace)) == 1


def test_bind_returns_failure_for_unknown_connection_id():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [MISSING_ID]})

    assert len(failures) == 1
    assert "ModelA" in failures[0]
    assert MISSING_ID in failures[0]


def test_bind_returns_failure_when_item_connections_call_fails():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint(item_connections_fail=True)

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert len(failures) == 1
    assert "ModelA" in failures[0]
    assert _post_calls(workspace) == []


def test_bind_returns_failure_for_non_200_bind_response():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint(bind_status=201)

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert len(failures) == 1
    assert "ModelA" in failures[0]
    assert GOOD_ID in failures[0]


def test_bind_returns_failure_when_bind_call_raises():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint(bind_fails=True)

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert len(failures) == 1
    assert "ModelA" in failures[0]
    assert GOOD_ID in failures[0]


def test_bind_failure_does_not_stop_other_bindings():
    workspace = _make_workspace("ModelA", "ModelB")
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(
        workspace, _make_connections(GOOD_ID), {"ModelA": [MISSING_ID], "ModelB": [GOOD_ID]}
    )

    assert len(failures) == 1
    assert "ModelA" in failures[0]
    post_urls = [c[1]["url"] for c in _post_calls(workspace)]
    assert len(post_urls) == 1
    assert "guid-ModelB" in post_urls[0]


def test_bind_missing_id_does_not_stop_valid_id_on_same_model():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(
        workspace, _make_connections(GOOD_ID), {"ModelA": [MISSING_ID, GOOD_ID]}
    )

    assert len(failures) == 1
    assert MISSING_ID in failures[0]
    post_calls = _post_calls(workspace)
    assert len(post_calls) == 1
    assert "guid-ModelA" in post_calls[0][1]["url"]


def test_bind_item_connections_failure_records_one_failure_per_connection_id():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint(item_connections_fail=True)
    other_id = "22222222-2222-2222-2222-222222222222"

    failures = bind_semanticmodel_to_connection(
        workspace, _make_connections(GOOD_ID, other_id), {"ModelA": [GOOD_ID, other_id]}
    )

    assert len(failures) == 2
    assert GOOD_ID in failures[0]
    assert other_id in failures[1]


def test_bind_skip_for_model_without_existing_connections_is_not_a_failure():
    workspace = _make_workspace("ModelA")
    workspace.endpoint.invoke.side_effect = _endpoint(item_connections=[])

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert failures == []


def test_bind_unknown_connection_on_excluded_model_is_not_a_failure():
    workspace = _make_workspace("ModelA")
    workspace.repository_items["SemanticModel"]["ModelA"].skip_publish = True
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [MISSING_ID]})

    assert failures == []


def test_bind_skip_for_excluded_model_is_not_a_failure():
    workspace = _make_workspace("ModelA")
    workspace.repository_items["SemanticModel"]["ModelA"].skip_publish = True
    workspace.endpoint.invoke.side_effect = _endpoint()

    failures = bind_semanticmodel_to_connection(workspace, _make_connections(GOOD_ID), {"ModelA": [GOOD_ID]})

    assert failures == []


# ---------------------------------------------------------------------------
# post_publish_all() with and without the flag
# ---------------------------------------------------------------------------


def _binding(connection_id: str) -> dict:
    return {"default": {"connection_id": {"PROD": connection_id}}}


def test_strict_flag_raises_after_binding_with_failures():
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    workspace = _make_workspace("ModelA", parameter=_binding(MISSING_ID))
    workspace.endpoint.invoke.side_effect = _endpoint()

    with pytest.raises(FailedPublishedItemStatusError) as error:
        SemanticModelPublisher(workspace).post_publish_all()

    assert "ModelA" in str(error.value)
    assert MISSING_ID in str(error.value)


def test_strict_flag_lists_every_failed_model_in_one_error():
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    parameter = {
        "models": [
            {"semantic_model_name": "ModelA", "connection_id": {"PROD": MISSING_ID}},
            {"semantic_model_name": "ModelB", "connection_id": {"PROD": MISSING_ID}},
        ]
    }
    workspace = _make_workspace("ModelA", "ModelB", parameter=parameter)
    workspace.endpoint.invoke.side_effect = _endpoint()

    with pytest.raises(FailedPublishedItemStatusError) as error:
        SemanticModelPublisher(workspace).post_publish_all()

    assert "ModelA" in str(error.value)
    assert "ModelB" in str(error.value)
    # Turns out log formatter treats constants.INDENT as marker for indentation and thus drops the [error] tag. 
    # So message must not contain it.
    assert constants.INDENT not in str(error.value)


def test_strict_flag_reports_list_call_failure_as_the_cause(caplog):
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    workspace = _make_workspace("ModelA", parameter=_binding(GOOD_ID))
    workspace.endpoint.invoke.side_effect = _endpoint(list_fails=True)

    with caplog.at_level("ERROR"), pytest.raises(FailedPublishedItemStatusError) as error:
        SemanticModelPublisher(workspace).post_publish_all()

    assert "Failed to retrieve connections" in caplog.text
    assert "Could not list connections" in str(error.value)
    assert "list connections failed" in str(error.value)
    assert "not found" not in str(error.value)


def test_strict_flag_ignores_list_call_failure_when_only_excluded_models_are_bound():
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    workspace = _make_workspace("ModelA", parameter=_binding(GOOD_ID))
    workspace.repository_items["SemanticModel"]["ModelA"].skip_publish = True
    workspace.endpoint.invoke.side_effect = _endpoint(list_fails=True)

    SemanticModelPublisher(workspace).post_publish_all()

    assert _post_calls(workspace) == []


def test_strict_flag_does_not_raise_when_all_bindings_succeed():
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    workspace = _make_workspace("ModelA", parameter=_binding(GOOD_ID))
    workspace.endpoint.invoke.side_effect = _endpoint()

    SemanticModelPublisher(workspace).post_publish_all()

    assert len(_post_calls(workspace)) == 1


def test_without_strict_flag_binding_failures_only_log(caplog):
    workspace = _make_workspace("ModelA", parameter=_binding(MISSING_ID))
    workspace.endpoint.invoke.side_effect = _endpoint()

    with caplog.at_level("WARNING"):
        SemanticModelPublisher(workspace).post_publish_all()

    assert f"Connection ID '{MISSING_ID}' not found for semantic model 'ModelA'" in caplog.text


FAILURE_PATHS = {
    "connection_not_found": ({}, MISSING_ID),
    "item_connections_call_fails": ({"item_connections_fail": True}, GOOD_ID),
    "bind_returns_non_200": ({"bind_status": 201}, GOOD_ID),
    "bind_call_raises": ({"bind_fails": True}, GOOD_ID),
    "list_connections_call_fails": ({"list_fails": True}, GOOD_ID),
}


@pytest.mark.parametrize("path", FAILURE_PATHS)
def test_each_failure_path_raises_with_strict_flag(path):
    endpoint_kwargs, connection_id = FAILURE_PATHS[path]
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    workspace = _make_workspace("ModelA", parameter=_binding(connection_id))
    workspace.endpoint.invoke.side_effect = _endpoint(**endpoint_kwargs)

    with pytest.raises(FailedPublishedItemStatusError):
        SemanticModelPublisher(workspace).post_publish_all()


@pytest.mark.parametrize("path", FAILURE_PATHS)
def test_each_failure_path_only_logs_without_strict_flag(path, caplog):
    endpoint_kwargs, connection_id = FAILURE_PATHS[path]
    workspace = _make_workspace("ModelA", parameter=_binding(connection_id))
    workspace.endpoint.invoke.side_effect = _endpoint(**endpoint_kwargs)

    with caplog.at_level("WARNING"):
        SemanticModelPublisher(workspace).post_publish_all()

    assert any(record.levelname in ("WARNING", "ERROR") for record in caplog.records)


def test_strict_flag_raises_for_legacy_list_format():
    constants.FEATURE_FLAG.add(STRICT_FLAG)
    parameter = [{"semantic_model_name": "ModelA", "connection_id": MISSING_ID}]
    workspace = _make_workspace("ModelA")
    workspace.environment_parameter = {"semantic_model_binding": parameter}
    workspace.endpoint.invoke.side_effect = _endpoint()

    with pytest.raises(FailedPublishedItemStatusError) as error:
        SemanticModelPublisher(workspace).post_publish_all()

    assert MISSING_ID in str(error.value)


def test_strict_flag_in_supported_feature_flags():
    assert STRICT_FLAG in get_supported_feature_flags()
