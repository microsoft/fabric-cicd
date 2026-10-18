# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_purge_data feature flag on Semantic Model definition updates."""

import tempfile
from unittest.mock import MagicMock, patch

import pytest
from fixtures.credentials import DummyTokenCredential

from fabric_cicd import constants, get_supported_feature_flags
from fabric_cicd._common._item import Item
from fabric_cicd._items._semanticmodel import SemanticModelPublisher
from fabric_cicd.constants import FeatureFlag, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

VALID_WORKSPACE_ID = "12345678-1234-5678-abcd-1234567890ab"


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint, capturing updateDefinition request bodies."""
    mock = MagicMock()
    mock.update_bodies = []

    def mock_invoke(method, url, body=None, **_kwargs):
        if method == "POST" and "updateDefinition" in url:
            mock.update_bodies.append(body)
            return {"body": {"message": "Definition updated successfully"}}
        return {"body": {"value": []}}

    mock.invoke.side_effect = mock_invoke
    return mock


@pytest.fixture
def deployed_semantic_model_workspace(mock_endpoint):
    """Create a workspace containing a single deployed Semantic Model item."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
        refresh_items_patch = patch.object(
            FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
        )
        refresh_folders_patch = patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        )
        with fabric_endpoint_patch, refresh_items_patch, refresh_folders_patch:
            workspace = FabricWorkspace(
                workspace_id=VALID_WORKSPACE_ID,
                repository_directory=str(temp_dir),
                item_type_in_scope=[ItemType.SEMANTIC_MODEL.value],
                token_credential=DummyTokenCredential(),
            )
            workspace.deployed_items = {}
            workspace.deployed_folders = {}
            # A guid marks the item as already deployed, triggering the updateDefinition branch
            item = Item(
                type=ItemType.SEMANTIC_MODEL.value,
                name="MyModel",
                description="",
                guid="existing-guid-123",
            )
            workspace.repository_items = {ItemType.SEMANTIC_MODEL.value: {"MyModel": item}}
            yield workspace


@pytest.fixture(autouse=True)
def _clear_feature_flags():
    """Clear the purge data feature flag before and after each test."""
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_PURGE_DATA.value)
    yield
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_PURGE_DATA.value)


def _publish(workspace):
    # Route through the publisher so the get_definition_options() flag mapping is exercised.
    # The test item has no definition files, so the empty-parts updateDefinition branch runs.
    item = workspace.repository_items[ItemType.SEMANTIC_MODEL.value]["MyModel"]
    SemanticModelPublisher(workspace).publish_one("MyModel", item)


def test_purge_data_flag_adds_options_to_update_body(deployed_semantic_model_workspace, mock_endpoint):
    """With the flag set, updateDefinition body includes options.allowPurgeData == True."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_PURGE_DATA.value)

    _publish(deployed_semantic_model_workspace)

    assert len(mock_endpoint.update_bodies) == 1
    body = mock_endpoint.update_bodies[0]
    assert body["options"]["allowPurgeData"] is True


def test_no_purge_data_flag_omits_options(deployed_semantic_model_workspace, mock_endpoint):
    """Without the flag, the updateDefinition body has no options key."""
    _publish(deployed_semantic_model_workspace)

    assert len(mock_endpoint.update_bodies) == 1
    body = mock_endpoint.update_bodies[0]
    assert "options" not in body


def test_purge_data_flag_in_supported_feature_flags():
    """The new flag is exposed via get_supported_feature_flags()."""
    assert FeatureFlag.ENABLE_PURGE_DATA.value in get_supported_feature_flags()
