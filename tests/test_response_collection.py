# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test response collection functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.constants as constants
import fabric_cicd.publish as publish
from fabric_cicd import append_feature_flag
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to return realistic responses."""
    mock = MagicMock()

    def mock_invoke(method, url, body=None, **_kwargs):
        if method == "GET" and "workspaces" in url and not url.endswith("/items"):
            return {"body": {"value": [], "capacityId": "test-capacity"}}
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}
        if method == "POST" and url.endswith("/folders"):
            return {"body": {"id": "mock-folder-id"}}
        if method == "POST" and url.endswith("/items"):
            return {
                "body": {
                    "id": "mock-item-id-12345",
                    "workspaceId": "mock-workspace-id",
                    "displayName": body.get("displayName", "Test Item"),
                    "type": body.get("type", "Notebook"),
                }
            }
        if method == "POST" and "updateDefinition" in url:
            return {"body": {"message": "Definition updated successfully"}}
        if method == "PATCH" and "items/" in url:
            return {"body": {"message": "Item metadata updated successfully"}}
        if method == "POST" and url.endswith("/move"):
            return {"body": {"message": "Item moved successfully"}}
        if method == "DELETE" and "items/" in url:
            return {"body": {}, "header": {}, "status_code": 200}
        return {"body": {"value": [], "capacityId": "test-capacity"}}

    mock.invoke.side_effect = mock_invoke
    mock.upn_auth = True
    return mock


@pytest.fixture
def test_workspace_with_notebook(mock_endpoint):
    """Create a test workspace with a notebook item."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a notebook item
        notebook_dir = temp_path / "TestNotebook.Notebook"
        notebook_dir.mkdir(parents=True, exist_ok=True)

        platform_file = notebook_dir / ".platform"
        platform_file.write_text(
            json.dumps({
                "metadata": {
                    "kernel_info": {"name": "synapse_pyspark"},
                    "language_info": {"name": "python"},
                }
            })
        )

        notebook_file = notebook_dir / "notebook-content.py"
        notebook_file.write_text("# Test notebook content\nprint('Hello World')")

        # Patch FabricEndpoint before creating workspace
        with (
            patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
            patch.object(
                FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
            ),
            patch.object(
                FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
            ),
            patch.object(FabricWorkspace, "_refresh_repository_items", new=lambda _: None),
            patch.object(FabricWorkspace, "_refresh_repository_folders", new=lambda _: None),
        ):
            workspace = FabricWorkspace(
                workspace_id="12345678-1234-5678-abcd-1234567890ab",
                repository_directory=str(temp_path),
                item_type_in_scope=["Notebook"],
            )
            # Manually set up repository items since we're patching the refresh methods
            workspace.repository_items = {
                "Notebook": {
                    "TestNotebook": MagicMock(
                        guid=None,
                        folder_id="mock-folder-id",
                        logical_id="test-notebook-logical-id",
                        item_files=[
                            MagicMock(
                                relative_path="notebook-content.py",
                                type="text",
                                file_path=notebook_file,
                                contents="# Test notebook content\nprint('Hello World')",
                                base64_payload={"path": "notebook-content.py", "payloadType": "InlineBase64"},
                            )
                        ],
                        skip_publish=False,
                        path=notebook_dir,
                    )
                }
            }
            workspace.deployed_items = {}
            # Set up parameter data to avoid parameter file warnings
            workspace.parameter_data = {}
            workspace.parameter_file_path = None
            yield workspace


# =============================================================================
# Initialization Tests
# =============================================================================


def test_responses_initialized_as_none(test_workspace_with_notebook):
    """Test that responses and unpublish_responses attributes are initialized as None by default."""
    workspace = test_workspace_with_notebook
    assert workspace.responses is None
    assert workspace.unpublish_responses is None


# =============================================================================
# Publish Response Collection Tests
# =============================================================================


def test_publish_item_without_response_collection(test_workspace_with_notebook):
    """Test that _publish_item works normally when responses is None."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        workspace._publish_item(item_name="TestNotebook", item_type="Notebook")
        assert workspace.responses is None
        assert workspace.unpublish_responses is None


def test_publish_item_with_response_collection(test_workspace_with_notebook):
    """Test that _publish_item stores responses when feature flag is enabled."""
    workspace = test_workspace_with_notebook

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.responses = {}

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

            assert workspace.responses is not None
            assert "Notebook" in workspace.responses
            assert "TestNotebook" in workspace.responses["Notebook"]
            response = workspace.responses["Notebook"]["TestNotebook"]
            assert response["body"]["id"] == "mock-item-id-12345"
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_publish_all_items_no_feature_flag(test_workspace_with_notebook):
    """Test that publish_all_items doesn't enable responses by default."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace)

    assert result is None
    assert workspace.responses is None


def test_publish_all_items_with_feature_flag(test_workspace_with_notebook):
    """Test that publish_all_items enables response collection when feature flag is set."""
    workspace = test_workspace_with_notebook

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        result = publish.publish_all_items(workspace)

        assert workspace.responses is not None
        assert isinstance(workspace.responses, dict)
        assert result is workspace.responses
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_workspace_responses_access_pattern(test_workspace_with_notebook):
    """Test the recommended access pattern for responses."""
    workspace = test_workspace_with_notebook
    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        publish.publish_all_items(workspace)

        assert hasattr(workspace, "responses")
        assert workspace.responses is not None

        if workspace.responses:
            for item_type, items in workspace.responses.items():
                assert isinstance(item_type, str)
                assert isinstance(items, dict)
                for item_name, response in items.items():
                    assert isinstance(item_name, str)
                    assert isinstance(response, dict)
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_publish_item_skipped_no_response_stored(test_workspace_with_notebook):
    """Test that skipped items don't store responses even when collection is enabled."""
    workspace = test_workspace_with_notebook

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.responses = {}
        workspace.publish_item_name_exclude_regex = "TestNotebook"

        workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        assert "Notebook" not in workspace.responses
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_append_feature_flag_enables_response_collection(test_workspace_with_notebook):
    """Test that using append_feature_flag enables response collection."""
    workspace = test_workspace_with_notebook

    append_feature_flag("enable_response_collection")

    try:
        result = publish.publish_all_items(workspace)

        assert workspace.responses is not None
        assert isinstance(workspace.responses, dict)
        assert result is workspace.responses
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


# =============================================================================
# Unpublish Response Collection Tests
# =============================================================================


def test_unpublish_item_without_response_collection(test_workspace_with_notebook):
    """Test that _unpublish_item does not store responses when collection is disabled."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid="mock-guid-123")}}

    workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

    assert workspace.unpublish_responses is None


def test_unpublish_item_with_response_collection(test_workspace_with_notebook):
    """Test that _unpublish_item stores responses in unpublish_responses."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid="mock-guid-123")}}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.unpublish_responses = {}

        workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

        assert "Notebook" in workspace.unpublish_responses
        assert "TestNotebook" in workspace.unpublish_responses["Notebook"]
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_unpublish_item_does_not_write_to_publish_responses(test_workspace_with_notebook):
    """Test that _unpublish_item does not write to self.responses."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid="mock-guid-123")}}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.responses = {"Notebook": {"ExistingItem": {"body": {"id": "existing"}}}}
        workspace.unpublish_responses = {}

        workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

        # publish responses unchanged — no TestNotebook added
        assert "TestNotebook" not in workspace.responses.get("Notebook", {})
        assert workspace.responses["Notebook"]["ExistingItem"]["body"]["id"] == "existing"
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_unpublish_item_failure_does_not_store_response(test_workspace_with_notebook, mock_endpoint):
    """Test that _unpublish_item does not store responses when the DELETE call fails."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid="mock-guid-123")}}

    constants.FEATURE_FLAG.add("enable_response_collection")

    # Make DELETE raise an exception
    original_side_effect = mock_endpoint.invoke.side_effect

    def failing_invoke(method, url, **kwargs):
        if method == "DELETE":
            msg = "API error"
            raise Exception(msg)
        return original_side_effect(method, url, **kwargs)

    mock_endpoint.invoke.side_effect = failing_invoke

    try:
        workspace.unpublish_responses = {}

        workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

        # No response stored due to failure
        assert "Notebook" not in workspace.unpublish_responses
    finally:
        mock_endpoint.invoke.side_effect = original_side_effect
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_unpublish_all_orphan_items_no_feature_flag(test_workspace_with_notebook):
    """Test that unpublish_all_orphan_items returns None without the feature flag."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {}

    result = publish.unpublish_all_orphan_items(workspace)

    assert result is None
    assert workspace.unpublish_responses is None


def test_unpublish_all_orphan_items_with_feature_flag(test_workspace_with_notebook):
    """Test that unpublish_all_orphan_items initializes unpublish_responses and returns populated dict."""
    workspace = test_workspace_with_notebook

    # Set up an orphaned item: deployed but not in repository
    orphan_deployed = {"Notebook": {"OrphanNotebook": MagicMock(guid="orphan-guid-456")}}
    orphan_repo = {}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        assert workspace.unpublish_responses is None

        with (
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
        ):
            result = publish.unpublish_all_orphan_items(workspace)

        assert workspace.unpublish_responses is not None
        assert isinstance(workspace.unpublish_responses, dict)
        assert "Notebook" in workspace.unpublish_responses
        assert "OrphanNotebook" in workspace.unpublish_responses["Notebook"]
        assert result is workspace.unpublish_responses
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_unpublish_all_orphan_items_empty_returns_none(test_workspace_with_notebook):
    """Test that unpublish_all_orphan_items returns None when no items are orphaned."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        result = publish.unpublish_all_orphan_items(workspace)

        # Empty responses dict is falsy, so return value is None
        assert result is None
        assert workspace.unpublish_responses is not None
        assert isinstance(workspace.unpublish_responses, dict)
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


# =============================================================================
# Publish and Unpublish Response Separation Tests
# =============================================================================


def test_unpublish_does_not_modify_publish_responses(test_workspace_with_notebook):
    """Test that unpublish_all_orphan_items does not modify publish responses."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.responses = {"Notebook": {"TestNotebook": {"body": {"id": "publish-response"}}}}

        publish.unpublish_all_orphan_items(workspace)

        # Publish responses untouched
        assert workspace.responses["Notebook"]["TestNotebook"]["body"]["id"] == "publish-response"
        # Unpublish responses initialized separately
        assert workspace.unpublish_responses is not None
        assert isinstance(workspace.unpublish_responses, dict)
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_publish_does_not_modify_unpublish_responses(test_workspace_with_notebook):
    """Test that publish_all_items does not modify unpublish responses."""
    workspace = test_workspace_with_notebook

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        workspace.unpublish_responses = {"Notebook": {"OldNotebook": {"body": {"id": "unpublish-response"}}}}

        publish.publish_all_items(workspace)

        # Unpublish responses untouched
        assert workspace.unpublish_responses["Notebook"]["OldNotebook"]["body"]["id"] == "unpublish-response"
        # Publish responses initialized separately
        assert workspace.responses is not None
        assert isinstance(workspace.responses, dict)
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")


def test_publish_and_unpublish_responses_are_separate_dicts(test_workspace_with_notebook):
    """Test that publish and unpublish use separate response dictionaries."""
    workspace = test_workspace_with_notebook
    workspace.deployed_items = {}

    constants.FEATURE_FLAG.add("enable_response_collection")

    try:
        publish.publish_all_items(workspace)
        publish.unpublish_all_orphan_items(workspace)

        assert workspace.responses is not workspace.unpublish_responses
    finally:
        constants.FEATURE_FLAG.discard("enable_response_collection")
