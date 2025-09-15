# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test response collection functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.publish as publish
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
                workspace_id="12345678-1234-5678-abcd-1234567890ab",  # Valid GUID format
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


def test_responses_initialized_as_none(test_workspace_with_notebook):
    """Test that responses attribute is initialized as None by default."""
    workspace = test_workspace_with_notebook
    assert workspace.responses is None


def test_publish_item_without_response_collection(test_workspace_with_notebook):
    """Test that _publish_item works normally when responses is None."""
    workspace = test_workspace_with_notebook

    # Patch the internal methods that process content to avoid mock issues
    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        workspace._publish_item(item_name="TestNotebook", item_type="Notebook")
        # Should not store any responses
        assert workspace.responses is None


def test_publish_item_with_response_collection(test_workspace_with_notebook):
    """Test that _publish_item stores responses when responses is enabled."""
    workspace = test_workspace_with_notebook
    workspace.responses = {}  # Enable response collection

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Should store the response
        assert workspace.responses is not None
        assert "TestNotebook.Notebook" in workspace.responses
        response = workspace.responses["TestNotebook.Notebook"]
        assert response["body"]["id"] == "mock-item-id-12345"


def test_publish_all_items_enable_responses_false(test_workspace_with_notebook):
    """Test that publish_all_items doesn't enable responses by default."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace)

    assert result is None
    assert workspace.responses is None


def test_publish_all_items_enable_responses_true(test_workspace_with_notebook):
    """Test that publish_all_items enables response collection when requested."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace, enable_responses=True)

    # Should have initialized responses as a dict
    assert workspace.responses is not None
    assert isinstance(workspace.responses, dict)
    # Should return the responses
    assert result is workspace.responses


def test_workspace_responses_access_pattern(test_workspace_with_notebook):
    """Test the recommended access pattern for responses."""
    workspace = test_workspace_with_notebook

    # Enable response collection and publish
    publish.publish_all_items(workspace, enable_responses=True)

    # Users can access responses directly from the workspace
    assert hasattr(workspace, "responses")
    assert workspace.responses is not None

    # Can access individual item responses
    if workspace.responses:
        for item_key, response in workspace.responses.items():
            assert isinstance(item_key, str)
            assert "." in item_key  # Should be in format "item_name.item_type"
            assert isinstance(response, dict)


def test_publish_item_skipped_no_response_stored(test_workspace_with_notebook):
    """Test that skipped items don't store responses even when collection is enabled."""
    workspace = test_workspace_with_notebook
    workspace.responses = {}  # Enable response collection
    workspace.publish_item_name_exclude_regex = "TestNotebook"  # Exclude the test item

    workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

    # Should not store response for skipped item
    assert "TestNotebook.Notebook" not in workspace.responses
