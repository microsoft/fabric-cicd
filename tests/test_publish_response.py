# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test response functionality for publish methods."""

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
        if method == "PATCH" and url.endswith("items/mock-item-id-12345"):
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


def test_publish_item_return_response_false_default(test_workspace_with_notebook):
    """Test that _publish_item returns None by default (backward compatibility)."""
    workspace = test_workspace_with_notebook

    # Patch the internal methods that process content to avoid mock issues
    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        # Test default behavior (return_response=False is the default)
        result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook")
        assert result is None


def test_publish_item_return_response_false_explicit(test_workspace_with_notebook):
    """Test that _publish_item returns None when return_response=False explicitly."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook", return_response=False)
        assert result is None


def test_publish_item_return_response_true_new_item(test_workspace_with_notebook):
    """Test that _publish_item returns response when return_response=True for new item."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook", return_response=True)

        # Should return the create-item response
        assert result is not None
        assert isinstance(result, dict)
        assert "body" in result
        assert result["body"]["id"] == "mock-item-id-12345"
        assert result["body"]["displayName"] == "TestNotebook"
        assert result["body"]["type"] == "Notebook"


def test_publish_item_return_response_true_existing_item(test_workspace_with_notebook):
    """Test that _publish_item returns response when return_response=True for existing item."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        # First publish to create the item
        workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Mock that the item is now deployed
        workspace.repository_items["Notebook"]["TestNotebook"].guid = "mock-item-id-12345"
        workspace.deployed_items = {
            "Notebook": {"TestNotebook": MagicMock(guid="mock-item-id-12345", folder_id="mock-folder-id")}
        }

        # Second publish should trigger update operation
        result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook", return_response=True)

        # Should return the update-definition response
        assert result is not None
        assert isinstance(result, dict)
        assert "body" in result
        assert result["body"]["message"] == "Definition updated successfully"


def test_publish_item_return_response_with_move(test_workspace_with_notebook):
    """Test that _publish_item returns combined response when item is moved."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        # Mock that the item is already deployed but in different folder
        workspace.repository_items["Notebook"]["TestNotebook"].guid = "mock-item-id-12345"
        workspace.repository_items["Notebook"]["TestNotebook"].folder_id = "new-folder-id"
        workspace.deployed_items = {
            "Notebook": {
                "TestNotebook": MagicMock(
                    guid="mock-item-id-12345",
                    folder_id="old-folder-id",  # Different folder ID to trigger move
                )
            }
        }

        result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook", return_response=True)

        # Should return combined response with both update and move
        assert result is not None
        assert isinstance(result, dict)
        # Should have both responses
        assert "publish_response" in result
        assert "move_response" in result
        assert result["move_response"]["body"]["message"] == "Item moved successfully"


def test_publish_item_skip_publish_returns_none(test_workspace_with_notebook):
    """Test that _publish_item returns None when item is skipped due to exclusion."""
    workspace = test_workspace_with_notebook
    workspace.publish_item_name_exclude_regex = "TestNotebook"

    result = workspace._publish_item(item_name="TestNotebook", item_type="Notebook", return_response=True)

    # Should return None when item is skipped
    assert result is None


def test_publish_all_items_return_response_false_default(test_workspace_with_notebook):
    """Test that publish_all_items returns None by default (backward compatibility)."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace)
    assert result is None


def test_publish_all_items_return_response_false_explicit(test_workspace_with_notebook):
    """Test that publish_all_items returns None when return_response=False explicitly."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace, return_response=False)
    assert result is None


def test_publish_all_items_return_response_true(test_workspace_with_notebook):
    """Test that publish_all_items returns response dict when return_response=True."""
    workspace = test_workspace_with_notebook

    result = publish.publish_all_items(workspace, return_response=True)

    # Should return a dict (even if empty for this test)
    assert result is not None
    assert isinstance(result, dict)


def test_publish_item_with_kwargs_compatibility(test_workspace_with_notebook):
    """Test that _publish_item maintains compatibility with existing kwargs."""
    workspace = test_workspace_with_notebook

    with (
        patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
        patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
        patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
    ):
        # Test with existing kwargs
        result = workspace._publish_item(
            item_name="TestNotebook", item_type="Notebook", skip_publish_logging=True, return_response=True
        )

        # Should still work and return response
        assert result is not None
        assert isinstance(result, dict)


def test_publish_item_function_signature():
    """Test that _publish_item has the correct function signature."""
    import inspect
    from typing import Optional

    sig = inspect.signature(FabricWorkspace._publish_item)
    params = sig.parameters

    # Verify new parameter exists with correct default
    assert "return_response" in params
    assert params["return_response"].default is False

    # Verify return type annotation
    assert sig.return_annotation == Optional[dict]


def test_publish_all_items_function_signature():
    """Test that publish_all_items has the correct function signature."""
    import inspect
    from typing import Optional

    sig = inspect.signature(publish.publish_all_items)
    params = sig.parameters

    # Verify new parameter exists with correct default
    assert "return_response" in params
    assert params["return_response"].default is False

    # Verify return type annotation
    assert sig.return_annotation == Optional[dict]
