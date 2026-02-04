# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for item creation recovery logic when 'already in use' error occurs during API throttling."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd.fabric_workspace import FabricWorkspace

# Error messages used in tests
ALREADY_IN_USE_ERROR_GENERIC = (
    "Unhandled error occurred calling POST on "
    "'https://api.fabric.microsoft.com/v1/workspaces/test/items'. "
    "Message: Requested 'TestNotebook' is already in use."
)
ALREADY_IN_USE_ERROR_WITH_CODE = "Item 'TestNotebook' already exists (ItemDisplayNameAlreadyInUse)"
ALREADY_IN_USE_ERROR_SIMPLE = "Message: Requested 'TestNotebook' is already in use."
UNRELATED_ERROR = "Some other unrelated error occurred"


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint for testing recovery scenarios."""
    mock = MagicMock()
    mock.upn_auth = True
    return mock


@pytest.fixture
def test_workspace_for_recovery(mock_endpoint):
    """Create a test workspace for recovery testing."""
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
            # Manually set up repository items
            workspace.repository_items = {
                "Notebook": {
                    "TestNotebook": MagicMock(
                        guid=None,
                        folder_id="",
                        logical_id="test-notebook-logical-id",
                        description="Test notebook description",
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
            workspace.parameter_data = {}
            workspace.parameter_file_path = None
            yield workspace, mock_endpoint


def _create_recovery_mock_invoke(recovered_guid, error_message, url_check=None):
    """Create a mock invoke function for recovery testing."""
    call_count = 0
    update_called = False

    def mock_invoke(method, url, body=None, **kwargs):  # noqa: ARG001
        nonlocal call_count, update_called
        call_count += 1

        if call_count == 1:
            raise Exception(error_message)
        if call_count == 2:
            return {
                "body": {
                    "value": [{"id": recovered_guid, "displayName": "TestNotebook", "type": "Notebook"}],
                    "continuationToken": None,
                }
            }
        if call_count == 3:
            if url_check and url_check in url:
                update_called = True
            return {"body": {"id": recovered_guid, "message": "Updated"}, "status_code": 200, "header": {}}
        return {"body": {}}

    return mock_invoke, lambda: call_count, lambda: update_called


def _create_recovery_not_found_mock_invoke(error_message):
    """Create a mock invoke function where recovery lookup returns empty results."""
    call_count = 0

    def mock_invoke(method, url, body=None, **kwargs):  # noqa: ARG001
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise Exception(error_message)
        if call_count == 2:
            return {"body": {"value": [], "continuationToken": None}}
        return {"body": {}}

    return mock_invoke


class TestItemCreationRecovery:
    """Tests for the item creation recovery logic when 'already in use' error occurs."""

    def test_recovery_on_already_in_use_error_from_generic_handler(self, test_workspace_for_recovery):
        """
        Test recovery when item creation fails with 'already in use' error from generic error handler.

        This simulates the actual error format: "Message: Requested 'X' is already in use."
        """
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-item-guid-456"

        mock_invoke, get_call_count, _ = _create_recovery_mock_invoke(recovered_guid, ALREADY_IN_USE_ERROR_GENERIC)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Verify the item GUID was recovered and stored
        assert workspace.repository_items["Notebook"]["TestNotebook"].guid == recovered_guid

        # Verify deployed_items cache was updated
        assert "Notebook" in workspace.deployed_items
        assert "TestNotebook" in workspace.deployed_items["Notebook"]
        assert workspace.deployed_items["Notebook"]["TestNotebook"].guid == recovered_guid
        assert get_call_count() == 3

    def test_recovery_on_itemdisplaynamealreadyinuse_error_code(self, test_workspace_for_recovery):
        """
        Test recovery when error contains 'ItemDisplayNameAlreadyInUse' error code.

        This simulates the error format from the explicit handler in _fabric_endpoint.py.
        """
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-via-error-code"

        mock_invoke, _, _ = _create_recovery_mock_invoke(recovered_guid, ALREADY_IN_USE_ERROR_WITH_CODE)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        assert workspace.repository_items["Notebook"]["TestNotebook"].guid == recovered_guid

    def test_recovery_preserves_logical_id(self, test_workspace_for_recovery):
        """Test that recovery logic correctly preserves the logical_id from the repository item."""
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-item-guid-789"

        mock_invoke, _, _ = _create_recovery_mock_invoke(recovered_guid, ALREADY_IN_USE_ERROR_SIMPLE)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Verify the deployed_items entry has correct logical_id
        deployed_item = workspace.deployed_items["Notebook"]["TestNotebook"]
        assert deployed_item.logical_id == "test-notebook-logical-id"

    def test_recovery_sets_api_response_for_tracking(self, test_workspace_for_recovery):
        """Test that recovery logic sets a synthetic api_response for response tracking."""
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-item-guid-tracking"

        # Enable response collection
        workspace.responses = {"Notebook": {}}

        mock_invoke, _, _ = _create_recovery_mock_invoke(recovered_guid, ALREADY_IN_USE_ERROR_SIMPLE)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Response should be stored
        assert "TestNotebook" in workspace.responses["Notebook"]

    def test_recovery_fails_when_item_not_found(self, test_workspace_for_recovery):
        """Test that when recovery fails to find the item, the original error is re-raised."""
        workspace, mock_endpoint = test_workspace_for_recovery

        mock_invoke = _create_recovery_not_found_mock_invoke(ALREADY_IN_USE_ERROR_SIMPLE)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
            pytest.raises(Exception, match="already in use"),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

    def test_non_already_in_use_errors_are_reraised(self, test_workspace_for_recovery):
        """Test that errors other than 'already in use' are re-raised without recovery attempt."""
        workspace, mock_endpoint = test_workspace_for_recovery

        mock_endpoint.invoke.side_effect = Exception(UNRELATED_ERROR)

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
            pytest.raises(Exception, match="Some other unrelated error"),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Verify invoke was only called once (no recovery attempt)
        assert mock_endpoint.invoke.call_count == 1

    def test_recovery_proceeds_with_update_after_guid_recovery(self, test_workspace_for_recovery):
        """Test that after recovering the GUID, the publish proceeds with an UPDATE operation."""
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-for-update"

        mock_invoke, get_call_count, get_update_called = _create_recovery_mock_invoke(
            recovered_guid, ALREADY_IN_USE_ERROR_SIMPLE, url_check="updateDefinition"
        )
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Verify that 3 invoke calls were made and update was called
        assert get_call_count() == 3
        assert get_update_called()

    def test_recovery_initializes_deployed_items_dict_if_missing(self, test_workspace_for_recovery):
        """Test that recovery correctly initializes the item_type dict in deployed_items if missing."""
        workspace, mock_endpoint = test_workspace_for_recovery
        recovered_guid = "recovered-init-dict"

        # Ensure deployed_items doesn't have Notebook key
        workspace.deployed_items = {}

        mock_invoke, _, _ = _create_recovery_mock_invoke(recovered_guid, ALREADY_IN_USE_ERROR_SIMPLE)
        mock_endpoint.invoke.side_effect = mock_invoke

        with (
            patch.object(workspace, "_replace_logical_ids", side_effect=lambda x: x),
            patch.object(workspace, "_replace_parameters", side_effect=lambda file, _: file.contents),
            patch.object(workspace, "_replace_workspace_ids", side_effect=lambda x: x),
        ):
            workspace._publish_item(item_name="TestNotebook", item_type="Notebook")

        # Verify the Notebook dict was created
        assert "Notebook" in workspace.deployed_items
        assert "TestNotebook" in workspace.deployed_items["Notebook"]
