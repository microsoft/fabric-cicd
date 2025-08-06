# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd import deploy_all_items
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": [], "capacityId": "test-capacity"}}
    mock.upn_auth = True
    return mock


@pytest.fixture
def temp_workspace_dir():
    """Create a temporary directory structure for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace_dir = Path(temp_dir)
        # Create some sample items
        (workspace_dir / "Environment").mkdir(parents=True, exist_ok=True)
        (workspace_dir / "Environment" / "test_env.json").write_text('{"displayName": "test_env"}')
        yield workspace_dir


@pytest.fixture
def valid_workspace_id():
    """Return a valid workspace ID in GUID format."""
    return "12345678-1234-5678-abcd-1234567890ab"


@pytest.fixture
def patched_fabric_workspace(mock_endpoint):
    """Provide a FabricWorkspace with patched endpoint."""
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        yield


def test_deploy_all_items_basic(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):  # noqa: ARG001
    """Test basic deploy_all_items functionality."""
    workspace = FabricWorkspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Environment"],
    )

    # Mock the individual functions to ensure they're called
    with (
        patch("fabric_cicd.publish.publish_all_items") as mock_publish,
        patch("fabric_cicd.publish.unpublish_all_orphan_items") as mock_unpublish,
    ):
        deploy_all_items(workspace)

        # Verify both functions were called with correct arguments
        mock_publish.assert_called_once_with(workspace, None)
        mock_unpublish.assert_called_once_with(workspace, "^$")


def test_deploy_all_items_with_exclusions(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):  # noqa: ARG001
    """Test deploy_all_items with exclusion patterns."""
    workspace = FabricWorkspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Environment"],
    )

    publish_regex = ".*_test"
    unpublish_regex = ".*_preserve"

    # Mock the individual functions to ensure they're called with correct parameters
    with (
        patch("fabric_cicd.publish.publish_all_items") as mock_publish,
        patch("fabric_cicd.publish.unpublish_all_orphan_items") as mock_unpublish,
    ):
        deploy_all_items(workspace, publish_exclude_regex=publish_regex, unpublish_exclude_regex=unpublish_regex)

        # Verify both functions were called with correct exclusion patterns
        mock_publish.assert_called_once_with(workspace, publish_regex)
        mock_unpublish.assert_called_once_with(workspace, unpublish_regex)


def test_deploy_all_items_order(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):  # noqa: ARG001
    """Test that deploy_all_items calls publish before unpublish."""
    workspace = FabricWorkspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Environment"],
    )

    call_order = []

    def mock_publish(*_args, **_kwargs):
        call_order.append("publish")

    def mock_unpublish(*_args, **_kwargs):
        call_order.append("unpublish")

    # Mock the individual functions to track order
    with (
        patch("fabric_cicd.publish.publish_all_items", side_effect=mock_publish),
        patch("fabric_cicd.publish.unpublish_all_orphan_items", side_effect=mock_unpublish),
    ):
        deploy_all_items(workspace)

        # Verify order: publish should be called before unpublish
        assert call_order == ["publish", "unpublish"]


def test_deploy_all_items_validation(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):  # noqa: ARG001
    """Test that deploy_all_items validates workspace object."""
    workspace = FabricWorkspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Environment"],
    )

    # Mock the validation function to track it's called
    with (
        patch("fabric_cicd.publish.validate_fabric_workspace_obj", return_value=workspace) as mock_validate,
        patch("fabric_cicd.publish.publish_all_items") as mock_publish,
        patch("fabric_cicd.publish.unpublish_all_orphan_items") as mock_unpublish,
    ):
        deploy_all_items(workspace)

        # Verify validation was called
        mock_validate.assert_called_once_with(workspace)
        mock_publish.assert_called_once()
        mock_unpublish.assert_called_once()


def test_deploy_all_items_import():
    """Test that deploy_all_items can be imported directly."""
    from fabric_cicd import deploy_all_items as imported_deploy

    # Verify it's the same function
    assert imported_deploy == deploy_all_items
