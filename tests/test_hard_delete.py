# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test enable_hard_delete feature flag functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fixtures.credentials import DummyTokenCredential

import fabric_cicd.constants as constants
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to capture DELETE requests."""
    mock = MagicMock()
    mock.delete_urls = []

    def mock_invoke(method, url, **_kwargs):
        if method == "DELETE":
            mock.delete_urls.append(url)
            return {"body": {}, "header": {}, "status_code": 200}
        if method == "GET" and "workspaces" in url and not url.endswith("/items"):
            return {"body": {"value": [], "capacityId": "test-capacity"}}
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}
        return {"body": {"value": []}}

    mock.invoke.side_effect = mock_invoke
    mock.upn_auth = True
    return mock


@pytest.fixture
def test_workspace(mock_endpoint):
    """Create a test workspace with a notebook item."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

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
                token_credential=DummyTokenCredential(),
            )
            yield workspace


@pytest.fixture(autouse=True)
def _clear_feature_flags():
    """Clear feature flags before and after each test to avoid state leakage."""
    constants.FEATURE_FLAG.discard("enable_hard_delete")
    yield
    constants.FEATURE_FLAG.discard("enable_hard_delete")


def test_unpublish_item_without_hard_delete_flag(test_workspace, mock_endpoint):
    """Test that _unpublish_item uses a plain DELETE URL when flag is not set."""
    item_guid = "mock-guid-123"
    test_workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid=item_guid)}}

    mock_endpoint.delete_urls.clear()

    test_workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

    assert len(mock_endpoint.delete_urls) == 1
    delete_url = mock_endpoint.delete_urls[0]
    assert delete_url.endswith(f"/items/{item_guid}")
    assert "hardDelete" not in delete_url


def test_unpublish_item_with_hard_delete_flag(test_workspace, mock_endpoint):
    """Test that _unpublish_item appends ?hardDelete=true when flag is set."""
    item_guid = "mock-guid-456"
    test_workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid=item_guid)}}

    constants.FEATURE_FLAG.add("enable_hard_delete")
    mock_endpoint.delete_urls.clear()

    test_workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

    assert len(mock_endpoint.delete_urls) == 1
    delete_url = mock_endpoint.delete_urls[0]
    assert delete_url == f"{test_workspace.base_api_url}/items/{item_guid}?hardDelete=true"


def test_hard_delete_flag_via_append_feature_flag(test_workspace, mock_endpoint):
    """Test that enable_hard_delete works when set via append_feature_flag."""
    from fabric_cicd import append_feature_flag

    item_guid = "mock-guid-789"
    test_workspace.deployed_items = {"Notebook": {"TestNotebook": MagicMock(guid=item_guid)}}

    append_feature_flag("enable_hard_delete")
    mock_endpoint.delete_urls.clear()

    test_workspace._unpublish_item(item_name="TestNotebook", item_type="Notebook")

    assert len(mock_endpoint.delete_urls) == 1
    assert "hardDelete=true" in mock_endpoint.delete_urls[0]
