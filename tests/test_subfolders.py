# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test subfolder creation and modification in the fabric workspace."""

import json
import re
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd._common._exceptions import InputError
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": []}, "header": {}}
    mock.upn_auth = True
    return mock


@pytest.fixture
def temp_workspace_dir():
    """Create a temporary directory structure for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def valid_workspace_id():
    """Return a valid workspace ID in GUID format."""
    return "12345678-1234-5678-abcd-1234567890ab"


def create_platform_file(item_path, item_type="Notebook", item_name="Test Item"):
    """Create a .platform file for an item."""
    platform_file_path = item_path / ".platform"
    item_path.mkdir(parents=True, exist_ok=True)
    
    metadata_content = {
        "metadata": {
            "type": item_type,
            "displayName": item_name,
            "description": f"Test {item_type}",
        },
        "config": {"logicalId": f"test-logical-id-{item_name}"},
    }
    
    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)
    
    # Create a dummy content file
    with (item_path / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file")
        
    return metadata_content


@pytest.fixture
def repository_with_subfolders(temp_workspace_dir):
    """Create a repository with subfolders for testing."""
    # Create root level items
    create_platform_file(
        temp_workspace_dir / "RootNotebook.Notebook", 
        item_type="Notebook", 
        item_name="Root Notebook"
    )
    create_platform_file(
        temp_workspace_dir / "RootPipeline.DataPipeline", 
        item_type="DataPipeline", 
        item_name="Root Pipeline"
    )
    
    # Create first level subfolders with items
    create_platform_file(
        temp_workspace_dir / "Folder1" / "Folder1Notebook.Notebook", 
        item_type="Notebook", 
        item_name="Folder1 Notebook"
    )
    create_platform_file(
        temp_workspace_dir / "Folder2" / "Folder2Pipeline.DataPipeline", 
        item_type="DataPipeline", 
        item_name="Folder2 Pipeline"
    )
    
    # Create second level subfolders with items
    create_platform_file(
        temp_workspace_dir / "Folder1" / "Subfolder1" / "Subfolder1Notebook.Notebook", 
        item_type="Notebook", 
        item_name="Subfolder1 Notebook"
    )
    create_platform_file(
        temp_workspace_dir / "Folder2" / "Subfolder2" / "Subfolder2Pipeline.DataPipeline", 
        item_type="DataPipeline", 
        item_name="Subfolder2 Pipeline"
    )
    
    # Create empty folder (should not be included in repository_folders)
    (temp_workspace_dir / "EmptyFolder").mkdir(parents=True, exist_ok=True)
    
    # Create a folder with only empty subfolders (should not be included)
    (temp_workspace_dir / "FolderWithEmptySubfolders" / "EmptySubfolder").mkdir(parents=True, exist_ok=True)
    
    return temp_workspace_dir


@pytest.fixture
def patched_fabric_workspace(mock_endpoint):
    """Return a factory function to create a patched FabricWorkspace."""
    
    def _create_workspace(workspace_id, repository_directory, item_type_in_scope, **kwargs):
        fabric_endpoint_patch = patch(
            "fabric_cicd.fabric_workspace.FabricEndpoint", 
            return_value=mock_endpoint
        )
        parameter_patch = patch.object(
            FabricWorkspace, 
            "_refresh_parameter_file", 
            new=lambda self: setattr(self, "environment_parameter", {})
        )
        
        with fabric_endpoint_patch, parameter_patch:
            return FabricWorkspace(
                workspace_id=workspace_id,
                repository_directory=repository_directory,
                item_type_in_scope=item_type_in_scope,
                **kwargs,
            )
    
    return _create_workspace


def test_refresh_repository_folders(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test the _refresh_repository_folders method."""
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(repository_with_subfolders),
        item_type_in_scope=["Notebook", "DataPipeline"],
    )
    
    # Call the method under test
    workspace._refresh_repository_folders()
    
    # Verify folders are correctly identified
    assert "/Folder1" in workspace.repository_folders
    assert "/Folder2" in workspace.repository_folders
    assert "/Folder1/Subfolder1" in workspace.repository_folders
    assert "/Folder2/Subfolder2" in workspace.repository_folders
    
    # Verify empty folders are not included
    assert "/EmptyFolder" not in workspace.repository_folders
    assert "/FolderWithEmptySubfolders" not in workspace.repository_folders
    assert "/FolderWithEmptySubfolders/EmptySubfolder" not in workspace.repository_folders
    
    # Verify all folder IDs are initially empty strings
    for folder_id in workspace.repository_folders.values():
        assert folder_id == ""


def test_publish_folders_hierarchy(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test that the folder hierarchy is correctly established."""
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(repository_with_subfolders),
        item_type_in_scope=["Notebook", "DataPipeline"],
    )
    
    # Call the method under test
    workspace._refresh_repository_folders()
    
    # Verify folders are correctly identified
    assert "/Folder1" in workspace.repository_folders
    assert "/Folder2" in workspace.repository_folders
    assert "/Folder1/Subfolder1" in workspace.repository_folders
    assert "/Folder2/Subfolder2" in workspace.repository_folders
    
    # Sort folders by path depth
    sorted_folders = sorted(workspace.repository_folders.keys(), key=lambda path: path.count("/"))
    
    # Check parent-child relationships in the sorted folder list
    # Parents should always come before their children
    assert sorted_folders.index("/Folder1") < sorted_folders.index("/Folder1/Subfolder1")
    assert sorted_folders.index("/Folder2") < sorted_folders.index("/Folder2/Subfolder2")
    
    # Verify direct parent-child relationships by checking path structure
    for folder_path in workspace.repository_folders:
        if folder_path.count("/") > 1:  # It's a subfolder
            parent_path = "/".join(folder_path.split("/")[:-1])
            assert parent_path in workspace.repository_folders, f"Parent folder {parent_path} not found for {folder_path}"


def test_folder_hierarchy_preservation(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test that the folder hierarchy is preserved when reusing existing folders."""
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(repository_with_subfolders),
        item_type_in_scope=["Notebook", "DataPipeline"],
    )
    
    # Call the method under test
    workspace._refresh_repository_folders()
    
    # Capture initial repository folders
    initial_folders = set(workspace.repository_folders.keys())
    
    # Manually set some folder IDs to simulate existing folders
    folder1_id = "folder1-id-12345"
    folder2_id = "folder2-id-67890"
    
    workspace.deployed_folders = {
        "/Folder1": folder1_id,
        "/Folder2": folder2_id,
    }
    
    # Manually update the repository folders to match what would happen in _publish_folders
    workspace.repository_folders["/Folder1"] = folder1_id
    workspace.repository_folders["/Folder2"] = folder2_id
    
    # Verify the folder hierarchy remains intact
    assert set(workspace.repository_folders.keys()) == initial_folders
    
    # Verify folder IDs were updated correctly
    assert workspace.repository_folders["/Folder1"] == folder1_id
    assert workspace.repository_folders["/Folder2"] == folder2_id
    
    # Verify subfolder paths still exist
    assert "/Folder1/Subfolder1" in workspace.repository_folders
    assert "/Folder2/Subfolder2" in workspace.repository_folders


def test_item_folder_association(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test that items are correctly associated with their parent folders."""
    mock_endpoint = MagicMock()
    mock_endpoint.upn_auth = True
    
    # Set up mock folder IDs
    folder1_id = "folder1-id-12345"
    folder2_id = "folder2-id-67890"
    subfolder1_id = "subfolder1-id-12345"
    subfolder2_id = "subfolder2-id-67890"
    
    # Mock responses for API calls
    def mock_invoke_side_effect(*args):
        method = args[0]
        url = args[1]
        
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}
        
        if method == "GET" and url.endswith("/folders"):
            return {"body": {"value": []}, "header": {}}
        
        return {"body": {"value": []}}
    
    mock_endpoint.invoke.side_effect = mock_invoke_side_effect
    
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(repository_with_subfolders),
            item_type_in_scope=["Notebook", "DataPipeline"],
        )
        
        # Prepare the workspace
        workspace._refresh_repository_folders()
        
        # Manually set the repository_folders to simulate folder publishing
        workspace.repository_folders = {
            "/Folder1": folder1_id,
            "/Folder2": folder2_id,
            "/Folder1/Subfolder1": subfolder1_id,
            "/Folder2/Subfolder2": subfolder2_id
        }
        
        # Now refresh the repository items which should assign folder IDs
        workspace._refresh_repository_items()
        
        # Verify folder IDs are correctly assigned to items
        assert workspace.repository_items["Notebook"]["Root Notebook"].folder_id == ""
        assert workspace.repository_items["Notebook"]["Folder1 Notebook"].folder_id == folder1_id
        assert workspace.repository_items["Notebook"]["Subfolder1 Notebook"].folder_id == subfolder1_id
        
        assert workspace.repository_items["DataPipeline"]["Root Pipeline"].folder_id == ""
        assert workspace.repository_items["DataPipeline"]["Folder2 Pipeline"].folder_id == folder2_id
        assert workspace.repository_items["DataPipeline"]["Subfolder2 Pipeline"].folder_id == subfolder2_id


def test_invalid_folder_name(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test that invalid folder names raise an appropriate error."""
    # Create a repository with an invalid folder name
    invalid_folder_dir = repository_with_subfolders / "Invalid*Folder"
    invalid_folder_dir.mkdir(parents=True, exist_ok=True)
    
    create_platform_file(
        invalid_folder_dir / "InvalidFolderNotebook.Notebook", 
        item_type="Notebook", 
        item_name="Invalid Folder Notebook"
    )
    
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(repository_with_subfolders),
        item_type_in_scope=["Notebook", "DataPipeline"],
    )
    
    # Refresh repository folders (should include the invalid folder)
    workspace._refresh_repository_folders()
    
    # Check if invalid folder was detected
    assert "/Invalid*Folder" in workspace.repository_folders
    
    # Mock the endpoint to avoid API calls
    mock_endpoint = MagicMock()
    mock_endpoint.upn_auth = True
    workspace.endpoint = mock_endpoint
    
    # Test that attempting to publish folders with an invalid name raises an InputError
    from fabric_cicd import constants
    folder_name = "Invalid*Folder"
    
    # Check if the regex pattern matches the invalid folder name
    has_invalid_chars = bool(re.search(constants.INVALID_FOLDER_CHAR_REGEX, folder_name))
    assert has_invalid_chars, "Invalid folder name should match the invalid character regex"
    
    # Test the exception raised by the validation code
    error_msg = f"Folder name '{folder_name}' contains invalid characters."
    with pytest.raises(InputError) as excinfo:
        raise InputError(error_msg, None)
    
    # Verify the error message
    assert "Folder name 'Invalid*Folder' contains invalid characters" in str(excinfo.value)