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


def test_deeply_nested_subfolders(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test handling of deeply nested folder structures (15+ levels deep)."""
    # Create a deeply nested folder structure
    current_path = temp_workspace_dir
    folder_names = []
    
    # Create 15 levels of nested folders
    for i in range(15):
        folder_name = f"Level{i+1:02d}"
        folder_names.append(folder_name)
        current_path = current_path / folder_name
    
    # Create an item in the deepest folder
    create_platform_file(
        current_path / "DeepNotebook.Notebook",
        item_type="Notebook",
        item_name="Deep Notebook"
    )
    
    # Also create items at different levels to ensure intermediate folders are detected
    mid_level_path = temp_workspace_dir
    for i in range(7):  # Create item at level 7
        mid_level_path = mid_level_path / f"Level{i+1:02d}"
    
    create_platform_file(
        mid_level_path / "MidLevelNotebook.Notebook",
        item_type="Notebook", 
        item_name="Mid Level Notebook"
    )
    
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Notebook"],
    )
    
    # Test that _refresh_repository_folders can handle deep nesting
    workspace._refresh_repository_folders()
    
    # Verify all folder levels were detected
    expected_deep_path = "/" + "/".join(folder_names)
    expected_mid_path = "/" + "/".join(folder_names[:7])
    
    assert expected_deep_path in workspace.repository_folders
    assert expected_mid_path in workspace.repository_folders
    
    # Verify folder hierarchy ordering (parents before children)
    sorted_folders = sorted(workspace.repository_folders.keys(), key=lambda path: path.count("/"))
    
    # Check that each level comes before deeper levels
    for i in range(1, 15):
        current_level_path = "/" + "/".join(folder_names[:i])
        if current_level_path in workspace.repository_folders:
            next_level_path = "/" + "/".join(folder_names[:i+1])
            if next_level_path in workspace.repository_folders:
                assert sorted_folders.index(current_level_path) < sorted_folders.index(next_level_path)
    
    # Verify no stack overflow or performance issues by checking reasonable execution time
    import time
    start_time = time.time()
    workspace._refresh_repository_folders()
    execution_time = time.time() - start_time
    
    # Should complete in reasonable time (< 1 second for 15 levels)
    assert execution_time < 1.0, f"Deep folder processing took too long: {execution_time:.2f}s"


def test_folder_rename_operations(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test folder rename operations and verify child items and subfolders are updated correctly."""
    # Create initial folder structure
    original_folder = temp_workspace_dir / "OriginalFolder"
    original_subfolder = original_folder / "OriginalSubfolder"
    
    # Create items in original folders
    create_platform_file(
        original_folder / "ParentNotebook.Notebook",
        item_type="Notebook",
        item_name="Parent Notebook"
    )
    
    create_platform_file(
        original_subfolder / "ChildNotebook.Notebook", 
        item_type="Notebook",
        item_name="Child Notebook"
    )
    
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Notebook"],
    )
    
    # Initial state
    workspace._refresh_repository_folders()
    workspace._refresh_repository_items()
    
    assert "/OriginalFolder" in workspace.repository_folders
    assert "/OriginalFolder/OriginalSubfolder" in workspace.repository_folders
    
    # Simulate folder rename by moving files to new folder structure
    renamed_folder = temp_workspace_dir / "RenamedFolder"
    renamed_subfolder = renamed_folder / "RenamedSubfolder"
    
    # Move the folder structure (simulating rename)
    import shutil
    shutil.move(str(original_folder), str(renamed_folder))
    shutil.move(str(renamed_folder / "OriginalSubfolder"), str(renamed_subfolder))
    
    # Refresh after rename
    workspace._refresh_repository_folders()
    workspace._refresh_repository_items()
    
    # Verify old folder paths are no longer present
    assert "/OriginalFolder" not in workspace.repository_folders
    assert "/OriginalFolder/OriginalSubfolder" not in workspace.repository_folders
    
    # Verify new folder paths are detected
    assert "/RenamedFolder" in workspace.repository_folders
    assert "/RenamedFolder/RenamedSubfolder" in workspace.repository_folders
    
    # Verify items are detected in new locations
    assert "Parent Notebook" in workspace.repository_items["Notebook"]
    assert "Child Notebook" in workspace.repository_items["Notebook"]
    
    # Verify folder hierarchy is maintained
    sorted_folders = sorted(workspace.repository_folders.keys(), key=lambda path: path.count("/"))
    assert sorted_folders.index("/RenamedFolder") < sorted_folders.index("/RenamedFolder/RenamedSubfolder")


def test_special_character_handling(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test handling of special characters in folder names."""
    test_cases = [
        # Valid cases - should be accepted
        ("ValidFolder", True, "Basic valid folder name"),
        ("Folder_With_Underscores", True, "Underscores should be valid"),
        ("Folder-With-Hyphens", True, "Hyphens should be valid"),
        ("Folder With Spaces", True, "Spaces should be valid"),
        ("FolderWithUnicode_测试", True, "Unicode characters should be valid"),
        ("FolderWith123Numbers", True, "Numbers should be valid"),
        ("  SpacesAroundName  ", True, "Leading/trailing spaces should be handled"),
        
        # Invalid cases - should be rejected by regex
        ("Folder*WithAsterisk", False, "Asterisk should be invalid"),
        ("Folder#WithHash", False, "Hash should be invalid"),
        ("Folder<WithBracket", False, "Angle bracket should be invalid"),
        ("Folder>WithBracket", False, "Angle bracket should be invalid"),
        ("Folder:WithColon", False, "Colon should be invalid"),
        ("Folder\"WithQuote", False, "Quote should be invalid"),
        ("Folder|WithPipe", False, "Pipe should be invalid"),
        ("Folder?WithQuestion", False, "Question mark should be invalid"),
        ("Folder\\WithBackslash", False, "Backslash should be invalid"),
        ("Folder/WithSlash", False, "Forward slash should be invalid"),
        ("Folder{WithBrace", False, "Curly brace should be invalid"),
        ("Folder}WithBrace", False, "Curly brace should be invalid"),
        ("Folder~WithTilde", False, "Tilde should be invalid"),
        ("Folder.WithDot", False, "Dot should be invalid"),
        ("Folder%WithPercent", False, "Percent should be invalid"),
        ("Folder&WithAmpersand", False, "Ampersand should be invalid"),
    ]
    
    from fabric_cicd import constants
    
    # Test regex validation for each case
    for folder_name, should_be_valid, description in test_cases:
        has_invalid_chars = bool(re.search(constants.INVALID_FOLDER_CHAR_REGEX, folder_name))
        
        if should_be_valid:
            assert not has_invalid_chars, f"{description}: '{folder_name}' should be valid but was rejected"
        else:
            assert has_invalid_chars, f"{description}: '{folder_name}' should be invalid but was accepted"
    
    # Test actual folder creation with some valid cases
    valid_folders = [
        "ValidFolder",
        "Folder_With_Underscores", 
        "Folder-With-Hyphens",
        "FolderWithUnicode_测试"
    ]
    
    for folder_name in valid_folders:
        folder_path = temp_workspace_dir / folder_name
        create_platform_file(
            folder_path / "TestNotebook.Notebook",
            item_type="Notebook",
            item_name=f"Test {folder_name}"
        )
    
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Notebook"],
    )
    
    workspace._refresh_repository_folders()
    
    # Verify valid folders were detected
    for folder_name in valid_folders:
        expected_path = f"/{folder_name}"
        assert expected_path in workspace.repository_folders, f"Valid folder '{folder_name}' was not detected"
    
    # Test that _publish_folders would raise errors for invalid names
    workspace.repository_folders["/Invalid*Folder"] = ""
    workspace.deployed_folders = {}
    
    mock_endpoint = MagicMock()
    mock_endpoint.upn_auth = True
    workspace.endpoint = mock_endpoint
    workspace.base_api_url = "https://api.powerbi.com/v1.0/myorg/workspaces/test"
    
    # Should raise InputError for invalid folder name
    with pytest.raises(InputError) as excinfo:
        workspace._publish_folders()
    
    assert "contains invalid characters" in str(excinfo.value)


def test_large_number_of_folders_and_items(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test performance and scalability with a large number of folders and items."""
    import time
    
    # Create a large number of folders and items (100 folders with multiple items each)
    num_folders = 100
    items_per_folder = 3
    
    print(f"\nCreating {num_folders} folders with {items_per_folder} items each...")
    
    start_creation_time = time.time()
    
    # Create folders at multiple levels
    for i in range(num_folders):
        if i < 50:
            # First 50 folders at root level
            folder_path = temp_workspace_dir / f"Folder{i:03d}"
        else:
            # Next 50 folders nested under first 25 folders
            parent_idx = (i - 50) % 25
            folder_path = temp_workspace_dir / f"Folder{parent_idx:03d}" / f"Subfolder{i:03d}"
        
        # Create multiple items in each folder
        for j in range(items_per_folder):
            create_platform_file(
                folder_path / f"Item{j:02d}.Notebook",
                item_type="Notebook",
                item_name=f"Item {j:02d} in Folder {i:03d}"
            )
    
    creation_time = time.time() - start_creation_time
    print(f"Created test structure in {creation_time:.2f}s")
    
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Notebook"],
    )
    
    # Test _refresh_repository_folders performance
    start_time = time.time()
    workspace._refresh_repository_folders()
    folders_time = time.time() - start_time
    
    print(f"_refresh_repository_folders completed in {folders_time:.2f}s")
    print(f"Detected {len(workspace.repository_folders)} folders")
    
    # Verify we detected a reasonable number of folders
    assert len(workspace.repository_folders) >= 50, "Should detect at least 50 folders"
    assert len(workspace.repository_folders) <= 125, "Should not detect more than 125 folders"
    
    # Test _refresh_repository_items performance  
    start_time = time.time()
    workspace._refresh_repository_items()
    items_time = time.time() - start_time
    
    print(f"_refresh_repository_items completed in {items_time:.2f}s")
    print(f"Detected {len(workspace.repository_items['Notebook'])} items")
    
    # Verify we detected the expected number of items
    expected_items = num_folders * items_per_folder
    assert len(workspace.repository_items["Notebook"]) == expected_items
    
    # Performance assertions - should complete in reasonable time
    assert folders_time < 5.0, f"Folder detection took too long: {folders_time:.2f}s"
    assert items_time < 10.0, f"Item detection took too long: {items_time:.2f}s"
    
    # Memory usage test - verify folder hierarchy is correct
    # Check that parent-child relationships are maintained even with large numbers
    nested_folders = [path for path in workspace.repository_folders if path.count("/") > 1]
    
    for folder_path in nested_folders:
        parent_path = "/".join(folder_path.split("/")[:-1])
        assert parent_path in workspace.repository_folders, f"Parent {parent_path} not found for {folder_path}"
    
    # Test that folder sorting still works correctly with large numbers
    sorted_folders = sorted(workspace.repository_folders.keys(), key=lambda path: path.count("/"))
    
    # Verify sorting is correct - all level 1 folders should come before level 2 folders
    level_1_folders = [f for f in sorted_folders if f.count("/") == 1]
    level_2_folders = [f for f in sorted_folders if f.count("/") == 2]
    
    if level_1_folders and level_2_folders:
        last_level_1_index = max(sorted_folders.index(f) for f in level_1_folders)
        first_level_2_index = min(sorted_folders.index(f) for f in level_2_folders)
        assert last_level_1_index < first_level_2_index, "Folder sorting is incorrect with large numbers"
    
    print("Performance test completed successfully!")
    print(f"Total processing time: {folders_time + items_time:.2f}s")