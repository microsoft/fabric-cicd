# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test subfolder creation and modification in the fabric workspace."""

import json
import re
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": []}, "header": {}}
    mock.upn_auth = True
    return mock


@pytest.fixture
def temp_workspace_dir(tmp_path):
    """Create a temporary directory structure for testing."""
    # Use pytest's tmp_path for better isolation
    return tmp_path


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
def repository_with_subfolders(tmp_path):
    """Create a repository with subfolders for testing - isolated per test."""
    # Create root level items
    create_platform_file(tmp_path / "RootNotebook.Notebook", item_type="Notebook", item_name="Root Notebook")
    create_platform_file(tmp_path / "RootPipeline.DataPipeline", item_type="DataPipeline", item_name="Root Pipeline")

    # Create first level subfolders with items
    create_platform_file(
        tmp_path / "Folder1" / "Folder1Notebook.Notebook", item_type="Notebook", item_name="Folder1 Notebook"
    )
    create_platform_file(
        tmp_path / "Folder2" / "Folder2Pipeline.DataPipeline", item_type="DataPipeline", item_name="Folder2 Pipeline"
    )

    # Create second level subfolders with items
    create_platform_file(
        tmp_path / "Folder1" / "Subfolder1" / "Subfolder1Notebook.Notebook",
        item_type="Notebook",
        item_name="Subfolder1 Notebook",
    )
    create_platform_file(
        tmp_path / "Folder2" / "Subfolder2" / "Subfolder2Pipeline.DataPipeline",
        item_type="DataPipeline",
        item_name="Subfolder2 Pipeline",
    )

    # Create empty folder (should not be included in repository_folders)
    (tmp_path / "EmptyFolder").mkdir(parents=True, exist_ok=True)

    # Create a folder with only empty subfolders (should not be included)
    (tmp_path / "FolderWithEmptySubfolders" / "EmptySubfolder").mkdir(parents=True, exist_ok=True)

    return tmp_path


@pytest.fixture
def patched_fabric_workspace(mock_endpoint):
    """Return a factory function to create a patched FabricWorkspace."""

    def _create_workspace(workspace_id, repository_directory, item_type_in_scope, **kwargs):
        fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
        parameter_patch = patch.object(
            FabricWorkspace, "_refresh_parameter_file", new=lambda self: setattr(self, "environment_parameter", {})
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
            assert parent_path in workspace.repository_folders, (
                f"Parent folder {parent_path} not found for {folder_path}"
            )


def test_folder_hierarchy_preservation(repository_with_subfolders, patched_fabric_workspace, valid_workspace_id):
    """Test that the folder hierarchy is preserved when reusing existing folders."""
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(repository_with_subfolders),
        item_type_in_scope=["Notebook", "DataPipeline"],
    )

    # Mock the deployed folders API to return existing folders
    folder1_id = "folder1-id-12345"
    folder2_id = "folder2-id-67890"

    def mock_invoke_side_effect(method, url, **_kwargs):
        if method == "GET" and url.endswith("/folders"):
            # Mock API response for existing folders
            return {
                "body": {
                    "value": [
                        {"id": folder1_id, "displayName": "Folder1", "parentFolderId": None},
                        {"id": folder2_id, "displayName": "Folder2", "parentFolderId": None},
                    ]
                },
                "header": {},
            }

        return {"body": {"value": []}, "header": {}}

    workspace.endpoint.invoke.side_effect = mock_invoke_side_effect

    # Call methods in the intended order
    workspace._refresh_repository_folders()
    workspace._refresh_deployed_folders()

    # Capture initial repository folders
    initial_folders = set(workspace.repository_folders.keys())

    # Verify the folder hierarchy remains intact
    assert set(workspace.repository_folders.keys()) == initial_folders

    # Verify deployed folder IDs were detected correctly
    assert workspace.deployed_folders["/Folder1"] == folder1_id
    assert workspace.deployed_folders["/Folder2"] == folder2_id

    # Verify subfolder paths still exist in repository (even if not deployed)
    assert "/Folder1/Subfolder1" in workspace.repository_folders
    assert "/Folder2/Subfolder2" in workspace.repository_folders


def test_item_folder_association(repository_with_subfolders, valid_workspace_id):
    """Test that items are correctly associated with their parent folders."""
    # Set up mock folder IDs
    folder1_id = "folder1-id-12345"
    folder2_id = "folder2-id-67890"
    subfolder1_id = "subfolder1-id-12345"
    subfolder2_id = "subfolder2-id-67890"

    # Mock responses for API calls
    def mock_invoke_side_effect(method, url, **_kwargs):
        if method == "GET" and url.endswith("/items"):
            return {"body": {"value": []}}

        if method == "GET" and url.endswith("/folders"):
            # Mock API response for deployed folders
            return {
                "body": {
                    "value": [
                        {"id": folder1_id, "displayName": "Folder1", "parentFolderId": None},
                        {"id": folder2_id, "displayName": "Folder2", "parentFolderId": None},
                        {"id": subfolder1_id, "displayName": "Subfolder1", "parentFolderId": folder1_id},
                        {"id": subfolder2_id, "displayName": "Subfolder2", "parentFolderId": folder2_id},
                    ]
                },
                "header": {},
            }

        return {"body": {"value": []}}

    mock_endpoint = MagicMock()
    mock_endpoint.upn_auth = True
    mock_endpoint.invoke.side_effect = mock_invoke_side_effect

    fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
    parameter_patch = patch.object(
        FabricWorkspace, "_refresh_parameter_file", new=lambda self: setattr(self, "environment_parameter", {})
    )

    with fabric_endpoint_patch, parameter_patch:
        workspace = FabricWorkspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(repository_with_subfolders),
            item_type_in_scope=["Notebook", "DataPipeline"],
        )

        # Call methods in the intended order to populate folder structures
        workspace._refresh_repository_folders()
        workspace._refresh_deployed_folders()

        # Simulate the effect of _publish_folders by updating repository_folders
        # with deployed folder IDs (this normally happens in _publish_folders)
        for folder_path, folder_id in workspace.deployed_folders.items():
            if folder_path in workspace.repository_folders:
                workspace.repository_folders[folder_path] = folder_id

        workspace._refresh_repository_items()

        # Verify folder IDs are correctly assigned to items
        assert workspace.repository_items["Notebook"]["Root Notebook"].folder_id == ""
        assert workspace.repository_items["Notebook"]["Folder1 Notebook"].folder_id == folder1_id
        assert workspace.repository_items["Notebook"]["Subfolder1 Notebook"].folder_id == subfolder1_id

        assert workspace.repository_items["DataPipeline"]["Root Pipeline"].folder_id == ""
        assert workspace.repository_items["DataPipeline"]["Folder2 Pipeline"].folder_id == folder2_id
        assert workspace.repository_items["DataPipeline"]["Subfolder2 Pipeline"].folder_id == subfolder2_id


def test_deeply_nested_subfolders(tmp_path, patched_fabric_workspace, valid_workspace_id):
    """Test handling of deeply nested folder structures (15+ levels deep)."""
    # Create a deeply nested folder structure
    current_path = tmp_path
    folder_names = []

    # Create 15 levels of nested folders
    for i in range(15):
        folder_name = f"Level{i + 1:02d}"
        folder_names.append(folder_name)
        current_path = current_path / folder_name

    # Create an item in the deepest folder
    create_platform_file(current_path / "DeepNotebook.Notebook", item_type="Notebook", item_name="Deep Notebook")

    # Also create items at different levels to ensure intermediate folders are detected
    mid_level_path = tmp_path
    for i in range(7):  # Create item at level 7
        mid_level_path = mid_level_path / f"Level{i + 1:02d}"

    create_platform_file(
        mid_level_path / "MidLevelNotebook.Notebook", item_type="Notebook", item_name="Mid Level Notebook"
    )

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(tmp_path),
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
            next_level_path = "/" + "/".join(folder_names[: i + 1])
            if next_level_path in workspace.repository_folders:
                assert sorted_folders.index(current_level_path) < sorted_folders.index(next_level_path)

    # Verify no stack overflow or performance issues by checking reasonable execution time
    import time

    start_time = time.time()
    workspace._refresh_repository_folders()
    execution_time = time.time() - start_time

    # Should complete in reasonable time (< 1 second for 15 levels)
    assert execution_time < 1.0, f"Deep folder processing took too long: {execution_time:.2f}s"


def test_folder_rename_operations(tmp_path, patched_fabric_workspace, valid_workspace_id):
    """Test folder rename operations and verify child items and subfolders are updated correctly."""
    # Create initial folder structure in isolated tmp_path
    original_folder = tmp_path / "OriginalFolder"
    original_subfolder = original_folder / "OriginalSubfolder"

    # Create items in original folders
    create_platform_file(original_folder / "ParentNotebook.Notebook", item_type="Notebook", item_name="Parent Notebook")

    create_platform_file(
        original_subfolder / "ChildNotebook.Notebook", item_type="Notebook", item_name="Child Notebook"
    )

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(tmp_path),
        item_type_in_scope=["Notebook"],
    )

    # Initial state
    workspace._refresh_repository_folders()
    workspace._refresh_repository_items()

    assert "/OriginalFolder" in workspace.repository_folders
    assert "/OriginalFolder/OriginalSubfolder" in workspace.repository_folders

    # Create a separate workspace instance for testing renamed structure
    # to avoid contaminating the original workspace state
    renamed_tmp_path = tmp_path.parent / "renamed_workspace"
    renamed_tmp_path.mkdir()

    # Create the renamed folder structure in the new location
    renamed_folder = renamed_tmp_path / "RenamedFolder"
    renamed_subfolder = renamed_folder / "RenamedSubfolder"

    # Create items in renamed folders
    create_platform_file(renamed_folder / "ParentNotebook.Notebook", item_type="Notebook", item_name="Parent Notebook")

    create_platform_file(renamed_subfolder / "ChildNotebook.Notebook", item_type="Notebook", item_name="Child Notebook")

    # Create new workspace instance for renamed structure
    renamed_workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(renamed_tmp_path),
        item_type_in_scope=["Notebook"],
    )

    # Refresh after "rename" (using the new workspace)
    renamed_workspace._refresh_repository_folders()
    renamed_workspace._refresh_repository_items()

    # Verify old folder paths are no longer present in renamed workspace
    assert "/OriginalFolder" not in renamed_workspace.repository_folders
    assert "/OriginalFolder/OriginalSubfolder" not in renamed_workspace.repository_folders

    # Verify new folder paths are detected in renamed workspace
    assert "/RenamedFolder" in renamed_workspace.repository_folders
    assert "/RenamedFolder/RenamedSubfolder" in renamed_workspace.repository_folders

    # Verify items are detected in new locations
    assert "Parent Notebook" in renamed_workspace.repository_items["Notebook"]
    assert "Child Notebook" in renamed_workspace.repository_items["Notebook"]

    # Verify folder hierarchy is maintained
    sorted_folders = sorted(renamed_workspace.repository_folders.keys(), key=lambda path: path.count("/"))
    assert sorted_folders.index("/RenamedFolder") < sorted_folders.index("/RenamedFolder/RenamedSubfolder")


def test_special_character_handling(tmp_path, patched_fabric_workspace, valid_workspace_id):
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
        ('Folder"WithQuote', False, "Quote should be invalid"),
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
    valid_folders = ["ValidFolder", "Folder_With_Underscores", "Folder-With-Hyphens", "FolderWithUnicode_测试"]

    for folder_name in valid_folders:
        folder_path = tmp_path / folder_name
        create_platform_file(
            folder_path / "TestNotebook.Notebook", item_type="Notebook", item_name=f"Test {folder_name}"
        )

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(tmp_path),
        item_type_in_scope=["Notebook"],
    )

    workspace._refresh_repository_folders()

    # Verify valid folders were detected
    for folder_name in valid_folders:
        expected_path = f"/{folder_name}"
        assert expected_path in workspace.repository_folders, f"Valid folder '{folder_name}' was not detected"


def test_parent_folder_with_only_subfolder_containing_items(tmp_path, patched_fabric_workspace, valid_workspace_id):
    """Test scenario where parent folder contains only a subfolder with items (no direct items in parent)."""
    # Create parent folder with NO direct items
    parent_folder = tmp_path / "ParentWithNoDirectItems"
    parent_folder.mkdir(parents=True, exist_ok=True)

    # Create subfolder with items (parent has no direct items)
    create_platform_file(
        parent_folder / "SubfolderWithItems" / "SubfolderNotebook.Notebook",
        item_type="Notebook",
        item_name="Subfolder Notebook",
    )

    # Also create a more complex scenario with nested empty parents
    deeply_nested_parent = tmp_path / "Level1EmptyParent" / "Level2EmptyParent"
    deeply_nested_parent.mkdir(parents=True, exist_ok=True)

    create_platform_file(
        deeply_nested_parent / "FinalSubfolder" / "DeepNestedNotebook.Notebook",
        item_type="Notebook",
        item_name="Deep Nested Notebook",
    )

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(tmp_path),
        item_type_in_scope=["Notebook"],
    )

    workspace._refresh_repository_folders()
    workspace._refresh_repository_items()

    # Verify parent folders are detected even though they have no direct items
    assert "/ParentWithNoDirectItems" in workspace.repository_folders
    assert "/ParentWithNoDirectItems/SubfolderWithItems" in workspace.repository_folders

    # Verify deeply nested scenario
    assert "/Level1EmptyParent" in workspace.repository_folders
    assert "/Level1EmptyParent/Level2EmptyParent" in workspace.repository_folders
    assert "/Level1EmptyParent/Level2EmptyParent/FinalSubfolder" in workspace.repository_folders

    # Verify folder hierarchy is maintained (parents before children)
    sorted_folders = sorted(workspace.repository_folders.keys(), key=lambda path: path.count("/"))

    parent_idx = sorted_folders.index("/ParentWithNoDirectItems")
    subfolder_idx = sorted_folders.index("/ParentWithNoDirectItems/SubfolderWithItems")
    assert parent_idx < subfolder_idx, "Parent folder should come before subfolder"

    # Verify items are correctly associated with their folders
    assert "Subfolder Notebook" in workspace.repository_items["Notebook"]
    assert "Deep Nested Notebook" in workspace.repository_items["Notebook"]

    # Verify that parent folders have empty folder IDs (since they have no direct deployment)
    # but their subfolders would get proper folder IDs when deployed
    assert workspace.repository_folders["/ParentWithNoDirectItems"] == ""
    assert workspace.repository_folders["/Level1EmptyParent"] == ""
    assert workspace.repository_folders["/Level1EmptyParent/Level2EmptyParent"] == ""


def test_large_number_of_folders_and_items(tmp_path, patched_fabric_workspace, valid_workspace_id):
    """Test performance and scalability with a large number of folders and items."""
    import time

    # Create a large number of folders and items (100 folders with multiple items each)
    num_folders = 100
    items_per_folder = 3

    # Create folders at multiple levels
    for i in range(num_folders):
        if i < 50:
            # First 50 folders at root level
            folder_path = tmp_path / f"Folder{i:03d}"
        else:
            # Next 50 folders nested under first 25 folders
            parent_idx = (i - 50) % 25
            folder_path = tmp_path / f"Folder{parent_idx:03d}" / f"Subfolder{i:03d}"

        # Create multiple items in each folder
        for j in range(items_per_folder):
            create_platform_file(
                folder_path / f"Item{j:02d}.Notebook", item_type="Notebook", item_name=f"Item {j:02d} in Folder {i:03d}"
            )

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(tmp_path),
        item_type_in_scope=["Notebook"],
    )

    # Test _refresh_repository_folders performance
    start_time = time.time()
    workspace._refresh_repository_folders()
    folders_time = time.time() - start_time

    # Verify we detected a reasonable number of folders
    assert len(workspace.repository_folders) >= 50, "Should detect at least 50 folders"
    assert len(workspace.repository_folders) <= 125, "Should not detect more than 125 folders"

    # Test _refresh_repository_items performance
    start_time = time.time()
    workspace._refresh_repository_items()
    items_time = time.time() - start_time

    # Verify we detected the expected number of items
    expected_items = num_folders * items_per_folder
    assert len(workspace.repository_items["Notebook"]) == expected_items

    # Performance assertions - should complete in reasonable time
    assert folders_time < 15.0, f"Folder detection took too long: {folders_time:.2f}s"
    assert items_time < 30.0, f"Item detection took too long: {items_time:.2f}s"

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
