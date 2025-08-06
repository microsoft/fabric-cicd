# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test for the thick report deployment fix."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    mock.invoke.return_value = {"body": {"value": []}}
    mock.upn_auth = True
    return mock


@pytest.fixture
def temp_workspace_dir():
    """Create a temporary directory structure for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def test_convert_path_to_id_missing_item_type(temp_workspace_dir, mock_endpoint):
    """Test that _convert_path_to_id handles missing item types gracefully."""
    # Create a workspace with only Report items, no SemanticModel items
    workspace_id = "12345678-1234-5678-abcd-1234567890ab"
    
    fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
    refresh_items_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
    )
    refresh_folders_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
    )
    refresh_repo_items_patch = patch.object(
        FabricWorkspace, "_refresh_repository_items"
    )
    
    with fabric_endpoint_patch, refresh_items_patch, refresh_folders_patch, refresh_repo_items_patch:
        workspace = FabricWorkspace(
            workspace_id=workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Report"]  # Only Report scope selected
        )
        
        # Manually set repository_items to simulate scenario where only Reports are loaded
        workspace.repository_items = {
            "Report": {
                "TestReport": MagicMock(logical_id="report-123", path=Path("/test/report"))
            }
            # Note: No "SemanticModel" key in repository_items
        }
        
        # This should not raise a KeyError, but return None
        result = workspace._convert_path_to_id("SemanticModel", "/test/semantic_model")
        assert result is None


def test_convert_path_to_id_existing_item_type(temp_workspace_dir, mock_endpoint):
    """Test that _convert_path_to_id works correctly when item type exists."""
    workspace_id = "12345678-1234-5678-abcd-1234567890ab"
    
    fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
    refresh_items_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
    )
    refresh_folders_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
    )
    refresh_repo_items_patch = patch.object(
        FabricWorkspace, "_refresh_repository_items"
    )
    
    with fabric_endpoint_patch, refresh_items_patch, refresh_folders_patch, refresh_repo_items_patch:
        workspace = FabricWorkspace(
            workspace_id=workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["SemanticModel"]
        )
        
        # Mock repository_items with SemanticModel
        test_path = Path("/test/semantic_model")
        workspace.repository_items = {
            "SemanticModel": {
                "TestModel": MagicMock(logical_id="model-123", path=test_path)
            }
        }
        
        # This should find the item and return the logical_id
        result = workspace._convert_path_to_id("SemanticModel", str(test_path))
        assert result == "model-123"


def test_thick_report_deployment_scenario(temp_workspace_dir, mock_endpoint):
    """Test the complete thick report deployment scenario that was failing."""
    workspace_id = "12345678-1234-5678-abcd-1234567890ab"
    
    # Create a report with a thick report definition
    report_dir = temp_workspace_dir / "TestReport.Report"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Create definition.pbir with a datasetReference
    definition_content = {
        "datasetReference": {
            "byPath": {
                "path": "../TestModel.SemanticModel"
            }
        }
    }
    
    definition_file = report_dir / "definition.pbir"
    definition_file.write_text(json.dumps(definition_content, indent=2))
    
    fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
    refresh_items_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
    )
    refresh_folders_patch = patch.object(
        FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
    )
    refresh_repo_items_patch = patch.object(
        FabricWorkspace, "_refresh_repository_items"
    )
    
    with fabric_endpoint_patch, refresh_items_patch, refresh_folders_patch, refresh_repo_items_patch:
        workspace = FabricWorkspace(
            workspace_id=workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Report"]  # Only Report scope selected
        )
        
        # Simulate repository_items containing only Report items (no SemanticModel)
        workspace.repository_items = {
            "Report": {
                "TestReport": MagicMock(
                    logical_id="report-123", 
                    path=report_dir,
                    name="TestReport"
                )
            }
            # Note: No "SemanticModel" key - this is what causes the KeyError
        }
        
        # Import the func_process_file function to test it directly
        from fabric_cicd._common._exceptions import ItemDependencyError
        from fabric_cicd._items._report import func_process_file
        
        # Create mock objects
        item_obj = MagicMock()
        item_obj.path = report_dir
        
        file_obj = MagicMock()
        file_obj.name = "definition.pbir"
        file_obj.contents = json.dumps(definition_content, indent=2)
        
        # This should raise ItemDependencyError, not KeyError
        with pytest.raises(ItemDependencyError) as exc_info:
            func_process_file(workspace, item_obj, file_obj)
        
        assert "Semantic model not found in the repository" in str(exc_info.value)