# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import publish_all_items


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint to avoid real API calls."""
    mock = MagicMock()
    # Mock different responses for different URLs
    def mock_invoke(_method=None, url=None, **_kwargs):
        if "/folders" in url:
            return {"body": {"value": []}, "header": {}}
        return {"body": {"value": []}}
    
    mock.invoke.side_effect = mock_invoke
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


def create_test_item(item_dir: Path, item_type: str, item_name: str, item_description: str = "Test description"):
    """Create a test item with .platform file in the given directory."""
    item_dir.mkdir(parents=True, exist_ok=True)
    
    # Create .platform file
    platform_content = {
        "metadata": {
            "type": item_type,
            "displayName": item_name,
            "description": item_description
        },
        "config": {
            "logicalId": f"logical-{item_name.lower().replace(' ', '-')}"
        }
    }
    
    platform_file = item_dir / ".platform"
    with platform_file.open("w", encoding="utf-8") as f:
        json.dump(platform_content, f, indent=2)
    
    # Create a dummy content file
    content_file = item_dir / "content.json"
    with content_file.open("w", encoding="utf-8") as f:
        json.dump({"test": "content"}, f)


def test_publish_all_items_returns_published_items(mock_endpoint, temp_workspace_dir, valid_workspace_id):
    """Test that publish_all_items returns information about published items."""
    # Create test items
    semantic_model_dir = temp_workspace_dir / "TestSemanticModel"
    report_dir = temp_workspace_dir / "TestReport"
    notebook_dir = temp_workspace_dir / "TestNotebook"
    
    create_test_item(semantic_model_dir, "SemanticModel", "Test Semantic Model")
    create_test_item(report_dir, "Report", "Test Report")
    create_test_item(notebook_dir, "Notebook", "Test Notebook")
    
    # Mock the deployed items response to simulate existing items
    def mock_invoke_with_items(_method=None, url=None, **_kwargs):
        if "/folders" in url:
            return {"body": {"value": []}, "header": {}}
        return {
            "body": {
                "value": [
                    {
                        "type": "SemanticModel",
                        "displayName": "Test Semantic Model",
                        "description": "Test description",
                        "id": "semantic-model-guid",
                        "folderId": ""
                    },
                    {
                        "type": "Report", 
                        "displayName": "Test Report",
                        "description": "Test description",
                        "id": "report-guid",
                        "folderId": ""
                    },
                    {
                        "type": "Notebook",
                        "displayName": "Test Notebook", 
                        "description": "Test description",
                        "id": "notebook-guid",
                        "folderId": ""
                    }
                ]
            }
        }
    
    mock_endpoint.invoke.side_effect = mock_invoke_with_items
    
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = FabricWorkspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["SemanticModel", "Report", "Notebook"]
        )
        
        # Mock the actual publish methods to avoid real API calls
        with patch("fabric_cicd._items.publish_semanticmodels"), \
             patch("fabric_cicd._items.publish_reports"), \
             patch("fabric_cicd._items.publish_notebooks"):
            
            result = publish_all_items(workspace)
    
    # Verify the return structure
    assert isinstance(result, dict)
    assert "SemanticModel" in result
    assert "Report" in result
    assert "Notebook" in result
    
    # Verify semantic model information
    semantic_models = result["SemanticModel"]
    assert "Test Semantic Model" in semantic_models
    sm_info = semantic_models["Test Semantic Model"]
    assert sm_info["type"] == "SemanticModel"
    assert sm_info["name"] == "Test Semantic Model"
    assert sm_info["description"] == "Test description"
    assert sm_info["guid"] == "semantic-model-guid"
    assert sm_info["logical_id"] == "logical-test-semantic-model"
    assert sm_info["deployment_status"] == "already_existed"  # Should be marked as already existed
    
    # Verify report information
    reports = result["Report"]
    assert "Test Report" in reports
    report_info = reports["Test Report"]
    assert report_info["type"] == "Report"
    assert report_info["name"] == "Test Report"
    assert report_info["description"] == "Test description"
    assert report_info["guid"] == "report-guid"
    assert report_info["logical_id"] == "logical-test-report"
    assert report_info["deployment_status"] == "already_existed"  # Should be marked as already existed


def test_publish_all_items_returns_empty_dict_for_no_items(mock_endpoint, temp_workspace_dir, valid_workspace_id):
    """Test that publish_all_items returns empty dict when no items are in scope."""
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = FabricWorkspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["SemanticModel", "Report"]
        )
        
        result = publish_all_items(workspace)
    
    # Should return empty dict since no items exist in the repository
    assert isinstance(result, dict)
    assert len(result) == 0


def test_publish_all_items_returns_only_items_in_scope(mock_endpoint, temp_workspace_dir, valid_workspace_id):
    """Test that publish_all_items only returns items that are in scope."""
    # Create test items 
    semantic_model_dir = temp_workspace_dir / "TestSemanticModel"
    report_dir = temp_workspace_dir / "TestReport"
    notebook_dir = temp_workspace_dir / "TestNotebook"
    
    create_test_item(semantic_model_dir, "SemanticModel", "Test Semantic Model")
    create_test_item(report_dir, "Report", "Test Report")
    create_test_item(notebook_dir, "Notebook", "Test Notebook")
    
    def mock_invoke_with_selective_items(_method=None, url=None, **_kwargs):
        if "/folders" in url:
            return {"body": {"value": []}, "header": {}}
        return {
            "body": {
                "value": [
                    {
                        "type": "SemanticModel",
                        "displayName": "Test Semantic Model",
                        "description": "Test description",
                        "id": "semantic-model-guid",
                        "folderId": ""
                    },
                    {
                        "type": "Report",
                        "displayName": "Test Report", 
                        "description": "Test description",
                        "id": "report-guid",
                        "folderId": ""
                    },
                    {
                        "type": "Notebook",
                        "displayName": "Test Notebook",
                        "description": "Test description", 
                        "id": "notebook-guid",
                        "folderId": ""
                    }
                ]
            }
        }
    
    mock_endpoint.invoke.side_effect = mock_invoke_with_selective_items
    
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        # Only include SemanticModel and Report in scope
        workspace = FabricWorkspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["SemanticModel", "Report"]
        )
        
        with patch("fabric_cicd._items.publish_semanticmodels"), \
             patch("fabric_cicd._items.publish_reports"):
            
            result = publish_all_items(workspace)
    
    # Should only contain SemanticModel and Report, not Notebook
    assert isinstance(result, dict)
    assert "SemanticModel" in result
    assert "Report" in result
    assert "Notebook" not in result
    
    assert len(result["SemanticModel"]) == 1
    assert len(result["Report"]) == 1


def test_publish_all_items_deployment_status_differentiation(mock_endpoint, temp_workspace_dir, valid_workspace_id):
    """Test that publish_all_items correctly differentiates between newly published and existing items."""
    # Create test items
    semantic_model_dir = temp_workspace_dir / "TestSemanticModel"
    report_dir = temp_workspace_dir / "TestReport"
    
    create_test_item(semantic_model_dir, "SemanticModel", "Test Semantic Model")
    create_test_item(report_dir, "Report", "Test Report")
    
    # Mock deployed items to simulate that only the semantic model already exists
    def mock_invoke_existing_items(_method=None, url=None, **_kwargs):
        if "/folders" in url:
            return {"body": {"value": []}, "header": {}}
        return {
            "body": {
                "value": [
                    # Only semantic model exists in workspace already
                    {
                        "type": "SemanticModel",
                        "displayName": "Test Semantic Model",
                        "description": "Test description",
                        "id": "semantic-model-guid",
                        "folderId": ""
                    }
                ]
            }
        }
    
    mock_endpoint.invoke.side_effect = mock_invoke_existing_items
    
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = FabricWorkspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["SemanticModel", "Report"]
        )
        
        with patch("fabric_cicd._items.publish_semanticmodels"), \
             patch("fabric_cicd._items.publish_reports"):
            
            result = publish_all_items(workspace)
    
    # Verify the deployment status differentiation
    assert isinstance(result, dict)
    assert "SemanticModel" in result
    assert "Report" in result
    
    # Semantic model should be marked as already existed
    semantic_models = result["SemanticModel"]
    sm_info = semantic_models["Test Semantic Model"]
    assert sm_info["deployment_status"] == "already_existed"
    
    # Report should be marked as newly published (not in deployed items initially)
    reports = result["Report"]
    report_info = reports["Test Report"]
    assert report_info["deployment_status"] == "newly_published"