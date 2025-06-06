# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for data pipeline notebook activity workspace ID replacement."""

import json
from unittest.mock import Mock

import pytest

from fabric_cicd._items._datapipeline import replace_activity_workspace_ids


class TestDataPipelineNotebookFix:
    """Test cases for notebook activity workspace ID replacement in data pipelines."""

    @pytest.fixture
    def mock_fabric_workspace(self):
        """Create a mock FabricWorkspace object."""
        workspace = Mock()
        workspace.workspace_id = "12345678-1234-1234-1234-123456789012"
        
        # Mock the _convert_id_to_name method to return the notebook name for valid IDs
        def mock_convert_id_to_name(item_type, generic_id, lookup_type):  # noqa: ARG001
            if item_type == "Notebook" and generic_id == "99b570c5-0c79-9dc4-4c9b-fa16c621384c":
                return "Hello World"
            if item_type == "Dataflow" and generic_id == "some-dataflow-id":
                return "Test Dataflow"
            return None
        
        workspace._convert_id_to_name = Mock(side_effect=mock_convert_id_to_name)
        return workspace

    @pytest.fixture
    def sample_pipeline_with_notebook(self):
        """Sample pipeline content with TridentNotebook activity."""
        return {
            "properties": {
                "activities": [
                    {
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c",
                            "workspaceId": "00000000-0000-0000-0000-000000000000"
                        },
                        "policy": {
                            "timeout": "0.12:00:00",
                            "retry": 0,
                            "retryIntervalInSeconds": 30,
                            "secureInput": False,
                            "secureOutput": False
                        },
                        "name": "Run Hello World",
                        "dependsOn": []
                    }
                ]
            }
        }

    @pytest.fixture
    def sample_pipeline_with_feature_branch_workspace(self):
        """Sample pipeline content with TridentNotebook activity using feature branch workspace ID."""
        return {
            "properties": {
                "activities": [
                    {
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c",
                            "workspaceId": "87654321-4321-4321-4321-210987654321"  # Feature branch workspace ID
                        },
                        "policy": {
                            "timeout": "0.12:00:00",
                            "retry": 0,
                            "retryIntervalInSeconds": 30,
                            "secureInput": False,
                            "secureOutput": False
                        },
                        "name": "Run Hello World",
                        "dependsOn": []
                    }
                ]
            }
        }

    @pytest.fixture 
    def sample_pipeline_mixed_activities(self):
        """Sample pipeline with both TridentNotebook and RefreshDataflow activities."""
        return {
            "properties": {
                "activities": [
                    {
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c",
                            "workspaceId": "87654321-4321-4321-4321-210987654321"
                        },
                        "name": "Run Notebook",
                        "dependsOn": []
                    },
                    {
                        "type": "RefreshDataflow",
                        "typeProperties": {
                            "dataflowId": "some-dataflow-id",
                            "workspaceId": "87654321-4321-4321-4321-210987654321"
                        },
                        "name": "Refresh Dataflow",
                        "dependsOn": []
                    }
                ]
            }
        }

    def test_replace_notebook_activity_workspace_id_default(self, mock_fabric_workspace, sample_pipeline_with_notebook):
        """Test that default workspace ID (all zeros) gets replaced for notebook activities."""
        file_obj = Mock()
        file_obj.contents = json.dumps(sample_pipeline_with_notebook)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        # The default workspace ID should be replaced with the target workspace ID
        notebook_activity = result_dict["properties"]["activities"][0]
        assert notebook_activity["typeProperties"]["workspaceId"] == mock_fabric_workspace.workspace_id
        
        # Verify the convert method was called correctly
        mock_fabric_workspace._convert_id_to_name.assert_called_with(
            item_type="Notebook", 
            generic_id="99b570c5-0c79-9dc4-4c9b-fa16c621384c", 
            lookup_type="Repository"
        )

    def test_replace_notebook_activity_workspace_id_feature_branch(self, mock_fabric_workspace, sample_pipeline_with_feature_branch_workspace):
        """Test that feature branch workspace ID gets replaced for notebook activities."""
        file_obj = Mock()
        file_obj.contents = json.dumps(sample_pipeline_with_feature_branch_workspace)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        # The feature branch workspace ID should be replaced with the target workspace ID
        notebook_activity = result_dict["properties"]["activities"][0]
        assert notebook_activity["typeProperties"]["workspaceId"] == mock_fabric_workspace.workspace_id

    def test_mixed_activities_both_replaced(self, mock_fabric_workspace, sample_pipeline_mixed_activities):
        """Test that both notebook and dataflow activities get their workspace IDs replaced."""
        file_obj = Mock()
        file_obj.contents = json.dumps(sample_pipeline_mixed_activities)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        activities = result_dict["properties"]["activities"]
        
        # Both activities should have their workspace IDs replaced
        notebook_activity = activities[0]
        dataflow_activity = activities[1]
        
        assert notebook_activity["typeProperties"]["workspaceId"] == mock_fabric_workspace.workspace_id
        assert dataflow_activity["typeProperties"]["workspaceId"] == mock_fabric_workspace.workspace_id

    def test_notebook_not_in_repository_no_replacement(self, mock_fabric_workspace):
        """Test that workspace ID is not replaced if notebook is not found in repository."""
        pipeline_with_unknown_notebook = {
            "properties": {
                "activities": [
                    {
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "unknown-notebook-id-1234-5678-9012-123456789012",
                            "workspaceId": "87654321-4321-4321-4321-210987654321"
                        },
                        "name": "Run Unknown Notebook"
                    }
                ]
            }
        }
        
        file_obj = Mock()
        file_obj.contents = json.dumps(pipeline_with_unknown_notebook)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        # The workspace ID should NOT be replaced since the notebook is not in the repository
        notebook_activity = result_dict["properties"]["activities"][0]
        assert notebook_activity["typeProperties"]["workspaceId"] == "87654321-4321-4321-4321-210987654321"

    def test_target_workspace_id_no_replacement(self, mock_fabric_workspace):
        """Test that workspace ID is not replaced if it's already the target workspace ID."""
        pipeline_with_target_workspace = {
            "properties": {
                "activities": [
                    {
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c",
                            "workspaceId": "12345678-1234-1234-1234-123456789012"  # Already target workspace ID
                        },
                        "name": "Run Notebook"
                    }
                ]
            }
        }
        
        file_obj = Mock()
        file_obj.contents = json.dumps(pipeline_with_target_workspace)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        # The workspace ID should remain unchanged
        notebook_activity = result_dict["properties"]["activities"][0]
        assert notebook_activity["typeProperties"]["workspaceId"] == mock_fabric_workspace.workspace_id
        
        # But the conversion method should not be called since workspace ID already matches
        mock_fabric_workspace._convert_id_to_name.assert_not_called()

    def test_no_notebook_activities_no_changes(self, mock_fabric_workspace):
        """Test that pipelines without notebook activities are unchanged."""
        pipeline_without_notebooks = {
            "properties": {
                "activities": [
                    {
                        "type": "Copy",  # Not a notebook activity
                        "typeProperties": {
                            "source": {},
                            "sink": {}
                        },
                        "name": "Copy Activity"
                    }
                ]
            }
        }
        
        file_obj = Mock()
        file_obj.contents = json.dumps(pipeline_without_notebooks)
        
        result = replace_activity_workspace_ids(mock_fabric_workspace, file_obj)
        result_dict = json.loads(result)
        
        # The pipeline should remain unchanged
        assert result_dict == pipeline_without_notebooks
        
        # No conversion calls should be made
        mock_fabric_workspace._convert_id_to_name.assert_not_called()