# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

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


@pytest.fixture
def valid_workspace_id():
    """Return a valid workspace ID in GUID format."""
    return "12345678-1234-5678-abcd-1234567890ab"


@pytest.fixture
def utf8_test_chars():
    """Provide sample UTF-8 characters for testing."""
    return {"nordic": "Ö æ ø", "european": "ñ é ü ß ç", "asian": "你好", "mixed": "ñ é ü ß ç 你好"}


def create_parameter_file(dir_path, utf8_chars):
    """Create a parameter file with UTF-8 characters."""
    parameter_file_path = dir_path / "parameter.yml"
    parameter_content = {
        "find_replace": [
            {
                "find_value": f"Production {utf8_chars['mixed']}",
                "replace_value": {
                    utf8_chars["nordic"]: "12345678-1234-5678-abcd-1234567890ab",
                    utf8_chars["asian"]: "21345678-1234-5678-abcd-1234567890ab",
                },
            }
        ]
    }

    with parameter_file_path.open("w", encoding="utf-8") as f:
        yaml.dump(parameter_content, f, allow_unicode=True)

    return parameter_content


def create_platform_metadata(dir_path, utf8_chars):
    """Create a .platform metadata file with UTF-8 characters."""
    item_dir = dir_path / "test_item"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"
    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": f"Test Notebook with {utf8_chars['nordic']}",
            "description": f"Description with {utf8_chars['mixed']}",
        },
        "config": {"logicalId": "test-logical-id"},
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file")

    return metadata_content


@pytest.fixture
def patched_fabric_workspace(mock_endpoint):
    """Return a factory function to create a patched FabricWorkspace."""

    def _create_workspace(workspace_id, repository_directory, item_type_in_scope, **kwargs):
        fabric_endpoint_patch = patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint)
        refresh_items_patch = patch.object(
            FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})
        )
        refresh_folders_patch = patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        )

        with fabric_endpoint_patch, refresh_items_patch, refresh_folders_patch:
            workspace = FabricWorkspace(
                workspace_id=workspace_id,
                repository_directory=repository_directory,
                item_type_in_scope=item_type_in_scope,
                **kwargs,
            )
            # Call refresh methods to populate workspace data
            workspace._refresh_deployed_folders()
            workspace._refresh_repository_folders()
            workspace._refresh_deployed_items()
            workspace._refresh_repository_items()

            return workspace

    return _create_workspace


def test_parameter_file_with_utf8_chars(
    temp_workspace_dir, patched_fabric_workspace, valid_workspace_id, utf8_test_chars
):
    """Test that parameter file with UTF-8 characters is read correctly."""
    create_parameter_file(temp_workspace_dir, utf8_test_chars)
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Environment"],
        )

    key1 = f"Production {utf8_test_chars['mixed']}"
    key2 = utf8_test_chars["nordic"]
    key3 = utf8_test_chars["asian"]

    for param_dict in workspace.environment_parameter.get("find_replace"):
        assert key1 == param_dict["find_value"]
        assert key2 in param_dict["replace_value"]
        assert key3 in param_dict["replace_value"]


def test_platform_metadata_with_utf8_chars(
    temp_workspace_dir, patched_fabric_workspace, valid_workspace_id, utf8_test_chars
):
    """Test that .platform metadata file with UTF-8 characters is read correctly."""
    create_platform_metadata(temp_workspace_dir, utf8_test_chars)
    with patch.object(FabricWorkspace, "_refresh_parameter_file"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    item_name = f"Test Notebook with {utf8_test_chars['nordic']}"
    item = workspace.repository_items["Notebook"][item_name]

    assert "Notebook" in workspace.repository_items
    assert item_name in workspace.repository_items["Notebook"]
    assert item.name == item_name
    assert item.description == f"Description with {utf8_test_chars['mixed']}"


def test_environment_param_with_utf8_chars(
    temp_workspace_dir, patched_fabric_workspace, valid_workspace_id, utf8_test_chars
):
    """Test that environment parameter with UTF-8 characters is preserved."""
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Environment"],
            environment=utf8_test_chars["nordic"],
        )

    assert workspace.environment == utf8_test_chars["nordic"]


def test_workspace_id_replacement_in_json(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that workspace IDs are properly replaced in JSON files (like pipeline-content.json)."""
    # JSON content with workspace ID that should be replaced
    json_content = '''{
  "properties": {
    "activities": [
      {
        "type": "TridentNotebook",
        "typeProperties": {
          "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c",
          "workspaceId": "00000000-0000-0000-0000-000000000000"
        }
      }
    ]
  }
}'''
    
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"]
        )
    
    # Test the workspace ID replacement function
    result = workspace._replace_workspace_ids(json_content)
    
    # Verify that the default workspace ID was replaced with the target workspace ID
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert valid_workspace_id in result
    assert '"workspaceId": "' + valid_workspace_id + '"' in result


def test_workspace_id_replacement_in_python(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that workspace IDs are properly replaced in Python files (like notebook-content.py)."""
    # Python content with workspace ID that should be replaced (as in notebook metadata)
    python_content = '''# META {
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }'''
    
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"]
        )
    
    # Test the workspace ID replacement function
    result = workspace._replace_workspace_ids(python_content)
    
    # Verify that the default workspace ID was replaced with the target workspace ID
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert valid_workspace_id in result
    assert 'workspaceId": "' + valid_workspace_id + '"' in result
