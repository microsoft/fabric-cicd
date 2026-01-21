# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import re
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fabric_cicd.fabric_workspace import FabricWorkspace, constants


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

    def _create_workspace(workspace_id, repository_directory, item_type_in_scope=None, **kwargs):
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
    json_content = """{
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
}"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"],
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
    python_content = """# META {
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "a277ea4a-e87f-8537-4ce0-39db11d4aade",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Test the workspace ID replacement function
    result = workspace._replace_workspace_ids(python_content)

    # Verify that the default workspace ID was replaced with the target workspace ID
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert valid_workspace_id in result
    assert 'workspaceId": "' + valid_workspace_id + '"' in result


def test_workspace_id_replacement_eventstream_json(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test workspace ID replacement in Eventstream JSON files with multiple occurrences."""
    eventstream_content = """{
  "destinations": [
    {
      "name": "DataActivator",
      "type": "Activator",
      "properties": {
        "workspaceId": "00000000-0000-0000-0000-000000000000",
        "itemId": "c3bf82de-14b6-af39-4852-dda67eccd7c0"
      }
    },
    {
      "name": "Lakehouse",
      "type": "Lakehouse",
      "properties": {
        "workspaceId": "00000000-0000-0000-0000-000000000000",
        "itemId": "c916eeb0-dd6a-ae32-4f4f-966d2414b239"
      }
    },
    {
      "name": "Eventhouse",
      "type": "Eventhouse",
      "properties": {
        "workspaceId": "00000000-0000-0000-0000-000000000000",
        "itemId": "a51e98dd-5993-8e1c-443f-02aa53d4db74"
      }
    }
  ]
}"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Eventstream"],
        )

    result = workspace._replace_workspace_ids(eventstream_content)

    # Verify all three workspace IDs were replaced
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert result.count(f'"workspaceId": "{valid_workspace_id}"') == 3


def test_workspace_id_replacement_yaml_format(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test workspace ID replacement in YAML-style formats."""
    yaml_content = """
configuration:
  lakehouse:
    default_lakehouse_workspace_id: "00000000-0000-0000-0000-000000000000"
  environment:
    workspaceId = "00000000-0000-0000-0000-000000000000"
  other:
    workspace: "00000000-0000-0000-0000-000000000000"
"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Environment"],
        )

    result = workspace._replace_workspace_ids(yaml_content)

    # Verify all different property name formats are replaced
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert f'default_lakehouse_workspace_id: "{valid_workspace_id}"' in result
    assert f'workspaceId = "{valid_workspace_id}"' in result
    assert f'workspace: "{valid_workspace_id}"' in result


def test_workspace_id_replacement_mixed_formats(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test workspace ID replacement with mixed JSON and YAML formats in same content."""
    mixed_content = """{
  "pipeline": {
    "properties": {
      "workspaceId": "00000000-0000-0000-0000-000000000000"
    }
  },
  "configuration": {
    "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
    "workspace" = "00000000-0000-0000-0000-000000000000"
  }
}"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"],
        )

    result = workspace._replace_workspace_ids(mixed_content)

    # Verify all formats are replaced correctly
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert f'"workspaceId": "{valid_workspace_id}"' in result
    assert f'"default_lakehouse_workspace_id": "{valid_workspace_id}"' in result
    assert f'"workspace" = "{valid_workspace_id}"' in result


def test_workspace_id_replacement_whitespace_variations(
    patched_fabric_workspace, valid_workspace_id, temp_workspace_dir
):
    """Test workspace ID replacement with various whitespace patterns."""
    whitespace_content = """
{
  "test1": {
    "workspaceId":"00000000-0000-0000-0000-000000000000"
  },
  "test2": {
    "workspaceId"  :  "00000000-0000-0000-0000-000000000000"
  },
  "test3": {
    workspaceId   =   "00000000-0000-0000-0000-000000000000"
  },
  "test4": {
    "workspace"    :    "00000000-0000-0000-0000-000000000000"
  }
}
"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"],
        )

    result = workspace._replace_workspace_ids(whitespace_content)

    # Verify all whitespace variations are handled
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert result.count(valid_workspace_id) == 4


def test_workspace_id_replacement_non_default_values_preserved(
    patched_fabric_workspace, valid_workspace_id, temp_workspace_dir
):
    """Test that non-default workspace IDs are NOT replaced (regression test)."""
    # Use a different workspace ID that should not be replaced
    other_workspace_id = "12345678-1234-1234-1234-123456789012"
    content_with_other_id = f'''{{
  "properties": {{
    "activities": [
      {{
        "type": "TridentNotebook",
        "typeProperties": {{
          "workspaceId": "{other_workspace_id}",
          "notebookId": "99b570c5-0c79-9dc4-4c9b-fa16c621384c"
        }}
      }},
      {{
        "type": "TridentNotebook",
        "typeProperties": {{
          "workspaceId": "00000000-0000-0000-0000-000000000000",
          "notebookId": "88a570c5-0c79-9dc4-4c9b-fa16c621384c"
        }}
      }}
    ]
  }}
}}'''

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"],
        )

    result = workspace._replace_workspace_ids(content_with_other_id)

    # Verify only default workspace ID was replaced, other ID preserved
    assert "00000000-0000-0000-0000-000000000000" not in result
    assert other_workspace_id in result  # This should be preserved
    assert result.count(valid_workspace_id) == 1  # Only one replacement
    assert result.count(other_workspace_id) == 1  # Original preserved


def test_workspace_id_replacement_edge_cases(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test workspace ID replacement edge cases and current regex behavior."""
    edge_cases_content = """
// Comment with workspaceId: "00000000-0000-0000-0000-000000000000" - this gets replaced due to current regex
{
  "validCase1": {
    "workspaceId": "00000000-0000-0000-0000-000000000000"
  },
  "validCase2": {
    "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
  },
  "invalidCase1": {
    "workspaceIdNot": "00000000-0000-0000-0000-000000000000"
  },
  "invalidCase2": {
    "notworkspaceId": "00000000-0000-0000-0000-000000000000"
  },
  "validCase3": {
    workspace: "00000000-0000-0000-0000-000000000000"
  }
}
"""

    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["DataPipeline"],
        )

    result = workspace._replace_workspace_ids(edge_cases_content)

    # Current regex behavior: matches comments and partial matches like "notworkspaceId"
    # This documents the current behavior for regression testing
    assert result.count(valid_workspace_id) == 5  # comment, validCase1, validCase2, invalidCase2, validCase3
    assert '"workspaceIdNot": "00000000-0000-0000-0000-000000000000"' in result  # Should not be replaced (prefix case)
    assert f'"notworkspaceId": "{valid_workspace_id}"' in result  # Gets replaced (suffix matches workspaceId)
    assert f'// Comment with workspaceId: "{valid_workspace_id}"' in result  # Comment gets replaced


def test_workspace_id_replacement_comprehensive_item_types(
    patched_fabric_workspace, valid_workspace_id, temp_workspace_dir
):
    """Test workspace ID replacement across different item type contexts."""
    # Test content that might appear in different item types
    comprehensive_content = """
{
  "notebook": {
    "metadata": {
      "environment": {
        "workspaceId": "00000000-0000-0000-0000-000000000000"
      }
    }
  },
  "pipeline": {
    "activities": [{
      "typeProperties": {
        "workspaceId": "00000000-0000-0000-0000-000000000000"
      }
    }]
  },
  "eventstream": {
    "destinations": [{
      "properties": {
        "workspaceId": "00000000-0000-0000-0000-000000000000"
      }
    }]
  },
  "lakehouse": {
    "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
  },
  "environment": {
    "workspace": "00000000-0000-0000-0000-000000000000"
  }
}
"""

    # Test with different item types to ensure the replacement works regardless of item type context
    item_types_to_test = ["Notebook", "DataPipeline", "Eventstream", "Lakehouse", "Environment"]

    for item_type in item_types_to_test:
        with patch.object(FabricWorkspace, "_refresh_repository_items"):
            workspace = patched_fabric_workspace(
                workspace_id=valid_workspace_id,
                repository_directory=str(temp_workspace_dir),
                item_type_in_scope=[item_type],
            )

        result = workspace._replace_workspace_ids(comprehensive_content)

        # Verify all workspace IDs are replaced regardless of item type context
        assert "00000000-0000-0000-0000-000000000000" not in result, f"Failed for item type: {item_type}"
        assert result.count(valid_workspace_id) == 5, f"Incorrect replacement count for item type: {item_type}"


def test_environment_parameter_replacement_issue(patched_fabric_workspace, temp_workspace_dir, valid_workspace_id):
    """Test that parameter replacement works correctly with different environment values.

    This test ensures that the issue where parameter replacement doesn't work when
    environment defaults to 'N/A' is properly handled.
    """
    # Create parameter.yml file with environment-specific replacements
    parameter_content = """
find_replace:
    - find_value: "test-guid-to-replace"
      replace_value:
        PPE: "ppe-replacement-value"
        PROD: "prod-replacement-value"
      item_type: "Notebook"
      item_name: ["Test Notebook"]
"""

    # Create notebook structure
    notebook_dir = temp_workspace_dir / "Test Notebook.Notebook"
    notebook_dir.mkdir(parents=True)

    notebook_content = 'test_value = "test-guid-to-replace"'

    # Write files
    (temp_workspace_dir / "parameter.yml").write_text(parameter_content)
    (notebook_dir / "notebook-content.py").write_text(notebook_content)

    from fabric_cicd._common._file import File
    from fabric_cicd._common._item import Item

    # Test 1: Without environment parameter (defaults to 'N/A')
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace_no_env = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Test 2: With environment parameter (PPE)
    with patch.object(FabricWorkspace, "_refresh_repository_items"):
        workspace_with_env = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
            environment="PPE",
        )

    # Create test objects for parameter replacement
    test_item = Item(type="Notebook", name="Test Notebook", description="", guid="test-guid", path=notebook_dir)
    test_file = File(item_path=notebook_dir, file_path=notebook_dir / "notebook-content.py")

    # Test parameter replacement with default environment
    replaced_content_no_env = workspace_no_env._replace_parameters(test_file, test_item)

    # Test parameter replacement with specific environment
    replaced_content_with_env = workspace_with_env._replace_parameters(test_file, test_item)

    # Assertions
    # With default environment ('N/A'), replacement should NOT occur
    assert "test-guid-to-replace" in replaced_content_no_env, "Original value should remain when environment is N/A"
    assert "ppe-replacement-value" not in replaced_content_no_env, (
        "Replacement should not occur with default environment"
    )

    # With specific environment (PPE), replacement SHOULD occur
    assert "test-guid-to-replace" not in replaced_content_with_env, (
        "Original value should be replaced when environment matches"
    )
    assert "ppe-replacement-value" in replaced_content_with_env, "Replacement should occur with matching environment"


def test_empty_logical_id_validation(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that empty logical IDs raise a ParsingError during repository refresh."""
    from fabric_cicd._common._exceptions import ParsingError

    # Create a .platform file with empty logical ID
    item_dir = temp_workspace_dir / "TestItem.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Item with Empty Logical ID",
            "description": "Test item for empty logical ID validation",
        },
        "config": {"logicalId": ""},  # Empty logical ID
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # Test that ParsingError is raised when trying to refresh repository items
    with pytest.raises(ParsingError) as exc_info:
        patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Verify the error message contains the expected information
    assert "logicalId cannot be empty" in str(exc_info.value)
    assert str(platform_file_path) in str(exc_info.value)


def test_whitespace_only_logical_id_validation(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that logical IDs with only whitespace raise a ParsingError."""
    from fabric_cicd._common._exceptions import ParsingError

    # Create a .platform file with whitespace-only logical ID
    item_dir = temp_workspace_dir / "TestItem.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Item with Whitespace Logical ID",
            "description": "Test item for whitespace logical ID validation",
        },
        "config": {"logicalId": "   "},  # Whitespace-only logical ID
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # Test that ParsingError is raised when trying to refresh repository items
    with pytest.raises(ParsingError) as exc_info:
        patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Verify the error message
    assert "logicalId cannot be empty" in str(exc_info.value)


def test_valid_logical_id_works_correctly(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that valid logical IDs continue to work correctly after adding validation."""
    # Create a .platform file with valid logical ID
    item_dir = temp_workspace_dir / "TestItem.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Item with Valid Logical ID",
            "description": "Test item for valid logical ID verification",
        },
        "config": {"logicalId": "valid-logical-id-123"},  # Valid logical ID
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # This should work without raising any exception
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id, repository_directory=str(temp_workspace_dir), item_type_in_scope=["Notebook"]
    )

    # Verify the item was loaded correctly (validation happens automatically during refresh)
    assert "Notebook" in workspace.repository_items
    assert "Test Item with Valid Logical ID" in workspace.repository_items["Notebook"]
    assert (
        workspace.repository_items["Notebook"]["Test Item with Valid Logical ID"].logical_id == "valid-logical-id-123"
    )


def test_empty_logical_id_validation_during_publish(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that empty logical IDs are caught during workspace initialization."""
    from fabric_cicd._common._exceptions import ParsingError

    # Create a .platform file with empty logical ID
    item_dir = temp_workspace_dir / "TestItem.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Item with Empty Logical ID",
            "description": "Test item for empty logical ID validation during publish",
        },
        "config": {"logicalId": ""},  # Empty logical ID
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # Test that ParsingError is raised during workspace initialization
    with pytest.raises(ParsingError) as exc_info:
        patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Verify the error message contains the expected information
    assert "logicalId cannot be empty" in str(exc_info.value)
    assert str(platform_file_path) in str(exc_info.value)


def test_multiple_empty_logical_ids_validation(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that multiple empty logical IDs are all reported at once."""
    from fabric_cicd._common._exceptions import ParsingError

    # Create multiple .platform files with empty logical IDs
    item_dirs = ["TestItem1.Notebook", "TestItem2.Notebook", "TestItem3.Environment"]
    platform_file_paths = []

    for item_dir_name in item_dirs:
        item_dir = temp_workspace_dir / item_dir_name
        item_dir.mkdir(parents=True, exist_ok=True)
        platform_file_path = item_dir / ".platform"
        platform_file_paths.append(platform_file_path)

        item_type = "Notebook" if "Notebook" in item_dir_name else "Environment"
        metadata_content = {
            "metadata": {
                "type": item_type,
                "displayName": f"Test Item {item_dir_name}",
                "description": "Test item for multiple empty logical ID validation",
            },
            "config": {"logicalId": ""},  # Empty logical ID
        }

        with platform_file_path.open("w", encoding="utf-8") as f:
            json.dump(metadata_content, f, ensure_ascii=False)

        # Create a dummy content file
        with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
            f.write("Dummy file content")

    # Test that ParsingError is raised when trying to refresh repository items
    with pytest.raises(ParsingError) as exc_info:
        patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook", "Environment"],
        )

    # Verify the error message contains information about all empty logical IDs
    error_message = str(exc_info.value)
    assert "logicalId cannot be empty in the following files:" in error_message
    for platform_file_path in platform_file_paths:
        assert str(platform_file_path) in error_message


def test_single_empty_logical_id_validation_message(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that a single empty logical ID shows the original error format."""
    from fabric_cicd._common._exceptions import ParsingError

    # Create a .platform file with empty logical ID
    item_dir = temp_workspace_dir / "TestItem.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Item with Empty Logical ID",
            "description": "Test item for single empty logical ID validation",
        },
        "config": {"logicalId": ""},  # Empty logical ID
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # Test that ParsingError is raised when trying to refresh repository items
    with pytest.raises(ParsingError) as exc_info:
        patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook"],
        )

    # Verify the error message uses single file format (not "following files:")
    error_message = str(exc_info.value)
    assert "logicalId cannot be empty in " in error_message
    assert "following files:" not in error_message
    assert str(platform_file_path) in error_message


def test_fabric_workspace_with_none_item_types_defaults_to_all(
    temp_workspace_dir, patched_fabric_workspace, valid_workspace_id
):
    """Test that FabricWorkspace works correctly when initialized with None item_type_in_scope (defaults to all available types)."""
    # Create a sample item to test with
    item_dir = temp_workspace_dir / "TestNotebook.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    platform_file_path = item_dir / ".platform"

    metadata_content = {
        "metadata": {
            "type": "Notebook",
            "displayName": "Test Notebook",
            "description": "Test notebook for None item types test",
        },
        "config": {"logicalId": "test-logical-id-none"},
    }

    with platform_file_path.open("w", encoding="utf-8") as f:
        json.dump(metadata_content, f, ensure_ascii=False)

    # Create a dummy content file
    with (item_dir / "dummy.txt").open("w", encoding="utf-8") as f:
        f.write("Dummy file content")

    # Test that workspace initializes correctly with None (default behavior)
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),  # item_type_in_scope=None (default)
    )

    # Verify that item_type_in_scope was expanded to all available types
    import fabric_cicd.constants as constants

    expected_types = list(constants.ACCEPTED_ITEM_TYPES)
    assert set(workspace.item_type_in_scope) == set(expected_types), (
        f"Expected all item types, got {workspace.item_type_in_scope}"
    )

    # Verify that the notebook item was loaded correctly
    assert "Notebook" in workspace.repository_items
    assert "Test Notebook" in workspace.repository_items["Notebook"]


def test_parameter_file_path_types(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test different path types for parameter_file_path in FabricWorkspace."""

    # Absolute path - accepted
    param_file = temp_workspace_dir / "parameters.yml"
    param_file.write_text("""
find_replace:
  - find_value: "test-value"
    replace_value:
      DEV: "dev-replacement"
""")

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        parameter_file_path=str(param_file),
    )

    assert workspace.parameter_file_path == str(param_file)

    # Relative path - now resolved against repository directory but file doesn't exist
    # This should not raise an exception now, it's handled gracefully
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        parameter_file_path="relative/path/parameters.yml",
    )

    # The workspace should be created successfully but with empty parameters
    assert workspace is not None
    assert hasattr(workspace, "environment_parameter")
    assert not workspace.environment_parameter


def test_parameter_file_path_none(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test None cases for parameter_file_path in FabricWorkspace."""
    # Create a workspace with parameter_file_path set to None
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id, repository_directory=str(temp_workspace_dir), parameter_file_path=None
    )
    assert workspace.parameter_file_path is None

    # Create a workspace without parameter_file_path provided
    workspace = patched_fabric_workspace(workspace_id=valid_workspace_id, repository_directory=str(temp_workspace_dir))
    assert workspace.parameter_file_path is None


def test_parameter_file_path_with_environment(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that FabricWorkspace works with both parameter_file_path and environment."""
    param_file = temp_workspace_dir / "dev_parameters.yml"
    param_file.write_text("""
find_replace:
  - find_value: "test-value"
    replace_value:
      DEV: "dev-replacement"
""")

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        parameter_file_path=str(param_file),
        environment="DEV",
    )

    assert workspace.parameter_file_path == str(param_file)
    assert workspace.environment == "DEV"


def test_parameter_file_path_backward_compatibility(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that existing code without parameter_file_path continues to work."""
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        item_type_in_scope=["Notebook"],
        environment="PROD",
    )

    # Should work as before without parameter_file_path
    assert workspace.parameter_file_path is None
    assert workspace.environment == "PROD"
    assert workspace.item_type_in_scope == ["Notebook"]


def test_parameter_file_path_integration_with_parameter_class(
    temp_workspace_dir, patched_fabric_workspace, valid_workspace_id
):
    """Test that parameter_file_path integrates correctly with Parameter class."""
    param_file = temp_workspace_dir / "test_parameters.yml"
    param_content = """
find_replace:
  - find_value: "test-value"
    replace_value:
      DEV: "dev-replacement"
      PROD: "prod-replacement"
"""
    param_file.write_text(param_content)

    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        parameter_file_path=str(param_file),
        environment="DEV",
    )

    # The workspace should use the parameter file path
    assert workspace.parameter_file_path == str(param_file)

    # The parameter data should be loaded correctly
    # (Note: This is testing the integration, actual Parameter behavior tested separately)
    assert hasattr(workspace, "environment_parameter")
    assert "find_replace" in workspace.environment_parameter


def test_parameter_file_path_invalid_type_rejected(temp_workspace_dir, patched_fabric_workspace, valid_workspace_id):
    """Test that FabricWorkspace handles invalid types for parameter_file_path."""
    # This should not raise an exception now since Parameter handles the error internally
    workspace = patched_fabric_workspace(
        workspace_id=valid_workspace_id,
        repository_directory=str(temp_workspace_dir),
        parameter_file_path=123,  # Invalid type
    )

    # The workspace should be created, but parameter loading should fail silently
    assert workspace is not None
    assert hasattr(workspace, "environment_parameter")
    # Environment parameter should be empty since the parameter file path was invalid
    assert not workspace.environment_parameter


def test_base_api_url_kwarg_raises_error(temp_workspace_dir, valid_workspace_id):
    """Test that passing base_api_url as kwarg raises an error."""
    from fabric_cicd._common._exceptions import InputError

    # Create a simple platform file
    notebook_dir = temp_workspace_dir / "Test Notebook"
    notebook_dir.mkdir()
    platform_file = notebook_dir / ".platform"
    platform_content = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "Test Notebook"},
        "config": {"version": "2.0", "logicalId": "12345678-1234-5678-abcd-1234567890ab"},
    }

    with platform_file.open("w", encoding="utf-8") as f:
        json.dump(platform_content, f)

    # Test that base_api_url kwarg raises InputError
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint"):
        with pytest.raises(InputError) as exc_info:
            FabricWorkspace(
                workspace_id=valid_workspace_id,
                repository_directory=str(temp_workspace_dir),
                base_api_url="https://custom.api.url",
            )

        # Verify the error message contains the expected text
        assert "base_api_url is no longer supported" in str(exc_info.value)
        assert "constants.DEFAULT_API_ROOT_URL" in str(exc_info.value)


def test_lookup_item_attribute(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that _lookup_item_attribute correctly finds items in another workspace."""
    # Mock endpoint response for workspace items
    mock_endpoint = MagicMock()

    # Ensure the mock response exactly matches what's expected
    mock_response = {
        "body": {
            "value": [
                {"id": "item-id-1234", "type": "Notebook", "displayName": "Test Notebook"},
                {"id": "item-id-5678", "type": "DataPipeline", "displayName": "Test Pipeline"},
            ]
        }
    }
    mock_endpoint.invoke.return_value = mock_response

    # Create a workspace with our mocked endpoint
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
            item_type_in_scope=["Notebook", "DataPipeline"],
        )

        # Replace the endpoint attribute to ensure our mock is being used
        workspace.endpoint = mock_endpoint

        # Test finding an existing item
        item_id = workspace._lookup_item_attribute("target-workspace-id", "Notebook", "Test Notebook", "id")
        assert item_id == "item-id-1234"

        # Test API was called with correct parameters
        mock_endpoint.invoke.assert_called_with(
            method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/target-workspace-id/items"
        )

        # Test finding a different item type
        item_id = workspace._lookup_item_attribute("target-workspace-id", "DataPipeline", "Test Pipeline", "id")
        assert item_id == "item-id-5678"

        # Test item not found - should raise InputError
        from fabric_cicd._common._exceptions import InputError

        with pytest.raises(InputError) as exc_info:
            workspace._lookup_item_attribute("target-workspace-id", "Notebook", "Non-Existent Notebook", "id")

        assert "Failed to look up item in workspace" in str(exc_info.value)
        assert "target-workspace-id" in str(exc_info.value)
        assert "Notebook" in str(exc_info.value)
        assert "Non-Existent Notebook" in str(exc_info.value)

        # Test item type not found - should raise InputError
        with pytest.raises(InputError) as exc_info:
            workspace._lookup_item_attribute("target-workspace-id", "NonExistentType", "Test Item", "id")

        assert "Failed to look up item in workspace" in str(exc_info.value)
        assert "target-workspace-id" in str(exc_info.value)
        assert "NonExistentType" in str(exc_info.value)
        assert "Test Item" in str(exc_info.value)


def test_kqldatabase_folder_regex_root_eventhouse():
    """KQLDatabase under top-level Eventhouse .children: group(1) is empty string."""
    pattern = re.compile(constants.KQL_DATABASE_FOLDER_PATH_REGEX)
    relative_path = "/SampleEventhouse.Eventhouse/.children/TaxiDB.KQLDatabase"
    match = pattern.match(relative_path)
    assert match is not None, "Regex should match a top-level Eventhouse .children path"
    assert match.group(1) == "", "Expected empty string for group(1) when Eventhouse is at repository root"


def test_kqldatabase_folder_regex_nested_subfolder():
    """KQLDatabase nested under a subfolder before Eventhouse: group(1) captures the subfolder path."""
    pattern = re.compile(constants.KQL_DATABASE_FOLDER_PATH_REGEX)
    relative_path = "/subfolder/EventhouseName.Eventhouse/.children/DB.KQLDatabase"
    match = pattern.match(relative_path)
    assert match is not None, "Regex should match nested Eventhouse .children path"
    assert match.group(1) == "/subfolder", "Expected '/subfolder' captured as the parent path"


def test_kqldatabase_folder_regex_no_match_edge_case():
    """Edge case: paths that do not follow the Eventhouse/.children pattern should not match."""
    pattern = re.compile(constants.KQL_DATABASE_FOLDER_PATH_REGEX)
    # Missing '.Eventhouse/.children' sequence
    bad_paths = [
        "/SomeFolder/TaxiDB.KQLDatabase",  # no Eventhouse container
        "/Another.Eventhouse/TaxiDB.KQLDatabase",  # missing '.children'
        "/prefix/.children/TaxiDB.KQLDatabase",  # missing Eventhouse segment
    ]
    for p in bad_paths:
        assert pattern.match(p) is None, f"Regex should not match path: {p}"


def test_get_item_attribute_caching_basic(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that _get_item_attribute caches results and returns expected values."""
    mock_endpoint = MagicMock()

    # Mock response for Lakehouse sqlendpoint attribute
    mock_response = {"body": {"properties": {"sqlEndpointProperties": {"connectionString": "test-connection-string"}}}}
    mock_endpoint.invoke.return_value = mock_response

    # Create workspace with mocked endpoint
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
        )
        workspace.endpoint = mock_endpoint

        # Test fetching an attribute
        result = workspace._get_item_attribute(
            workspace_id="test-workspace-id",
            item_type="Lakehouse",
            item_guid="test-item-guid",
            item_name="Test Lakehouse",
            attribute_name="sqlendpoint",
        )

        # Verify the result is as expected
        assert result == "test-connection-string"

        # Verify API was called once
        assert mock_endpoint.invoke.call_count == 1
        mock_endpoint.invoke.assert_called_with(
            method="GET",
            url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/test-workspace-id/lakehouses/test-item-guid",
        )


def test_get_item_attribute_caching_prevents_api_call(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that fetching the same attribute again uses cache and doesn't make API call."""
    mock_endpoint = MagicMock()

    # Mock response for Lakehouse sqlendpoint attribute
    mock_response = {"body": {"properties": {"sqlEndpointProperties": {"connectionString": "test-connection-string"}}}}
    mock_endpoint.invoke.return_value = mock_response

    # Create workspace with mocked endpoint
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
        )
        workspace.endpoint = mock_endpoint

        # First call - should make API call
        result1 = workspace._get_item_attribute(
            workspace_id="test-workspace-id",
            item_type="Lakehouse",
            item_guid="test-item-guid",
            item_name="Test Lakehouse",
            attribute_name="sqlendpoint",
        )

        # Second call with same parameters - should use cache
        result2 = workspace._get_item_attribute(
            workspace_id="test-workspace-id",
            item_type="Lakehouse",
            item_guid="test-item-guid",
            item_name="Test Lakehouse",
            attribute_name="sqlendpoint",
        )

        # Verify results are the same
        assert result1 == result2 == "test-connection-string"

        # Verify API was called only once (cached on second call)
        assert mock_endpoint.invoke.call_count == 1


def test_get_item_attribute_different_cache_keys(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test that different cache keys don't collide and each makes separate API calls."""
    mock_endpoint = MagicMock()

    # Mock response for different item types
    def mock_invoke_side_effect(*args, **kwargs):
        url = kwargs.get("url", args[1] if len(args) > 1 else "")
        if "lakehouses" in url:
            return {
                "body": {
                    "properties": {
                        "sqlEndpointProperties": {
                            "id": "endpoint-id-123",
                            "connectionString": "lakehouse-connection-string",
                        }
                    }
                }
            }
        if "warehouses" in url:
            return {"body": {"properties": {"connectionString": "warehouse-connection-string"}}}
        if "eventhouses" in url:
            return {"body": {"properties": {"queryServiceUri": "eventhouse-query-uri"}}}
        return {"body": {}}

    mock_endpoint.invoke.side_effect = mock_invoke_side_effect

    # Create workspace with mocked endpoint
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
        )
        workspace.endpoint = mock_endpoint

        # Test different combinations to ensure no cache collisions

        # Different item types
        lakehouse_result = workspace._get_item_attribute("ws1", "Lakehouse", "guid1", "name1", "sqlendpoint")
        warehouse_result = workspace._get_item_attribute("ws1", "Warehouse", "guid1", "name1", "sqlendpoint")
        eventhouse_result = workspace._get_item_attribute("ws1", "Eventhouse", "guid1", "name1", "queryserviceuri")

        # Different workspace IDs
        lakehouse_ws2_result = workspace._get_item_attribute("ws2", "Lakehouse", "guid1", "name1", "sqlendpoint")

        # Different item GUIDs
        lakehouse_guid2_result = workspace._get_item_attribute("ws1", "Lakehouse", "guid2", "name1", "sqlendpoint")

        # Different item names
        lakehouse_name2_result = workspace._get_item_attribute("ws1", "Lakehouse", "guid1", "name2", "sqlendpoint")

        # Different attributes
        lakehouse_sqlendpointid_result = workspace._get_item_attribute(
            "ws1", "Lakehouse", "guid1", "name1", "sqlendpointid"
        )

        # Verify all results are different and correct
        assert lakehouse_result == "lakehouse-connection-string"
        assert warehouse_result == "warehouse-connection-string"
        assert eventhouse_result == "eventhouse-query-uri"
        assert lakehouse_ws2_result == "lakehouse-connection-string"  # Same API response
        assert lakehouse_guid2_result == "lakehouse-connection-string"  # Same API response
        assert lakehouse_name2_result == "lakehouse-connection-string"  # Same API response

        # Mock the API to return different values for sqlendpointid
        def mock_invoke_side_effect_extended(*args, **kwargs):
            url = kwargs.get("url", args[1] if len(args) > 1 else "")
            if "lakehouses" in url:
                if "guid1" in url:
                    return {
                        "body": {
                            "properties": {
                                "sqlEndpointProperties": {
                                    "id": "endpoint-id-123",
                                    "connectionString": "lakehouse-connection-string",
                                }
                            }
                        }
                    }
                # guid2
                return {
                    "body": {
                        "properties": {
                            "sqlEndpointProperties": {
                                "id": "endpoint-id-456",
                                "connectionString": "lakehouse-connection-string-2",
                            }
                        }
                    }
                }
            return {"body": {}}

        mock_endpoint.invoke.side_effect = mock_invoke_side_effect_extended

        # Fetch sqlendpointid for guid1
        lakehouse_sqlendpointid_result = workspace._get_item_attribute(
            "ws1", "Lakehouse", "guid1", "name1", "sqlendpointid"
        )
        assert lakehouse_sqlendpointid_result == "endpoint-id-123"

        # Verify API was called for each unique cache key
        # We expect 7 calls: 3 initial + 1 for ws2 + 1 for guid2 + 1 for name2 + 1 for sqlendpointid
        assert mock_endpoint.invoke.call_count == 7


def test_get_item_attribute_edge_cases(patched_fabric_workspace, valid_workspace_id, temp_workspace_dir):
    """Test edge cases for _get_item_attribute to ensure cache doesn't introduce regressions."""
    mock_endpoint = MagicMock()
    mock_endpoint.invoke.return_value = {"body": {}}

    # Create workspace with mocked endpoint
    with patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint):
        workspace = patched_fabric_workspace(
            workspace_id=valid_workspace_id,
            repository_directory=str(temp_workspace_dir),
        )
        workspace.endpoint = mock_endpoint

        # Test empty item_guid - should return empty string without API call
        result = workspace._get_item_attribute("ws1", "Lakehouse", "", "name1", "sqlendpoint")
        assert result == ""
        assert mock_endpoint.invoke.call_count == 0  # No API call made

        # Test None item_guid - should return empty string without API call
        result = workspace._get_item_attribute("ws1", "Lakehouse", None, "name1", "sqlendpoint")
        assert result == ""
        assert mock_endpoint.invoke.call_count == 0  # No API call made

        # Test unsupported item type - should return empty string without API call
        result = workspace._get_item_attribute("ws1", "UnsupportedType", "guid1", "name1", "someattr")
        assert result == ""
        assert mock_endpoint.invoke.call_count == 0  # No API call made

        # Test unsupported attribute for supported item type - should return empty string without API call
        result = workspace._get_item_attribute("ws1", "Lakehouse", "guid1", "name1", "unsupportedattr")
        assert result == ""
        assert mock_endpoint.invoke.call_count == 0  # No API call made

        # Test valid call that results in empty attribute value - should raise InputError
        mock_endpoint.invoke.return_value = {
            "body": {
                "properties": {
                    "sqlEndpointProperties": {
                        "connectionString": ""  # Empty value
                    }
                }
            }
        }

        from fabric_cicd._common._exceptions import InputError

        with pytest.raises(InputError) as exc_info:
            workspace._get_item_attribute("ws1", "Lakehouse", "guid1", "name1", "sqlendpoint")

        assert "Attribute value not found" in str(exc_info.value)
        assert "Lakehouse" in str(exc_info.value)
        assert "name1" in str(exc_info.value)

        # Verify the error case was not cached
        with pytest.raises(InputError):
            workspace._get_item_attribute("ws1", "Lakehouse", "guid1", "name1", "sqlendpoint")
        # Should still be only 1 API call (cached error)
        assert mock_endpoint.invoke.call_count == 2
