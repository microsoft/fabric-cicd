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
