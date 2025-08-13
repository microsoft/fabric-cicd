# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the parameter utility functions in _utils.py.
The tests focused on path handling functions should be compatible with both Windows and Linux.
"""

import json
import logging
import re
import shutil
import tempfile
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest

import fabric_cicd.constants as constants

# Logger for testing
logger = logging.getLogger(__name__)


@pytest.fixture
def temp_repository():
    """Creates a temporary directory structure mocking a repository for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    try:
        # Create test directory structure
        (temp_dir / "folder1").mkdir()
        (temp_dir / "folder1" / "subfolder").mkdir()
        (temp_dir / "folder2").mkdir()

        # Create test files
        (temp_dir / "file1.txt").write_text("content1")
        (temp_dir / "file2.json").write_text("content2")
        (temp_dir / "folder1" / "file3.py").write_text("content3")
        (temp_dir / "folder1" / "subfolder" / "file4.md").write_text("content4")
        (temp_dir / "folder2" / "file5.txt").write_text("content5")

        # Return the temporary directory path
        yield temp_dir
    finally:
        # Clean up temporary directory after tests
        shutil.rmtree(temp_dir)


from fabric_cicd._common._exceptions import InputError, ParsingError
from fabric_cicd._parameter._utils import (
    _check_parameter_structure,
    _extract_item_attribute,
    _find_match,
    _process_regular_path,
    _process_wildcard_path,
    _resolve_file_path,
    _validate_wildcard_syntax,
    check_replacement,
    extract_find_value,
    extract_parameter_filters,
    extract_replace_value,
    is_valid_structure,
    process_environment_key,
    process_input_path,
    replace_key_value,
    replace_variables_in_parameter_file,
)


class TestParameterUtilities:
    """Tests for parameter utilities in _utils.py."""

    @pytest.fixture
    def mock_workspace(self):
        """Creates a mock FabricWorkspace for testing."""
        mock_ws = mock.MagicMock()
        mock_ws.repository_directory = Path("/mock/repository")
        mock_ws.workspace_id = "mock-workspace-id"
        mock_ws.workspace_items = {
            "Notebook": {
                "Test Notebook": {"id": "notebook-id", "sqlendpoint": "", "queryserviceuri": ""},
            },
            "Warehouse": {
                "TestWarehouse": {"id": "warehouse-id", "sqlendpoint": "warehouse-endpoint", "queryserviceuri": ""},
            },
            "Lakehouse": {
                "Test_Lakehouse": {"id": "lakehouse-id", "sqlendpoint": "lakehouse-endpoint", "queryserviceuri": ""},
            },
            "Eventhouse": {
                "Test Eventhouse": {
                    "id": "eventhouse-id",
                    "sqlendpoint": "",
                    "queryserviceuri": "eventhouse-query-uri",
                },
            },
        }
        mock_ws.repository_items = {
            "Dataflow": {
                "Source Dataflow": {"id": "source-dataflow-id"},
            }
        }
        # Mock _refresh_deployed_items to avoid API calls in all tests using this fixture
        mock_ws._refresh_deployed_items = MagicMock()
        return mock_ws

    def test_extract_find_value(self):
        """Tests extract_find_value with string."""
        # Test with plain text
        param_dict = {"find_value": "test-value"}
        assert extract_find_value(param_dict, "content with test-value", True) == "test-value"
        assert extract_find_value(param_dict, "unrelated content", True) == "test-value"

    def test_extract_find_value_valid_regex(self):
        """Tests extract_find_value with regex pattern."""
        param_dict = {"find_value": "id=([\\w-]+)", "is_regex": "true"}

        # Test with regex
        assert extract_find_value(param_dict, "content with id=abc-123", True) == "abc-123"
        # Test with non-matching regex
        assert extract_find_value(param_dict, "unrelated content", True) == "id=([\\w-]+)"
        # Test with regex but filter_match=False
        assert extract_find_value(param_dict, "content with id=abc-123", False) == "id=([\\w-]+)"

    def test_extract_find_value_invalid_regex(self):
        """Tests extract_find_value with invalid regex capturing groups."""
        # Test with regex that has no capturing groups
        param_dict = {"find_value": "id=\\w+", "is_regex": "true"}
        with pytest.raises(InputError):
            extract_find_value(param_dict, "content with id=abc123", True)

        # Test with regex that has multiple capturing groups
        param_dict = {"find_value": "(id)=([\\w-]+)", "is_regex": "true"}
        with pytest.raises(InputError):
            extract_find_value(param_dict, "content with id=abc-123", True)

        # Test with regex that captures empty value
        param_dict = {"find_value": "id=()", "is_regex": "true"}
        with pytest.raises(InputError):
            extract_find_value(param_dict, "content with id=", True)

    def test_extract_replace_value_default(self, mock_workspace):
        """Tests extract_replace_value with different inputs, get_dataflow_name=False."""
        # Regular string should be returned as is
        assert extract_replace_value(mock_workspace, "literal string") == "literal string"

        # Workspace ID variable should return the workspace ID
        assert extract_replace_value(mock_workspace, "$workspace.id", False) == "mock-workspace-id"

        # Item attribute variables should extract values from workspace items
        with mock.patch("fabric_cicd._parameter._utils._extract_item_attribute") as mock_extract:
            mock_extract.return_value = "notebook-id"
            result = extract_replace_value(mock_workspace, "$items.Notebook.Test Notebook.id")
            assert result == "notebook-id"
            mock_extract.assert_called_once_with(mock_workspace, "$items.Notebook.Test Notebook.id", False)

    def test_extract_replace_value_get_dataflow_name(self, mock_workspace):
        """Tests extract_replace_value with different inputs, get_dataflow_name=True."""
        # With get_dataflow_name=True for regular string, should return None
        assert extract_replace_value(mock_workspace, "literal string", True) is None

        # With get_dataflow_name=True for workspace ID, should return an error
        with pytest.raises(
            InputError,
            match=re.escape(
                "Invalid replace_value variable format: '$workspace.id'. Expected format to get dataflow name: $items.type.name.attribute"
            ),
        ):
            result = extract_replace_value(mock_workspace, "$workspace.id", True)

        # With get_dataflow_name=True for non-Dataflow item, should return None
        with mock.patch("fabric_cicd._parameter._utils._extract_item_attribute") as mock_extract:
            mock_extract.return_value = None
            result = extract_replace_value(mock_workspace, "$items.Notebook.Test Notebook.id", True)
            assert result == None
            mock_extract.assert_called_once_with(mock_workspace, "$items.Notebook.Test Notebook.id", True)

        # With get_dataflow_name=True for a Dataflow item, should return the Dataflow name
        with mock.patch("fabric_cicd._parameter._utils._extract_item_attribute") as mock_extract:
            mock_extract.return_value = "Source Dataflow"
            result = extract_replace_value(mock_workspace, "$items.Dataflow.Source Dataflow.id", True)
            assert result == "Source Dataflow"
            mock_extract.assert_called_once_with(mock_workspace, "$items.Dataflow.Source Dataflow.id", True)

    def test_extract_item_attribute_valid(self, mock_workspace):
        """Tests _extract_item_attribute with valid variables."""
        # Test with valid notebook item
        result = _extract_item_attribute(mock_workspace, "$items.Notebook.Test Notebook.id", False)
        assert result == "notebook-id"

        # Test with valid lakehouse item
        result = _extract_item_attribute(mock_workspace, "$items.Lakehouse.Test_Lakehouse.sqlendpoint", False)
        assert result == "lakehouse-endpoint"

        # Test with valid warehouse item
        result = _extract_item_attribute(mock_workspace, "$items.Warehouse.TestWarehouse.id", False)
        assert result == "warehouse-id"

        # Test with valid eventhouse item
        result = _extract_item_attribute(mock_workspace, "$items.Eventhouse.Test Eventhouse.queryserviceuri", False)
        assert result == "eventhouse-query-uri"

    def test_extract_item_attribute_invalid(self, mock_workspace):
        """Tests _extract_item_attribute with invalid variable cases."""
        # Test with invalid syntax
        with pytest.raises(ParsingError, match="Invalid \\$items variable syntax"):
            _extract_item_attribute(mock_workspace, "$items.Notebook", False)
        with pytest.raises(ParsingError, match="Invalid \\$items variable syntax"):
            _extract_item_attribute(mock_workspace, "$items.Notebook.Test Notebook", False)
        with pytest.raises(ParsingError, match="Invalid \\$items variable syntax"):
            _extract_item_attribute(mock_workspace, "$items.Notebook.Test Notebook.id.extra", False)

        mock_items_attr_lookup = list(constants.ITEM_ATTR_LOOKUP)

        # Test with invalid item types, names, or attributes
        with pytest.raises(ParsingError, match="Item type 'InvalidType' is invalid"):
            _extract_item_attribute(mock_workspace, "$items.InvalidType.Test Notebook.id", False)
        with pytest.raises(ParsingError, match="Item 'InvalidName' not found"):
            _extract_item_attribute(mock_workspace, "$items.Notebook.InvalidName.id", False)
        with pytest.raises(
            ParsingError,
            match=re.escape(f"Attribute 'guid' is invalid. Supported attributes: {mock_items_attr_lookup}"),
        ):
            _extract_item_attribute(mock_workspace, "$items.Notebook.Test Notebook.guid", False)

        # Test wrong type and attribute combination
        with pytest.raises(
            ParsingError, match="Value does not exist for attribute 'sqlendpoint' in the Notebook item 'Test Notebook'"
        ):
            _extract_item_attribute(mock_workspace, "$items.Notebook.Test Notebook.sqlendpoint", False)

    def test_extract_item_attribute_get_dataflow_name(self, mock_workspace):
        """Test _extract_item_attribute with special handling for Dataflow references."""
        # Test when Dataflow references another Dataflow in the repository
        result = _extract_item_attribute(mock_workspace, "$items.Dataflow.Source Dataflow.id", True)
        assert result == "Source Dataflow"

        # Test when source Dataflow doesn't exist in repository - should return None
        result = _extract_item_attribute(mock_workspace, "$items.Dataflow.NonExistentDataflow.id", True)
        assert result is None

        # Test when source Dataflow type doesn't match (case sensitive) - should return None
        result = _extract_item_attribute(mock_workspace, "$items.dataflow.Source Dataflow.id", get_dataflow_name=True)
        assert result is None

        # Test when source Dataflow name doesn't match (case sensitive) - should return None
        result = _extract_item_attribute(mock_workspace, "$items.Dataflow.source dataflow.id", get_dataflow_name=True)
        assert result is None

        # Test with non-Dataflow item, should return None
        result = _extract_item_attribute(mock_workspace, "$items.Lakehouse.Test Lakehouse.id", True)
        assert result is None

        # Test with Dataflow item, but incorrect attribute should return None
        result = _extract_item_attribute(mock_workspace, "$items.Dataflow.Source Dataflow.sqlendpoint", True)
        assert result is None

        # Test syntax error in variable
        with pytest.raises(ParsingError, match="Invalid \\$items variable syntax"):
            _extract_item_attribute(mock_workspace, "$item.Dataflow.Source Dataflow.id", True)

        # Test with invalid attribute
        mock_items_attr_lookup = list(constants.ITEM_ATTR_LOOKUP)
        with pytest.raises(
            ParsingError,
            match=re.escape(f"Attribute 'guid' is invalid. Supported attributes: {mock_items_attr_lookup}"),
        ):
            _extract_item_attribute(mock_workspace, "$items.Dataflow.Source Dataflow.guid", True)

    def test_extract_parameter_filters(self, mock_workspace):
        """Tests extract_parameter_filters function."""
        # Test with all filters
        param_dict = {"item_type": "Notebook", "item_name": "TestNotebook", "file_path": "path/to/file.txt"}

        with mock.patch("fabric_cicd._parameter._utils.process_input_path") as mock_process:
            # Return a list of Path objects as expected
            processed_path = Path("processed/path")
            mock_process.return_value = [processed_path]
            item_type, item_name, file_path = extract_parameter_filters(mock_workspace, param_dict)

            assert item_type == "Notebook"
            assert item_name == "TestNotebook"
            # Assert that file_path is a list containing the processed path
            assert file_path == [processed_path]
            mock_process.assert_called_once_with(mock_workspace.repository_directory, "path/to/file.txt")

        # Test with missing filters
        param_dict = {}
        with mock.patch("fabric_cicd._parameter._utils.process_input_path") as mock_process:
            # When no file_path in param_dict, process_input_path should return an empty list
            mock_process.return_value = []
            item_type, item_name, file_path = extract_parameter_filters(mock_workspace, param_dict)

            assert item_type is None
            assert item_name is None
            assert file_path == []

    def test_check_parameter_structure(self):
        """Tests _check_parameter_structure function."""
        # Test with valid list
        assert _check_parameter_structure([1, 2, 3]) is True
        assert _check_parameter_structure([]) is True

        # Test with invalid types
        assert _check_parameter_structure("string") is False
        assert _check_parameter_structure(123) is False
        assert _check_parameter_structure({"key": "value"}) is False
        assert _check_parameter_structure(None) is False

    def test_is_valid_structure(self):
        """Tests is_valid_structure function."""
        # Test with valid structures
        valid_dict = {
            "find_replace": [{"find_value": "test"}],
            "key_value_replace": [{"find_key": "$.test"}],
            "spark_pool": [{"instance_pool_id": "test"}],
        }
        assert is_valid_structure(valid_dict) is True
        assert is_valid_structure(valid_dict, "find_replace") is True

        # Test with invalid structures
        invalid_dict = {
            "find_replace": "not a list",
            "key_value_replace": [{"find_key": "$.test"}],
        }
        assert is_valid_structure(invalid_dict) is False
        assert is_valid_structure(invalid_dict, "find_replace") is False

        # Test with missing parameters
        missing_dict = {
            "unknown_param": [{"test": "value"}],
        }
        assert is_valid_structure(missing_dict) is False

        # Test with empty dict
        assert is_valid_structure({}) is False

    @mock.patch("fabric_cicd._parameter._parameter.Parameter")
    @mock.patch("fabric_cicd._common._validate_input.validate_repository_directory")
    @mock.patch("fabric_cicd._common._validate_input.validate_item_type_in_scope")
    @mock.patch("fabric_cicd._common._validate_input.validate_environment")
    def test_validate_parameter_file(self, mock_validate_env, mock_validate_item_type, mock_validate_repo, mock_param):
        """Tests validate_parameter_file function."""
        # Setup mocks
        mock_validate_repo.return_value = Path("/mock/repo")
        mock_validate_item_type.return_value = ["Notebook", "Lakehouse"]
        mock_validate_env.return_value = "Test"
        mock_param_instance = mock.MagicMock()
        mock_param.return_value = mock_param_instance
        mock_param_instance._validate_parameter_file.return_value = True

        # Call the function
        from fabric_cicd._parameter._utils import validate_parameter_file

        # Patch the FabricEndpoint inside the test since we need it to run successfully
        with mock.patch("fabric_cicd._common._fabric_endpoint.FabricEndpoint", return_value=mock.MagicMock()):
            result = validate_parameter_file(
                repository_directory=Path("/mock/repo"),
                item_type_in_scope=["Notebook", "Lakehouse"],
                environment="Test",
            )

        # Verify the result
        assert result is True
        mock_param.assert_called_once()
        mock_param_instance._validate_parameter_file.assert_called_once()

    def test_find_match(self):
        """Tests _find_match function with various inputs."""
        # Test with None param_value
        assert _find_match(None, "value") is True

        # Test with string param_value
        assert _find_match("value", "value") is True
        assert _find_match("value", "other") is False

        # Test with list param_value
        assert _find_match(["value1", "value2"], "value1") is True
        assert _find_match(["value1", "value2"], "value3") is False

        # Test with list of Paths
        path_list = [Path("test1.txt"), Path("test2.txt")]
        assert _find_match(path_list, Path("test1.txt")) is True
        assert _find_match(path_list, Path("test3.txt")) is False

        # Test with invalid type
        assert _find_match(123, "value") is False

    def test_check_replacement(self, temp_repository):
        """Tests check_replacement function with various combinations of inputs."""
        file_path = temp_repository / "file1.txt"

        # Test with no filters
        assert check_replacement(None, None, None, "type1", "name1", file_path) is True

        # Test with matching filters
        assert check_replacement("type1", "name1", [file_path], "type1", "name1", file_path) is True

        # Test with non-matching filters
        assert check_replacement("type2", "name1", [file_path], "type1", "name1", file_path) is False
        assert check_replacement("type1", "name2", [file_path], "type1", "name1", file_path) is False
        assert check_replacement("type1", "name1", [Path("other.txt")], "type1", "name1", file_path) is False

        # Test with combination of matching/non-matching filters
        assert check_replacement("type1", "name2", [file_path], "type1", "name1", file_path) is False
        assert check_replacement("type1", "name1", [Path("other.txt")], "type1", "name1", file_path) is False

    def test_replace_key_value_valid_json(self, mock_workspace):
        """Tests replace_key_value with valid JSON content and environment."""
        # Test JSON with server host configuration
        test_json = '{"server": {"host": "localhost", "port": 8080}}'
        param_dict = {
            "find_key": "$.server.host",
            "replace_value": {"dev": "dev-server.example.com", "prod": "prod-server.example.com"},
        }

        # Test successful replacement for dev environment
        result = replace_key_value(mock_workspace, param_dict, test_json, "dev")
        result_data = json.loads(result)
        assert result_data["server"]["host"] == "dev-server.example.com"
        assert result_data["server"]["port"] == 8080  # Verify other values unchanged

        # Test successful replacement for prod environment
        result = replace_key_value(mock_workspace, param_dict, test_json, "prod")
        result_data = json.loads(result)
        assert result_data["server"]["host"] == "prod-server.example.com"

    def test_replace_key_value_environment_not_found(self, mock_workspace):
        """Tests replace_key_value when environment is not in the replace_value dictionary."""
        test_json = '{"server": {"host": "localhost", "port": 8080}}'
        param_dict = {
            "find_key": "$.server.host",
            "replace_value": {"dev": "dev-server.example.com", "prod": "prod-server.example.com"},
        }

        # Test when environment not in replace_value
        result = replace_key_value(mock_workspace, param_dict, test_json, "test")
        result_data = json.loads(result)
        assert result_data["server"]["host"] == "localhost"  # Original value unchanged

    def test_replace_key_value_invalid_json(self, mock_workspace):
        """Tests replace_key_value with invalid JSON content."""
        invalid_json = "{invalid json content}"
        param_dict = {"find_key": "$.server.host", "replace_value": {"dev": "test-server"}}

        # JSONDecodeError will be raised for invalid JSON and wrapped in ValueError
        with pytest.raises(ValueError, match="Expecting property name"):
            replace_key_value(mock_workspace, param_dict, invalid_json, "dev")

    def test_replace_key_value(self, mock_workspace):
        """Test replace_key_value function with JSON content."""
        # Create test parameter dictionary and JSON content
        param_dict = {
            "find_key": "$.server.host",
            "replace_value": {"dev": "dev-server.example.com", "prod": "prod-server.example.com"},
        }
        json_content = '{"server": {"host": "localhost", "port": 8080}}'

        # Test successful replacement
        result = replace_key_value(mock_workspace, param_dict, json_content, "dev")

        # Parse the JSON result and check the exact value (avoid substring sanitization issues)
        result_json = json.loads(result)
        assert result_json["server"]["host"] == "dev-server.example.com"

        # Test with environment not in replace_value
        result = replace_key_value(mock_workspace, param_dict, json_content, "test")
        result_json = json.loads(result)
        assert result_json["server"]["host"] == "localhost"

        # Test with invalid JSON content
        with pytest.raises(ValueError, match="Expecting property name"):
            replace_key_value(mock_workspace, param_dict, "{invalid json}", "dev")

    def test_replace_variables_in_parameter_file(self, monkeypatch):
        """Test replace_variables_in_parameter_file with feature flag enabled."""
        # Set up test environment variables
        test_env_vars = {
            "$ENV:TEST_VAR": "test_value",
            "$ENV:ANOTHER_VAR": "another_value",
            "NORMAL_VAR": "normal_value",  # Should be ignored
        }
        # Mock os.environ
        monkeypatch.setattr("os.environ", test_env_vars)

        # Mock feature flag to be enabled
        monkeypatch.setattr(constants, "FEATURE_FLAG", ["enable_environment_variable_replacement"])

        # Test parameter file content with environment variables
        test_content = """
        parameter:
          value: $ENV:TEST_VAR
          other: $ENV:ANOTHER_VAR
          normal: NORMAL_VAR
        """
        result = replace_variables_in_parameter_file(test_content)
        # Verify replacements
        assert "value: test_value" in result
        assert "other: another_value" in result
        assert "normal: NORMAL_VAR" in result  # Normal var unchanged

    def test_replace_variables_in_parameter_file_feature_disabled(self, monkeypatch):
        """Test replace_variables_in_parameter_file with feature flag disabled."""
        # Set up test environment variables with $ENV: prefix
        test_env_vars = {
            "$ENV:TEST_VAR": "test_value",
            "$ENV:ANOTHER_VAR": "another_value",
        }
        # Mock os.environ
        monkeypatch.setattr("os.environ", test_env_vars)

        # Mock feature flag to be disabled (empty list)
        monkeypatch.setattr(constants, "FEATURE_FLAG", [])

        # Test parameter file content with environment variables
        test_content = """
        parameter:
          value: $ENV:TEST_VAR
          other: $ENV:ANOTHER_VAR
          normal: NORMAL_VAR
        """
        result = replace_variables_in_parameter_file(test_content)

        # Verify NO replacements occurred since feature is disabled
        # Environment variables should remain as-is in the output
        assert "$ENV:TEST_VAR" in result
        assert "$ENV:ANOTHER_VAR" in result
        assert "NORMAL_VAR" in result  # Normal var unchanged

        # Make sure no replacements happened
        assert "test_value" not in result
        assert "another_value" not in result

    def test_replace_env_variables_in_content(self, monkeypatch):
        """Test replace_variables_in_parameter_file with feature flag enabled."""
        # Set up test environment variables with $ENV: prefix
        # This is required because the function filters os.environ for keys starting with $ENV:
        test_env_vars = {
            "$ENV:TEST_VAR": "test_value",
            "$ENV:ANOTHER_VAR": "another_value",
            "NORMAL_VAR": "normal_value",  # Should be ignored (no $ENV: prefix)
        }
        # Mock os.environ
        monkeypatch.setattr("os.environ", test_env_vars)

        # Mock feature flag to be enabled
        monkeypatch.setattr(constants, "FEATURE_FLAG", ["enable_environment_variable_replacement"])

        # Test parameter file content with environment variables
        test_content = """
        parameter:
          value: $ENV:TEST_VAR
          other: $ENV:ANOTHER_VAR
          normal: NORMAL_VAR
        """
        result = replace_variables_in_parameter_file(test_content)
        # Verify replacements
        assert "value: test_value" in result
        assert "other: another_value" in result
        assert "normal: NORMAL_VAR" in result  # Normal var unchanged

    def test_process_environment_key(self, mock_workspace):
        """Test process_environment_key function with ALL environment key replacement."""
        # Test with ALL key only - should replace with target environment
        replace_value_dict_1 = {"_ALL_": "universal-value"}
        replace_value_dict_2 = {"_all_": "universal-value"}
        replace_value_dict_3 = {"_All_": "universal-value"}
        replace_value_dict_4 = {"ALL": "universal-value"}

        # Mock the workspace environment
        mock_workspace.environment = "TEST"

        # Call the function
        result_1 = process_environment_key(mock_workspace, replace_value_dict_1)
        result_2 = process_environment_key(mock_workspace, replace_value_dict_2)
        result_3 = process_environment_key(mock_workspace, replace_value_dict_3)
        result_4 = process_environment_key(mock_workspace, replace_value_dict_4)

        # Verify _ALL_ key is replaced with the target environment
        assert "_ALL_" not in result_1
        assert "TEST" in result_1
        assert result_1["TEST"] == "universal-value"

        # Verify _all_ key is replaced with the target environment
        assert "_all_" not in result_2
        assert "TEST" in result_2
        assert result_2["TEST"] == "universal-value"

        # Verify _All_ key is replaced with the target environment
        assert "_All_" not in result_3
        assert "TEST" in result_3
        assert result_3["TEST"] == "universal-value"

        # Verify ALL key is replaced with the target environment
        assert "ALL" in result_4
        assert "TEST" not in result_4
        assert result_4["ALL"] == "universal-value"

        assert result_1 == {"TEST": "universal-value"}
        assert result_1 == result_2 == result_3 != result_4

        # Test without ALL key - should return unchanged dictionary
        replace_value_dict_5 = {
            "DEV": "dev-value",
            "PROD": "prod-value",
        }

        # Mock the workspace environment
        mock_workspace.environment = "TEST"

        # Call the function
        result = process_environment_key(mock_workspace, replace_value_dict_5)

        # Dictionary should remain unchanged
        assert result == replace_value_dict_5
        assert "TEST" not in result


class TestPathUtilities:
    """Tests for path utility functions in _utils.py."""

    def test_process_input_path_none(self, temp_repository):
        """Tests process_input_path with none input."""
        result = process_input_path(temp_repository, None)
        assert result == []

    def test_process_input_path_string(self, temp_repository, monkeypatch):
        """Tests process_input_path with string input."""

        # Mock the helper functions and glob.has_magic
        def mock_process_regular_path(path, repo, valid_paths, _):
            if path == "file1.txt":
                valid_paths.add(repo / "file1.txt")

        def mock_process_wildcard_path(path, repo, valid_paths, _):
            if path == "*.txt":
                valid_paths.add(repo / "file1.txt")
                valid_paths.add(repo / "file2.txt")

        def mock_has_magic(path):
            return "*" in path

        # Apply the mocks
        monkeypatch.setattr("fabric_cicd._parameter._utils._process_regular_path", mock_process_regular_path)
        monkeypatch.setattr("fabric_cicd._parameter._utils._process_wildcard_path", mock_process_wildcard_path)
        monkeypatch.setattr("glob.has_magic", mock_has_magic)

        # Test with string path
        result = process_input_path(temp_repository, "file1.txt")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].name == "file1.txt"

        # Test with wildcard string
        result = process_input_path(temp_repository, "*.txt")
        assert isinstance(result, list)
        assert len(result) == 2  # Should find the 2 .txt files in root

    def test_process_input_path_list(self, temp_repository, monkeypatch):
        """Tests process_input_path with list input."""
        # Create a mapping of paths to the files they should find
        path_results = {
            "file1.txt": [temp_repository / "file1.txt"],
            "*.json": [temp_repository / "file2.json"],
            "folder1/*.py": [temp_repository / "folder1" / "file3.py"],
        }

        # Mock the helper functions and glob.has_magic
        def mock_process_regular_path(path, _, valid_paths, __):
            if path in path_results and "*" not in path:
                valid_paths.update(path_results[path])

        def mock_process_wildcard_path(path, _, valid_paths, __):
            if path in path_results and "*" in path:
                valid_paths.update(path_results[path])

        def mock_has_magic(path):
            return "*" in path

        # Apply the mocks
        monkeypatch.setattr("fabric_cicd._parameter._utils._process_regular_path", mock_process_regular_path)
        monkeypatch.setattr("fabric_cicd._parameter._utils._process_wildcard_path", mock_process_wildcard_path)
        monkeypatch.setattr("glob.has_magic", mock_has_magic)

        # Test with list of paths including both regular and wildcard patterns
        paths = ["file1.txt", "*.json", "folder1/*.py"]
        result = process_input_path(temp_repository, paths)
        assert isinstance(result, list)
        assert len(result) == 3  # Should find file1.txt, file2.json, and folder1/file3.py
        assert any(p.name == "file1.txt" for p in result)
        assert any(p.name == "file2.json" for p in result)
        assert any(p.name == "file3.py" for p in result)

    def test_process_input_path_has_magic_exception(self, temp_repository, monkeypatch):
        """Tests process_input_path when glob.has_magic raises an exception."""
        # Create a mock logger to verify logging
        mock_logger = mock.MagicMock()
        monkeypatch.setattr("fabric_cicd._parameter._utils.logger", mock_logger)

        # Mock glob.has_magic to raise an exception
        def mock_has_magic(_):
            msg = "Mock exception in has_magic"
            raise ValueError(msg)

        monkeypatch.setattr("glob.has_magic", mock_has_magic)

        # Test with a single path - should handle the exception gracefully
        result = process_input_path(temp_repository, "file1.txt", False)

        # Verify the result is a list and it's empty (since we couldn't process the path)
        assert isinstance(result, list)
        assert len(result) == 0

        # Verify that the error was logged
        assert mock_logger.debug.called
        assert "Error checking for wildcard" in mock_logger.debug.call_args_list[0][0][0]
        mock_logger.reset_mock()

        # Test with a list of paths - should attempt to process each path but return empty list
        # since all paths will fail the glob.has_magic check with an exception
        result = process_input_path(temp_repository, ["file1.txt", "file2.txt"], False)
        assert isinstance(result, list)
        assert len(result) == 0

        # Verify that errors were logged for both paths
        assert mock_logger.debug.call_count == 2
        assert "Error checking for wildcard" in mock_logger.debug.call_args_list[0][0][0]
        assert "Error checking for wildcard" in mock_logger.debug.call_args_list[1][0][0]

    def test_resolve_input_path_with_invalid_wildcard_syntax(self, temp_repository, monkeypatch):
        """Tests _resolve_input_path when _validate_wildcard_syntax returns False."""
        # Create a valid path in the temp repository
        valid_path = temp_repository / "test.txt"
        valid_path.write_text("test content")

        # Mock _validate_wildcard_syntax to return False for our test pattern
        def mock_validate_wildcard_syntax(pattern, _):
            return pattern != "invalid*.txt"  # Return False only for our test pattern

        monkeypatch.setattr("fabric_cicd._parameter._utils._validate_wildcard_syntax", mock_validate_wildcard_syntax)

        # Use a public function that calls _resolve_input_path with wildcard=True
        result = process_input_path(temp_repository, "invalid*.txt")

        # Should be empty because the wildcard validation failed
        assert len(result) == 0

    def test_process_input_path_some_invalid(self, temp_repository, monkeypatch):
        """Tests process_input_path with some invalid paths."""
        # Create a mock logger
        mock_logger = mock.MagicMock()
        monkeypatch.setattr("fabric_cicd._parameter._utils.logger", mock_logger)

        # Create test files we need for this test
        (temp_repository / "valid_file.txt").write_text("valid content")

        # Mock glob.has_magic to succeed for specific paths and fail for others
        import glob as glob_module

        original_has_magic = glob_module.has_magic

        def mock_has_magic(path):
            if path == "error_path.txt":
                msg = "Mock error for specific path"
                raise ValueError(msg)
            return original_has_magic(path)

        monkeypatch.setattr("glob.has_magic", mock_has_magic)

        # Mock _resolve_file_path to return a valid path for specific files
        def mock_resolve_file_path(path, *_):
            if "valid_file.txt" in str(path):
                return path
            return None

        monkeypatch.setattr("fabric_cicd._parameter._utils._resolve_file_path", mock_resolve_file_path)

        # Test with a mix of valid and problematic paths
        result = process_input_path(temp_repository, ["valid_file.txt", "error_path.txt", "nonexistent_file.txt"])

        # Should return only valid paths
        assert isinstance(result, list)
        assert len(result) == 1
        assert "valid_file.txt" in str(result[0])

        # Verify errors were logged for problematic paths
        assert mock_logger.debug.called
        assert any("Error checking for wildcard" in call[0][0] for call in mock_logger.debug.call_args_list)

    def test_process_wildcard_path(self, temp_repository, monkeypatch):
        """Tests _process_wildcard_path function."""
        # Create the test files we need for this test
        (temp_repository / "file1.txt").write_text("content1")
        (temp_repository / "file2.txt").write_text("content2")
        (temp_repository / "folder2" / "file5.txt").write_text("content5")

        # We need to patch the actual Path.glob method with our own implementation
        original_glob = Path.glob

        def patched_glob(self, pattern):
            # Special case for our test - return predefined results
            if str(self) == str(temp_repository):
                if pattern == "*.txt":
                    return [temp_repository / "file1.txt", temp_repository / "file2.txt"]
                if pattern == "**/*.txt":
                    return [
                        temp_repository / "file1.txt",
                        temp_repository / "file2.txt",
                        temp_repository / "folder2" / "file5.txt",
                    ]
            # Fall back to original method for other cases
            return original_glob(self, pattern)

        # Apply the patch
        monkeypatch.setattr(Path, "glob", patched_glob)

        # Set up a valid paths set
        valid_paths = set()
        mock_log = mock.MagicMock()

        # Mock _set_wildcard_path_pattern to return our test pattern
        def mock_set_pattern(pattern, _repo, _log):
            return "*.txt" if pattern == "*.txt" else "**/*.txt"

        monkeypatch.setattr("fabric_cicd._parameter._utils._set_wildcard_path_pattern", mock_set_pattern)

        # Mock _resolve_file_path to return valid paths
        def mock_resolve_path(path, _repo, _path_type, _log):
            return path

        monkeypatch.setattr("fabric_cicd._parameter._utils._resolve_file_path", mock_resolve_path)

        # Test with wildcard pattern for txt files
        _process_wildcard_path("*.txt", temp_repository, valid_paths, mock_log)
        assert len(valid_paths) == 2  # Should find file1.txt and file2.txt in root
        assert all(path.suffix == ".txt" for path in valid_paths)

        # Reset paths and test with recursive pattern
        valid_paths.clear()
        _process_wildcard_path("**/*.txt", temp_repository, valid_paths, mock_log)
        assert len(valid_paths) == 3  # Should find all .txt files (including in subdirectories)

    def test_process_regular_path(self, temp_repository, monkeypatch):
        """Tests _process_regular_path with regular file paths."""

        # Set up a valid paths set
        valid_paths = set()
        mock_log = mock.MagicMock()

        # Mock _resolve_file_path to return valid paths for specific files
        def mock_resolve_file_path(path, _repo, _path_type, _log):
            if path.name == "file1.txt" or path.name == "file2.json":
                return path.resolve()
            return None

        monkeypatch.setattr("fabric_cicd._parameter._utils._resolve_file_path", mock_resolve_file_path)

        # Test with specific file path
        _process_regular_path("file1.txt", temp_repository, valid_paths, mock_log)
        assert len(valid_paths) == 1
        assert next(iter(valid_paths)).name == "file1.txt"

        # Reset and test with absolute path
        valid_paths.clear()
        abs_path = str(temp_repository / "file2.json")
        _process_regular_path(abs_path, temp_repository, valid_paths, mock_log)
        assert len(valid_paths) == 1
        assert next(iter(valid_paths)).name == "file2.json"

        # Test with nonexistent file
        valid_paths.clear()
        _process_regular_path("nonexistent.txt", temp_repository, valid_paths, mock_log)
        assert len(valid_paths) == 0  # Should not add nonexistent files

    def test_resolve_nonexistent_file_path(self, temp_repository):
        """Tests _resolve_file_path with nonexistent files."""
        # Test nonexistent file
        file_path = temp_repository / "nonexistent.txt"
        result = _resolve_file_path(file_path, temp_repository, "Relative", logger.debug)
        assert result is None

    def test_resolve_directory_file_path(self, temp_repository):
        """Tests _resolve_file_path with directories."""
        # Test with directory instead of file
        dir_path = temp_repository / "folder1"
        result = _resolve_file_path(dir_path, temp_repository, "Relative", logger.debug)
        assert result is None

    def test_resolve_input_path_absolute_path(self):
        """Test _resolve_input_path with absolute path."""
        # Using a standard logger function format that takes a string message
        mock_logger = MagicMock()
        repo_dir = Path("c:/test_repo").resolve()  # Make sure it's resolved

        # Test with absolute path outside repository
        outside_path = Path("c:/outside/file.txt").resolve()  # Make sure it's resolved

        # Simulate a path outside the repo by mocking the relative_to method
        with mock.patch.object(Path, "relative_to", side_effect=ValueError("Path outside repo")):
            result = _resolve_file_path(outside_path, repo_dir, "Absolute", mock_logger)
            # Check that the function returns None (path rejected)
            assert result is None
            # Check that the logger was called with an error about the path being outside
            mock_logger.assert_called_once_with(f"Absolute path '{outside_path}' is outside the repository directory")

    def test_resolve_outside_repo_file_path(self, temp_repository):
        """Tests _resolve_file_path with paths outside the repository."""
        # Create a file outside the repository
        outside_dir = Path(tempfile.mkdtemp())
        try:
            outside_file = outside_dir / "outside.txt"
            outside_file.write_text("outside content")

            # Test with file outside repository
            result = _resolve_file_path(outside_file, temp_repository, "Absolute", logger.debug)
            assert result is None
        finally:
            shutil.rmtree(outside_dir)

    def test_resolve_invalid_file_path(self, temp_repository, monkeypatch):
        """Tests _resolve_file_path with a path that causes exception."""

        # Set up a mock that raises an exception when checking if file exists
        def mock_path_exists(_):
            msg = "Permission denied"
            raise PermissionError(msg)

        # Apply the mock
        monkeypatch.setattr(Path, "exists", mock_path_exists)

        # Test the exception handling
        file_path = temp_repository / "file1.txt"
        result = _resolve_file_path(file_path, temp_repository, "Test", logger.debug)
        assert result is None

    def test_validate_wildcard_syntax_invalid(self):
        """Test _validate_wildcard_syntax with invalid wildcard syntax."""
        # Create a mock function to pass as log_func
        mock_log_func = MagicMock()

        # Test with invalid recursive wildcard format - double asterisk without proper format
        # This will trigger the check: "**" in p and not ("**/" in p or "/**" in p)
        invalid_path = "src**invalid.py"  # Missing slash between src and **

        # Call the function being tested
        result = _validate_wildcard_syntax(invalid_path, mock_log_func)

        # Verify validation fails
        assert result is False

        # Check that log_func was called exactly once with the expected message
        mock_log_func.assert_called_once_with(f"Invalid recursive wildcard format (use **/ or /**): '{invalid_path}'")

    def test_valid_wildcard_syntax(self):
        """Tests that valid wildcard patterns pass validation."""
        # Create a mock logger
        mock_log_func = mock.MagicMock()

        valid_patterns = [
            "*.txt",
            "**/*.py",
            "folder1/*.json",
            "folder?/*.txt",
            "folder[1-3]/*.txt",
            "file[!1-3].txt",
            "file{1,2,3}.txt",
            "**/subfolder/*.md",
        ]

        for pattern in valid_patterns:
            assert _validate_wildcard_syntax(pattern, mock_log_func) is True, f"Pattern should be valid: {pattern}"
            mock_log_func.assert_not_called()  # No errors should be logged

    def test_invalid_wildcard_syntax(self):
        """Tests that invalid wildcard patterns fail validation, including complex bracket/brace nesting issues."""
        # Create a mock logger
        mock_log_func = mock.MagicMock()

        # Group 1: Basic validation errors
        basic_invalid_patterns = [
            "",  # Empty string
            "   ",  # Whitespace only
            "../file.txt",  # Path traversal
            "folder/../file.txt",  # Path traversal
            "..%2Ffile.txt",  # Encoded path traversal
        ]

        # Group 2: Wildcard pattern errors
        wildcard_invalid_patterns = [
            "/**/*/",  # Invalid combination
            "**/**",  # Invalid combination
            "folder//file.txt",  # Double slashes
            "folder\\\\file.txt",  # Double backslashes
            "**file.txt",  # Incorrect recursive format
            "//**/test.txt",  # Absolute path with recursive pattern
        ]

        # Group 3: Bracket/brace validation errors
        bracket_brace_invalid_patterns = [
            "folder[].txt",  # Empty brackets
            "folder[abc.txt",  # Unclosed bracket
            "folder{}.txt",  # Empty braces
            "folder{abc.txt",  # Unclosed brace
            "folder{,}.txt",  # Invalid comma in braces
            "folder{a,,b}.txt",  # Empty option in braces
            "folder{abc}.txt",  # Brace without comma
            "folder[a-",  # Unclosed bracket with range
        ]

        # Test all invalid patterns
        all_invalid_patterns = basic_invalid_patterns + wildcard_invalid_patterns + bracket_brace_invalid_patterns
        for pattern in all_invalid_patterns:
            assert _validate_wildcard_syntax(pattern, mock_log_func) is False, f"Pattern should be invalid: {pattern}"
            mock_log_func.assert_called()  # Error should be logged
            mock_log_func.reset_mock()

        complex_invalid_nested_patterns = [
            "folder[[a[b]c].txt",  # Unbalanced nested brackets
            "folder{a{b,c}.txt",  # Unbalanced nested braces
        ]

        # Test more complex bracket/brace nesting scenarios
        for pattern in complex_invalid_nested_patterns:
            assert _validate_wildcard_syntax(pattern, mock_log_func) is False, f"Pattern should be invalid: {pattern}"
            mock_log_func.assert_called()  # Error should be logged
            mock_log_func.reset_mock()

    def test_validate_nested_brackets_braces(self):
        """Tests the _validate_nested_brackets_braces function to ensure proper validation of bracket/brace nesting."""
        from fabric_cicd._parameter._utils import _validate_nested_brackets_braces as validate_func

        mock_log_func = mock.MagicMock()

        valid_nested_patterns = [
            "file[abc].txt",  # Simple bracket
            "file{a,b,c}.txt",  # Simple brace
            "file[abc]{1,2,3}.txt",  # Both brackets and braces
            "file[a[b]c].txt",  # Nested brackets (valid in some glob implementations)
            "file{a{b,c},d}.txt",  # Nested braces
            "file[[]].txt",  # Escaped bracket in character class
            "file[a-z].{txt,md}",  # Multiple bracket/brace pairs
        ]

        # Test valid patterns
        for pattern in valid_nested_patterns:
            assert validate_func(pattern, mock_log_func) is True, f"Pattern should be valid: {pattern}"
            mock_log_func.assert_not_called()
            mock_log_func.reset_mock()

        invalid_nested_patterns = [
            "file[abc.txt",  # Unclosed bracket
            "file{a,b.txt",  # Unclosed brace
            "file]abc[.txt",  # Closing before opening
            "file}abc{.txt",  # Closing before opening
            "file[abc}.txt",  # Mismatched pairs
            "file{abc].txt",  # Mismatched pairs
            "file[a{b]c}.txt",  # Interleaved mismatched pairs
            "file{a[b}c].txt",  # Interleaved mismatched pairs
        ]

        # Test invalid patterns
        for pattern in invalid_nested_patterns:
            assert validate_func(pattern, mock_log_func) is False, f"Pattern should be invalid: {pattern}"
            mock_log_func.assert_called_once()
            mock_log_func.reset_mock()
