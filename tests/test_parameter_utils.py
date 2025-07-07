# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the _parameter/_utils.py module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import fabric_cicd.constants as constants
from fabric_cicd._common._exceptions import InputError, ParsingError
from fabric_cicd._parameter._utils import (
    _check_parameter_structure,
    _extract_item_attribute,
    _find_match,
    _resolve_input_path,
    check_replacement,
    extract_find_value,
    extract_parameter_filters,
    extract_replace_value,
    is_valid_structure,
    process_input_path,
    replace_key_value,
    replace_variables_in_parameter_file,
)


class TestPathResolutionFunctions:
    """Tests for path resolution functions in _utils.py."""

    def test_resolve_input_path_simple(self, tmp_path):
        """Test _resolve_input_path with a simple file path."""
        # Create a test file
        test_file = tmp_path / "test_file.txt"
        test_file.touch()

        # Test with a relative path
        paths = _resolve_input_path(tmp_path, "test_file.txt")
        assert len(paths) == 1
        assert paths[0] == test_file

    def test_resolve_input_path_wildcard(self, tmp_path):
        """Test _resolve_input_path with wildcard patterns."""
        # Create test files
        (tmp_path / "test1.txt").touch()
        (tmp_path / "test2.txt").touch()
        (tmp_path / "subfolder").mkdir()
        (tmp_path / "subfolder" / "test3.txt").touch()

        # Test with a simple wildcard
        paths = _resolve_input_path(tmp_path, "*.txt", is_wildcard=True)
        assert len(paths) == 2
        assert all(p.name in ["test1.txt", "test2.txt"] for p in paths)

        # Test with a recursive wildcard
        paths = _resolve_input_path(tmp_path, "**/*.txt", is_wildcard=True)
        assert len(paths) == 3
        filenames = [p.name for p in paths]
        assert "test1.txt" in filenames
        assert "test2.txt" in filenames
        assert "test3.txt" in filenames

    def test_resolve_input_path_absolute(self, tmp_path):
        """Test _resolve_input_path with absolute paths."""
        # Create a test file
        test_file = tmp_path / "test_file.txt"
        test_file.touch()

        # Test with an absolute path inside the repository
        paths = _resolve_input_path(tmp_path, test_file)
        assert len(paths) == 1
        assert paths[0] == test_file

        # Test with an absolute path outside the repository
        outside_file = tmp_path.parent / "outside.txt"
        paths = _resolve_input_path(tmp_path, outside_file)
        assert len(paths) == 0

    def test_resolve_input_path_nonexistent(self, tmp_path):
        """Test _resolve_input_path with non-existent paths."""
        # Test with a non-existent file
        paths = _resolve_input_path(tmp_path, "nonexistent.txt")
        assert len(paths) == 0

    def test_process_input_path_none(self, tmp_path):
        """Test process_input_path with None input."""
        result = process_input_path(tmp_path, None)
        assert result is None

    def test_process_input_path_string(self, tmp_path):
        """Test process_input_path with a string input."""
        # Create a test file
        test_file = tmp_path / "test_file.txt"
        test_file.touch()

        # Test with a valid path
        result = process_input_path(tmp_path, "test_file.txt")
        assert len(result) == 1
        assert result[0] == test_file

    def test_process_input_path_list(self, tmp_path):
        """Test process_input_path with a list of paths."""
        # Create test files
        (tmp_path / "test1.txt").touch()
        (tmp_path / "test2.txt").touch()

        # Test with a list of valid paths
        result = process_input_path(tmp_path, ["test1.txt", "test2.txt"])
        assert len(result) == 2
        assert all(p.name in ["test1.txt", "test2.txt"] for p in result)

        # Test with a mix of valid and invalid paths
        result = process_input_path(tmp_path, ["test1.txt", "nonexistent.txt"])
        assert len(result) == 1
        assert result[0].name == "test1.txt"

    def test_process_input_path_wildcard_in_list(self, tmp_path):
        """Test process_input_path with wildcards in a list."""
        # Create test files
        (tmp_path / "test1.txt").touch()
        (tmp_path / "test2.txt").touch()
        (tmp_path / "test.py").touch()

        # Test with a list containing a wildcard
        result = process_input_path(tmp_path, ["*.txt", "*.py"])
        assert len(result) == 3
        assert all(p.name in ["test1.txt", "test2.txt", "test.py"] for p in result)


class TestCheckReplacementFunctions:
    """Tests for check_replacement and _find_match functions."""

    def test_check_replacement_no_filters(self):
        """Test check_replacement with no filters."""
        result = check_replacement(
            input_type=None,
            input_name=None,
            input_path=None,
            item_type="Notebook",
            item_name="Test",
            file_path=Path("/test/path.txt"),
        )
        assert result is True

    def test_check_replacement_matching_filters(self):
        """Test check_replacement with matching filters."""
        result = check_replacement(
            input_type="Notebook",
            input_name="Test",
            input_path=Path("/test/path.txt"),
            item_type="Notebook",
            item_name="Test",
            file_path=Path("/test/path.txt"),
        )
        assert result is True

    def test_check_replacement_non_matching_filters(self):
        """Test check_replacement with non-matching filters."""
        result = check_replacement(
            input_type="Notebook",
            input_name="Test",
            input_path=Path("/test/path.txt"),
            item_type="Dataflow",
            item_name="Test",
            file_path=Path("/test/path.txt"),
        )
        assert result is False

    def test_find_match_none(self):
        """Test _find_match with None parameter value."""
        result = _find_match(None, "test")
        assert result is True

    def test_find_match_string(self):
        """Test _find_match with string parameter value."""
        # Matching
        result = _find_match("test", "test")
        assert result is True

        # Non-matching
        result = _find_match("test", "other")
        assert result is False

    def test_find_match_list(self):
        """Test _find_match with list parameter value."""
        # Matching
        result = _find_match(["test", "other"], "test")
        assert result is True

        # Non-matching
        result = _find_match(["test1", "test2"], "other")
        assert result is False


class TestExtractFindValue:
    """Tests for extract_find_value function."""

    def test_extract_find_value_simple(self):
        """Test extract_find_value with a simple value."""
        param_dict = {"find_value": "test_value"}
        result = extract_find_value(param_dict, "file content", False)
        assert result == "test_value"

    def test_extract_find_value_regex_match(self):
        """Test extract_find_value with a regex pattern that matches."""
        param_dict = {"find_value": r"value: (\d+)", "is_regex": "true"}
        file_content = "This is a test with value: 123 in it."
        result = extract_find_value(param_dict, file_content, True)
        assert result == "123"

    def test_extract_find_value_regex_no_match(self):
        """Test extract_find_value with a regex pattern that doesn't match."""
        param_dict = {"find_value": r"value: (\d+)", "is_regex": "true"}
        file_content = "This is a test with no matching value."
        result = extract_find_value(param_dict, file_content, True)
        assert result == r"value: (\d+)"

    def test_extract_find_value_regex_multiple_groups_error(self):
        """Test extract_find_value with a regex pattern with multiple groups."""
        param_dict = {"find_value": r"value: (\d+) and (\d+)", "is_regex": "true"}
        file_content = "This is a test with value: 123 and 456 in it."
        with pytest.raises(InputError):
            extract_find_value(param_dict, file_content, True)

    def test_extract_find_value_regex_empty_group_error(self):
        """Test extract_find_value with a regex pattern that captures an empty group."""
        param_dict = {"find_value": r"value: ()", "is_regex": "true"}
        file_content = "This is a test with value: in it."
        with pytest.raises(InputError):
            extract_find_value(param_dict, file_content, True)


class TestExtractReplaceValue:
    """Tests for extract_replace_value function."""

    def test_extract_replace_value_simple(self):
        """Test extract_replace_value with a simple value."""
        mock_workspace = MagicMock()
        result = extract_replace_value(mock_workspace, "simple_value")
        assert result == "simple_value"

    def test_extract_replace_value_workspace_id(self):
        """Test extract_replace_value with $workspace.id variable."""
        mock_workspace = MagicMock()
        mock_workspace.workspace_id = "test-workspace-id"
        result = extract_replace_value(mock_workspace, "$workspace.id")
        assert result == "test-workspace-id"

    @patch("fabric_cicd._parameter._utils._extract_item_attribute")
    def test_extract_replace_value_items_variable(self, mock_extract_item_attr):
        """Test extract_replace_value with $items variable."""
        mock_workspace = MagicMock()
        mock_extract_item_attr.return_value = "item-attribute-value"

        result = extract_replace_value(mock_workspace, "$items.type.name.id")

        mock_extract_item_attr.assert_called_once_with(mock_workspace, "$items.type.name.id")
        assert result == "item-attribute-value"


class TestExtractItemAttribute:
    """Tests for _extract_item_attribute function."""

    def test_extract_item_attribute_valid(self):
        """Test _extract_item_attribute with valid input."""
        # Setup
        mock_workspace = MagicMock()
        constants.ITEM_ATTR_LOOKUP = ["id", "sqlendpoint"]

        # Configure the workspace mock
        mock_workspace.workspace_items = {"Warehouse": {"TestWarehouse": {"id": "warehouse-id-123"}}}

        # Test
        result = _extract_item_attribute(mock_workspace, "$items.Warehouse.TestWarehouse.id")

        # Assertions
        mock_workspace._refresh_deployed_items.assert_called_once()
        assert result == "warehouse-id-123"

    def test_extract_item_attribute_invalid_syntax(self):
        """Test _extract_item_attribute with invalid variable syntax."""
        mock_workspace = MagicMock()

        with pytest.raises(ParsingError):
            _extract_item_attribute(mock_workspace, "$items.type.name")

    def test_extract_item_attribute_invalid_item_type(self):
        """Test _extract_item_attribute with invalid item type."""
        mock_workspace = MagicMock()
        mock_workspace.workspace_items = {}

        with pytest.raises(ParsingError):
            _extract_item_attribute(mock_workspace, "$items.InvalidType.name.id")

    def test_extract_item_attribute_invalid_item_name(self):
        """Test _extract_item_attribute with invalid item name."""
        mock_workspace = MagicMock()
        mock_workspace.workspace_items = {"Warehouse": {}}

        with pytest.raises(ParsingError):
            _extract_item_attribute(mock_workspace, "$items.Warehouse.InvalidName.id")

    def test_extract_item_attribute_invalid_attribute(self):
        """Test _extract_item_attribute with invalid attribute."""
        mock_workspace = MagicMock()
        constants.ITEM_ATTR_LOOKUP = ["id", "sqlendpoint"]

        mock_workspace.workspace_items = {"Warehouse": {"TestWarehouse": {"id": "warehouse-id-123"}}}

        with pytest.raises(ParsingError):
            _extract_item_attribute(mock_workspace, "$items.Warehouse.TestWarehouse.invalid")

    def test_extract_item_attribute_missing_value(self):
        """Test _extract_item_attribute with missing attribute value."""
        mock_workspace = MagicMock()
        constants.ITEM_ATTR_LOOKUP = ["id", "sqlendpoint"]

        mock_workspace.workspace_items = {"Warehouse": {"TestWarehouse": {"id": None}}}

        with pytest.raises(ParsingError):
            _extract_item_attribute(mock_workspace, "$items.Warehouse.TestWarehouse.id")


class TestExtractParameterFilters:
    """Tests for extract_parameter_filters function."""

    def test_extract_parameter_filters_all_fields(self):
        """Test extract_parameter_filters with all fields present."""
        mock_workspace = MagicMock()
        mock_workspace.repository_directory = Path("/repo")

        # Create a test file for process_input_path to find
        with patch("fabric_cicd._parameter._utils.process_input_path") as mock_process:
            mock_process.return_value = [Path("/repo/test_file.txt")]

            param_dict = {"item_type": "Notebook", "item_name": "TestNotebook", "file_path": "test_file.txt"}

            item_type, item_name, file_path = extract_parameter_filters(mock_workspace, param_dict)

            assert item_type == "Notebook"
            assert item_name == "TestNotebook"
            assert file_path == [Path("/repo/test_file.txt")]
            mock_process.assert_called_once_with(mock_workspace.repository_directory, "test_file.txt")

    def test_extract_parameter_filters_missing_fields(self):
        """Test extract_parameter_filters with some fields missing."""
        mock_workspace = MagicMock()
        mock_workspace.repository_directory = Path("/repo")

        with patch("fabric_cicd._parameter._utils.process_input_path") as mock_process:
            mock_process.return_value = None

            param_dict = {
                "item_type": "Notebook",
            }

            item_type, item_name, file_path = extract_parameter_filters(mock_workspace, param_dict)

            assert item_type == "Notebook"
            assert item_name is None
            assert file_path is None


class TestReplaceKeyValue:
    """Tests for replace_key_value function."""

    def test_replace_key_value_valid_json(self):
        """Test replace_key_value with valid JSON."""
        param_dict = {"find_key": "$.name", "replace_value": {"DEV": "dev-name", "PROD": "prod-name"}}

        json_content = '{"name": "old-name", "value": 123}'
        result = replace_key_value(param_dict, json_content, "DEV")

        # Parse the result back to a dict for easier comparison
        result_dict = json.loads(result)
        assert result_dict["name"] == "dev-name"
        assert result_dict["value"] == 123

    def test_replace_key_value_invalid_json(self):
        """Test replace_key_value with invalid JSON."""
        param_dict = {"find_key": "$.name", "replace_value": {"DEV": "dev-name", "PROD": "prod-name"}}

        json_content = '{"name": "old-name", "value": 123'  # Missing closing brace

        with pytest.raises(ValueError, match="Expecting.*delimiter"):
            replace_key_value(param_dict, json_content, "DEV")

    def test_replace_key_value_env_not_in_replace_value(self):
        """Test replace_key_value when env is not in replace_value."""
        param_dict = {"find_key": "$.name", "replace_value": {"DEV": "dev-name", "PROD": "prod-name"}}

        json_content = '{"name": "old-name", "value": 123}'
        result = replace_key_value(param_dict, json_content, "TEST")

        # Parse the result back to a dict for easier comparison
        result_dict = json.loads(result)
        assert result_dict["name"] == "old-name"  # Name should not be changed
        assert result_dict["value"] == 123


class TestReplaceVariablesInParameterFile:
    """Tests for replace_variables_in_parameter_file function."""

    def test_replace_variables_feature_flag_disabled(self):
        """Test replace_variables_in_parameter_file with feature flag disabled."""
        # Save the original feature flag value
        original_flag = constants.FEATURE_FLAG.copy() if hasattr(constants, "FEATURE_FLAG") else {}

        # Disable the feature flag
        constants.FEATURE_FLAG = {}

        raw_file = "Value: $ENV:TEST_VAR"
        result = replace_variables_in_parameter_file(raw_file)

        # Restore the original feature flag
        constants.FEATURE_FLAG = original_flag

        assert result == "Value: $ENV:TEST_VAR"  # No replacement should happen

    def test_replace_variables_feature_flag_enabled(self, monkeypatch):
        """Test replace_variables_in_parameter_file with feature flag enabled."""
        # Save the original feature flag value
        original_flag = constants.FEATURE_FLAG.copy() if hasattr(constants, "FEATURE_FLAG") else {}

        # Enable the feature flag
        constants.FEATURE_FLAG = {"enable_environment_variable_replacement": True}

        # Mock environment variables
        monkeypatch.setenv("$ENV:TEST_VAR", "test_value")

        raw_file = "Value: $ENV:TEST_VAR"
        result = replace_variables_in_parameter_file(raw_file)

        # Restore the original feature flag
        constants.FEATURE_FLAG = original_flag

        assert result == "Value: test_value"


class TestStructureValidation:
    """Tests for structure validation functions."""

    def test_check_parameter_structure_list(self):
        """Test _check_parameter_structure with a list."""
        assert _check_parameter_structure([1, 2, 3]) is True

    def test_check_parameter_structure_non_list(self):
        """Test _check_parameter_structure with non-list values."""
        assert _check_parameter_structure("string") is False
        assert _check_parameter_structure(123) is False
        assert _check_parameter_structure({"key": "value"}) is False
        assert _check_parameter_structure(None) is False

    def test_is_valid_structure_no_param_name(self):
        """Test is_valid_structure without specifying a param_name."""
        # Valid structure (all parameters have list values)
        param_dict = {"find_replace": [1, 2], "key_value_replace": [3, 4], "spark_pool": [5, 6]}
        assert is_valid_structure(param_dict) is True

        # Invalid structure (mixed value types)
        param_dict = {"find_replace": [1, 2], "key_value_replace": "not a list", "spark_pool": [5, 6]}
        assert is_valid_structure(param_dict) is False

        # No recognized parameters
        param_dict = {"unknown_param": [1, 2]}
        assert is_valid_structure(param_dict) is False

    def test_is_valid_structure_with_param_name(self):
        """Test is_valid_structure with a specified param_name."""
        # Valid structure
        param_dict = {"find_replace": [1, 2], "key_value_replace": "not a list"}
        assert is_valid_structure(param_dict, "find_replace") is True

        # Invalid structure
        assert is_valid_structure(param_dict, "key_value_replace") is False

        # Parameter doesn't exist
        assert is_valid_structure(param_dict, "nonexistent") is False
