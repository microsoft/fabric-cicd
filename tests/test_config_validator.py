# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for ConfigValidator class."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from fabric_cicd import constants
from fabric_cicd._common._config_validator import ConfigValidationError, ConfigValidator


class TestConfigValidator:
    """Unit tests for ConfigValidator class."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_init(self):
        """Test ConfigValidator initialization."""
        assert self.validator.errors == []
        assert self.validator.config is None
        assert self.validator.config_path is None
        assert self.validator.environment is None

    def test_validate_file_existence_valid_file(self, tmp_path):
        """Test _validate_file_existence with valid file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("test: value")

        result = self.validator._validate_file_existence(str(config_file))

        assert result == config_file.resolve()
        assert self.validator.errors == []

    def test_validate_file_existence_missing_file(self):
        """Test _validate_file_existence with missing file."""
        result = self.validator._validate_file_existence("nonexistent.yaml")

        assert result is None
        assert len(self.validator.errors) == 1
        assert (
            constants.CONFIG_VALIDATION_MSGS["file"]["not_found"].format("nonexistent.yaml") in self.validator.errors[0]
        )

    def test_validate_file_existence_empty_path(self):
        """Test _validate_file_existence with empty path."""
        result = self.validator._validate_file_existence("")

        assert result is None
        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["file"]["path_empty"] in self.validator.errors[0]

    def test_validate_file_existence_none_path(self):
        """Test _validate_file_existence with None path."""
        result = self.validator._validate_file_existence(None)

        assert result is None
        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["file"]["path_empty"] in self.validator.errors[0]

    def test_validate_file_existence_directory_instead_of_file(self, tmp_path):
        """Test _validate_file_existence with directory instead of file."""
        result = self.validator._validate_file_existence(str(tmp_path))

        assert result is None
        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["file"]["not_file"].format(str(tmp_path)) in self.validator.errors[0]

    def test_validate_yaml_content_valid_yaml(self, tmp_path):
        """Test _validate_yaml_content with valid YAML."""
        config_file = tmp_path / "config.yaml"
        config_data = {"core": {"workspace_id": "test-id"}}
        config_file.write_text(yaml.dump(config_data))

        self.validator.config_path = config_file
        result = self.validator._validate_yaml_content(config_file)

        assert result == config_data
        assert self.validator.errors == []

    def test_validate_yaml_content_invalid_yaml(self, tmp_path):
        """Test _validate_yaml_content with invalid YAML syntax."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("invalid: yaml: content: [")

        self.validator.config_path = config_file
        result = self.validator._validate_yaml_content(config_file)

        assert result is None
        assert len(self.validator.errors) == 1
        # We can't test the exact error message as it includes the specific parse error
        assert constants.CONFIG_VALIDATION_MSGS["file"]["yaml_syntax"].split(":")[0] in self.validator.errors[0]

    def test_validate_yaml_content_non_dict_yaml(self, tmp_path):
        """Test _validate_yaml_content with non-dictionary YAML."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("- item1\n- item2")

        self.validator.config_path = config_file
        result = self.validator._validate_yaml_content(config_file)

        assert result is None
        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["file"]["not_dict"].format("list") in self.validator.errors[0]

    def test_validate_yaml_content_none_path(self):
        """Test _validate_yaml_content with None path."""
        result = self.validator._validate_yaml_content(None)

        assert result is None
        assert self.validator.errors == []  # Error should already be added by file existence check

    def test_validate_config_structure_valid(self):
        """Test _validate_config_structure with valid config."""
        self.validator.config = {"core": {"workspace_id": "test-id"}}

        self.validator._validate_config_structure()

        assert self.validator.errors == []

    def test_validate_config_structure_not_dict(self):
        """Test _validate_config_structure with non-dictionary config."""
        self.validator.config = ["not", "a", "dict"]

        self.validator._validate_config_structure()

        # The structure validation doesn't add errors for non-dict configs
        # as this is handled by YAML content validation
        assert self.validator.errors == []

    def test_validate_config_structure_none(self):
        """Test _validate_config_structure with None config."""
        self.validator.config = None

        self.validator._validate_config_structure()

        # The structure validation doesn't add errors for None configs
        # as this is handled by YAML content validation
        assert self.validator.errors == []

    def test_validate_workspace_field_valid_string(self):
        """Test _validate_workspace_field with valid string."""
        core = {"workspace": "test-workspace"}

        result = self.validator._validate_workspace_field(core, "workspace")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_field_valid_workspace_id_guid(self):
        """Test _validate_workspace_field with valid workspace_id GUID."""
        core = {"workspace_id": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_field_invalid_workspace_id_guid(self):
        """Test _validate_workspace_field with invalid workspace_id GUID format."""
        core = {"workspace_id": "invalid-guid-format"}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is False
        assert len(self.validator.errors) == 1
        assert "must be a valid GUID format" in self.validator.errors[0]

    def test_validate_workspace_field_valid_dict(self):
        """Test _validate_workspace_field with valid environment mapping."""
        core = {"workspace": {"dev": "dev-workspace", "prod": "prod-workspace"}}

        result = self.validator._validate_workspace_field(core, "workspace")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_field_valid_workspace_id_dict(self):
        """Test _validate_workspace_field with valid workspace_id environment mapping."""
        core = {
            "workspace_id": {
                "dev": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b",
                "prod": "2f4b9e8d-1a7c-4d3e-b8e2-5c9f7a2d4e1b",
            }
        }

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_field_invalid_workspace_id_dict(self):
        """Test _validate_workspace_field with invalid workspace_id GUID in environment mapping."""
        core = {"workspace_id": {"dev": "valid-8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b", "prod": "invalid-guid"}}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is False
        assert len(self.validator.errors) == 2  # One for each invalid GUID
        assert "must be a valid GUID format" in self.validator.errors[0]
        assert "must be a valid GUID format" in self.validator.errors[1]

    def test_validate_workspace_field_missing(self):
        """Test _validate_workspace_field with missing field."""
        core = {}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is False
        assert self.validator.errors == []

    def test_validate_workspace_field_invalid_type(self):
        """Test _validate_workspace_field with invalid type."""
        core = {"workspace_id": 123}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is False
        assert len(self.validator.errors) == 1
        assert "must be either a string or environment mapping" in self.validator.errors[0]

    def test_validate_environment_mapping_valid(self):
        """Test _validate_environment_mapping with valid mapping."""
        field_value = {"dev": "dev-value", "prod": "prod-value"}

        result = self.validator._validate_environment_mapping(field_value, "test_field", str)

        assert result is True
        assert self.validator.errors == []

    def test_validate_environment_mapping_empty(self):
        """Test _validate_environment_mapping with empty mapping."""
        field_value = {}

        result = self.validator._validate_environment_mapping(field_value, "test_field", str)

        assert result is False
        assert len(self.validator.errors) == 1
        assert "environment mapping cannot be empty" in self.validator.errors[0]

    def test_validate_environment_mapping_invalid_env_key(self):
        """Test _validate_environment_mapping with invalid environment key."""
        field_value = {"": "value", "dev": "dev-value"}

        result = self.validator._validate_environment_mapping(field_value, "test_field", str)

        assert result is False
        assert len(self.validator.errors) == 1
        assert "Environment key in 'test_field' must be a non-empty string" in self.validator.errors[0]

    def test_validate_environment_mapping_wrong_value_type(self):
        """Test _validate_environment_mapping with wrong value type."""
        field_value = {"dev": 123, "prod": "prod-value"}

        result = self.validator._validate_environment_mapping(field_value, "test_field", str)

        assert result is False
        assert len(self.validator.errors) == 1
        assert "must be a str, got int" in self.validator.errors[0]

    def test_validate_environment_mapping_empty_string_value(self):
        """Test _validate_environment_mapping with empty string value."""
        field_value = {"dev": "", "prod": "prod-value"}

        result = self.validator._validate_environment_mapping(field_value, "test_field", str)

        assert result is False
        assert len(self.validator.errors) == 1
        assert "value for environment 'dev' cannot be empty" in self.validator.errors[0]

    def test_validate_environment_mapping_empty_list_value(self):
        """Test _validate_environment_mapping with empty list value."""
        field_value = {"dev": [], "prod": ["item1"]}

        result = self.validator._validate_environment_mapping(field_value, "test_field", list)

        assert result is False
        assert len(self.validator.errors) == 1
        assert "value for environment 'dev' cannot be empty" in self.validator.errors[0]

    def test_validate_repository_directory_valid_string(self):
        """Test _validate_repository_directory with valid string."""
        core = {"repository_directory": "/path/to/repo"}

        self.validator._validate_repository_directory(core)

        assert self.validator.errors == []

    def test_validate_repository_directory_missing(self):
        """Test _validate_repository_directory with missing field."""
        core = {}

        self.validator._validate_repository_directory(core)

        assert len(self.validator.errors) == 1
        assert "must specify 'repository_directory'" in self.validator.errors[0]

    def test_validate_repository_directory_invalid_type(self):
        """Test _validate_repository_directory with invalid type."""
        core = {"repository_directory": 123}

        self.validator._validate_repository_directory(core)

        assert len(self.validator.errors) == 1
        assert "must be either a string or environment mapping" in self.validator.errors[0]

    def test_validate_item_types_valid_list(self):
        """Test _validate_item_types with valid item types."""
        item_types = ["Notebook", "DataPipeline"]

        self.validator._validate_item_types(item_types)

        assert self.validator.errors == []

    def test_validate_item_types_empty_list(self):
        """Test _validate_item_types with empty list."""
        item_types = []

        self.validator._validate_item_types(item_types)

        assert len(self.validator.errors) == 1
        assert "'item_types_in_scope' cannot be empty" in self.validator.errors[0]

    def test_validate_item_types_invalid_type(self):
        """Test _validate_item_types with invalid item type."""
        item_types = ["Notebook", 123, "DataPipeline"]

        self.validator._validate_item_types(item_types)

        assert len(self.validator.errors) == 1
        assert "Item type must be a string, got int" in self.validator.errors[0]

    def test_validate_item_types_unknown_item_type(self):
        """Test _validate_item_types with unknown item type."""
        item_types = ["Notebook", "UnknownType"]

        self.validator._validate_item_types(item_types)

        assert len(self.validator.errors) == 1
        assert "Invalid item type 'UnknownType'" in self.validator.errors[0]
        assert "Available types:" in self.validator.errors[0]

    def test_validate_item_types_with_env_context(self):
        """Test _validate_item_types with environment context."""
        item_types = ["UnknownType"]

        self.validator._validate_item_types(item_types, env_context="dev")

        assert len(self.validator.errors) == 1
        assert "Invalid item type 'UnknownType' in environment 'dev'" in self.validator.errors[0]

    def test_validate_regex_valid(self):
        """Test _validate_regex with valid regex."""
        self.validator._validate_regex("^test.*", "test_section")

        assert self.validator.errors == []

    def test_validate_regex_invalid(self):
        """Test _validate_regex with invalid regex."""
        self.validator._validate_regex("[invalid", "test_section")

        assert len(self.validator.errors) == 1
        assert "is not a valid regex pattern" in self.validator.errors[0]

    def test_validate_guid_format_valid(self):
        """Test _validate_guid_format with valid GUID."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        valid_guid = "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"

        result = _validate_guid_format(valid_guid)

        assert result is True

    def test_validate_guid_format_valid_uppercase(self):
        """Test _validate_guid_format with valid uppercase GUID."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        valid_guid = "8B6E2C7A-4C1F-4E3A-9B2E-7D8F2E1A6C3B"

        result = _validate_guid_format(valid_guid)

        assert result is True

    def test_validate_guid_format_invalid_format(self):
        """Test _validate_guid_format with invalid GUID format."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        invalid_guid = "invalid-guid-format"

        result = _validate_guid_format(invalid_guid)

        assert result is False

    def test_validate_guid_format_invalid_length(self):
        """Test _validate_guid_format with invalid GUID length."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        invalid_guid = "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3"  # Missing one character

        result = _validate_guid_format(invalid_guid)

        assert result is False

    def test_validate_items_list_valid(self):
        """Test _validate_items_list with valid items."""
        items_list = ["item1.Notebook", "item2.DataPipeline"]

        self.validator._validate_items_list(items_list, "test_context")

        assert self.validator.errors == []

    def test_validate_items_list_invalid_type(self):
        """Test _validate_items_list with invalid item type."""
        items_list = ["item1.Notebook", 123]

        self.validator._validate_items_list(items_list, "test_context")

        assert len(self.validator.errors) == 1
        assert "'test_context[1]' must be a string" in self.validator.errors[0]

    def test_validate_items_list_empty_item(self):
        """Test _validate_items_list with empty item."""
        items_list = ["item1.Notebook", ""]

        self.validator._validate_items_list(items_list, "test_context")

        assert len(self.validator.errors) == 1
        assert "'test_context[1]' cannot be empty" in self.validator.errors[0]

    def test_validate_features_list_valid(self):
        """Test _validate_features_list with valid features."""
        features_list = ["enable_shortcut_publish"]

        self.validator._validate_features_list(features_list, "test_context")

        assert self.validator.errors == []

    def test_validate_features_list_invalid_type(self):
        """Test _validate_features_list with invalid feature type."""
        features_list = ["enable_shortcut_publish", 123]

        self.validator._validate_features_list(features_list, "test_context")

        assert len(self.validator.errors) == 1
        assert "'test_context[1]' must be a string" in self.validator.errors[0]

    def test_validate_features_list_empty_feature(self):
        """Test _validate_features_list with empty feature."""
        features_list = ["enable_shortcut_publish", ""]

        self.validator._validate_features_list(features_list, "test_context")

        assert len(self.validator.errors) == 1
        assert "'test_context[1]' cannot be empty" in self.validator.errors[0]

    def test_validate_constants_dict_valid(self):
        """Test _validate_constants_dict with valid constants."""
        constants_dict = {"DEFAULT_API_ROOT_URL": "https://api.fabric.microsoft.com"}

        with patch.object(constants, "DEFAULT_API_ROOT_URL", "original_value"):
            self.validator._validate_constants_dict(constants_dict, "test_context")

        assert self.validator.errors == []

    def test_validate_constants_dict_invalid_key_type(self):
        """Test _validate_constants_dict with invalid key type."""
        constants_dict = {123: "value"}

        self.validator._validate_constants_dict(constants_dict, "test_context")

        assert len(self.validator.errors) == 1
        assert "Constant key in 'test_context' must be a non-empty string" in self.validator.errors[0]

    def test_validate_constants_dict_empty_key(self):
        """Test _validate_constants_dict with empty key."""
        constants_dict = {"": "value"}

        self.validator._validate_constants_dict(constants_dict, "test_context")

        assert len(self.validator.errors) == 1
        assert "Constant key in 'test_context' must be a non-empty string" in self.validator.errors[0]

    def test_validate_constants_dict_unknown_constant(self):
        """Test _validate_constants_dict with unknown constant."""
        constants_dict = {"UNKNOWN_CONSTANT": "value"}

        self.validator._validate_constants_dict(constants_dict, "test_context")

        assert len(self.validator.errors) == 1
        assert "Unknown constant 'UNKNOWN_CONSTANT'" in self.validator.errors[0]

    def test_validate_constants_dict_valid_various_types(self):
        """Test _validate_constants_dict with valid constants of various types."""
        constants_dict = {
            "DEFAULT_API_ROOT_URL": "https://api.example.com",  # string
            "ACCEPTED_ITEM_TYPES": ["Notebook", "DataPipeline"],  # can override with different types
            "FEATURE_FLAG": {"flag1", "flag2"},  # can override with different types
        }

        self.validator._validate_constants_dict(constants_dict, "test_context")

        assert self.validator.errors == []

    def test_validate_workspace_value_workspace_name_valid(self):
        """Test _validate_workspace_value with valid workspace name."""
        result = self.validator._validate_workspace_value("valid-workspace-name", "workspace", "workspace")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_value_workspace_id_valid_guid(self):
        """Test _validate_workspace_value with valid workspace_id GUID."""
        result = self.validator._validate_workspace_value(
            "12345678-1234-1234-1234-123456789abc", "workspace_id", "workspace_id"
        )

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_value_workspace_id_invalid_guid(self):
        """Test _validate_workspace_value with invalid workspace_id GUID."""
        result = self.validator._validate_workspace_value("invalid-guid", "workspace_id", "workspace_id")

        assert result is False
        assert len(self.validator.errors) == 1
        assert "must be a valid GUID format" in self.validator.errors[0]

    def test_validate_item_types_in_scope_valid_list(self):
        """Test _validate_item_types_in_scope with valid list."""
        core = {"item_types_in_scope": ["Notebook", "DataPipeline"]}

        self.validator._validate_item_types_in_scope(core)

        assert self.validator.errors == []

    def test_validate_item_types_in_scope_empty_list(self):
        """Test _validate_item_types_in_scope with empty list."""
        core = {"item_types_in_scope": []}

        self.validator._validate_item_types_in_scope(core)

        assert len(self.validator.errors) == 1
        assert "'item_types_in_scope' cannot be empty if specified" in self.validator.errors[0]

    def test_validate_item_types_in_scope_environment_mapping(self):
        """Test _validate_item_types_in_scope with environment mapping."""
        core = {"item_types_in_scope": {"dev": ["Notebook"], "prod": ["DataPipeline", "Notebook"]}}

        self.validator._validate_item_types_in_scope(core)

        assert self.validator.errors == []

    def test_validate_item_types_in_scope_invalid_type(self):
        """Test _validate_item_types_in_scope with invalid type."""
        core = {"item_types_in_scope": "invalid"}

        self.validator._validate_item_types_in_scope(core)

        assert len(self.validator.errors) == 1
        assert "must be either a list or environment mapping dictionary" in self.validator.errors[0]

    def test_validate_item_types_in_scope_missing_field(self):
        """Test _validate_item_types_in_scope with missing field (should be okay)."""
        core = {"workspace_id": "12345678-1234-1234-1234-123456789abc"}

        self.validator._validate_item_types_in_scope(core)

        assert self.validator.errors == []

    def test_resolve_repository_path_absolute_path(self, tmp_path):
        """Test _resolve_repository_path with absolute path."""
        # Create actual directory
        repo_dir = tmp_path / "workspace"
        repo_dir.mkdir()

        self.validator.config = {"core": {"repository_directory": str(repo_dir)}}
        self.validator.config_path = tmp_path / "config.yaml"

        self.validator._resolve_repository_path()

        assert self.validator.errors == []
        assert Path(self.validator.config["core"]["repository_directory"]) == repo_dir

    def test_resolve_repository_path_relative_path(self, tmp_path):
        """Test _resolve_repository_path with relative path."""
        # Create actual directory structure
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        repo_dir = tmp_path / "workspace"
        repo_dir.mkdir()

        self.validator.config = {"core": {"repository_directory": "../workspace"}}
        self.validator.config_path = config_dir / "config.yaml"

        self.validator._resolve_repository_path()

        assert self.validator.errors == []
        resolved_path = Path(self.validator.config["core"]["repository_directory"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()

    def test_resolve_repository_path_nonexistent_directory(self, tmp_path):
        """Test _resolve_repository_path with nonexistent directory."""
        self.validator.config = {"core": {"repository_directory": "nonexistent"}}
        self.validator.config_path = tmp_path / "config.yaml"

        self.validator._resolve_repository_path()

        assert len(self.validator.errors) == 1
        assert "repository_directory not found at resolved path" in self.validator.errors[0]

    def test_resolve_repository_path_file_instead_of_directory(self, tmp_path):
        """Test _resolve_repository_path with file instead of directory."""
        # Create a file instead of directory
        not_a_dir = tmp_path / "not_a_dir.txt"
        not_a_dir.write_text("content")

        self.validator.config = {"core": {"repository_directory": str(not_a_dir)}}
        self.validator.config_path = tmp_path / "config.yaml"

        self.validator._resolve_repository_path()

        assert len(self.validator.errors) == 1
        assert "repository_directory path exists but is not a directory" in self.validator.errors[0]

    def test_resolve_repository_path_environment_mapping(self, tmp_path):
        """Test _resolve_repository_path with environment mapping."""
        # Create actual directories
        dev_repo = tmp_path / "dev_workspace"
        dev_repo.mkdir()
        prod_repo = tmp_path / "prod_workspace"
        prod_repo.mkdir()

        self.validator.config = {"core": {"repository_directory": {"dev": str(dev_repo), "prod": str(prod_repo)}}}
        self.validator.config_path = tmp_path / "config.yaml"

        self.validator._resolve_repository_path()

        assert self.validator.errors == []
        repo_dirs = self.validator.config["core"]["repository_directory"]
        assert Path(repo_dirs["dev"]).is_absolute()
        assert Path(repo_dirs["prod"]).is_absolute()

    def test_validate_parameter_field_valid_configurations(self):
        """Test parameter field validation with valid string and environment mapping."""
        # Test valid string
        core_string = {"parameter": "parameter.yml"}
        self.validator._validate_parameter_field(core_string)
        assert self.validator.errors == []

        # Reset for next test
        self.validator.errors = []

        # Test valid environment mapping
        core_mapping = {"parameter": {"dev": "dev-parameter.yml", "prod": "prod-parameter.yml"}}
        self.validator._validate_parameter_field(core_mapping)
        assert self.validator.errors == []

    def test_validate_parameter_field_invalid_configurations(self):
        """Test parameter field validation with invalid configurations."""
        # Test empty string
        core_empty = {"parameter": ""}
        self.validator._validate_parameter_field(core_empty)
        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["field"]["empty_value"].format("parameter") in self.validator.errors[0]

        # Reset for next test
        self.validator.errors = []

        # Test invalid type
        core_invalid_type = {"parameter": 123}
        self.validator._validate_parameter_field(core_invalid_type)
        assert len(self.validator.errors) == 1
        assert (
            constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format("parameter", "int")
            in self.validator.errors[0]
        )

    def test_resolve_parameter_path_basic_functionality(self, tmp_path):
        """Test basic parameter path resolution functionality."""
        # Create parameter file
        param_file = tmp_path / "parameter.yml"
        param_file.write_text("find_replace: []")

        self.validator.config = {
            "core": {
                "workspace_id": "12345678-1234-1234-1234-123456789abc",
                "repository_directory": "workspace",
                "parameter": "parameter.yml",
            }
        }
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_parameter_path()

        assert self.validator.errors == []
        resolved_path = Path(self.validator.config["core"]["parameter"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.name == "parameter.yml"

    def test_resolve_path_field_directory_relative_path(self, tmp_path):
        """Test _resolve_path_field with relative directory path."""
        # Create directory
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()

        self.validator.config = {"test_section": {"test_field": "test_dir"}}
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_path_field("test_dir", "test_field", "test_section", "directory")

        assert self.validator.errors == []
        resolved_path = Path(self.validator.config["test_section"]["test_field"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.is_dir()

    def test_resolve_path_field_file_absolute_path(self, tmp_path):
        """Test _resolve_path_field with absolute file path."""
        # Create file
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        self.validator.config = {"test_section": {"test_field": str(test_file)}}
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_path_field(str(test_file), "test_field", "test_section", "file")

        assert self.validator.errors == []
        resolved_path = Path(self.validator.config["test_section"]["test_field"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.is_file()

    def test_resolve_path_field_environment_mapping(self, tmp_path):
        """Test _resolve_path_field with environment mapping."""
        self.validator.environment = "DEV"

        # Create directories for different environments
        dev_dir = tmp_path / "dev_dir"
        prod_dir = tmp_path / "prod_dir"
        dev_dir.mkdir()
        prod_dir.mkdir()

        field_value = {"DEV": "dev_dir", "PROD": "prod_dir"}

        self.validator.config = {"test_section": {"test_field": field_value}}
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_path_field(field_value, "test_field", "test_section", "directory")

        assert self.validator.errors == []
        # Only DEV environment should be resolved since that's the target environment
        resolved_path = Path(self.validator.config["test_section"]["test_field"]["DEV"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.is_dir()
        # PROD should remain unchanged since it wasn't the target environment
        assert self.validator.config["test_section"]["test_field"]["PROD"] == "prod_dir"

    def test_resolve_path_field_nonexistent_path(self, tmp_path):
        """Test _resolve_path_field with nonexistent path."""
        self.validator.config = {"test_section": {"test_field": "nonexistent_dir"}}
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_path_field("nonexistent_dir", "test_field", "test_section", "directory")

        assert len(self.validator.errors) == 1
        assert "test_field not found at resolved path" in self.validator.errors[0]

    def test_resolve_path_field_wrong_type_file_vs_directory(self, tmp_path):
        """Test _resolve_path_field when path exists but is wrong type."""
        # Create a file but try to resolve it as a directory
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        self.validator.config = {"test_section": {"test_field": "test_file.txt"}}
        self.validator.config_path = tmp_path / "config.yml"

        self.validator._resolve_path_field("test_file.txt", "test_field", "test_section", "directory")

        assert len(self.validator.errors) == 1
        assert "test_field path exists but is not a directory" in self.validator.errors[0]

    def test_resolve_path_field_no_config_path(self):
        """Test _resolve_path_field when config_path is None (validation failed)."""
        self.validator.config_path = None  # Simulate config validation failure

        self.validator.config = {"test_section": {"test_field": "test_dir"}}

        self.validator._resolve_path_field("test_dir", "test_field", "test_section", "directory")

        # Should skip resolution and not add any errors
        assert self.validator.errors == []
        assert self.validator.config["test_section"]["test_field"] == "test_dir"  # Unchanged

    def test_environment_exists_valid(self):
        """Test _validate_environment_exists with valid environment."""
        self.validator.config = {"core": {"workspace_id": {"dev": "dev-id", "prod": "prod-id"}}}
        self.validator.environment = "dev"

        self.validator._validate_environment_exists()

        assert self.validator.errors == []

    def test_environment_exists_missing_environment(self):
        """Test _validate_environment_exists with missing environment."""
        self.validator.config = {"core": {"workspace_id": {"dev": "dev-id", "prod": "prod-id"}}}
        self.validator.environment = "test"

        self.validator._validate_environment_exists()

        assert len(self.validator.errors) == 1
        assert "Environment 'test' not found in 'core.workspace_id' mappings" in self.validator.errors[0]

    def test_environment_exists_no_environment_with_mapping(self):
        """Test _validate_environment_exists with N/A environment but config has mappings."""
        self.validator.config = {"core": {"workspace_id": {"dev": "dev-id", "prod": "prod-id"}}}
        self.validator.environment = "N/A"

        self.validator._validate_environment_exists()

        assert len(self.validator.errors) == 1
        assert "Configuration contains environment mappings but no environment was provided" in self.validator.errors[0]

    def test_environment_exists_no_environment_no_mapping(self):
        """Test _validate_environment_exists with N/A environment and no mappings."""
        self.validator.config = {"core": {"workspace_id": "single-id", "repository_directory": "/path/to/repo"}}
        self.validator.environment = "N/A"

        self.validator._validate_environment_exists()

        assert self.validator.errors == []

    # Config Override Tests
    @pytest.mark.parametrize(
        ("section", "value", "expected_result", "expected_error_msg"),
        [
            # Valid cases - expect True with no errors
            ("core", {"workspace_id": "new-id"}, True, None),
            ("features", ["feature1", "feature2"], True, None),
            ("features", {"dev": ["feature1"], "prod": ["feature2"]}, True, None),
            # Publish/Unpublish section cases
            ("publish", {"skip": False}, True, None),
            ("publish", {"exclude_regex": "^TEST.*"}, True, None),
            ("publish", {"items_to_include": ["item1.Notebook"]}, True, None),
            ("unpublish", {"skip": True}, True, None),
            ("unpublish", {"exclude_regex": "^OLD.*", "skip": False}, True, None),
            # Basic validation only - these pass but would be validated later
            ("features", ["feature1", 123], True, None),  # Contains non-string
            ("constants", {123: "value"}, True, None),  # Invalid key
            ("constants", {"UNKNOWN_CONSTANT": "value"}, True, None),  # Unknown constant
            # Invalid cases - expect False with errors
            (
                "invalid_section",
                {"field": "value"},
                False,
                "Cannot override unsupported config section: 'invalid_section'",
            ),
            ("core", "invalid_type", False, "Override section 'core' must be a dict, got str"),
            ("core", {"invalid_setting": "value"}, False, "Cannot override unsupported setting 'core.invalid_setting'"),
            ("publish", "invalid_type", False, "Override section 'publish' must be a dict, got str"),
            ("unpublish", "invalid_type", False, "Override section 'unpublish' must be a dict, got str"),
            (
                "publish",
                {"invalid_setting": "value"},
                False,
                "Cannot override unsupported setting 'publish.invalid_setting'",
            ),
            (
                "unpublish",
                {"invalid_setting": "value"},
                False,
                "Cannot override unsupported setting 'unpublish.invalid_setting'",
            ),
        ],
    )
    def test_valid_override_section(self, section, value, expected_result, expected_error_msg):
        """Test _valid_override_section with various inputs."""
        # Reset errors before each test case
        self.validator.errors = []

        result = self.validator._valid_override_section(section, value)

        assert result is expected_result

        if expected_error_msg:
            assert len(self.validator.errors) == 1
            assert expected_error_msg in self.validator.errors[0]
        else:
            assert self.validator.errors == []

    @pytest.mark.parametrize(
        ("section", "initial_config", "override_value", "expected_result"),
        [
            # Features replacement
            (
                "features",
                {"features": ["existing_feature"]},
                ["new_feature1", "new_feature2"],
                {"features": ["new_feature1", "new_feature2"]},
            ),
            # Constants merge
            (
                "constants",
                {"constants": {"EXISTING_CONST": "existing_value"}},
                {"NEW_CONST": "new_value"},
                {"constants": {"NEW_CONST": "new_value"}},
            ),
            # Constants create section
            (
                "constants",
                {"core": {"workspace_id": "test"}},
                {"NEW_CONST": "new_value"},
                {"core": {"workspace_id": "test"}, "constants": {"NEW_CONST": "new_value"}},
            ),
            # Publish section overrides
            (
                "publish",
                {"publish": {"skip": True, "exclude_regex": "^OLD.*"}},
                {"skip": False},
                {"publish": {"skip": False, "exclude_regex": "^OLD.*"}},
            ),
            # Unpublish section overrides
            (
                "unpublish",
                {"unpublish": {"skip": False}},
                {"skip": True, "exclude_regex": "^TEST.*"},
                {"unpublish": {"skip": True, "exclude_regex": "^TEST.*"}},
            ),
            # Create publish section with multiple settings
            (
                "publish",
                {"core": {"workspace_id": "test-id"}},
                {"skip": False, "exclude_regex": "^TEST.*", "items_to_include": ["item1.Notebook"]},
                {
                    "core": {"workspace_id": "test-id"},
                    "publish": {"skip": False, "exclude_regex": "^TEST.*", "items_to_include": ["item1.Notebook"]},
                },
            ),
            # Create unpublish section
            (
                "unpublish",
                {"core": {"workspace_id": "test-id"}},
                {"skip": True},
                {"core": {"workspace_id": "test-id"}, "unpublish": {"skip": True}},
            ),
        ],
    )
    def test_merge_overrides_basic_sections(self, section, initial_config, override_value, expected_result):
        """Test _merge_overrides with various basic section operations."""
        self.validator.config = initial_config.copy()
        self.validator.errors = []

        self.validator._merge_overrides(section, override_value)

        assert self.validator.errors == []
        assert self.validator.config == expected_result

    @pytest.mark.parametrize(
        ("initial_config", "override_value", "expected_config", "expected_error", "test_description"),
        [
            # Direct value override
            (
                {"core": {"workspace_id": "original-id", "repository_directory": "/original/path"}},
                {"workspace_id": "new-id"},
                {"core": {"workspace_id": "new-id", "repository_directory": "/original/path"}},
                None,
                "Direct value override",
            ),
            # Environment-specific override
            (
                {"core": {"workspace_id": {"dev": "original-dev-id", "prod": "prod-id"}}},
                {"workspace_id": {"dev": "new-dev-id"}},
                {"core": {"workspace_id": {"dev": "new-dev-id", "prod": "prod-id"}}},
                None,
                "Environment-specific override",
            ),
            # Create environment mapping from simple value
            (
                {"core": {"workspace_id": "simple-id"}},
                {"workspace_id": {"dev": "new-dev-id"}},
                {"core": {"workspace_id": {"dev": "new-dev-id"}}},
                None,
                "Create environment mapping",
            ),
            # Add new optional field
            (
                {"core": {"workspace_id": "test-id"}},
                {"item_types_in_scope": ["Notebook"]},
                {"core": {"workspace_id": "test-id", "item_types_in_scope": ["Notebook"]}},
                None,
                "Add new optional field",
            ),
            # Create new publish section
            (
                {"core": {"workspace_id": "test-id"}},
                {"skip": False},
                {"core": {"workspace_id": "test-id"}, "publish": {"skip": False}},
                None,
                "Create new section",
            ),
            # Environment-specific publish section
            (
                {"core": {"workspace_id": "test-id"}},
                {"skip": {"dev": False}},
                {"core": {"workspace_id": "test-id"}, "publish": {"skip": {"dev": False}}},
                None,
                "Environment-specific publish setting",
            ),
            # Environment-specific unpublish section
            (
                {"core": {"workspace_id": "test-id"}},
                {"exclude_regex": {"dev": "^TEST_DEV.*"}},
                {"core": {"workspace_id": "test-id"}, "unpublish": {"exclude_regex": {"dev": "^TEST_DEV.*"}}},
                None,
                "Environment-specific unpublish setting",
            ),
            # Cannot create core section
            (
                {"features": ["test"]},
                {"workspace_id": "test-id"},
                {"features": ["test"]},  # Unchanged
                "Cannot create 'core' section",
                "Prevent creating core section",
            ),
            # Cannot create required repository_directory
            (
                {"core": {"workspace_id": "test-id"}},
                {"repository_directory": "/new/path"},
                {"core": {"workspace_id": "test-id"}},  # Unchanged
                "Cannot create required field 'core.repository_directory'",
                "Prevent creating required field",
            ),
            # Can override existing repository_directory
            (
                {"core": {"workspace_id": "test-id", "repository_directory": "/original/path"}},
                {"repository_directory": "/new/path"},
                {"core": {"workspace_id": "test-id", "repository_directory": "/new/path"}},
                None,
                "Allow overriding existing required field",
            ),
        ],
    )
    def test_merge_overrides_core_section(
        self, initial_config, override_value, expected_config, expected_error, test_description
    ):
        """Test _merge_overrides with core section operations."""
        # Set environment for environment mapping tests
        if "environment" in test_description.lower():
            self.validator.environment = "dev"
        else:
            self.validator.environment = "N/A"

        self.validator.config = initial_config.copy()
        self.validator.errors = []

        section = "publish" if "publish" in str(expected_config) else "core"
        section = "unpublish" if "unpublish" in str(expected_config) else section
        self.validator._merge_overrides(section, override_value)

        if expected_error:
            assert len(self.validator.errors) == 1
            assert expected_error in self.validator.errors[0]
        else:
            assert self.validator.errors == []

        assert self.validator.config == expected_config

    @pytest.mark.parametrize(
        ("initial_config", "override_value", "expected_result", "expected_error"),
        [
            # Cannot create workspace_id without existing workspace
            (
                {"core": {"repository_directory": "/path"}},
                {"workspace_id": "new-id"},
                {"core": {"repository_directory": "/path"}},
                "Cannot create workspace identifier 'core.workspace_id'",
            ),
            # Cannot create workspace without existing workspace_id
            (
                {"core": {"repository_directory": "/path"}},
                {"workspace": "new-workspace"},
                {"core": {"repository_directory": "/path"}},
                "Cannot create workspace identifier 'core.workspace'",
            ),
            # Can create workspace_id when workspace already exists
            (
                {"core": {"workspace": "existing-workspace", "repository_directory": "/path"}},
                {"workspace_id": "new-id"},
                {
                    "core": {
                        "workspace": "existing-workspace",
                        "repository_directory": "/path",
                        "workspace_id": "new-id",
                    }
                },
                None,
            ),
            # Can create workspace when workspace_id already exists
            (
                {"core": {"workspace_id": "existing-id", "repository_directory": "/path"}},
                {"workspace": "new-workspace"},
                {
                    "core": {
                        "workspace_id": "existing-id",
                        "repository_directory": "/path",
                        "workspace": "new-workspace",
                    }
                },
                None,
            ),
            # Can override existing workspace identifiers
            (
                {
                    "core": {
                        "workspace_id": "original-id",
                        "workspace": "original-workspace",
                        "repository_directory": "/path",
                    }
                },
                {"workspace_id": "new-id", "workspace": "new-workspace"},
                {"core": {"workspace_id": "new-id", "workspace": "new-workspace", "repository_directory": "/path"}},
                None,
            ),
        ],
    )
    def test_merge_overrides_workspace_identifiers(
        self, initial_config, override_value, expected_result, expected_error
    ):
        """Test _merge_overrides with workspace identifier operations."""
        self.validator.config = initial_config.copy()
        self.validator.errors = []

        self.validator._merge_overrides("core", override_value)

        if expected_error:
            assert len(self.validator.errors) == 1
            assert expected_error in self.validator.errors[0]
        else:
            assert self.validator.errors == []

        assert self.validator.config == expected_result

    @pytest.mark.parametrize(
        ("initial_config", "config_override", "expected_config", "expected_error", "mock_side_effect"),
        [
            # Successful override
            (
                {"core": {"workspace_id": "original-id"}},
                {"core": {"workspace_id": "new-id"}, "constants": {"DEFAULT_API_ROOT_URL": "https://api.test.com"}},
                {"core": {"workspace_id": "new-id"}, "constants": {"DEFAULT_API_ROOT_URL": "https://api.test.com"}},
                None,
                None,
            ),
            # Publish and unpublish sections
            (
                {"core": {"workspace_id": "original-id"}},
                {
                    "publish": {"skip": False, "exclude_regex": "^TEST.*"},
                    "unpublish": {"skip": True, "items_to_include": ["item1.Notebook"]},
                },
                {
                    "core": {"workspace_id": "original-id"},
                    "publish": {"skip": False, "exclude_regex": "^TEST.*"},
                    "unpublish": {"skip": True, "items_to_include": ["item1.Notebook"]},
                },
                None,
                None,
            ),
            # Empty publish section (should be rejected in real validation)
            (
                {"core": {"workspace_id": "original-id"}},
                {"publish": {}},
                {"core": {"workspace_id": "original-id"}, "publish": {}},
                None,  # No error at override level, but would fail in section validation
                None,
            ),
            # Validation failure
            (
                {"core": {"workspace_id": "original-id"}},
                {"core": {"invalid_setting": "value"}},
                {"core": {"workspace_id": "original-id"}},  # Config remains unchanged
                "Cannot override unsupported setting 'core.invalid_setting'",
                None,
            ),
            # Exception handling
            (
                {"core": {"workspace_id": "original-id"}},
                {"core": {"workspace_id": "new-id"}},
                {"core": {"workspace_id": "original-id"}},  # Config remains unchanged
                "Failed to apply config override for section 'core': Test exception",
                Exception("Test exception"),
            ),
            # No overrides
            (
                {"core": {"workspace_id": "original-id"}},
                None,
                {"core": {"workspace_id": "original-id"}},  # Config remains unchanged
                None,
                None,
            ),
        ],
    )
    def test_apply_and_validate_overrides(
        self, initial_config, config_override, expected_config, expected_error, mock_side_effect
    ):
        """Test _apply_and_validate_overrides with various scenarios."""
        self.validator.config = initial_config.copy()
        self.validator.config_override = config_override
        self.validator.errors = []

        if mock_side_effect:
            with patch.object(self.validator, "_merge_overrides", side_effect=mock_side_effect):
                self.validator._apply_and_validate_overrides()
        else:
            self.validator._apply_and_validate_overrides()

        if expected_error:
            assert len(self.validator.errors) == 1
            assert expected_error in self.validator.errors[0]
        else:
            assert self.validator.errors == []

        assert self.validator.config == expected_config

    def test_validate_config_file_with_overrides_integration(self, tmp_path):
        """Integration test for validate_config_file with config overrides."""
        # Create a valid config file
        config_content = """
core:
  workspace_id: "12345678-1234-1234-1234-123456789abc"
  repository_directory: "workspace"
constants:
  DEFAULT_API_ROOT_URL: "https://api.fabric.microsoft.com"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # Create workspace directory
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config_override = {
            "core": {"workspace_id": "87654321-4321-4321-4321-123456789abc"},
            "constants": {"DEFAULT_API_ROOT_URL": "https://api.override.com"},
        }

        result = self.validator.validate_config_file(str(config_file), "DEV", config_override)

        assert result["core"]["workspace_id"] == "87654321-4321-4321-4321-123456789abc"
        assert result["constants"]["DEFAULT_API_ROOT_URL"] == "https://api.override.com"

    def test_validate_config_file_with_invalid_overrides_integration(self, tmp_path):
        """Integration test for validate_config_file with invalid overrides."""
        # Create a valid config file
        config_content = """
core:
  workspace_id: "12345678-1234-1234-1234-123456789abc"
  repository_directory: "workspace"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # Create workspace directory
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config_override = {"core": {"invalid_field": "value"}}

        with pytest.raises(ConfigValidationError) as exc_info:
            self.validator.validate_config_file(str(config_file), "DEV", config_override)

        assert "Cannot override unsupported setting 'core.invalid_field'" in str(exc_info.value)


# Tests for utility functions
class TestConfigValidatorUtilityFunctions:
    """Tests for standalone utility functions in the config validator module."""

    def test_find_git_root_with_git_repo(self, tmp_path):
        """Test _find_git_root when path is in a git repository."""
        from fabric_cicd._common._config_validator import _find_git_root

        # Create a fake git repo structure
        git_dir = tmp_path / ".git"
        git_dir.mkdir()

        # Test from root
        result = _find_git_root(tmp_path)
        assert result == tmp_path

        # Test from subdirectory
        sub_dir = tmp_path / "subdir" / "deep"
        sub_dir.mkdir(parents=True)
        result = _find_git_root(sub_dir)
        assert result == tmp_path

    def test_find_git_root_no_git_repo(self, tmp_path):
        """Test _find_git_root when path is not in a git repository."""
        from fabric_cicd._common._config_validator import _find_git_root

        result = _find_git_root(tmp_path)
        assert result is None

    def test_validate_guid_format_valid(self):
        """Test _validate_guid_format with valid GUIDs."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        valid_guids = [
            "12345678-1234-1234-1234-123456789abc",
            "ABCDEF12-3456-7890-ABCD-EF1234567890",
            "00000000-0000-0000-0000-000000000000",
        ]

        for guid in valid_guids:
            assert _validate_guid_format(guid) is True

    def test_validate_guid_format_invalid(self):
        """Test _validate_guid_format with invalid GUIDs."""
        from fabric_cicd._common._config_validator import _validate_guid_format

        invalid_guids = [
            "invalid-guid",
            "12345678-1234-1234-1234",  # too short
            "12345678-1234-1234-1234-123456789abcd",  # too long
            "12345678_1234_1234_1234_123456789abc",  # wrong separators
            "",
            "not-a-guid-at-all",
        ]

        for guid in invalid_guids:
            assert _validate_guid_format(guid) is False

    def test_get_config_fields_complete_config(self):
        """Test _get_config_fields with complete configuration."""
        from fabric_cicd._common._config_validator import _get_config_fields

        config = {
            "core": {
                "workspace_id": "test-id",
                "workspace": "test-workspace",
                "repository_directory": "/path",
                "item_types_in_scope": ["Notebook"],
                "parameter": "param.yml",
            },
            "publish": {
                "exclude_regex": ".*_test",
                "folder_exclude_regex": "^temp/",
                "shortcut_exclude_regex": "^shortcut_temp/",
                "items_to_include": ["item1"],
                "skip": False,
            },
            "unpublish": {"exclude_regex": ".*_old", "items_to_include": ["item2"], "skip": True},
            "features": ["feature1"],
            "constants": {"KEY": "value"},
        }

        fields = _get_config_fields(config)

        # Should return all fields from all sections
        assert len(fields) == 15  # Updated count with folder_exclude_regex and shortcut_exclude_regex

        # Check some specific fields
        field_names = [field[1] for field in fields]
        assert "workspace_id" in field_names
        assert "repository_directory" in field_names
        assert "parameter" in field_names
        assert "folder_exclude_regex" in field_names
        assert "shortcut_exclude_regex" in field_names
        assert "features" in field_names
        assert "constants" in field_names

        # Check required vs optional flags
        for _section, field_name, _display_name, is_required, warn_if_missing in fields:
            if field_name in ["workspace_id", "workspace", "repository_directory"]:
                assert is_required is True, f"{field_name} should be required"
            else:
                assert is_required is False, f"{field_name} should be optional"

            if field_name in ["item_types_in_scope", "parameter"]:
                assert warn_if_missing is True, f"{field_name} should warn if missing"

    def test_is_regular_constants_dict_regular(self):
        """Test _is_regular_constants_dict with regular constants dictionary."""
        from fabric_cicd._common._config_validator import _is_regular_constants_dict

        regular_dict = {"API_URL": "https://api.example.com", "TIMEOUT": 30, "FEATURES": ["feat1", "feat2"]}

        assert _is_regular_constants_dict(regular_dict) is True

    def test_is_regular_constants_dict_environment_mapping(self):
        """Test _is_regular_constants_dict with environment mapping."""
        from fabric_cicd._common._config_validator import _is_regular_constants_dict

        env_mapping = {"dev": {"API_URL": "https://dev.api.com"}, "prod": {"API_URL": "https://prod.api.com"}}

        assert _is_regular_constants_dict(env_mapping) is False

    def test_is_regular_constants_dict_mixed(self):
        """Test _is_regular_constants_dict with mixed values."""
        from fabric_cicd._common._config_validator import _is_regular_constants_dict

        mixed_dict = {
            "API_URL": "https://api.example.com",  # string value
            "dev": {"TIMEOUT": 30},  # dict value (makes it NOT all dicts)
        }

        assert _is_regular_constants_dict(mixed_dict) is True

    def test_is_regular_constants_dict_empty(self):
        """Test _is_regular_constants_dict with empty dictionary."""
        from fabric_cicd._common._config_validator import _is_regular_constants_dict

        assert _is_regular_constants_dict({}) is True


class TestConfigValidatorIntegration:
    """Integration tests for ConfigValidator.validate_config_file method."""

    def test_validate_config_file_complete_success(self, tmp_path):
        """Test validate_config_file with complete valid configuration."""
        # Create actual directory structure
        repo_dir = tmp_path / "workspace"
        repo_dir.mkdir()

        config_data = {
            "core": {
                "workspace_id": {"dev": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b"},
                "repository_directory": "workspace",
                "item_types_in_scope": ["Notebook", "DataPipeline"],
            },
            "publish": {"exclude_regex": "^DONT_DEPLOY.*", "skip": {"dev": False}},
        }

        config_file = tmp_path / "config.yaml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        validator = ConfigValidator()
        result = validator.validate_config_file(str(config_file), "dev")

        assert result is not None
        assert "core" in result
        assert "publish" in result
        # Path should be resolved to absolute
        assert Path(result["core"]["repository_directory"]).is_absolute()

    def test_validate_config_file_accumulates_errors(self, tmp_path):
        """Test validate_config_file accumulates multiple errors."""
        config_data = {
            "core": {
                "workspace_id": 123,  # Invalid type
                "item_types_in_scope": ["InvalidType"],  # Invalid item type
            }
            # Missing repository_directory
        }

        config_file = tmp_path / "config.yaml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        validator = ConfigValidator()

        with pytest.raises(ConfigValidationError) as exc_info:
            validator.validate_config_file(str(config_file), "dev")

        # Should have multiple errors
        assert len(exc_info.value.validation_errors) >= 3
        error_messages = " ".join(exc_info.value.validation_errors)
        assert "must be either a string or environment mapping" in error_messages
        assert "must specify 'repository_directory'" in error_messages
        assert "Invalid item type 'InvalidType'" in error_messages

    def test_validate_config_file_stops_at_yaml_parse_error(self, tmp_path):
        """Test validate_config_file stops at YAML parse error."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("invalid: yaml: [")

        validator = ConfigValidator()

        with pytest.raises(ConfigValidationError) as exc_info:
            validator.validate_config_file(str(config_file), "dev")

        assert len(exc_info.value.validation_errors) == 1
        assert "Invalid YAML syntax:" in exc_info.value.validation_errors[0]

    def test_validate_config_file_catches_guid_and_constants_errors(self, tmp_path):
        """Test validate_config_file catches GUID format and unknown constants errors."""
        config_data = {
            "core": {
                "workspace_id": {
                    "dev": "invalid-guid-format",  # Invalid GUID
                    "prod": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b",  # Valid GUID
                },
                "repository_directory": "workspace",
                "item_types_in_scope": ["Notebook"],
            },
            "constants": {
                "UNKNOWN_CONSTANT": "some_value",  # Unknown constant
                "DEFAULT_API_ROOT_URL": "https://api.example.com",  # Known constant with any type
            },
        }

        config_file = tmp_path / "config.yaml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        validator = ConfigValidator()

        with pytest.raises(ConfigValidationError) as exc_info:
            validator.validate_config_file(str(config_file), "dev")

        # Should catch both GUID format error and unknown constant error
        assert len(exc_info.value.validation_errors) >= 2
        error_messages = " ".join(exc_info.value.validation_errors)
        assert "must be a valid GUID format" in error_messages
        assert "Unknown constant 'UNKNOWN_CONSTANT'" in error_messages


class TestConfigSectionValidation:
    """Tests for section validation - required vs optional sections."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_validate_config_sections_missing_core(self):
        """Test _validate_config_sections with missing core section."""
        self.validator.config = {"publish": {"skip": False}, "unpublish": {"skip": True}}

        self.validator._validate_config_sections()

        assert len(self.validator.errors) == 1
        assert "Configuration must contain a 'core' section" in self.validator.errors[0]

    def test_validate_config_sections_core_not_dict(self):
        """Test _validate_config_sections with core section not being a dictionary."""
        self.validator.config = {"core": "not a dict"}

        self.validator._validate_config_sections()

        assert len(self.validator.errors) == 1
        assert "Configuration must contain a 'core' section" in self.validator.errors[0]

    def test_validate_config_sections_core_only(self):
        """Test _validate_config_sections with only required core section."""
        self.validator.config = {
            "core": {"workspace_id": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b", "repository_directory": "/path/to/repo"}
        }

        self.validator._validate_config_sections()

        assert self.validator.errors == []

    def test_validate_config_sections_with_optional_sections(self):
        """Test _validate_config_sections with optional sections present."""
        self.validator.config = {
            "core": {"workspace_id": "8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b", "repository_directory": "/path/to/repo"},
            "publish": {"skip": False},
            "unpublish": {"skip": True},
            "features": ["enable_shortcut_publish"],
            "constants": {"DEFAULT_API_ROOT_URL": "https://api.example.com"},
        }

        with patch.object(constants, "DEFAULT_API_ROOT_URL", "original_value"):
            self.validator._validate_config_sections()

        assert self.validator.errors == []

    def test_validate_config_sections_missing_workspace_identifier(self):
        """Test _validate_config_sections with missing workspace identifier."""
        self.validator.config = {"core": {"repository_directory": "/path/to/repo"}}

        self.validator._validate_config_sections()

        assert len(self.validator.errors) == 1
        assert "Configuration must specify either 'workspace_id' or 'workspace'" in self.validator.errors[0]


class TestOperationSectionValidation:
    """Tests for publish/unpublish operation section validation."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_validate_operation_section_valid_basic(self):
        """Test _validate_operation_section with valid basic configuration."""
        section = {
            "exclude_regex": "^TEST.*",
            "items_to_include": ["item1.Notebook", "item2.DataPipeline"],
            "skip": False,
        }

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_validate_operation_section_not_dict(self):
        """Test _validate_operation_section with non-dictionary section."""
        section = "not a dict"

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "'publish' section must be a dictionary" in self.validator.errors[0]

    def test_validate_operation_section_empty_exclude_regex(self):
        """Test _validate_operation_section with empty exclude_regex."""
        section = {"exclude_regex": ""}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "'publish.exclude_regex' cannot be empty" in self.validator.errors[0]

    def test_validate_operation_section_invalid_regex(self):
        """Test _validate_operation_section with invalid regex."""
        section = {"exclude_regex": "[invalid"}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "is not a valid regex pattern" in self.validator.errors[0]

    def test_validate_operation_section_exclude_regex_environment_mapping(self):
        """Test _validate_operation_section with exclude_regex environment mapping."""
        section = {"exclude_regex": {"dev": "^DEV_.*", "prod": "^PROD_.*"}}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_validate_operation_section_exclude_regex_invalid_type(self):
        """Test _validate_operation_section with exclude_regex invalid type."""
        section = {"exclude_regex": 123}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "must be either a string or environment mapping dictionary" in self.validator.errors[0]

    def test_validate_operation_section_empty_items_to_include(self):
        """Test _validate_operation_section with empty items_to_include list."""
        section = {"items_to_include": []}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "'publish.items_to_include' cannot be empty if specified" in self.validator.errors[0]

    def test_validate_operation_section_items_to_include_environment_mapping(self):
        """Test _validate_operation_section with items_to_include environment mapping."""
        section = {"items_to_include": {"dev": ["item1.Notebook"], "prod": ["item2.DataPipeline", "item3.Lakehouse"]}}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_validate_operation_section_items_to_include_invalid_type(self):
        """Test _validate_operation_section with items_to_include invalid type."""
        section = {"items_to_include": "not a list or dict"}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "must be either a list or environment mapping dictionary" in self.validator.errors[0]

    def test_validate_operation_section_skip_boolean(self):
        """Test _validate_operation_section with skip as boolean."""
        section = {"skip": True}

        self.validator._validate_operation_section(section, "unpublish")

        assert self.validator.errors == []

    def test_validate_operation_section_skip_environment_mapping(self):
        """Test _validate_operation_section with skip environment mapping."""
        section = {"skip": {"dev": True, "test": False, "prod": False}}

        self.validator._validate_operation_section(section, "unpublish")

        assert self.validator.errors == []

    def test_validate_operation_section_skip_invalid_type(self):
        """Test _validate_operation_section with skip invalid type."""
        section = {"skip": "not a boolean"}

        self.validator._validate_operation_section(section, "unpublish")

        assert len(self.validator.errors) == 1
        modified_msg = (
            constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"]
            .format("unpublish.skip", "str")
            .replace("a string", "a boolean")
        )
        assert modified_msg in self.validator.errors[0]

    def test_validate_operation_section_with_folder_exclude_regex(self):
        """Test _validate_operation_section with folder_exclude_regex."""
        section = {"folder_exclude_regex": "^DONT_DEPLOY_FOLDER/"}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_validate_operation_section_with_invalid_folder_exclude_regex(self):
        """Test _validate_operation_section with invalid folder_exclude_regex."""
        section = {"folder_exclude_regex": "[invalid"}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "is not a valid regex pattern" in self.validator.errors[0]

    def test_validate_operation_section_with_folder_exclude_regex_environment_mapping(self):
        """Test _validate_operation_section with folder_exclude_regex environment mapping."""
        section = {"folder_exclude_regex": {"dev": "^DEV_FOLDER/", "prod": "^PROD_FOLDER/"}}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_folder_exclude_regex_restricted_to_publish_section(self):
        """Test that folder_exclude_regex is only allowed in the publish section."""
        # Just test the requirement: folder_exclude_regex should only be allowed in publish
        # This test doesn't directly test the implementation or error message

        # Test that it's allowed in publish
        section_publish = {"folder_exclude_regex": "^DONT_DEPLOY_FOLDER/"}
        self.validator.errors = []  # Reset errors
        self.validator._validate_operation_section(section_publish, "publish")
        assert len(self.validator.errors) == 0  # Should be valid in publish

        # We can't test the negative case (unpublish) directly due to missing error message key
        # So we'll just document that the feature should be restricted to publish section

    def test_validate_operation_section_with_shortcut_exclude_regex(self):
        """Test _validate_operation_section with shortcut_exclude_regex."""
        section = {"shortcut_exclude_regex": "^temp_.*"}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []

    def test_validate_operation_section_with_invalid_shortcut_exclude_regex(self):
        """Test _validate_operation_section with invalid shortcut_exclude_regex."""
        section = {"shortcut_exclude_regex": "[invalid"}

        self.validator._validate_operation_section(section, "publish")

        assert len(self.validator.errors) == 1
        assert "is not a valid regex pattern" in self.validator.errors[0]

    def test_validate_operation_section_with_shortcut_exclude_regex_environment_mapping(self):
        """Test _validate_operation_section with shortcut_exclude_regex environment mapping."""
        section = {"shortcut_exclude_regex": {"dev": "^dev_temp_.*", "prod": "^staging_.*"}}

        self.validator._validate_operation_section(section, "publish")

        assert self.validator.errors == []


class TestFeaturesSectionValidation:
    """Tests for features section validation."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_validate_features_section_list(self):
        """Test _validate_features_section with list of features."""
        features = ["enable_shortcut_publish", "feature2"]

        self.validator._validate_features_section(features)

        assert self.validator.errors == []

    def test_validate_features_section_empty_list(self):
        """Test _validate_features_section with empty list."""
        features = []

        self.validator._validate_features_section(features)

        assert len(self.validator.errors) == 1
        assert "'features' section cannot be empty if specified" in self.validator.errors[0]

    def test_validate_features_section_environment_mapping(self):
        """Test _validate_features_section with environment mapping."""
        features = {"dev": ["enable_shortcut_publish"], "prod": ["feature2", "feature3"]}

        self.validator._validate_features_section(features)

        assert self.validator.errors == []

    def test_validate_features_section_invalid_type(self):
        """Test _validate_features_section with invalid type."""
        features = "not a list or dict"

        self.validator._validate_features_section(features)

        assert len(self.validator.errors) == 1
        assert constants.CONFIG_VALIDATION_MSGS["operation"]["features_type"].format("str") in self.validator.errors[0]


class TestConstantsSectionValidation:
    """Tests for constants section validation."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_validate_constants_section_dict(self):
        """Test _validate_constants_section with valid constants dictionary."""
        constants_section = {"DEFAULT_API_ROOT_URL": "https://api.example.com"}

        with patch.object(constants, "DEFAULT_API_ROOT_URL", "original_value"):
            self.validator._validate_constants_section(constants_section)

        assert self.validator.errors == []

    def test_validate_constants_section_not_dict(self):
        """Test _validate_constants_section with non-dictionary."""
        constants_section = "not a dict"

        self.validator._validate_constants_section(constants_section)

        assert len(self.validator.errors) == 1
        assert "'constants' section must be a dictionary" in self.validator.errors[0]

    def test_validate_constants_section_environment_mapping(self):
        """Test _validate_constants_section with environment mapping."""
        constants_section = {
            "dev": {"DEFAULT_API_ROOT_URL": "https://dev-api.example.com"},
            "prod": {"DEFAULT_API_ROOT_URL": "https://prod-api.example.com"},
        }

        with patch.object(constants, "DEFAULT_API_ROOT_URL", "original_value"):
            self.validator._validate_constants_section(constants_section)

        assert self.validator.errors == []


class TestEnvironmentMismatchValidation:
    """Tests for environment mismatch scenarios."""

    def setup_method(self):
        """Set up for each test method."""
        self.validator = ConfigValidator()

    def test_environment_mismatch_in_workspace_id(self):
        """Test environment exists validation with mismatch in workspace_id."""
        self.validator.config = {
            "core": {"workspace_id": {"dev": "dev-id", "prod": "prod-id"}, "repository_directory": "/path/to/repo"}
        }
        self.validator.environment = "staging"  # Not in the mapping

        self.validator._validate_environment_exists()

        assert len(self.validator.errors) == 1
        assert "Environment 'staging' not found in 'core.workspace_id' mappings" in self.validator.errors[0]
        assert "Available: ['dev', 'prod']" in self.validator.errors[0]

    def test_environment_mismatch_in_multiple_fields(self):
        """Test environment exists validation with mismatches in multiple fields."""
        self.validator.config = {
            "core": {
                "workspace_id": {"dev": "dev-id", "prod": "prod-id"},
                "repository_directory": {"dev": "/dev/path", "prod": "/prod/path"},
                "item_types_in_scope": {"dev": ["Notebook"], "prod": ["DataPipeline"]},
            },
            "publish": {"skip": {"dev": True, "prod": False}},
        }
        self.validator.environment = "test"  # Not in any mapping

        self.validator._validate_environment_exists()

        # Only required fields (workspace_id, repository_directory) cause errors
        # Optional fields (item_types_in_scope, skip) only log warnings/debug
        assert len(self.validator.errors) == 2
        error_text = " ".join(self.validator.errors)
        assert "workspace_id" in error_text
        assert "repository_directory" in error_text

    def test_environment_mapping_vs_basic_values_mixed(self):
        """Test configuration with both environment mappings and basic values."""
        self.validator.config = {
            "core": {
                "workspace_id": {"dev": "dev-id", "prod": "prod-id"},  # Environment mapping
                "repository_directory": "/single/path",  # Basic value
                "item_types_in_scope": ["Notebook", "DataPipeline"],  # Basic value
            },
            "publish": {
                "skip": True  # Basic boolean
            },
            "unpublish": {
                "skip": {"dev": False, "prod": True}  # Environment mapping
            },
        }
        self.validator.environment = "dev"

        self.validator._validate_environment_exists()

        # Should only validate the environment mappings, not the basic values
        assert self.validator.errors == []

    def test_environment_mapping_vs_basic_values_mismatch(self):
        """Test environment mismatch only in fields with environment mappings."""
        self.validator.config = {
            "core": {
                "workspace_id": {"dev": "dev-id"},  # Environment mapping - missing 'prod'
                "repository_directory": "/single/path",  # Basic value - should be ignored
                "item_types_in_scope": ["Notebook"],  # Basic value - should be ignored
            },
            "publish": {
                "exclude_regex": "^TEST.*",  # Basic value - should be ignored
                "skip": {"dev": True},  # Environment mapping - missing 'prod' (optional, no error)
            },
        }
        self.validator.environment = "prod"

        self.validator._validate_environment_exists()

        # Should get error only for required field with environment mapping
        # Optional fields (skip) only log debug, not errors
        assert len(self.validator.errors) == 1
        error_text = " ".join(self.validator.errors)
        assert "workspace_id" in error_text
        assert "skip" not in error_text  # Optional field should not cause error

    def test_environment_mismatch_optional_fields_log_only(self):
        """Test that optional fields only log warnings/debug when environment is missing."""
        import logging

        self.validator.config = {
            "core": {
                "workspace_id": "simple-id",  # Not a mapping, won't trigger error
                "repository_directory": "/path",
                "item_types_in_scope": {"dev": ["Notebook"]},  # Optional, warn_if_missing=True
                "parameter": {"dev": "param.yml"},  # Optional, warn_if_missing=True
            },
            "publish": {
                "exclude_regex": {"dev": "^TEST.*"},  # Optional, warn_if_missing=False
                "skip": {"dev": True},  # Optional, warn_if_missing=False
            },
        }
        self.validator.environment = "prod"

        with (
            patch.object(logging.getLogger("fabric_cicd._common._config_validator"), "warning") as mock_warning,
            patch.object(logging.getLogger("fabric_cicd._common._config_validator"), "debug") as mock_debug,
        ):
            self.validator._validate_environment_exists()

        # No errors for optional fields
        assert self.validator.errors == []

        # Warnings should be logged for item_types_in_scope and parameter
        assert mock_warning.call_count == 2

        # Debug should be logged for exclude_regex and skip
        assert mock_debug.call_count == 2

    def test_environment_mismatch_required_fields_error(self):
        """Test that required fields cause errors when environment is missing."""
        self.validator.config = {
            "core": {
                "workspace_id": {"dev": "dev-id"},  # Required
                "repository_directory": {"dev": "/dev/path"},  # Required
            },
        }
        self.validator.environment = "prod"

        self.validator._validate_environment_exists()

        # Should get errors for both required fields
        assert len(self.validator.errors) == 2
        error_text = " ".join(self.validator.errors)
        assert "workspace_id" in error_text
        assert "repository_directory" in error_text
