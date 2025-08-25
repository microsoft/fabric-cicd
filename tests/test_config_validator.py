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
        assert "Configuration file not found" in self.validator.errors[0]

    def test_validate_file_existence_empty_path(self):
        """Test _validate_file_existence with empty path."""
        result = self.validator._validate_file_existence("")

        assert result is None
        assert len(self.validator.errors) == 1
        assert "must be a non-empty string" in self.validator.errors[0]

    def test_validate_file_existence_none_path(self):
        """Test _validate_file_existence with None path."""
        result = self.validator._validate_file_existence(None)

        assert result is None
        assert len(self.validator.errors) == 1
        assert "must be a non-empty string" in self.validator.errors[0]

    def test_validate_file_existence_directory_instead_of_file(self, tmp_path):
        """Test _validate_file_existence with directory instead of file."""
        result = self.validator._validate_file_existence(str(tmp_path))

        assert result is None
        assert len(self.validator.errors) == 1
        assert "Path is not a file" in self.validator.errors[0]

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
        assert "Invalid YAML syntax:" in self.validator.errors[0]

    def test_validate_yaml_content_non_dict_yaml(self, tmp_path):
        """Test _validate_yaml_content with non-dictionary YAML."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("- item1\n- item2")

        self.validator.config_path = config_file
        result = self.validator._validate_yaml_content(config_file)

        assert result is None
        assert len(self.validator.errors) == 1
        assert "Configuration must be a YAML dictionary" in self.validator.errors[0]

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
        core = {"workspace_id": "test-id"}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is True
        assert self.validator.errors == []

    def test_validate_workspace_field_valid_dict(self):
        """Test _validate_workspace_field with valid environment mapping."""
        core = {"workspace_id": {"dev": "dev-id", "prod": "prod-id"}}

        result = self.validator._validate_workspace_field(core, "workspace_id")

        assert result is True
        assert self.validator.errors == []

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
        assert "Repository directory not found at resolved path" in self.validator.errors[0]

    def test_resolve_repository_path_file_instead_of_directory(self, tmp_path):
        """Test _resolve_repository_path with file instead of directory."""
        # Create a file instead of directory
        not_a_dir = tmp_path / "not_a_dir.txt"
        not_a_dir.write_text("content")

        self.validator.config = {"core": {"repository_directory": str(not_a_dir)}}
        self.validator.config_path = tmp_path / "config.yaml"

        self.validator._resolve_repository_path()

        assert len(self.validator.errors) == 1
        assert "Repository path exists but is not a directory" in self.validator.errors[0]

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


class TestConfigValidatorIntegration:
    """Integration tests for ConfigValidator.validate_config_file method."""

    def test_validate_config_file_complete_success(self, tmp_path):
        """Test validate_config_file with complete valid configuration."""
        # Create actual directory structure
        repo_dir = tmp_path / "workspace"
        repo_dir.mkdir()

        config_data = {
            "core": {
                "workspace_id": {"dev": "dev-id"},
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
        self.validator.config = {"core": {"workspace_id": "test-id", "repository_directory": "/path/to/repo"}}

        self.validator._validate_config_sections()

        assert self.validator.errors == []

    def test_validate_config_sections_with_optional_sections(self):
        """Test _validate_config_sections with optional sections present."""
        self.validator.config = {
            "core": {"workspace_id": "test-id", "repository_directory": "/path/to/repo"},
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
        assert "must be either a boolean or environment mapping dictionary" in self.validator.errors[0]


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
        assert "'features' section must be either a list or environment mapping dictionary" in self.validator.errors[0]


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

        # Should get multiple errors for each field that has environment mapping
        assert len(self.validator.errors) >= 3  # At least workspace_id, repository_directory, and item_types
        error_text = " ".join(self.validator.errors)
        assert "Environment 'test' not found" in error_text

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
                "skip": {"dev": True},  # Environment mapping - missing 'prod'
            },
        }
        self.validator.environment = "prod"

        self.validator._validate_environment_exists()

        # Should get errors only for fields with environment mappings
        assert len(self.validator.errors) == 2
        error_text = " ".join(self.validator.errors)
        assert "workspace_id" in error_text
        assert "skip" in error_text
        assert "repository_directory" not in error_text  # Basic value should not cause error
        assert "exclude_regex" not in error_text  # Basic value should not cause error
