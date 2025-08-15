# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration validation for YAML-based deployment configuration."""

import logging
import re
from pathlib import Path
from typing import Any, Optional

import yaml

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)


class ConfigValidationError(InputError):
    """Specific exception for configuration validation errors."""

    def __init__(self, errors: list[str], logger_instance: logging.Logger) -> None:
        """Initialize with list of validation errors."""
        self.validation_errors = errors
        error_msg = f"Configuration validation failed with {len(errors)} error(s):\n" + "\n".join(
            f"  - {error}" for error in errors
        )
        super().__init__(error_msg, logger_instance)


class ConfigValidator:
    """Validates YAML configuration files for fabric-cicd deployment."""

    def __init__(self) -> None:
        """Initialize the validator."""
        self.errors: list[str] = []
        self.config: Optional[dict[str, Any]] = None

    def validate_config_file(self, config_file: str) -> dict[str, Any]:
        """
        Validate configuration file and return parsed config if valid.

        Args:
            config_file: Path to the configuration file

        Returns:
            Parsed configuration dictionary

        Raises:
            ConfigValidationError: If validation fails
        """
        self.errors = []

        # Step 1: Validate file existence and accessibility
        config_path = self._validate_file_existence(config_file)

        # Step 2: Validate file content and YAML syntax
        self.config = self._validate_yaml_content(config_path)

        # Step 3: Validate configuration structure and required fields
        if self.config is not None:
            self._validate_config_structure()
            self._validate_core_section()
            self._validate_optional_sections()

        # If there are validation errors, raise them all at once
        if self.errors:
            raise ConfigValidationError(self.errors, logger)

        return self.config

    def _validate_file_existence(self, config_file: str) -> Path:
        """Validate file path and existence."""
        if not config_file or not isinstance(config_file, str):
            self.errors.append("Configuration file path must be a non-empty string")
            return None

        try:
            config_path = Path(config_file).resolve()
        except (OSError, RuntimeError) as e:
            self.errors.append(f"Invalid file path '{config_file}': {e}")
            return None

        if not config_path.exists():
            self.errors.append(f"Configuration file not found: {config_file}")
            return None

        if not config_path.is_file():
            self.errors.append(f"Path is not a file: {config_file}")
            return None

        return config_path

    def _validate_yaml_content(self, config_path: Optional[Path]) -> Optional[dict[str, Any]]:
        """Validate YAML syntax and basic structure."""
        if config_path is None:
            return None

        try:
            with config_path.open(encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            self.errors.append(f"Invalid YAML syntax: {e}")
            return None
        except UnicodeDecodeError as e:
            self.errors.append(f"File encoding error (expected UTF-8): {e}")
            return None
        except PermissionError as e:
            self.errors.append(f"Permission denied reading file: {e}")
            return None
        except Exception as e:
            self.errors.append(f"Unexpected error reading file: {e}")
            return None

        # Handle empty file case
        if config is None:
            self.errors.append("Configuration file is empty or contains only comments")
            return None

        if not isinstance(config, dict):
            self.errors.append(f"Configuration must be a YAML dictionary, got {type(config).__name__}")
            return None

        return config

    def _validate_config_structure(self) -> None:
        """Validate top-level configuration structure."""
        if not isinstance(self.config, dict):
            return

        # Check for required top-level sections
        if "core" not in self.config:
            self.errors.append("Configuration must contain a 'core' section")
            return

        if not isinstance(self.config["core"], dict):
            self.errors.append(f"'core' section must be a dictionary, got {type(self.config['core']).__name__}")

    def _validate_core_section(self) -> None:
        """Validate core configuration section."""
        if "core" not in self.config or not isinstance(self.config["core"], dict):
            return

        core = self.config["core"]

        # Validate workspace identification (must have either workspace_id or workspace)
        has_workspace_id = self._validate_workspace_field(core, "workspace_id")
        has_workspace_name = self._validate_workspace_field(core, "workspace")

        if not has_workspace_id and not has_workspace_name:
            self.errors.append("Configuration must specify either 'workspace_id' or 'workspace' in core section")

        # Validate repository_directory
        self._validate_repository_directory(core)

        # Validate optional item_types_in_scope
        self._validate_item_types_in_scope(core)

    def _validate_workspace_field(self, core: dict[str, Any], field_name: str) -> bool:
        """Validate workspace_id or workspace field."""
        if field_name not in core:
            return False

        field_value = core[field_name]

        # Must be environment mapping dictionary
        if not isinstance(field_value, dict):
            err_str = "workspace_name" if field_name == "workspace" else "workspace_id"
            self.errors.append(
                f"'{field_name}' must be an environment mapping dictionary (e.g., {{ppe: {err_str}, prod: {err_str}}}), got type {type(field_value).__name__}"
            )
            return False

        if not field_value:
            self.errors.append(f"'{field_name}' environment mapping cannot be empty")
            return False

        # Validate each environment mapping
        for env, workspace_value in field_value.items():
            if not isinstance(env, str) or not env.strip():
                self.errors.append(f"Environment key in '{field_name}' must be a non-empty string")
                continue

            if not isinstance(workspace_value, str) or not workspace_value.strip():
                self.errors.append(f"'{field_name}' value for environment '{env}' must be a non-empty string")

        return True

    def _validate_repository_directory(self, core: dict[str, Any]) -> None:
        """Validate repository_directory field."""
        if "repository_directory" not in core:
            self.errors.append("Configuration must specify 'repository_directory' in core section")
            return

        repo_dir = core["repository_directory"]
        if not isinstance(repo_dir, str):
            self.errors.append(f"'repository_directory' must be a string, got {type(repo_dir).__name__}")
            return

        if not repo_dir.strip():
            self.errors.append("'repository_directory' cannot be empty")

    def _validate_item_types_in_scope(self, core: dict[str, Any]) -> None:
        """Validate item_types_in_scope field if present."""
        if "item_types_in_scope" not in core:
            return  # Optional field

        item_types = core["item_types_in_scope"]

        if not isinstance(item_types, list):
            self.errors.append(f"'item_types_in_scope' must be a list, got {type(item_types).__name__}")
            return

        if not item_types:
            self.errors.append("'item_types_in_scope' cannot be empty if specified")
            return

        # Validate each item type
        for item_type in item_types:
            if not isinstance(item_type, str):
                self.errors.append(f"Item type must be a string, got {type(item_type).__name__}: {item_type}")
                continue

            if item_type not in constants.ACCEPTED_ITEM_TYPES:
                available_types = ", ".join(sorted(constants.ACCEPTED_ITEM_TYPES))
                self.errors.append(f"Invalid item type '{item_type}'. Available types: {available_types}")

    def _validate_optional_sections(self) -> None:
        """Validate optional configuration sections."""
        # Validate publish section
        if "publish" in self.config:
            self._validate_operation_section(self.config["publish"], "publish")

        # Validate unpublish section
        if "unpublish" in self.config:
            self._validate_operation_section(self.config["unpublish"], "unpublish")

        # Validate features section
        if "features" in self.config:
            self._validate_features_section(self.config["features"])

        # Validate constants section
        if "constants" in self.config:
            self._validate_constants_section(self.config["constants"])

    def _validate_operation_section(self, section: dict[str, Any], section_name: str) -> None:
        """Validate publish/unpublish section structure."""
        if not isinstance(section, dict):
            self.errors.append(f"'{section_name}' section must be a dictionary, got {type(section).__name__}")
            return

        # Validate exclude_regex if present
        if "exclude_regex" in section:
            exclude_regex = section["exclude_regex"]
            if not isinstance(exclude_regex, str):
                self.errors.append(
                    f"'{section_name}.exclude_regex' must be a string, got {type(exclude_regex).__name__}"
                )
            else:
                # Test if it's a valid regex
                try:
                    re.compile(exclude_regex)
                except re.error as e:
                    self.errors.append(f"'{section_name}.exclude_regex' is not a valid regex pattern: {e}")

        # Validate items_to_include if present
        if "items_to_include" in section:
            items = section["items_to_include"]
            if not isinstance(items, list):
                self.errors.append(f"'{section_name}.items_to_include' must be a list, got {type(items).__name__}")
            else:
                for i, item in enumerate(items):
                    if not isinstance(item, str):
                        self.errors.append(
                            f"'{section_name}.items_to_include[{i}]' must be a string, got {type(item).__name__}"
                        )
                    elif not item.strip():
                        self.errors.append(f"'{section_name}.items_to_include[{i}]' cannot be empty")

        # Validate skip if present
        if "skip" in section:
            skip_value = section["skip"]
            if isinstance(skip_value, dict):
                # Environment-specific skip mapping
                if not skip_value:
                    self.errors.append(f"'{section_name}.skip' environment mapping cannot be empty")
            else:
                for env, skip_env_value in skip_value.items():
                    if not isinstance(env, str) or not env.strip():
                        self.errors.append(f"Environment key in '{section_name}.skip' must be a non-empty string")
                    if not isinstance(skip_env_value, bool):
                        self.errors.append(
                            f"'{section_name}.skip.{env}' must be a boolean, got {type(skip_env_value).__name__}"
                        )
        else:
            self.errors.append(
                f"'{section_name}.skip' must be an environment mapping dictionary (e.g., {{ppe: true, prod: false}}), got {type(skip_value).__name__}"
            )

    def _validate_features_section(self, features: list[str]) -> None:
        """Validate features section."""
        if not isinstance(features, list):
            self.errors.append(f"'features' section must be a list, got {type(features).__name__}")
            return

        for i, feature in enumerate(features):
            if not isinstance(feature, str):
                self.errors.append(f"features[{i}] must be a string, got {type(feature).__name__}")
            elif not feature.strip():
                self.errors.append(f"features[{i}] cannot be empty")

    def _validate_constants_section(self, constants_section: dict[str, object]) -> None:
        """Validate constants section."""
        if not isinstance(constants_section, dict):
            self.errors.append(f"'constants' section must be a dictionary, got {type(constants_section).__name__}")
            return

        for key, _ in constants_section.items():
            if not isinstance(key, str) or not key.strip():
                self.errors.append(f"Constant key must be a non-empty string, got: {key}")

            # Validate that the constant exists in the constants module
            if not hasattr(constants, key):
                self.errors.append(f"Unknown constant '{key}' - this constant does not exist in fabric_cicd.constants")


def validate_config_for_environment(config: dict[str, Any], environment: str) -> None:
    """
    Validate that the config contains all necessary environment-specific values.

    Args:
        config: Parsed configuration dictionary
        environment: Target environment name

    Raises:
        ConfigValidationError: If environment-specific validation fails
    """
    validator = ConfigValidator()
    validator.config = config
    validator.errors = []

    if not isinstance(environment, str) or not environment.strip():
        validator.errors.append("Environment must be a non-empty string")
    else:
        environment = environment.strip()
        core = config.get("core", {})

        # Check if environment exists in workspace mappings
        if (
            "workspace_id" in core
            and isinstance(core["workspace_id"], dict)
            and environment not in core["workspace_id"]
        ):
            available_envs = list(core["workspace_id"].keys())
            validator.errors.append(
                f"Environment '{environment}' not found in workspace_id mappings. Available: {available_envs}"
            )

        if "workspace" in core and isinstance(core["workspace"], dict) and environment not in core["workspace"]:
            available_envs = list(core["workspace"].keys())
            validator.errors.append(
                f"Environment '{environment}' not found in workspace mappings. Available: {available_envs}"
            )

    if validator.errors:
        raise ConfigValidationError(validator.errors, logger)
