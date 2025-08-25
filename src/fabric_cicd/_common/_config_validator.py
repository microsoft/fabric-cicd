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
        self.errors: list = []
        self.config: dict = None
        self.config_path: Path = None
        self.environment: str = None

    def validate_config_file(self, config_file_path: str, environment: str) -> dict[str, Any]:
        """
        Validate configuration file and return parsed config if valid.

        Args:
            config_file_path: String path to the configuration file
            environment: The target environment for the deployment

        Returns:
            Parsed configuration dictionary

        Raises:
            ConfigValidationError: If validation fails
        """
        self.errors = []
        self.environment = environment

        # Step 1: Validate file existence and accessibility
        config_path = self._validate_file_existence(config_file_path)

        # Step 2: Validate file content and YAML syntax
        self.config = self._validate_yaml_content(config_path)

        # Step 3: Validate configuration structure and required fields
        if self.config is not None:
            self._validate_config_structure()
            self._validate_config_sections()

            # Step 4: Validate environment-specific mapping
            self._validate_environment_exists()

            # Step 5: Resolve paths after environment validation passes
            if not self.errors:
                self._resolve_repository_path()

        # If there are validation errors, raise them all at once
        if self.errors:
            raise ConfigValidationError(self.errors, logger)

        return self.config

    def _validate_file_existence(self, config_file_path: str) -> Path:
        """Validate file path and existence."""
        if not config_file_path or not isinstance(config_file_path, str):
            self.errors.append("Configuration file path must be a non-empty string")
            return None

        try:
            config_path = Path(config_file_path).resolve()
        except (OSError, RuntimeError) as e:
            self.errors.append(f"Invalid file path '{config_file_path}': {e}")
            return None

        if not config_path.exists():
            self.errors.append(f"Configuration file not found: {config_file_path}")
            return None

        if not config_path.is_file():
            self.errors.append(f"Path is not a file: {config_file_path}")
            return None

        self.config_path = config_path
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

    def _validate_config_sections(self) -> None:
        """Validate the configuration sections"""
        # Validate core section (required)
        if "core" not in self.config or not isinstance(self.config["core"], dict):
            self.errors.append("Configuration must contain a 'core' section")
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

        # Validate optional sections
        # publish section
        if "publish" in self.config:
            self._validate_operation_section(self.config["publish"], "publish")

        # unpublish section
        if "unpublish" in self.config:
            self._validate_operation_section(self.config["unpublish"], "unpublish")

        # features section
        if "features" in self.config:
            self._validate_features_section(self.config["features"])

        # constants section
        if "constants" in self.config:
            self._validate_constants_section(self.config["constants"])

    def _validate_environment_exists(self) -> None:
        """Validate that target environment exists in all environment mappings."""
        if self.environment == "N/A":
            # Handle no target environment case
            if any(
                field_name in section and isinstance(section[field_name], dict)
                for section, field_name, _ in _get_config_fields(self.config)
                if not (field_name == "constants" and _is_regular_constants_dict(section.get(field_name, {})))
            ):
                self.errors.append(
                    "Configuration contains environment mappings but no environment was provided. "
                    "Please specify an environment or remove environment mappings."
                )
            return

        # Check each field for target environment presence
        for section, field_name, display_name in _get_config_fields(self.config):
            if field_name in section:
                field_value = section[field_name]
                # Handle constants special case
                if field_name == "constants" and _is_regular_constants_dict(field_value):
                    continue

                # If it's a dict (environment mapping), check if target environment exists
                if isinstance(field_value, dict) and self.environment not in field_value:
                    available_envs = list(field_value.keys())
                    self.errors.append(
                        f"Environment '{self.environment}' not found in '{display_name}' mappings. Available: {available_envs}"
                    )

    def _validate_environment_mapping(self, field_value: dict, field_name: str, accepted_type: type) -> bool:
        """Validate field with environment mapping."""
        if not field_value:
            self.errors.append(f"'{field_name}' environment mapping cannot be empty")
            return False

        valid = True
        for env, value in field_value.items():
            # Validate environment key
            if not isinstance(env, str) or not env.strip():
                self.errors.append(f"Environment key in '{field_name}' must be a non-empty string")
                valid = False
                continue

            # Validate environment value type
            if not isinstance(value, accepted_type):
                self.errors.append(
                    f"'{field_name}' value for environment '{env}' must be a {accepted_type.__name__}, got {type(value).__name__}"
                )
                valid = False
                continue

            # Validate environment value content (type-specific)
            if accepted_type == str:
                if not value.strip():
                    self.errors.append(f"'{field_name}' value for environment '{env}' cannot be empty")
                    valid = False
            elif accepted_type == list and not value:
                self.errors.append(f"'{field_name}' value for environment '{env}' cannot be empty")
                valid = False

        return valid

    def _validate_workspace_field(self, core: dict[str, Any], field_name: str) -> bool:
        """Validate workspace_id or workspace field."""
        if field_name not in core:
            return False

        field_value = core[field_name]

        # Support both string values and environment mappings
        if isinstance(field_value, str):
            if not field_value.strip():
                self.errors.append(f"'{field_name}' cannot be empty")
                return False

            return self._validate_workspace_value(field_value, field_name, field_name)

        if isinstance(field_value, dict):
            valid = self._validate_environment_mapping(field_value, field_name, str)

            # Apply field-specific validation to each environment value
            if valid:
                for env, value in field_value.items():
                    if isinstance(value, str) and not self._validate_workspace_value(
                        value, field_name, f"{field_name}.{env}"
                    ):
                        valid = False

            return valid

        err_str = "workspace_name" if field_name == "workspace" else "workspace_id"
        self.errors.append(
            f"'{field_name}' must be either a string or environment mapping dictionary (e.g., {{dev: {err_str}, prod: {err_str}}}), got type {type(field_value).__name__}"
        )
        return False

    def _validate_workspace_value(self, value: str, field_name: str, context: str) -> bool:
        """Validate a workspace value (applies GUID validation for workspace_id)."""
        if field_name == "workspace_id" and not self._validate_guid_format(value):
            self.errors.append(f"'{context}' must be a valid GUID format: {value}")
            return False
        return True

    def _validate_repository_directory(self, core: dict[str, Any]) -> None:
        """Validate repository_directory field."""
        if "repository_directory" not in core:
            self.errors.append("Configuration must specify 'repository_directory' in core section")
            return

        repository_directory = core["repository_directory"]

        # Support both string values and environment mappings
        if isinstance(repository_directory, str):
            if not repository_directory.strip():
                self.errors.append("'repository_directory' cannot be empty")
                return

        elif isinstance(repository_directory, dict):
            if not self._validate_environment_mapping(repository_directory, "repository_directory", str):
                return

        else:
            self.errors.append(
                f"'repository_directory' must be either a string or environment mapping dictionary (e.g., {{dev: 'path/dev', prod: 'path/prod'}}), got type {type(repository_directory).__name__}"
            )
            return

    def _resolve_repository_path(self) -> None:
        """Resolve repository directory paths after environment validation."""
        core = self.config["core"]
        repository_directory = core["repository_directory"]

        # Prepare repo_dirs for path resolution
        if isinstance(repository_directory, str):
            repo_dirs = {"_default": repository_directory}
        else:  # isinstance(repository_directory, dict) - already validated
            repo_dirs = repository_directory

        # Resolve and validate paths
        if not self.config_path:
            logger.debug("Skipping repository directory path resolution due to config file validation failure")
            return

        # If environment mapping is used and target environment is provided, only process that environment
        if self.environment and self.environment != "N/A" and isinstance(repository_directory, dict):
            repo_dirs = {self.environment: repo_dirs[self.environment]}

        for env_key, repo_dir in repo_dirs.items():
            try:
                repo_path = Path(repo_dir)

                # Use absolute path if provided
                if repo_path.is_absolute():
                    resolved_repo_path = repo_path
                    logger.info(f"Using absolute repository directory path for {env_key}: '{resolved_repo_path}'")

                    # Validate the absolute path exists in the same repository as config file
                    config_repo_root = _find_git_root(self.config_path.parent)
                    items_repo_root = _find_git_root(resolved_repo_path)

                    if config_repo_root and items_repo_root and config_repo_root != items_repo_root:
                        env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""
                        self.errors.append(
                            f"Repository directory{env_desc} must be in the same git repository as the configuration file. "
                            f"Config repository: {config_repo_root}, Items repository: {items_repo_root}"
                        )
                        continue
                else:
                    # Resolve relative to config path location
                    config_dir = self.config_path.parent
                    resolved_repo_path = (config_dir / repo_dir).resolve()
                    env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""
                    logger.info(
                        f"Repository directory '{repo_dir}' resolved relative to config path{env_desc}: '{resolved_repo_path}'"
                    )

                # Validate the resolved directory exists
                if not resolved_repo_path.exists():
                    env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""
                    self.errors.append(
                        f"Repository directory not found at resolved path{env_desc}: '{resolved_repo_path}'"
                    )
                    continue

                if not resolved_repo_path.is_dir():
                    env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""
                    self.errors.append(
                        f"Repository path exists but is not a directory{env_desc}: '{resolved_repo_path}'"
                    )
                    continue

                # Store the resolved path back
                if isinstance(repository_directory, str):
                    core["repository_directory"] = str(resolved_repo_path)
                else:
                    core["repository_directory"][env_key] = str(resolved_repo_path)

            except (OSError, ValueError) as e:
                env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""
                self.errors.append(f"Invalid repository_directory path '{repo_dir}'{env_desc}: {e}")
                continue

    def _validate_item_types_in_scope(self, core: dict[str, Any]) -> None:
        """Validate item_types_in_scope field if present."""
        if "item_types_in_scope" not in core:
            return  # Optional field

        item_types = core["item_types_in_scope"]

        if isinstance(item_types, list):
            if not item_types:
                self.errors.append("'item_types_in_scope' cannot be empty if specified")
                return

            self._validate_item_types(item_types)
            return

        if isinstance(item_types, dict):
            # Validate environment mapping
            if not self._validate_environment_mapping(item_types, "item_types_in_scope", list):
                return

            # Validate each environment's item types
            for env, item_type_list in item_types.items():
                self._validate_item_types(item_type_list, env_context=env)
            return

        self.errors.append(
            f"'item_types_in_scope' must be either a list or environment mapping dictionary (e.g., {{dev: ['Notebook'], prod: ['DataPipeline']}}), got type {type(item_types).__name__}"
        )

    def _validate_item_types(self, item_types: list, env_context: Optional[str] = None) -> None:
        """Validate a list of item types."""
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
                if env_context:
                    msg = f"Invalid item type '{item_type}' in environment '{env_context}'. Available types: {available_types}"
                else:
                    msg = f"Invalid item type '{item_type}'. Available types: {available_types}"

                self.errors.append(msg)

    def _validate_operation_section(self, section: dict[str, Any], section_name: str) -> None:
        """Validate publish/unpublish section structure."""
        if not isinstance(section, dict):
            self.errors.append(f"'{section_name}' section must be a dictionary, got {type(section).__name__}")
            return

        # Validate exclude_regex if present
        if "exclude_regex" in section:
            exclude_regex = section["exclude_regex"]
            if isinstance(exclude_regex, str):
                if not exclude_regex.strip():
                    self.errors.append(f"'{section_name}.exclude_regex' cannot be empty")
                else:
                    self._validate_regex(exclude_regex, section_name)

            elif isinstance(exclude_regex, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(exclude_regex, f"{section_name}.exclude_regex", str):
                    return

                # Validate each environment's regex pattern
                for env, regex_pattern in exclude_regex.items():
                    if not regex_pattern.strip():
                        self.errors.append(f"'{section_name}.exclude_regex.{env}' cannot be empty")
                        continue

                    self._validate_regex(regex_pattern, f"{section_name}.exclude_regex.{env}")
            else:
                self.errors.append(
                    f"'{section_name}.exclude_regex' must be either a string or environment mapping dictionary (e.g., {{dev: 'pattern1', prod: 'pattern2'}}), got type {type(exclude_regex).__name__}"
                )

        # Validate items_to_include if present
        if "items_to_include" in section:
            items = section["items_to_include"]

            if isinstance(items, list):
                if not items:
                    self.errors.append(f"'{section_name}.items_to_include' cannot be empty if specified")
                else:
                    self._validate_items_list(items, f"{section_name}.items_to_include")

            elif isinstance(items, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(items, f"{section_name}.items_to_include", list):
                    return

                # Validate each environment's items list
                for env, items_list in items.items():
                    if not items_list:
                        self.errors.append(f"'{section_name}.items_to_include.{env}' cannot be empty if specified")
                        continue
                    self._validate_items_list(items_list, f"{section_name}.items_to_include.{env}")

            else:
                self.errors.append(
                    f"'{section_name}.items_to_include' must be either a list or environment mapping dictionary (e.g., {{dev: ['item1'], prod: ['item2']}}), got type {type(items).__name__}"
                )

        # Validate skip if present
        if "skip" in section:
            skip_value = section["skip"]

            if isinstance(skip_value, bool):
                # Single boolean value
                return

            if isinstance(skip_value, dict):
                # Use the reusable environment mapping validation
                if not self._validate_environment_mapping(skip_value, f"{section_name}.skip", bool):
                    return

            else:
                self.errors.append(
                    f"'{section_name}.skip' must be either a boolean or environment mapping dictionary (e.g., {{dev: true, prod: false}}), got type {type(skip_value).__name__}"
                )

    def _validate_regex(self, regex: str, section_name: str) -> None:
        """Validate regex value."""
        try:
            re.compile(regex)
        except re.error as e:
            self.errors.append(f"'{regex}' in {section_name} is not a valid regex pattern: {e}")

    def _validate_guid_format(self, guid: str) -> bool:
        """Validate GUID format using the pattern from constants."""
        return bool(re.match(constants.VALID_GUID_REGEX, guid))

    def _validate_items_list(self, items_list: list, context: str) -> None:
        """Validate a list of items with proper context for error messages."""
        for i, item in enumerate(items_list):
            if not isinstance(item, str):
                self.errors.append(f"'{context}[{i}]' must be a string, got {type(item).__name__}")
            elif not item.strip():
                self.errors.append(f"'{context}[{i}]' cannot be empty")

    def _validate_features_section(self, features: any) -> None:
        """Validate features section."""
        if isinstance(features, list):
            if not features:
                self.errors.append("'features' section cannot be empty if specified")
                return

            self._validate_features_list(features, "features")
            return

        if isinstance(features, dict):
            # Validate environment mapping
            if not self._validate_environment_mapping(features, "features", list):
                return

            # Validate each environment's features list
            for env, features_list in features.items():
                if not features_list:
                    self.errors.append(f"'features.{env}' cannot be empty if specified")
                    continue
                self._validate_features_list(features_list, f"features.{env}")
            return

        self.errors.append(
            f"'features' section must be either a list or environment mapping dictionary (e.g., {{dev: ['feature1'], prod: ['feature2']}}), got type {type(features).__name__}"
        )

    def _validate_features_list(self, features_list: list, context: str) -> None:
        """Validate a list of features with proper context for error messages."""
        for i, feature in enumerate(features_list):
            if not isinstance(feature, str):
                self.errors.append(f"'{context}[{i}]' must be a string, got {type(feature).__name__}")
            elif not feature.strip():
                self.errors.append(f"'{context}[{i}]' cannot be empty")

    def _validate_constants_section(self, constants_section: any) -> None:
        """Validate constants section."""
        if not isinstance(constants_section, dict):
            self.errors.append(f"'constants' section must be a dictionary, got {type(constants_section).__name__}")
            return

        # Check if all values are dictionaries (contains environment mapping)
        if constants_section and all(isinstance(value, dict) for value in constants_section.values()):
            # Validate environment mapping
            if not self._validate_environment_mapping(constants_section, "constants", dict):
                return

            # Validate each environment's constants dictionary
            for env, env_constants in constants_section.items():
                if not env_constants:
                    self.errors.append(f"'constants.{env}' cannot be empty if specified")
                    continue
                self._validate_constants_dict(env_constants, f"constants.{env}")
        else:
            # Simple constants dictionary
            self._validate_constants_dict(constants_section, "constants")

    def _validate_constants_dict(self, constants_dict: dict, context: str) -> None:
        """Validate a constants dictionary with proper context for error messages."""
        for key, _ in constants_dict.items():
            if not isinstance(key, str) or not key.strip():
                self.errors.append(f"Constant key in '{context}' must be a non-empty string, got: {key}")
                continue

            # Validate that the constant exists in the constants module
            if not hasattr(constants, key):
                self.errors.append(
                    f"Unknown constant '{key}' in '{context}' - this constant does not exist in fabric_cicd.constants"
                )


def _get_config_fields(config: dict) -> list[tuple[dict, str, str]]:
    """Get list of all fields that support environment mappings."""
    return [
        # Core section fields
        (config.get("core", {}), "workspace_id", "core.workspace_id"),
        (config.get("core", {}), "workspace", "core.workspace"),
        (config.get("core", {}), "repository_directory", "core.repository_directory"),
        (config.get("core", {}), "item_types_in_scope", "core.item_types_in_scope"),
        # Publish section fields
        (config.get("publish", {}), "exclude_regex", "publish.exclude_regex"),
        (config.get("publish", {}), "items_to_include", "publish.items_to_include"),
        (config.get("publish", {}), "skip", "publish.skip"),
        # Unpublish section fields
        (config.get("unpublish", {}), "exclude_regex", "unpublish.exclude_regex"),
        (config.get("unpublish", {}), "items_to_include", "unpublish.items_to_include"),
        (config.get("unpublish", {}), "skip", "unpublish.skip"),
        # Top-level sections
        (config, "features", "features"),
        (config, "constants", "constants"),
    ]


def _is_regular_constants_dict(constants_value: dict) -> bool:
    """Check if constants section is a regular dict (not environment mapping)."""
    if not isinstance(constants_value, dict) or not constants_value:
        return True
    # Environment mapping if ALL values are dicts, regular dict otherwise
    return not all(isinstance(value, dict) for value in constants_value.values())


def _find_git_root(path: Path) -> Optional[Path]:
    """Find the git repository root for a given path."""
    current = path.resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None
