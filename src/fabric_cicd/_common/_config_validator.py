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
        self.config_override: Optional[dict] = None

    def validate_config_file(
        self, config_file_path: str, environment: str, config_override: Optional[dict] = None
    ) -> dict[str, Any]:
        """
        Validate configuration file and return parsed config if valid.

        Args:
            config_file_path: String path to the configuration file
            environment: The target environment for the deployment
            config_override: Optional dictionary to override specific configuration values

        Returns:
            Parsed configuration dictionary (includes overrides, if any)

        Raises:
            ConfigValidationError: If validation fails
        """
        self.errors = []
        self.environment = environment
        self.config_override = config_override

        # Step 1: Validate file existence and accessibility
        config_path = self._validate_file_existence(config_file_path)

        # Step 2: Validate file content and YAML syntax
        self.config = self._validate_yaml_content(config_path)

        # Step 3: Apply and validate config overrides
        if self.config is not None and self.config_override is not None:
            self._apply_and_validate_overrides()

        # Step 4: Validate configuration structure and required fields
        if self.config is not None:
            self._validate_config_structure()
            self._validate_config_sections()

            # Step 5: Validate environment-specific mapping
            self._validate_environment_exists()

            # Step 6: Resolve paths after environment validation passes
            if not self.errors:
                self._resolve_repository_path()
                self._resolve_parameter_path()

        # If there are validation errors, raise them all at once
        if self.errors:
            raise ConfigValidationError(self.errors, logger)

        return self.config

    def _validate_file_existence(self, config_file_path: str) -> Path:
        """Validate file path and existence."""
        if not config_file_path or not isinstance(config_file_path, str):
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["path_empty"])
            return None

        try:
            config_path = Path(config_file_path).resolve()
        except (OSError, RuntimeError) as e:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["invalid_path"].format(config_file_path, e))
            return None

        if not config_path.exists():
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["not_found"].format(config_file_path))
            return None

        if not config_path.is_file():
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["not_file"].format(config_file_path))
            return None

        self.config_path = config_path
        return config_path

    def _validate_yaml_content(self, config_path: Optional[Path]) -> Optional[dict]:
        """Validate YAML syntax and basic structure."""
        if config_path is None:
            return None

        try:
            with config_path.open(encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["yaml_syntax"].format(e))
            return None
        except UnicodeDecodeError as e:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["encoding_error"].format(e))
            return None
        except PermissionError as e:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["permission_denied"].format(e))
            return None
        except Exception as e:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["unexpected_error"].format(e))
            return None

        # Handle empty file case
        if config is None:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["empty_file"])
            return None

        if not isinstance(config, dict):
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["file"]["not_dict"].format(type(config).__name__))
            return None

        return config

    def _apply_and_validate_overrides(self) -> None:
        """Apply and validate config overrides."""
        if not self.config_override:
            return

        for section, value in self.config_override.items():
            try:
                # Validate the section
                if not self._valid_override_section(section, value):
                    continue

                # Merge overrides into config
                self._merge_overrides(section, value)

            except Exception as e:
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["override"]["apply_failed"].format(section, e))

    def _valid_override_section(self, section: str, value: any) -> bool:
        """Validates the override section and structure are correct."""
        # Check section is supported
        if section not in constants.CONFIG_SECTIONS:
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["override"]["unsupported_section"].format(
                    section, list(constants.CONFIG_SECTIONS.keys())
                )
            )
            return False

        # Check type is valid
        expected_types = constants.CONFIG_SECTIONS[section]["type"]
        if not isinstance(value, expected_types):
            type_names = (
                " or ".join(t.__name__ for t in expected_types)
                if isinstance(expected_types, tuple)
                else expected_types.__name__
            )
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["override"]["wrong_type"].format(
                    section, type_names, type(value).__name__
                )
            )
            return False

        # Check setting is supported for applicable sections
        if isinstance(value, dict) and section in ["core", "publish", "unpublish"]:
            supported = constants.CONFIG_SECTIONS[section]["settings"]
            for setting in value:
                if setting not in supported:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["override"]["unsupported_setting"].format(
                            section, setting, supported
                        )
                    )
                    return False

        return True

    def _merge_overrides(self, section: str, value: dict | list) -> None:
        """Merge section and setting overrides into config file."""
        # Special handling for features and constants sections
        if section == "features":
            action = "added" if not self.config.get("features") else "updated"
            self.config["features"] = value
            logger.warning(constants.CONFIG_VALIDATION_MSGS["log"]["override_section"].format(action, section, value))
            return

        if section == "constants":
            action = "added" if "constants" not in self.config else "updated"
            self.config["constants"] = value
            logger.warning(constants.CONFIG_VALIDATION_MSGS["log"]["override_section"].format(action, section, value))
            return

        # Add section if it doesn't already exist (publish, unpublish only)
        if section not in self.config:
            if section == "core":
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["override"]["cannot_create_core"])
                return

            self.config[section] = {}
            logger.warning(constants.CONFIG_VALIDATION_MSGS["log"]["override_added_section"].format(section))

        # Process field by field for other sections (core, publish, unpublish)
        for setting, setting_value in value.items():
            exists = setting in self.config[section]

            # Validate required fields can only be overridden, not added
            if not exists and section == "core":
                if setting == "repository_directory":
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["override"]["cannot_create_required"].format(setting)
                    )
                    continue
                if setting in ["workspace_id", "workspace"]:
                    # Check if the other workspace identifier exists
                    other_workspace_field = "workspace" if setting == "workspace_id" else "workspace_id"
                    if other_workspace_field not in self.config[section]:
                        self.errors.append(
                            constants.CONFIG_VALIDATION_MSGS["override"]["cannot_create_workspace_id"].format(setting)
                        )
                        continue

            # Handle environment specific override
            if isinstance(setting_value, dict) and self.environment in setting_value:
                env_value = setting_value[self.environment]

                # Replace existing environment value with override value
                if exists and isinstance(self.config[section][setting], dict):
                    self.config[section][setting][self.environment] = env_value
                    logger.warning(
                        constants.CONFIG_VALIDATION_MSGS["log"]["override_env_specific"].format(
                            section, setting, self.environment, env_value
                        )
                    )

                # Otherwise, add new environment value
                else:
                    self.config[section][setting] = {self.environment: env_value}
                    logger.warning(
                        constants.CONFIG_VALIDATION_MSGS["log"]["override_env_mapping"].format(
                            section, setting, self.environment, env_value
                        )
                    )

            # Otherwise, handle direct value override
            else:
                self.config[section][setting] = setting_value
                action = "updated" if exists else "added"
                logger.warning(
                    constants.CONFIG_VALIDATION_MSGS["log"]["override_setting"].format(
                        action, section, setting, setting_value
                    )
                )

    def _validate_config_structure(self) -> None:
        """Validate top-level configuration structure."""
        if not isinstance(self.config, dict):
            return

        # Check for required top-level sections
        if "core" not in self.config:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["structure"]["missing_core"])
            return

        if not isinstance(self.config["core"], dict):
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["structure"]["core_not_dict"].format(
                    type(self.config["core"]).__name__
                )
            )

    def _validate_config_sections(self) -> None:
        """Validate the configuration sections"""
        # Validate core section (required)
        if "core" not in self.config or not isinstance(self.config["core"], dict):
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["structure"]["missing_core"])
            return

        core = self.config["core"]

        # Validate workspace identification (must have either workspace_id or workspace)
        has_workspace_id = self._validate_workspace_field(core, "workspace_id")
        has_workspace_name = self._validate_workspace_field(core, "workspace")

        if not has_workspace_id and not has_workspace_name:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["structure"]["missing_workspace_id"])

        # Validate repository_directory
        self._validate_repository_directory(core)

        # Validate optional item_types_in_scope
        self._validate_item_types_in_scope(core)

        # Validate optional parameter field
        self._validate_parameter_field(core)

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
                for section, field_name, _, _, _ in _get_config_fields(self.config)
                if not (field_name == "constants" and _is_regular_constants_dict(section.get(field_name, {})))
            ):
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["environment"]["no_env_with_mappings"])
            return

        # Check each field for target environment presence
        for section, field_name, display_name, is_required, log_warning in _get_config_fields(self.config):
            if field_name in section:
                field_value = section[field_name]
                # Handle constants special case
                if field_name == "constants" and _is_regular_constants_dict(field_value):
                    continue

                # If it's a dict (environment mapping), check if target environment exists
                if isinstance(field_value, dict) and self.environment not in field_value:
                    available_envs = list(field_value.keys())
                    msg = (
                        f"Environment '{self.environment}' not found in '{display_name}'. "
                        f"Available environments: {available_envs}. This setting will be skipped."
                    )

                    if is_required:
                        self.errors.append(
                            constants.CONFIG_VALIDATION_MSGS["environment"]["env_not_found"].format(
                                self.environment, display_name, available_envs
                            )
                        )
                    elif log_warning:
                        logger.warning(msg)
                    else:
                        logger.debug(msg)

    def _validate_environment_mapping(self, field_value: dict, field_name: str, accepted_type: type) -> bool:
        """Validate field with environment mapping."""
        if not field_value:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["environment"]["empty_mapping"].format(field_name))
            return False

        valid = True
        for env, value in field_value.items():
            # Validate environment key
            if not isinstance(env, str) or not env.strip():
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["environment"]["invalid_env_key"].format(
                        field_name, type(env).__name__
                    )
                )
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
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["environment"]["empty_env_value"].format(field_name, env)
                    )
                    valid = False
            elif accepted_type == list and not value:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["environment"]["empty_env_value"].format(field_name, env)
                )
                valid = False

        return valid

    def _validate_workspace_field(self, core: dict, field_name: str) -> bool:
        """Validate workspace_id or workspace field."""
        if field_name not in core:
            return False

        field_value = core[field_name]

        # Support both string values and environment mappings
        if isinstance(field_value, str):
            if not field_value.strip():
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["field"]["empty_value"].format(field_name))
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

        self.errors.append(
            constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(field_name, type(field_value).__name__)
        )
        return False

    def _validate_workspace_value(self, value: str, field_name: str, context: str) -> bool:
        """Validate a workspace value (applies GUID validation for workspace_id)."""
        if field_name == "workspace_id" and not _validate_guid_format(value):
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["field"]["invalid_guid"].format(context, value))
            return False
        return True

    def _validate_repository_directory(self, core: dict) -> None:
        """Validate repository_directory field."""
        if "repository_directory" not in core:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["structure"]["missing_repository_dir"])
            return

        repository_directory = core["repository_directory"]

        # Support both string values and environment mappings
        if isinstance(repository_directory, str):
            if not repository_directory.strip():
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["empty_value"].format("repository_directory")
                )
                return

        elif isinstance(repository_directory, dict):
            if not self._validate_environment_mapping(repository_directory, "repository_directory", str):
                return

        else:
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(
                    "repository_directory", type(repository_directory).__name__
                )
            )
            return

    def _validate_item_types_in_scope(self, core: dict[str, Any]) -> None:
        """Validate item_types_in_scope field if present."""
        if "item_types_in_scope" not in core:
            return  # Optional field

        item_types = core["item_types_in_scope"]

        if isinstance(item_types, list):
            if not item_types:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["empty_list"].format("item_types_in_scope")
                )
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
            constants.CONFIG_VALIDATION_MSGS["field"]["item_types_list_or_dict"].format(type(item_types).__name__)
        )

    def _validate_item_types(self, item_types: list, env_context: Optional[str] = None) -> None:
        """Validate a list of item types."""
        if not item_types:
            self.errors.append(constants.CONFIG_VALIDATION_MSGS["field"]["empty_list"].format("item_types_in_scope"))
            return

        # Validate each item type
        for item_type in item_types:
            if not isinstance(item_type, str):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["invalid_item_type"].format(
                        type(item_type).__name__, item_type
                    )
                )
                continue

            if item_type not in constants.ACCEPTED_ITEM_TYPES:
                available_types = ", ".join(sorted(constants.ACCEPTED_ITEM_TYPES))
                if env_context:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["field"]["unsupported_item_type_env"].format(
                            item_type, env_context, available_types
                        )
                    )
                else:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["field"]["unsupported_item_type"].format(
                            item_type, available_types
                        )
                    )

    def _validate_parameter_field(self, core: dict) -> None:
        """Validate parameter field if present."""
        if "parameter" not in core:
            return  # Optional field

        parameter_value = core["parameter"]

        # Support both string values and environment mappings
        if isinstance(parameter_value, str):
            if not parameter_value.strip():
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["field"]["empty_value"].format("parameter"))
                return
        elif isinstance(parameter_value, dict):
            if not self._validate_environment_mapping(parameter_value, "parameter", str):
                return
        else:
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(
                    "parameter", type(parameter_value).__name__
                )
            )

    def _resolve_path_field(
        self, field_value: str | dict, field_name: str, section_name: str, path_type: str = "directory"
    ) -> None:
        """Path resolution for configuration "path" fields (e.g, repository_directory, parameter)."""
        # Prepare paths for resolution
        paths_to_resolve = {"_default": field_value} if isinstance(field_value, str) else field_value

        # Skip resolution if config validation failed
        if not self.config_path:
            logger.debug(constants.CONFIG_VALIDATION_MSGS["path"]["skip"].format(field_name))
            return

        # If environment mapping is used and target environment is provided, only process that environment path
        if self.environment and self.environment != "N/A" and isinstance(field_value, dict):
            if self.environment not in paths_to_resolve:
                # Skip if environment not in mapping (for parameter field, which is optional)
                logger.debug(
                    f"Skipping path resolution for '{field_name}' - environment '{self.environment}' not in mapping"
                )
                return
            paths_to_resolve = {self.environment: paths_to_resolve[self.environment]}

        for env_key, path_str in paths_to_resolve.items():
            try:
                path = Path(path_str)
                env_desc = f" for environment '{env_key}'" if env_key != "_default" else ""

                if path.is_absolute():
                    resolved_path = path
                    logger.info(
                        constants.CONFIG_VALIDATION_MSGS["path"]["absolute"].format(field_name, env_desc, resolved_path)
                    )

                    # Validate absolute paths are in the same git repository as config file
                    config_repo_root = _find_git_root(self.config_path.parent)
                    path_repo_root = _find_git_root(resolved_path.parent if path_type == "file" else resolved_path)

                    if config_repo_root and path_repo_root and config_repo_root != path_repo_root:
                        self.errors.append(
                            constants.CONFIG_VALIDATION_MSGS["path"]["git_repo"].format(
                                field_name, env_desc, config_repo_root, field_name, path_repo_root
                            )
                        )
                        continue
                else:
                    # Resolve relative to config path location
                    config_dir = self.config_path.parent
                    resolved_path = (config_dir / path_str).resolve()
                    logger.info(
                        constants.CONFIG_VALIDATION_MSGS["path"]["resolved"].format(
                            field_name, path_str, env_desc, resolved_path
                        )
                    )

                # Validate the resolved path exists
                if not resolved_path.exists():
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["path"]["not_found"].format(
                            field_name, env_desc, resolved_path
                        )
                    )
                    continue

                # Path type-specific validation
                if path_type == "directory":
                    if not resolved_path.is_dir():
                        self.errors.append(
                            constants.CONFIG_VALIDATION_MSGS["path"]["not_directory"].format(
                                field_name, env_desc, resolved_path
                            )
                        )
                        continue

                elif path_type == "file" and not resolved_path.is_file():
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["path"]["not_file"].format(field_name, env_desc, resolved_path)
                    )
                    continue

                # Store the resolved path back in config
                if isinstance(field_value, str):
                    if section_name:
                        self.config[section_name][field_name] = str(resolved_path)
                    else:
                        self.config[field_name] = str(resolved_path)
                else:
                    if section_name:
                        self.config[section_name][field_name][env_key] = str(resolved_path)
                    else:
                        self.config[field_name][env_key] = str(resolved_path)

            except (OSError, ValueError) as e:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["path"]["invalid"].format(field_name, path_str, env_desc, e)
                )

    def _resolve_repository_path(self) -> None:
        """Resolve repository directory paths after environment validation."""
        core = self.config["core"]
        repository_directory = core["repository_directory"]
        self._resolve_path_field(repository_directory, "repository_directory", "core", "directory")

    def _resolve_parameter_path(self) -> None:
        """Resolve parameter file paths after environment validation."""
        core = self.config["core"]
        if "parameter" not in core:
            return  # Optional field

        parameter_value = core["parameter"]
        self._resolve_path_field(parameter_value, "parameter", "core", "file")

    def _validate_operation_section(self, section: dict[str, Any], section_name: str) -> None:
        """Validate publish/unpublish section structure."""
        if not isinstance(section, dict):
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["operation"]["not_dict"].format(section_name, type(section).__name__)
            )
            return

        # Validate exclude_regex if present
        if "exclude_regex" in section:
            exclude_regex = section["exclude_regex"]

            if isinstance(exclude_regex, str):
                if not exclude_regex.strip():
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_string"].format(
                            f"{section_name}.exclude_regex"
                        )
                    )
                else:
                    self._validate_regex(exclude_regex, f"{section_name}.exclude_regex")

            elif isinstance(exclude_regex, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(exclude_regex, f"{section_name}.exclude_regex", str):
                    return

                # Validate each environment's regex pattern
                for env, regex_pattern in exclude_regex.items():
                    self._validate_regex(regex_pattern, f"{section_name}.exclude_regex.{env}")

            else:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(
                        f"{section_name}.exclude_regex", type(exclude_regex).__name__
                    )
                )

        # Validate items_to_include if present
        if "items_to_include" in section:
            items = section["items_to_include"]

            if isinstance(items, list):
                if not items:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_list"].format(
                            f"{section_name}.items_to_include"
                        )
                    )
                else:
                    self._validate_items_list(items, f"{section_name}.items_to_include")

            elif isinstance(items, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(items, f"{section_name}.items_to_include", list):
                    return

                # Validate each environment's items list
                for env, items_list in items.items():
                    self._validate_items_list(items_list, f"{section_name}.items_to_include.{env}")

            else:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["list_or_dict"].format(
                        f"{section_name}.items_to_include", type(items).__name__
                    )
                )

        # Validate folder_exclude_regex if present (publish only)
        if "folder_exclude_regex" in section:
            if section_name != "publish":
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["unsupported_field"].format(
                        "folder_exclude_regex", section_name
                    )
                )

            folder_exclude_regex = section["folder_exclude_regex"]
            if isinstance(folder_exclude_regex, str):
                if not folder_exclude_regex.strip():
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_string"].format(
                            f"{section_name}.folder_exclude_regex"
                        )
                    )
                else:
                    self._validate_regex(folder_exclude_regex, f"{section_name}.folder_exclude_regex")

            elif isinstance(folder_exclude_regex, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(
                    folder_exclude_regex, f"{section_name}.folder_exclude_regex", str
                ):
                    return

                # Validate each environment's regex pattern
                for env, regex_pattern in folder_exclude_regex.items():
                    self._validate_regex(regex_pattern, f"{section_name}.folder_exclude_regex.{env}")

            else:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(
                        f"{section_name}.folder_exclude_regex", type(folder_exclude_regex).__name__
                    )
                )

        # Validate folder_path_to_include if present (publish only)
        if "folder_path_to_include" in section:
            if section_name != "publish":
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["unsupported_field"].format(
                        "folder_path_to_include", section_name
                    )
                )

            folders = section["folder_path_to_include"]
            if isinstance(folders, list):
                if not folders:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_list"].format(
                            f"{section_name}.folder_path_to_include"
                        )
                    )
                else:
                    self._validate_folders_list(folders, f"{section_name}.folder_path_to_include")

            elif isinstance(folders, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(folders, f"{section_name}.folder_path_to_include", list):
                    return

                # Validate each environment's folders list
                for env, folders_list in folders.items():
                    self._validate_folders_list(folders_list, f"{section_name}.folder_path_to_include.{env}")

            else:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["list_or_dict"].format(
                        f"{section_name}.folder_path_to_include", type(folders).__name__
                    )
                )

        # Validate shortcut_exclude_regex if present (publish only)
        if "shortcut_exclude_regex" in section:
            if section_name != "publish":
                self.errors.append(
                    f"'{section_name}.shortcut_exclude_regex' is not supported - shortcut exclusion is only available in the 'publish' section"
                )

            shortcut_exclude_regex = section["shortcut_exclude_regex"]
            if isinstance(shortcut_exclude_regex, str):
                if not shortcut_exclude_regex.strip():
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_string"].format(
                            f"{section_name}.shortcut_exclude_regex"
                        )
                    )
                else:
                    self._validate_regex(shortcut_exclude_regex, f"{section_name}.shortcut_exclude_regex")

            elif isinstance(shortcut_exclude_regex, dict):
                # Validate environment mapping
                if not self._validate_environment_mapping(
                    shortcut_exclude_regex, f"{section_name}.shortcut_exclude_regex", str
                ):
                    return

                # Validate each environment's regex pattern
                for env, regex_pattern in shortcut_exclude_regex.items():
                    self._validate_regex(regex_pattern, f"{section_name}.shortcut_exclude_regex.{env}")

            else:
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"].format(
                        f"{section_name}.shortcut_exclude_regex", type(shortcut_exclude_regex).__name__
                    )
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
                    constants.CONFIG_VALIDATION_MSGS["field"]["string_or_dict"]
                    .format(f"{section_name}.skip", type(skip_value).__name__)
                    .replace("a string", "a boolean")
                )

        # Validate mutual exclusivity of folder filtering options
        self._validate_mutually_exclusive_fields(
            section, "folder_exclude_regex", "folder_path_to_include", section_name
        )

    def _validate_regex(self, regex: str, section_name: str) -> None:
        """Validate regex value."""
        try:
            re.compile(regex)
        except re.error as e:
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["operation"]["invalid_regex"].format(regex, section_name, e)
            )

    def _validate_items_list(self, items_list: list, context: str) -> None:
        """Validate a list of items with proper context for error messages."""
        for i, item in enumerate(items_list):
            if not isinstance(item, str):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_type"].format(
                        context, i, type(item).__name__
                    )
                )
            elif not item.strip():
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_empty"].format(context, i))

    def _validate_folders_list(self, folders_list: list, context: str) -> None:
        """Validate a list of folder paths with proper context for error messages."""
        for i, folder in enumerate(folders_list):
            if not isinstance(folder, str):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_type"].format(
                        context, i, type(folder).__name__
                    )
                )
            elif not folder.strip():
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_empty"].format(context, i))
            elif not folder.startswith("/"):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["folders_list_prefix"].format(context, i, folder)
                )

    def _validate_mutually_exclusive_fields(self, section: dict, field1: str, field2: str, section_name: str) -> None:
        """Validate that two fields are not both specified for the same environment."""
        if field1 not in section or field2 not in section:
            return

        value1 = section[field1]
        value2 = section[field2]

        # Both are direct values (not environment-specific), throw error
        if not isinstance(value1, dict) and not isinstance(value2, dict):
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["operation"]["mutually_exclusive"].format(
                    f"{section_name}.{field1}", f"{section_name}.{field2}"
                )
            )
            return

        # Determine which environments each field contains (if they are environment mappings)
        value1_envs = set(value1.keys()) if isinstance(value1, dict) else set()
        value2_envs = set(value2.keys()) if isinstance(value2, dict) else set()

        # Determine if it is a direct value
        value1_is_direct = not isinstance(value1, dict)
        value2_is_direct = not isinstance(value2, dict)

        # Check if both fields would resolve for the target environment
        value1_applies = value1_is_direct or self.environment in value1_envs
        value2_applies = value2_is_direct or self.environment in value2_envs

        if value1_applies and value2_applies:
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["operation"]["mutually_exclusive_env"].format(
                    f"{section_name}.{field1}",
                    f"{section_name}.{field2}",
                    [self.environment],
                )
            )

    def _validate_features_section(self, features: any) -> None:
        """Validate features section."""
        if isinstance(features, list):
            if not features:
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["operation"]["empty_section"].format("features"))
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
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_section_env"].format("features", env)
                    )
                    continue
                self._validate_features_list(features_list, f"features.{env}")
            return

        self.errors.append(
            constants.CONFIG_VALIDATION_MSGS["operation"]["features_type"].format(type(features).__name__)
        )

    def _validate_features_list(self, features_list: list, context: str) -> None:
        """Validate a list of features with proper context for error messages."""
        for i, feature in enumerate(features_list):
            if not isinstance(feature, str):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_type"].format(
                        context, i, type(feature).__name__
                    )
                )
            elif not feature.strip():
                self.errors.append(constants.CONFIG_VALIDATION_MSGS["operation"]["list_entry_empty"].format(context, i))

    def _validate_constants_section(self, constants_section: any) -> None:
        """Validate constants section."""
        if not isinstance(constants_section, dict):
            self.errors.append(
                constants.CONFIG_VALIDATION_MSGS["operation"]["not_dict"].format(
                    "constants", type(constants_section).__name__
                )
            )
            return

        # Check if all values are dictionaries (contains environment mapping)
        if constants_section and all(isinstance(value, dict) for value in constants_section.values()):
            # Validate environment mapping
            if not self._validate_environment_mapping(constants_section, "constants", dict):
                return

            # Validate each environment's constants dictionary
            for env, env_constants in constants_section.items():
                if not env_constants:
                    self.errors.append(
                        constants.CONFIG_VALIDATION_MSGS["operation"]["empty_section_env"].format("constants", env)
                    )
                    continue
                self._validate_constants_dict(env_constants, f"constants.{env}")
        else:
            # Simple constants dictionary
            self._validate_constants_dict(constants_section, "constants")

    def _validate_constants_dict(self, constants_dict: dict, context: str) -> None:
        """Validate a constants dictionary with proper context for error messages."""
        for key, _ in constants_dict.items():
            if not isinstance(key, str) or not key.strip():
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["invalid_constant_key"].format(context, key)
                )
                continue

            # Validate that the constant exists in the constants module
            if not hasattr(constants, key):
                self.errors.append(
                    constants.CONFIG_VALIDATION_MSGS["operation"]["unknown_constant"].format(key, context)
                )


def _get_config_fields(config: dict) -> list[tuple[dict, str, str, bool, bool]]:
    """Get list of all fields that support environment mappings.

    Returns:
        List of tuples: (section_dict, field_name, display_name, is_required, log_warning)
        - is_required: If True, missing environment causes error.
        - log_warning: logging type (e.g., warning (True), debug (False)).
    """
    return [
        # Core section fields - required
        (config.get("core", {}), "workspace_id", "core.workspace_id", True, False),
        (config.get("core", {}), "workspace", "core.workspace", True, False),
        (config.get("core", {}), "repository_directory", "core.repository_directory", True, False),
        # Core section fields - optional but important (warn if missing)
        (config.get("core", {}), "item_types_in_scope", "core.item_types_in_scope", False, True),
        (config.get("core", {}), "parameter", "core.parameter", False, True),
        # Publish section fields - optional (debug if missing)
        (config.get("publish", {}), "exclude_regex", "publish.exclude_regex", False, False),
        (config.get("publish", {}), "folder_exclude_regex", "publish.folder_exclude_regex", False, False),
        (config.get("publish", {}), "folder_path_to_include", "publish.folder_path_to_include", False, False),
        (config.get("publish", {}), "shortcut_exclude_regex", "publish.shortcut_exclude_regex", False, False),
        (config.get("publish", {}), "items_to_include", "publish.items_to_include", False, False),
        (config.get("publish", {}), "skip", "publish.skip", False, False),
        # Unpublish section fields - optional (debug if missing)
        (config.get("unpublish", {}), "exclude_regex", "unpublish.exclude_regex", False, False),
        (config.get("unpublish", {}), "items_to_include", "unpublish.items_to_include", False, False),
        (config.get("unpublish", {}), "skip", "unpublish.skip", False, False),
        # Top-level sections - optional (debug if missing)
        (config, "features", "features", False, False),
        (config, "constants", "constants", False, False),
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


def _validate_guid_format(guid: str) -> bool:
    """Validate GUID format using the pattern from constants."""
    return bool(re.match(constants.VALID_GUID_REGEX, guid))
