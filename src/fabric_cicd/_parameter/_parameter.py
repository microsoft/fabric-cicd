# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the Parameter class to load and validate the parameter file used for deployment configurations."""

import json
import logging
import os
import re
from pathlib import Path
from typing import ClassVar, Optional

import yaml

import fabric_cicd.constants as constants
from fabric_cicd._parameter._utils import (
    is_valid_structure,
    process_input_path,
    replace_variables_in_parameter_file,
)

# Configure logging to output to the console
logger = logging.getLogger(__name__)


class Parameter:
    """A class to validate the parameter file."""

    PARAMETER_KEYS: ClassVar[dict] = {
        "find_replace": {
            "minimum": {"find_value", "replace_value"},
            "maximum": {"find_value", "replace_value", "is_regex", "item_type", "item_name", "file_path"},
        },
        "spark_pool": {
            "minimum": {"instance_pool_id", "replace_value"},
            "maximum": {"instance_pool_id", "replace_value", "item_name"},
        },
        "spark_pool_replace_value": {"type", "name"},
        "key_value_replace": {
            "minimum": {"find_key", "replace_value"},
            "maximum": {"find_key", "replace_value", "item_type", "item_name", "file_path"},
        },
        "gateway_binding": {
            "minimum": {"gateway_id", "dataset_name"},
            "maximum": {"gateway_id", "dataset_name"},
        },
        "semantic_model_binding": {
            "minimum": set(),
            "maximum": {"connection_id", "semantic_model_name", "default", "models"},
        },
        "extend": {"minimum": set(), "maximum": set()},
    }

    LOAD_ERROR_MSG = ""

    def __init__(
        self,
        repository_directory: Path,
        item_type_in_scope: list[str],
        environment: str,
        parameter_file_name: str = "parameter.yml",
        parameter_file_path: Optional[str] = None,
    ) -> None:
        """
        Initializes the Parameter instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
            parameter_file_name: The name of the parameter file, default is "parameter.yml".
            parameter_file_path: The path to the parameter file, if not using the default.
        """
        # Set class variables
        self.repository_directory = repository_directory
        self.item_type_in_scope = item_type_in_scope
        self.environment = environment
        self.parameter_file_name = parameter_file_name
        self.parameter_file_path = parameter_file_path

        self._set_parameter_file_path()
        self._refresh_parameter_file()

    def _set_parameter_file_path(self) -> None:
        """Set the parameter file path based on the provided path or default name."""
        is_param_path = False
        original_param_path = None

        # Determine which input to use for parameter file path
        if self.parameter_file_path and isinstance(self.parameter_file_path, str):
            original_param_path = self.parameter_file_path
            if self.parameter_file_name != "parameter.yml":
                is_param_path = True
                logger.warning(
                    constants.PARAMETER_MSGS["both_param_path_and_name"].format(
                        self.parameter_file_name, original_param_path
                    )
                )
            else:
                is_param_path = True

        try:
            # Resolve parameter file path, if provided
            if is_param_path and original_param_path:
                try:
                    param_path = Path(original_param_path)
                    # Handle relative path (must be relative to repository_directory)
                    if not param_path.is_absolute():
                        logger.debug(constants.PARAMETER_MSGS["resolving_relative_path"].format(original_param_path))
                        param_path = Path(self.repository_directory, original_param_path)

                    self.parameter_file_path = param_path.resolve()
                    logger.debug(constants.PARAMETER_MSGS["using_param_file_path"].format(self.parameter_file_path))

                except (TypeError, ValueError) as e:
                    logger.error(f"Error setting parameter file path: {e}")
                    is_param_path = False

            # Otherwise, resolve with default path
            if not is_param_path:
                self.parameter_file_path = Path(self.repository_directory, self.parameter_file_name).resolve()
                logger.debug(constants.PARAMETER_MSGS["using_default_param_file_path"].format(self.parameter_file_path))

        except Exception as e:
            logger.error(f"Unexpected error setting parameter file path: {e}")
            self.parameter_file_path = None

    def _refresh_parameter_file(self) -> None:
        """Load parameters if file is present."""
        self.environment_parameter = {}

        # Only proceed if the parameter file exists
        if self._validate_parameter_file_exists():
            is_valid, environment_parameter = self._validate_load_parameters_to_dict()
            if is_valid:
                self.environment_parameter = environment_parameter

    def _validate_parameter_file_exists(self) -> bool:
        """Validate the parameter file exists."""
        if self.parameter_file_path is None:
            return False

        return self.parameter_file_path.is_file()

    def _validate_load_parameters_to_dict(self) -> tuple[bool, dict]:
        """Validate loading the parameter file to a dictionary, including any templates."""
        parameter_dict = {}
        try:
            # Load the base parameter file
            with Path.open(self.parameter_file_path, encoding="utf-8") as yaml_file:
                yaml_content = yaml_file.read()
                yaml_content = replace_variables_in_parameter_file(yaml_content)
                validation_errors = self._validate_yaml_content(yaml_content)
                if validation_errors:
                    self.LOAD_ERROR_MSG = constants.PARAMETER_MSGS["invalid load"].format(validation_errors)
                    return False, parameter_dict

                parameter_dict = yaml.full_load(yaml_content) or {}
                logger.debug(constants.PARAMETER_MSGS["passed"].format("YAML content is valid"))

                # Process template parameter files if present
                if parameter_dict.get("extend"):
                    parameter_dict = self._process_template_parameters(parameter_dict)

                return True, parameter_dict
        except yaml.YAMLError as e:
            self.LOAD_ERROR_MSG = constants.PARAMETER_MSGS["invalid load"].format(e)
            return False, parameter_dict

    def _process_template_parameters(self, base_parameter_dict: dict) -> dict:
        """
        Process template parameter files and merge them with the base parameter dictionary.
        Template files are resolved relative to the main parameter file's location.
        """
        # Step 1: Check extend contains files
        if not isinstance(base_parameter_dict.get("extend"), list):
            logger.warning("No template parameter files specified under 'extend'")
            del base_parameter_dict["extend"]
            return base_parameter_dict

        template_files = base_parameter_dict["extend"]
        successful_templates = 0
        failed_templates = []
        processed_templates = set()

        # Step 2: Get the directory containing the main parameter file
        param_file_dir = self.parameter_file_path.parent

        # Step 3: Process each template file
        for param_file in template_files:
            try:
                # Check if this template file has been already processed to prevent duplication
                if param_file in processed_templates:
                    logger.warning(f"Skipping duplicate template parameter file reference: {param_file}")
                    continue

                # Step a: Resolve the path relative to the main parameter file's directory
                template_path = (param_file_dir / str(param_file)).resolve()

                # Check if the template file exists
                if not template_path.is_file():
                    error_msg = f"Template file not found: {param_file}"
                    failed_templates.append((param_file, error_msg))
                    continue

                # Step b: Load and validate the parameter file
                template_dict = self._load_template_parameter_file(template_path)
                if not template_dict:
                    continue

                # Step c: Check for nested templates
                if "extend" in template_dict:
                    error_msg = f"Nested templates are not supported in {param_file}"
                    failed_templates.append((param_file, error_msg))
                    continue

                # Step d: Merge the template dict with the base parameter dict
                base_parameter_dict = self._merge_template_dict(base_parameter_dict, template_dict)
                successful_templates += 1
                # Mark template parameter file as processed
                processed_templates.add(param_file)
                logger.debug(constants.PARAMETER_MSGS["template_file_loaded"].format(template_path))

            except Exception as e:
                error_msg = f"Error processing template file: {e!s}"
                failed_templates.append((param_file, error_msg))
                continue

        # Step 4: Log results
        if successful_templates > 0:
            logger.debug(constants.PARAMETER_MSGS["template_files_processed"].format(successful_templates))

        if failed_templates:
            for failed_file, reason in failed_templates:
                logger.error(f"Validation failed for template file: {failed_file}")
                logger.error(f"{reason}")
                logger.warning(
                    f"Template parameter '{failed_file}' content will not be included in the parameter dictionary"
                )
        elif successful_templates == 0:
            logger.warning(constants.PARAMETER_MSGS["template_files_none_valid"])

        # Step 5: Remove the extend key after processing
        del base_parameter_dict["extend"]
        return base_parameter_dict

    def _load_template_parameter_file(self, file_path: Path) -> dict:
        """Load and validate a template parameter file."""
        try:
            with Path.open(file_path, encoding="utf-8") as param_file:
                param_content = param_file.read()
                param_content = replace_variables_in_parameter_file(param_content)

                # Validate the content
                param_validation_errors = self._validate_yaml_content(param_content)
                if param_validation_errors:
                    logger.error(
                        constants.PARAMETER_MSGS["template_file_invalid"].format(file_path, param_validation_errors)
                    )
                    return {}

                # Load the content
                return yaml.full_load(param_content) or {}

        except yaml.YAMLError as e:
            logger.error(constants.PARAMETER_MSGS["template_file_error"].format(file_path, e))
            return {}

    def _merge_template_dict(self, base_dict: dict, template_dict: dict) -> dict:
        """
        Merge the template dictionary with the base dictionary, properly handling lists and nested structures.
        Preserves all entries, letting validation handle any issues later.
        """
        result = base_dict.copy()

        for key, template_value in template_dict.items():
            # Skip the 'extend' key as it's processed separately
            if key == "extend":
                continue

            # If the key doesn't exist in the base dict, just add it
            if key not in result:
                result[key] = template_value
                continue

            base_value = result[key]

            # Handle merging based on value types
            if isinstance(base_value, list) and isinstance(template_value, list):
                # For parameter lists like find_replace, append items from template
                result[key] = base_value + template_value

            elif isinstance(base_value, dict) and isinstance(template_value, dict):
                # For nested dictionaries, recursively merge them
                result[key] = self._merge_template_dict(base_value, template_value)

            else:
                # Add both values into a list for later validation
                result[key] = [base_value, template_value]
                logger.debug(f"Type mismatch for key '{key}': creating list of values for validation")

        return result

    def _validate_yaml_content(self, content: str) -> list[str]:
        """Validate the yaml content of the parameter file."""
        errors = []
        msgs = constants.PARAMETER_MSGS["invalid content"]

        # Check for empty YAML content
        if content.strip() == "":
            errors.append("YAML content is empty")
            return errors

        # Regex patterns to match all valid UTF-8 characters
        utf8_pattern = r"""
        (
        [\x00-\x7F] # Single-byte sequences (ASCII)
        | [\xC2-\xDF][\x80-\xBF] # Two-byte sequences
        | [\xE0][\xA0-\xBF][\x80-\xBF] # Three-byte sequences (special case)
        | [\xE1-\xEC][\x80-\xBF]{2} # Three-byte sequences
        | [\xED][\x80-\x9F][\x80-\xBF] # Three-byte sequences (special case)
        | [\xEE-\xEF][\x80-\xBF]{2} # Three-byte sequences
        | [\xF0][\x90-\xBF][\x80-\xBF]{2} # Four-byte sequences (special case)
        | [\xF1-\xF3][\x80-\xBF]{3} # Four-byte sequences
        | [\xF4][\x80-\x8F][\x80-\xBF]{2} # Four-byte sequences (special case)
        )
        """

        # Compile the pattern with the re.VERBOSE flag to allow comments and whitespace
        compiled_utf8_pattern = re.compile(utf8_pattern, re.VERBOSE)

        # Check for invalid characters (non-UTF-8)
        if not re.match(compiled_utf8_pattern, content):
            errors.append(msgs["char"])

        return errors

    def _validate_parameter_load(self) -> tuple[bool, str]:
        """Validate the parameter file load."""
        if self.parameter_file_path is None:
            return False, "not set"

        if not self.environment_parameter:
            # Check if the file exists
            if not self._validate_parameter_file_exists():
                logger.warning(constants.PARAMETER_MSGS["not found"].format(self.parameter_file_path))
                return False, "not found"
            logger.debug(constants.PARAMETER_MSGS["found"])
            return False, self.LOAD_ERROR_MSG

        return True, constants.PARAMETER_MSGS["valid load"]

    def _validate_parameter_file(self) -> bool:
        """Validate the parameter file."""
        # Handle gateway_binding deprecation
        if "gateway_binding" in self.environment_parameter or "semantic_model_binding" in self.environment_parameter:
            self._handle_gateway_binding_parameter()

        validation_steps = [
            ("parameter file load", self._validate_parameter_load),
            ("parameter names", self._validate_parameter_names),
            ("parameter file structure", self._validate_parameter_structure),
            ("find_replace parameter", lambda: self._validate_parameter("find_replace")),
            ("spark_pool parameter", lambda: self._validate_parameter("spark_pool")),
            ("key_value_replace parameter", lambda: self._validate_parameter("key_value_replace")),
            ("semantic_model_binding parameter", lambda: self._validate_parameter("semantic_model_binding")),
        ]
        for step, validation_func in validation_steps:
            logger.debug(constants.PARAMETER_MSGS["validating"].format(step))
            is_valid, msg = validation_func()
            if not is_valid:
                # Return True for specific not is_valid case
                if step == "parameter file load" and msg == "not found":
                    logger.warning(constants.PARAMETER_MSGS["terminate"].format(msg))
                    return True
                # Discontinue validation check for absent parameter
                if (
                    step
                    in (
                        "find_replace parameter",
                        "key_value_replace parameter",
                        "spark_pool parameter",
                        "semantic_model_binding parameter",
                    )
                    and msg == "parameter not found"
                ):
                    continue
                # Otherwise, return False with error message
                logger.error(constants.PARAMETER_MSGS["failed"].format(msg))
                return False
            logger.debug(constants.PARAMETER_MSGS["passed"].format(msg))

        # Return True if all validation steps pass
        logger.info(constants.PARAMETER_MSGS["validation_complete"])
        return True

    def _handle_gateway_binding_parameter(self) -> None:
        """This code will be removed in future releases, after deprecating 'gateway_binding' support."""
        # Extend semantic_model_binding by adding gateway_binding into one dict
        sm_param = self.environment_parameter.get("semantic_model_binding", [])
        gw_list = self.environment_parameter.get("gateway_binding", [])

        if gw_list:
            # Only merge gateway_binding if semantic_model_binding is legacy format (list) or doesn't exist
            if isinstance(sm_param, list):
                # Transform gateway entries to semantic_model_binding shape
                sm_param.extend(
                    {
                        "connection_id": entry.get("gateway_id"),
                        "semantic_model_name": entry.get("dataset_name", []),
                    }
                    for entry in gw_list
                )
                self.environment_parameter["semantic_model_binding"] = sm_param
            else:
                # New format (dict) - cannot merge gateway_binding, log warning
                logger.warning(
                    "Cannot merge 'gateway_binding' with new format 'semantic_model_binding'. "
                    "Please migrate gateway_binding entries to the new semantic_model_binding format."
                )

            # Remove original gateway_binding after processing
            del self.environment_parameter["gateway_binding"]
            logger.warning(constants.PARAMETER_MSGS["gateway_deprecated"])

    def _validate_parameter_structure(self) -> tuple[bool, str]:
        """Validate the parameter file structure."""
        if not is_valid_structure(self.environment_parameter):
            return False, constants.PARAMETER_MSGS["invalid structure"]

        return True, constants.PARAMETER_MSGS["valid structure"]

    def _validate_parameter_names(self) -> tuple[bool, str]:
        """Validate the parameter names in the parameter dictionary."""
        params = list(self.PARAMETER_KEYS.keys())[:6]
        for param in self.environment_parameter:
            if param not in params:
                return False, constants.PARAMETER_MSGS["invalid name"].format(param)

        return True, constants.PARAMETER_MSGS["valid name"]

    def _validate_parameter(self, param_name: str) -> tuple[bool, str]:
        """Validate the specific parameter and its contents."""
        if param_name not in self.environment_parameter:
            logger.debug(constants.PARAMETER_MSGS["param_not_found"].format(param_name))
            return False, "parameter not found"

        logger.debug(constants.PARAMETER_MSGS["param_found"].format(param_name))

        param_count = len(self.environment_parameter[param_name])
        multiple_param = param_count > 1
        if multiple_param:
            logger.debug(constants.PARAMETER_MSGS["param_count"].format(param_count, param_name))

        # Run a separate validation for semantic_model_binding parameter
        if param_name == "semantic_model_binding":
            return self._validate_semantic_model_binding_parameter(param_name, multiple_param)

        # Validation for other kinds of parameters
        validation_steps = [
            ("keys", lambda param_dict: self._validate_parameter_keys(param_name, list(param_dict.keys()))),
            ("required values", lambda param_dict: self._validate_required_values(param_name, param_dict)),
            ("replace_value", lambda param_dict: self._validate_replace_value(param_name, param_dict["replace_value"])),
            ("optional values", lambda param_dict: self._validate_optional_values(param_name, param_dict)),
        ]
        # Set the proper find_value key name based on the parameter
        if param_name == "key_value_replace":
            key_name = "find_key"
        elif param_name == "spark_pool":
            key_name = "instance_pool_id"
        else:
            key_name = "find_value"

        for param_num, parameter_dict in enumerate(self.environment_parameter[param_name], start=1):
            param_num_str = str(param_num) if multiple_param else ""
            find_value = parameter_dict.get(key_name)
            for step, validation_func in validation_steps:
                logger.debug(constants.PARAMETER_MSGS["validating"].format(f"{param_name} {param_num_str} {step}"))
                is_valid, msg = validation_func(parameter_dict)
                if not is_valid:
                    return False, msg
                logger.debug(constants.PARAMETER_MSGS["passed"].format(msg))

            # Check if replacement will be skipped for a given find value
            is_valid_env, env_type = self._validate_environment(parameter_dict["replace_value"])
            is_valid_optional_val, msg = self._validate_optional_values(param_name, parameter_dict, check_match=True)
            log_func = logger.debug if param_name == "key_value_replace" else logger.warning

            # Set value_type based on regex flag once
            value_type = (
                "find value regex"
                if (parameter_dict.get("is_regex") and parameter_dict["is_regex"].lower() == "true")
                else "find value"
            )

            if self.environment != "N/A" and not is_valid_env:
                if env_type.lower() == "_all_":
                    return False, constants.PARAMETER_MSGS["other target env"].format(
                        env_type, parameter_dict["replace_value"]
                    )

                skip_msg = constants.PARAMETER_MSGS["no target env"].format(self.environment, param_name)
                log_func(
                    constants.PARAMETER_MSGS["skip"].format(
                        value_type, find_value, skip_msg, param_name + " " + param_num_str
                    )
                )
                continue

            if env_type.lower() == "_all_":
                logger.warning(
                    constants.PARAMETER_MSGS["all target env"].format(parameter_dict["replace_value"][env_type])
                )

            if msg == "no match" and not is_valid_optional_val:
                skip_msg = constants.PARAMETER_MSGS["no filter match"]
                log_func(
                    constants.PARAMETER_MSGS["skip"].format(
                        value_type, find_value, skip_msg, param_name + " " + param_num_str
                    )
                )

        return True, constants.PARAMETER_MSGS["valid parameter"].format(param_name)

    def _validate_semantic_model_binding_parameter(
        self, param_name: str, multiple_param: bool = False
    ) -> tuple[bool, str]:
        """Validate semantic_model_binding parameter, supporting both legacy and new formats.

        Legacy format (list): Only string connection_id allowed (no environment mapping)
        New format (dict): Only dict connection_id allowed (environment mapping required, use _ALL_ for all)
        """
        param_value = self.environment_parameter.get(param_name)
        is_new_format = isinstance(param_value, dict)
        legacy_keys = {"connection_id", "semantic_model_name"}
        new_keys = {"default", "models"}

        if is_new_format:
            # New format: dict with 'default' and/or 'models'
            param_keys_set = set(param_value.keys())

            # Validate top-level keys
            if param_keys_set & legacy_keys:
                return False, constants.PARAMETER_MSGS["mixed format"].format(param_name)
            if not param_keys_set <= new_keys:
                return False, constants.PARAMETER_MSGS["invalid key"].format(param_name)

            # Require at least one of 'default' or 'models'
            if "default" not in param_value and "models" not in param_value:
                return False, constants.PARAMETER_MSGS["missing key"].format(
                    f"{param_name} (requires 'default' or 'models')"
                )

            # Validate 'default' section
            if "default" in param_value:
                default_value = param_value["default"]
                is_valid, msg = self._validate_data_type(default_value, "dictionary", "default", param_name)
                if not is_valid:
                    return False, msg
                if "connection_id" not in default_value:
                    return False, constants.PARAMETER_MSGS["missing key"].format(
                        f"{param_name}.default (requires 'connection_id')"
                    )
                # New format requires dict connection_id
                is_valid, msg = self._validate_connection_id(
                    default_value["connection_id"], f"{param_name}.default.connection_id", require_dict=True
                )
                if not is_valid:
                    return False, msg

            # Validate 'models' section
            if "models" in param_value:
                models_value = param_value["models"]
                section_name = f"{param_name}.models"

                if not isinstance(models_value, list) or not models_value:
                    return False, f"{section_name} must be a non-empty list"

                for idx, entry in enumerate(models_value):
                    entry_name = f"{section_name}[{idx}]"

                    is_valid, msg = self._validate_data_type(entry, "dictionary", entry_name, section_name)
                    if not is_valid:
                        return False, msg
                    if "semantic_model_name" not in entry:
                        return False, constants.PARAMETER_MSGS["missing key"].format(
                            f"{entry_name} (requires 'semantic_model_name')"
                        )
                    if "connection_id" not in entry:
                        return False, constants.PARAMETER_MSGS["missing key"].format(
                            f"{entry_name} (requires 'connection_id')"
                        )

                    is_valid, msg = self._validate_data_type(
                        entry["semantic_model_name"], "string or list[string]", "semantic_model_name", entry_name
                    )
                    if not is_valid:
                        return False, msg

                    # New format requires dict connection_id
                    is_valid, msg = self._validate_connection_id(
                        entry["connection_id"], f"{entry_name}.connection_id", require_dict=True
                    )
                    if not is_valid:
                        return False, msg

        else:
            # Legacy format: list of dicts with 'connection_id' and 'semantic_model_name'
            for param_num, entry in enumerate(param_value, start=1):
                context_name = f"{param_name} {param_num}" if multiple_param else param_name
                param_keys_set = set(entry.keys())

                # Validate keys
                if param_keys_set & new_keys:
                    return False, constants.PARAMETER_MSGS["mixed format"].format(param_name)
                if not legacy_keys <= param_keys_set:
                    return False, constants.PARAMETER_MSGS["missing key"].format(param_name)
                if not param_keys_set <= legacy_keys:
                    return False, constants.PARAMETER_MSGS["invalid key"].format(param_name)

                connection_id = entry.get("connection_id")
                semantic_model_name = entry.get("semantic_model_name")

                if not connection_id:
                    return False, constants.PARAMETER_MSGS["missing required value"].format("connection_id", param_name)
                if not semantic_model_name:
                    return False, constants.PARAMETER_MSGS["missing required value"].format(
                        "semantic_model_name", param_name
                    )

                is_valid, msg = self._validate_data_type(
                    semantic_model_name, "string or list[string]", "semantic_model_name", param_name
                )
                if not is_valid:
                    return False, msg

                # Legacy format requires string connection_id (no environment mapping)
                is_valid, msg = self._validate_connection_id(connection_id, context_name, require_string=True)
                if not is_valid:
                    return False, msg

        # Check for duplicate semantic model names
        self._check_duplicate_semantic_model_names(param_value, is_new_format)

        return True, constants.PARAMETER_MSGS["valid parameter"].format(param_name)

    def _validate_connection_id(
        self, connection_id: any, context_name: str, require_string: bool = False, require_dict: bool = False
    ) -> tuple[bool, str]:
        """Validate a connection_id value."""
        # Legacy format: require string GUID only
        if require_string:
            if not isinstance(connection_id, str):
                return False, (
                    f"connection_id in {context_name} must be a string GUID. "
                    "Environment-specific dictionaries are not supported in the legacy format. "
                    "Please migrate to the new dictionary format with 'default' and 'models' keys."
                )
            if not re.match(constants.VALID_GUID_REGEX, connection_id):
                return False, f"connection_id '{connection_id}' is not a valid GUID in {context_name}"
            return True, f"Valid {context_name}"

        # New format: require dict with environment keys
        if require_dict:
            if not isinstance(connection_id, dict):
                return False, (
                    f"{context_name} must be a dictionary with environment keys (e.g., DEV, PPE, PROD) or '_ALL_'. "
                    "Use '_ALL_' to apply the same connection to all environments."
                )
            if not connection_id:
                return False, f"{context_name} must be a non-empty dictionary"

            for env_key, guid_value in connection_id.items():
                if not isinstance(guid_value, str):
                    return False, f"connection_id value for environment '{env_key}' must be a string (GUID)"
                if not re.match(constants.VALID_GUID_REGEX, guid_value):
                    return False, f"connection_id for environment '{env_key}' is not a valid GUID: '{guid_value}'"

            # Validate environment exists
            is_valid_env, env_type = self._validate_environment(connection_id)
            if self.environment != "N/A" and not is_valid_env:
                if env_type.lower() == "_all_":
                    return False, constants.PARAMETER_MSGS["other target env"].format(env_type, connection_id)
                logger.warning(constants.PARAMETER_MSGS["no target env"].format(self.environment, context_name))

            return True, f"Valid {context_name}"

        # Should not reach here - caller must specify require_string or require_dict
        return False, f"Invalid validation call for {context_name}: must specify require_string or require_dict"

    def _check_duplicate_semantic_model_names(self, param_value: any, is_new_format: bool) -> None:
        """Check for duplicate semantic model names and log a warning if found."""
        names = []

        if is_new_format:
            for item in param_value.get("models", []):
                raw = item.get("semantic_model_name", [])
                if isinstance(raw, str):
                    names.append(raw)
                elif isinstance(raw, list):
                    names.extend(n for n in raw if isinstance(n, str))
        else:
            for entry in param_value:
                raw = entry.get("semantic_model_name", [])
                if isinstance(raw, str):
                    names.append(raw)
                elif isinstance(raw, list):
                    names.extend(n for n in raw if isinstance(n, str))

        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            logger.warning(constants.PARAMETER_MSGS["duplicate_semantic_model"].format(", ".join(sorted(duplicates))))

    def _validate_parameter_keys(self, param_name: str, param_keys: list) -> tuple[bool, str]:
        """Validate the keys in the parameter."""
        param_keys_set = set(param_keys)

        # Validate minimum set
        if not self.PARAMETER_KEYS[param_name]["minimum"] <= param_keys_set:
            return False, constants.PARAMETER_MSGS["missing key"].format(param_name)

        # Validate maximum set
        if not param_keys_set <= self.PARAMETER_KEYS[param_name]["maximum"]:
            return False, constants.PARAMETER_MSGS["invalid key"].format(param_name)

        return True, constants.PARAMETER_MSGS["valid keys"].format(param_name)

    def _validate_required_values(self, param_name: str, param_dict: dict) -> tuple[bool, str]:
        """Validate required values in the parameter."""
        for key in self.PARAMETER_KEYS[param_name]["minimum"]:
            if not param_dict.get(key):
                return False, constants.PARAMETER_MSGS["missing required value"].format(key, param_name)

            expected_type = "dictionary" if key == "replace_value" else "string"
            is_valid, msg = self._validate_data_type(param_dict[key], expected_type, key, param_name)
            if not is_valid:
                return False, msg

        # Validate find_value is a valid regex if is_regex is set to true
        if param_name == "find_replace":
            is_valid, msg = self._validate_find_regex(param_name, param_dict)
            if not is_valid:
                return False, msg

        if param_name == "key_value_replace":
            is_valid, msg = self._validate_key_value_find_key(param_dict)
            if not is_valid:
                return False, msg

        return True, constants.PARAMETER_MSGS["valid required values"].format(param_name)

    def _validate_key_value_find_key(self, param_dict: dict) -> tuple[bool, str]:
        """Validate the `find_key` JSONPath for key_value_replace (compile-only)."""
        find_key = param_dict.get("find_key")
        if not find_key or not isinstance(find_key, str) or not find_key.strip():
            return False, "Missing or empty 'find_key' for key_value_replace"

        # Require absolute JSONPath root to avoid ambiguous relative paths
        if not find_key.strip().startswith("$"):
            return False, "find_key must be an absolute JSONPath starting with '$'"

        try:
            # jsonpath_ng.ext.parse supports extended JSONPath (dot/bracket)
            from jsonpath_ng.ext import parse

            parse(find_key)
        except Exception as e:
            return False, f"Invalid JSONPath expression '{find_key}': {e}"

        return True, "Valid JSONPath"

    def _validate_find_regex(self, param_name: str, param_dict: dict) -> tuple[bool, str]:
        """Validate the find_value is a valid regex if is_regex is set to true."""
        # Return True if is_regex is not present or set
        if not param_dict.get("is_regex"):
            return True, "No regex present"

        # First validate is_regex value
        is_valid, msg = self._validate_data_type(param_dict.get("is_regex"), "string", "is_regex", param_name)
        if not is_valid:
            return False, msg

        # Skip regex validation if is_regex is not set to true
        if param_dict["is_regex"].lower() != "true":
            logger.warning(constants.PARAMETER_MSGS["regex_ignored"])
            return True, "Skip regex validation"

        # Validate the find_value is a valid regex
        pattern = param_dict["find_value"]
        try:
            re.compile(pattern)
            return True, "Valid regex"
        except re.error as e:
            return False, f"Invalid regex {pattern}: {e}"

    def _validate_replace_value(self, param_name: str, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary."""
        # Validate replace_value dictionary values
        if param_name == "find_replace":
            is_valid, msg = self._validate_find_replace_replace_value(replace_value)

        if param_name == "key_value_replace":
            is_valid, msg = self._validate_key_value_replace_replace_value(replace_value)

        if param_name == "spark_pool":
            is_valid, msg = self._validate_spark_pool_replace_value(replace_value)

        if not is_valid:
            return False, msg

        return True, msg

    def _validate_find_replace_replace_value(self, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary values in find_replace parameters."""
        for environment in replace_value:
            if not replace_value[environment]:
                return False, constants.PARAMETER_MSGS["missing replace value"].format("find_replace", environment)
            is_valid, msg = self._validate_data_type(
                replace_value[environment], "string", environment + " replace_value", param_name="find_replace"
            )
            if not is_valid:
                return False, msg

        return True, constants.PARAMETER_MSGS["valid replace value"].format("find_replace")

    def _validate_key_value_replace_replace_value(self, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary values in key_value_replace parameters.

        For key_value_replace, we allow any data type but all values should be of the same type
        to ensure consistency when replacing values in JSON/YAML files.
        """
        if not replace_value:
            return False, constants.PARAMETER_MSGS["missing replace value"].format("key_value_replace", "any")

        # Get the first value to determine the expected type
        first_env = next(iter(replace_value))
        first_value = replace_value[first_env]
        expected_type = type(first_value)

        for environment in replace_value:
            value = replace_value[environment]

            # Check if value is None/empty (not allowed)
            if value is None:
                return False, constants.PARAMETER_MSGS["missing replace value"].format("key_value_replace", environment)

            # Check type consistency across all environments
            if type(value) != expected_type:
                return (
                    False,
                    f"Inconsistent data types in key_value_replace replace_value: "
                    f"'{first_env}' has type {expected_type.__name__} but "
                    f"'{environment}' has type {type(value).__name__}. "
                    f"All values must be of the same type.",
                )

        return True, constants.PARAMETER_MSGS["valid replace value"].format("key_value_replace")

    def _validate_spark_pool_replace_value(self, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary values in spark_pool parameter."""
        for environment, environment_dict in replace_value.items():
            # Check if environment_dict is empty
            if not environment_dict:
                return False, constants.PARAMETER_MSGS["missing replace value"].format("spark_pool", environment)

            is_valid, msg = self._validate_data_type(
                environment_dict, "dictionary", environment + " key", param_name="spark_pool"
            )
            if not is_valid:
                return False, msg

            msgs = constants.PARAMETER_MSGS["invalid replace value"]
            # Validate keys for the environment
            config_keys = list(environment_dict.keys())
            required_keys = self.PARAMETER_KEYS["spark_pool_replace_value"]
            if not required_keys.issubset(config_keys) or len(config_keys) != len(required_keys):
                return False, msgs["missing key"].format(environment)

            # Validate values for the environment dict
            for key in config_keys:
                if not environment_dict[key]:
                    return False, msgs["missing value"].format(environment, key)

                is_valid, msg = self._validate_data_type(environment_dict[key], "string", key, param_name="spark_pool")
                if not is_valid:
                    return False, msg

            if environment_dict["type"] not in ["Capacity", "Workspace"]:
                return False, msgs["invalid value"].format(environment)

        return True, constants.PARAMETER_MSGS["valid replace value"].format("spark_pool")

    def _validate_optional_values(
        self, param_name: str, param_dict: dict, check_match: bool = False
    ) -> tuple[bool, str]:
        """Validate the optional filter values in the parameter."""
        optional_values = {
            "item_type": param_dict.get("item_type"),
            "item_name": param_dict.get("item_name"),
            "file_path": param_dict.get("file_path"),
        }
        if (param_name == "find_replace" and not any(optional_values.values())) or (
            param_name == "spark_pool" and not optional_values["item_name"]
        ):
            return True, constants.PARAMETER_MSGS["no optional"].format(param_name)

        validation_methods = {
            "item_type": self._validate_item_type,
            "item_name": self._validate_item_name,
            "file_path": self._validate_file_path,
        }

        for param, value in optional_values.items():
            if value:
                # Check value data type
                is_valid, msg = self._validate_data_type(value, "string or list[string]", param, param_name)
                if not is_valid:
                    return False, msg

                # Validate specific optional values and check for matches
                if check_match and param in validation_methods:
                    values = value if isinstance(value, list) else [value]
                    if param == "file_path":
                        is_valid, msg = validation_methods[param](values)
                    else:
                        for item in values:
                            is_valid, msg = validation_methods[param](item)

                    if not is_valid:
                        logger.error(msg)
                        return False, "no match"

        return True, constants.PARAMETER_MSGS["valid optional"].format(param_name)

    def _validate_data_type(
        self, input_value: any, expected_type: str, input_name: str, param_name: str
    ) -> tuple[bool, str]:
        """Validate the data type of the input value."""
        type_validators = {
            "string": lambda x: isinstance(x, str),
            "string or list[string]": lambda x: (isinstance(x, str))
            or (isinstance(x, list) and all(isinstance(item, str) for item in x)),
            "dictionary": lambda x: isinstance(x, dict),
        }
        # Check if the expected type is valid and if the input matches the expected type
        if expected_type not in type_validators or not type_validators[expected_type](input_value):
            return False, constants.PARAMETER_MSGS["invalid data type"].format(input_name, expected_type, param_name)

        return True, "Data type is valid"

    def _validate_environment(self, replace_value: dict) -> tuple[bool, str]:
        """
        Check the target environment exists as a key in the replace_value dictionary.
        If "_ALL_" (case insensitive) is present, it must be the only key.
        """
        # Check for _ALL_ in any case variation
        all_key = None
        for key in replace_value:
            if key.lower() == "_all_":
                logger.warning(f"Found the reserved environment key '{key}'")
                all_key = key
                break
        if all_key:
            # If _ALL_ is present, it must be the only key
            return len(replace_value) == 1, all_key

        # If _ALL_ is not present, check if target environment is present
        return self.environment in replace_value, "env"

    def _validate_item_type(self, input_type: str) -> tuple[bool, str]:
        """Validate the item type is in scope."""
        if input_type not in self.item_type_in_scope:
            return False, constants.PARAMETER_MSGS["invalid item type"].format(input_type)

        return True, "Valid item type"

    def _validate_item_name(self, input_name: str) -> tuple[bool, str]:
        """Validate the item name is found in the repository directory."""
        item_name_list = []
        for root, _dirs, files in os.walk(self.repository_directory):
            directory = Path(root)
            # valid item directory with .platform file within
            if ".platform" in files:
                item_metadata_path = Path(directory, ".platform")
                with Path.open(item_metadata_path) as file:
                    item_metadata = json.load(file)
                # Ensure required metadata fields are present
                if item_metadata and "type" in item_metadata["metadata"] and "displayName" in item_metadata["metadata"]:
                    item_name = item_metadata["metadata"]["displayName"]
                    item_name_list.append(item_name)

        # Check if item name is valid
        if input_name not in item_name_list:
            return False, constants.PARAMETER_MSGS["invalid item name"].format(input_name)

        return True, "Valid item name"

    def _validate_file_path(self, input_path: list[str]) -> tuple[bool, str]:
        """Validate that the file paths exist within the repository directory."""
        # Convert input path to Path objects, returned as a list of valid paths
        valid_paths = process_input_path(self.repository_directory, input_path, validation_flag=True)

        # If list of paths is empty, all paths were invalid
        if not valid_paths:
            return False, constants.PARAMETER_MSGS["no valid file path"].format(input_path)

        # Check for some invalid paths
        path_diff = len(input_path) - len(valid_paths)
        if path_diff > 0:
            return False, constants.PARAMETER_MSGS["invalid file path"].format(input_path, path_diff)

        return True, "Valid file path"
