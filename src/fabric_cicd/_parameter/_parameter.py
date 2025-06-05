# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the Parameter class to load and validate the parameter file used for deployment configurations."""

import json
import logging
import os
import re
from pathlib import Path
from typing import ClassVar

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
    }

    LOAD_ERROR_MSG = ""

    def __init__(
        self,
        repository_directory: str,
        item_type_in_scope: list[str],
        environment: str,
        parameter_file_name: str = "parameter.yml",
    ) -> None:
        """
        Initializes the Parameter instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
            parameter_file_name: The name of the parameter file, default is "parameter.yml".
        """
        # Set class variables
        self.repository_directory = repository_directory
        self.item_type_in_scope = item_type_in_scope
        self.environment = environment
        self.parameter_file_name = parameter_file_name

        # Initialize the parameter file path and load parameters
        self.parameter_file_path = Path(self.repository_directory, parameter_file_name)
        self._refresh_parameter_file()

    def _refresh_parameter_file(self) -> None:
        """Load parameters if file is present."""
        self.environment_parameter = {}

        if self._validate_parameter_file_exists():
            is_valid, environment_parameter = self._validate_load_parameters_to_dict()
            if is_valid:
                self.environment_parameter = environment_parameter

    def _validate_parameter_file_exists(self) -> bool:
        """Validate the parameter file exists."""
        return self.parameter_file_path.is_file()

    def _validate_load_parameters_to_dict(self) -> tuple[bool, dict]:
        """Validate loading the parameter file to a dictionary."""
        parameter_dict = {}
        try:
            with Path.open(self.parameter_file_path, encoding="utf-8") as yaml_file:
                yaml_content = yaml_file.read()
                yaml_content = replace_variables_in_parameter_file(yaml_content)
                validation_errors = self._validate_yaml_content(yaml_content)
                if validation_errors:
                    self.LOAD_ERROR_MSG = constants.PARAMETER_MSGS["invalid load"].format(validation_errors)
                    return False, parameter_dict

                parameter_dict = yaml.full_load(yaml_content)
                logger.debug(constants.PARAMETER_MSGS["passed"].format("YAML content is valid"))
                return True, parameter_dict
        except yaml.YAMLError as e:
            self.LOAD_ERROR_MSG = constants.PARAMETER_MSGS["invalid load"].format(e)
            return False, parameter_dict

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
        if not self.environment_parameter:
            if not self._validate_parameter_file_exists():
                logger.warning(constants.PARAMETER_MSGS["not found"].format(self.parameter_file_path))
                return False, "not found"
            logger.debug(constants.PARAMETER_MSGS["found"])
            return False, self.LOAD_ERROR_MSG

        return True, constants.PARAMETER_MSGS["valid load"]

    def _validate_parameter_file(self) -> bool:
        """Validate the parameter file."""
        validation_steps = [
            ("parameter file load", self._validate_parameter_load),
            ("parameter names", self._validate_parameter_names),
            ("parameter file structure", self._validate_parameter_structure),
            ("find_replace parameter", lambda: self._validate_parameter("find_replace")),
            ("spark_pool parameter", lambda: self._validate_parameter("spark_pool")),
            ("key_value_replace parameter", lambda: self._validate_parameter("key_value_replace")),
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
                    step in ("find_replace parameter", "key_value_replace parameter", "spark_pool parameter")
                    and msg == "parameter not found"
                ):
                    continue
                # Otherwise, return False with error message
                logger.error(constants.PARAMETER_MSGS["failed"].format(msg))
                return False
            logger.debug(constants.PARAMETER_MSGS["passed"].format(msg))

        # Return True if all validation steps pass
        logger.info("Parameter file validation passed")
        return True

    def _validate_parameter_structure(self) -> tuple[bool, str]:
        """Validate the parameter file structure."""
        if not is_valid_structure(self.environment_parameter):
            return False, constants.PARAMETER_MSGS["invalid structure"]

        return True, constants.PARAMETER_MSGS["valid structure"]

    def _validate_parameter_names(self) -> tuple[bool, str]:
        """Validate the parameter names in the parameter dictionary."""
        params = list(self.PARAMETER_KEYS.keys())[:4]
        for param in self.environment_parameter:
            if param not in params:
                return False, constants.PARAMETER_MSGS["invalid name"].format(param)

        return True, constants.PARAMETER_MSGS["valid name"]

    def _validate_parameter(self, param_name: str) -> tuple[bool, str]:
        """Validate the specified parameter."""
        if param_name not in self.environment_parameter:
            logger.debug(f"The {param_name} parameter was not found")
            return False, "parameter not found"

        logger.debug(f"Found the {param_name} parameter")
        param_count = len(self.environment_parameter[param_name])
        multiple_param = param_count > 1
        if multiple_param:
            logger.debug(f"{param_count} {param_name} parameters found")

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
            find_value = parameter_dict[key_name]
            for step, validation_func in validation_steps:
                logger.debug(constants.PARAMETER_MSGS["validating"].format(f"{param_name} {param_num_str} {step}"))
                is_valid, msg = validation_func(parameter_dict)
                if not is_valid:
                    return False, msg
                logger.debug(constants.PARAMETER_MSGS["passed"].format(msg))

            # Check if replacement will be skipped for a given find value
            is_valid_env = self._validate_environment(parameter_dict["replace_value"])
            is_valid_optional_val, msg = self._validate_optional_values(param_name, parameter_dict, check_match=True)
            log_func = logger.debug if param_name == "key_value_replace" else logger.warning

            # Set value_type based on regex flag once
            value_type = (
                "find value regex"
                if (parameter_dict.get("is_regex") and parameter_dict["is_regex"].lower() == "true")
                else "find value"
            )

            # Replacement skipped if target environment is not present
            if self.environment != "N/A" and not is_valid_env:
                skip_msg = constants.PARAMETER_MSGS["no target env"].format(self.environment, param_name)
                log_func(
                    constants.PARAMETER_MSGS["skip"].format(
                        value_type, find_value, skip_msg, param_name + " " + param_num_str
                    )
                )
                continue

            # Replacement skipped if optional filter values don't match
            if msg == "no match" and not is_valid_optional_val:
                skip_msg = constants.PARAMETER_MSGS["no filter match"]
                log_func(
                    constants.PARAMETER_MSGS["skip"].format(
                        value_type, find_value, skip_msg, param_name + " " + param_num_str
                    )
                )

        return True, constants.PARAMETER_MSGS["valid parameter"].format(param_name)

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

        return True, constants.PARAMETER_MSGS["valid required values"].format(param_name)

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
            logger.warning("The provided is_regex value is not set to 'true', regex matching will be ignored.")
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
            is_valid, msg = self._validate_find_replace_replace_value(replace_value)

        if param_name == "spark_pool":
            is_valid, msg = self._validate_spark_pool_replace_value(replace_value)

        if not is_valid:
            return False, msg

        return True, msg

    def _validate_find_replace_replace_value(self, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary values in find_replace and key_value_replace parameters."""
        for environment in replace_value:
            if not replace_value[environment]:
                return False, constants.PARAMETER_MSGS["missing replace value"].format("find_replace", environment)
            is_valid, msg = self._validate_data_type(
                replace_value[environment], "string", environment + " replace_value", param_name="find_replace"
            )
            if not is_valid:
                return False, msg

        return True, constants.PARAMETER_MSGS["valid replace value"].format("find_replace")

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
                    for item in values:
                        is_valid, msg = validation_methods[param](item)
                        if not is_valid:
                            logger.debug(msg)
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

    def _validate_environment(self, replace_value: dict) -> bool:
        """Check the target environment exists as a key in the replace_value dictionary."""
        return self.environment in replace_value

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

    def _validate_file_path(self, input_path: str) -> tuple[bool, str]:
        """Validate the file path exists."""
        # Convert input path to Path object
        input_path_new = process_input_path(self.repository_directory, input_path)

        # Check if the file path exists
        if not input_path_new.exists():
            return False, constants.PARAMETER_MSGS["invalid file path"].format(input_path)

        return True, "Valid file path"
