# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the Parameter class to load and validate the parameter file used for deployment configurations."""

import json
import logging
import os
import re
from pathlib import Path
from typing import Union

import yaml

from fabric_cicd._common._constants import (
    INVALID_KEYS,
    INVALID_PARAMETER_FILE_STRUCTURE,
    INVALID_PARAMETER_NAME,
    INVALID_PARAMETERS,
    INVALID_REPLACE_VALUE,
    MISSING_KEY_VALUE,
    MISSING_KEYS,
    MISSING_REPLACE_VALUE,
    OLD_PARAMETER_FILE_STUCTURE_WARNING,
    OPTIONAL_PARAMETERS_MSG,
    PARAMETER_FILE_FOUND,
    PARAMETER_FILE_NAME,
    PARAMETER_FILE_NOT_FOUND,
    PARAMETER_KEYS_SET,
    PARAMETER_NOT_PRESENT,
    REQUIRED_VALUES,
    SPARK_POOL_REPLACE_VALUE_ERRORS,
    VALID_KEYS,
    VALID_PARAMETER,
    VALID_PARAMETER_FILE_STRUCTURE,
    VALID_PARAMETER_NAMES,
    VALID_PARAMETERS,
    VALID_REPLACE_VALUE,
    VALIDATING_PARAMETER,
    VALIDATION_PASSED,
)
from fabric_cicd._parameter._utils import (
    check_parameter_structure,
    process_input_path,
)

# Configure logging to output to the console
logger = logging.getLogger(__name__)


class Parameter:
    """A class to validate the parameter file."""

    def __init__(
        self,
        repository_directory: str,
        item_type_in_scope: list[str],
        environment: str,
    ) -> None:
        """
        Initializes the Parameter instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
        """
        # Set class variables
        self.repository_directory = repository_directory
        self.item_type_in_scope = item_type_in_scope
        self.environment = environment

        # Initialize the parameter dictionary
        self.environment_parameter = {}

    def _validate_parameter_file(self) -> tuple[bool, str, str]:
        """Validate the parameter file."""
        validation_steps = [
            ("parameter file exists", self._validate_parameter_file_exists),
            ("parameter file load", self._validate_load_parameters_to_dict),
            ("parameter file structure", self._validate_parameter_structure),
            ("parameter names", self._validate_parameter_names),
            ("parameters", self._validate_parameters),
        ]
        for step, validation_func in validation_steps:
            logger.info(VALIDATING_PARAMETER.format(step))
            is_valid, msg = validation_func()
            if not is_valid:
                if step == "parameter file exists":
                    return True, msg, "Not found"
                if step == "parameter file structure":
                    if msg == "old":
                        return True, OLD_PARAMETER_FILE_STUCTURE_WARNING, "old"
                    if msg == "invalid":
                        return False, INVALID_PARAMETER_FILE_STRUCTURE, ""
                return False, msg, ""
            logger.info(VALIDATION_PASSED.format(msg))

        return True, "Parameter file validation passed", ""

    def _validate_parameter_file_exists(self) -> tuple[bool, str]:
        """Validate the parameter file exists."""
        parameter_file_path = Path(self.repository_directory, PARAMETER_FILE_NAME)

        if not parameter_file_path.is_file():
            return False, PARAMETER_FILE_NOT_FOUND.format(parameter_file_path)

        return True, PARAMETER_FILE_FOUND

    def _validate_load_parameters_to_dict(self) -> tuple[bool, str]:
        """Validate loading the parameter file to a dictionary."""
        parameter_file_path = Path(self.repository_directory, PARAMETER_FILE_NAME)

        try:
            # logger.info(PARAMETER_FILE_FOUND)
            # logger.info(VALIDATING_PARAMETER.format("file"))
            with Path.open(parameter_file_path, encoding="utf-8") as yaml_file:
                yaml_content = yaml_file.read()

                logger.debug(VALIDATING_PARAMETER.format("file content"))
                validation_errors = self._validate_yaml_content(yaml_content)
                if validation_errors:
                    for error_msg in validation_errors:
                        return False, error_msg

                self.environment_parameter = yaml.full_load(yaml_content)
                return True, f"Successfully loaded {PARAMETER_FILE_NAME}"

        except yaml.YAMLError as e:
            return False, f"Error loading {PARAMETER_FILE_NAME}: {e}"

    def _validate_yaml_content(self, content: str) -> list[str]:
        """Validate the yaml content of the parameter file"""
        errors = []

        # Check for invalid characters (non-UTF-8)
        if not re.match(r"^[\u0000-\uFFFF]*$", content):
            errors.append("Invalid characters found.")

        # Check for unclosed brackets or quotes
        brackets = ["()", "[]", "{}"]
        for bracket in brackets:
            if content.count(bracket) != content.count(bracket):
                errors.append(f"Unclosed bracket: {bracket}")

        quotes = ['"', "'"]
        for quote in quotes:
            if content.count(quote) % 2 != 0:
                errors.append(f"Unclosed quote: {quote}")

        return errors

    def _validate_parameter_names(self) -> tuple[bool, str]:
        """Validate the parameter names in the parameter dictionary."""
        for param in self.environment_parameter:
            if param not in ["find_replace", "spark_pool"]:
                return False, INVALID_PARAMETER_NAME.format(param)

        if "find_replace" not in self.environment_parameter:
            logger.warning(PARAMETER_NOT_PRESENT.format("find_replace"))
        if "spark_pool" not in self.environment_parameter:
            logger.warning(PARAMETER_NOT_PRESENT.format("spark_pool"))

        return True, VALID_PARAMETER_NAMES

    def _validate_parameter_structure(self) -> tuple[bool, str]:
        """Validate the parameter file structure."""
        # TODO: Deprecate old structure check in future versions
        if check_parameter_structure(self.environment_parameter) == "old":
            return False, "old"
        if check_parameter_structure(self.environment_parameter) == "invalid":
            return False, "invalid"

        return True, VALID_PARAMETER_FILE_STRUCTURE

    def _validate_parameters(self) -> tuple[bool, str]:
        """Validate the parameters in the parameter dictionary."""
        params = list(self.environment_parameter.keys())
        if len(params) == 1:
            is_valid, msg = self._validate_parameter(params[0])
            if is_valid:
                return True, msg.format(params[0])
            logger.error(msg)
            return False, msg

        is_valid_find_replace, _ = self._validate_parameter("find_replace")
        is_valid_spark_pool, _ = self._validate_parameter("spark_pool")

        if is_valid_find_replace and is_valid_spark_pool:
            return True, VALID_PARAMETERS

        return False, INVALID_PARAMETERS

    def _validate_parameter(self, param_name: str) -> tuple[bool, str]:
        """Validate the specified parameter."""
        validation_steps = [
            ("keys", lambda param_dict: self._validate_parameter_keys(param_name, list(param_dict.keys()))),
            ("required values", lambda param_dict: self._validate_required_values(param_name, param_dict)),
            (
                "replace_value",
                lambda param_dict: self._validate_replace_value(param_name, param_dict["replace_value"]),
            ),
            ("optional values", lambda param_dict: self._validate_optional_values(param_name, param_dict)),
        ]
        for parameter_dict in self.environment_parameter[param_name]:
            for step, validation_func in validation_steps:
                logger.info(VALIDATING_PARAMETER.format(param_name + " " + step))
                is_valid, msg = validation_func(parameter_dict)
                if not is_valid:  # validation_func(parameter_dict):
                    logger.error(msg)
                    # msg = MISSING_OR_INVALID_MSG.format(param_name, step)
                    # if step == "optional values":
                    # msg = VALIDATING_OPTIONAL_VALUE.format(param_name)
                    return False, msg

        return True, VALID_PARAMETER

    def _validate_parameter_keys(self, param_name: str, param_keys: list) -> tuple[bool, str]:
        """Validate the keys in the parameter."""
        param_keys_set = set(param_keys)

        # Validate minimum set
        if not PARAMETER_KEYS_SET[param_name]["minimum"] <= param_keys_set:
            return False, MISSING_KEYS.format(param_name)

        # Validate maximum set
        if not param_keys_set <= PARAMETER_KEYS_SET[param_name]["maximum"]:
            return False, INVALID_KEYS.format(param_name)

        return True, VALID_KEYS.format(param_name)

    def _validate_required_values(self, param_name: str, param_dict: dict) -> tuple[bool, str]:
        """Validate required values in the parameter."""
        for key in PARAMETER_KEYS_SET[param_name]["minimum"]:
            if not param_dict.get(key):
                return False, MISSING_KEY_VALUE.format(key, param_name)
            if key != "replace_value" and not self._validate_data_type(param_dict[key], "string", key):
                return False, ""
            if key == "replace_value" and not self._validate_data_type(param_dict[key], "dictionary", key):
                return False, ""

        return True, REQUIRED_VALUES.format(param_name)

    def _validate_replace_value(self, param_name: str, replace_value: dict) -> tuple[bool, str]:
        """Validate the replace_value dictionary."""
        # Validate environment keys in replace_value
        self._validate_environment(param_name, replace_value)

        # Validate replace_value dictionary values
        if (param_name == "find_replace" and not self._validate_find_replace_replace_value(replace_value)) or (
            param_name == "spark_pool" and not self._validate_spark_pool_replace_value(replace_value)
        ):
            return False, INVALID_REPLACE_VALUE.format(param_name)

        return True, VALID_REPLACE_VALUE.format(param_name)

    def _validate_find_replace_replace_value(self, replace_value_dict: dict) -> bool:
        """Validate the replace_value dictionary values in find_replace parameter."""
        for environment in replace_value_dict:
            if not replace_value_dict[environment]:
                logger.debug(MISSING_REPLACE_VALUE.format("find_replace", environment))
                return False
            if not self._validate_data_type(replace_value_dict[environment], "string", environment):
                return False

        return True

    def _validate_spark_pool_replace_value(self, replace_value_dict: dict) -> bool:
        """Validate the replace_value dictionary values in spark_pool parameter."""
        for environment, environment_dict in replace_value_dict.items():
            # Check if environment_dict is empty
            if not environment_dict:
                logger.debug(MISSING_REPLACE_VALUE.format("spark_pool", environment))
                return False
            if not self._validate_data_type(environment_dict, "dictionary", environment + " key"):
                return False

            # Validate keys for the environment
            config_keys = list(environment_dict.keys())
            required_keys = PARAMETER_KEYS_SET["spark_pool_replace_value"]
            if not required_keys.issubset(config_keys) or len(config_keys) != len(required_keys):
                logger.debug(SPARK_POOL_REPLACE_VALUE_ERRORS[0].format(environment))
                return False

            # Validate values for the environment dict
            for key in config_keys:
                if not environment_dict[key]:
                    logger.debug(SPARK_POOL_REPLACE_VALUE_ERRORS[1].format(environment, key))
                    return False
                if not self._validate_data_type(environment_dict[key], "string", key + " key"):
                    return False

            if environment_dict["type"] not in ["Capacity", "Workspace"]:
                logger.debug(SPARK_POOL_REPLACE_VALUE_ERRORS[2].format(environment, environment_dict["type"]))
                return False

        return True

    def _validate_optional_values(self, param_name: str, param_dict: dict) -> tuple[bool, str]:
        """Validate the optional values in the parameter."""
        optional_values = {
            "item_type": param_dict.get("item_type"),
            "item_name": param_dict.get("item_name"),
            "file_path": param_dict.get("file_path"),
        }
        if param_name == "find_replace" and not any(optional_values.values()):
            # logger.debug(OPTIONAL_PARAMETERS_MSG[0].format(param_name))
            return True, OPTIONAL_PARAMETERS_MSG[0].format(param_name)
        if param_name == "spark_pool" and not optional_values["item_name"]:
            # logger.debug(OPTIONAL_PARAMETERS_MSG[0].format(param_name))
            return True, OPTIONAL_PARAMETERS_MSG[0].format(param_name)

        for param, value in optional_values.items():
            if value:
                # Check value data type
                if not self._validate_data_type(value, "string or list[string]", param):
                    return False
                # Validate specific optional values
                if param == "item_type" and not self._validate_item_type(value):
                    # logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False, OPTIONAL_PARAMETERS_MSG[1].format(param, param_name)
                if param == "item_name" and not self._validate_item_name(value):
                    # logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False, OPTIONAL_PARAMETERS_MSG[1].format(param, param_name)
                if param == "file_path" and not self._validate_file_path(value):
                    # logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False, OPTIONAL_PARAMETERS_MSG[1].format(param, param_name)

        # logger.debug(f"Optional parameter values in {param_name} are valid")
        return True, OPTIONAL_PARAMETERS_MSG[2].format(param_name)

    def _validate_data_type(self, input_value: any, expected_type: str, input_name: str) -> bool:
        """Validate the data type of the input value."""
        type_validators = {
            "string": lambda x: isinstance(x, str),
            "string or list[string]": lambda x: (isinstance(x, str))
            or (isinstance(x, list) and all(isinstance(item, str) for item in x)),
            "dictionary": lambda x: isinstance(x, dict),
        }

        # Check if the expected type is valid and if the input matches the expected type
        if expected_type not in type_validators or not type_validators[expected_type](input_value):
            msg = f"The provided {input_name} is not of type {expected_type}"
            logger.error(msg)
            return False

        return True

    def _validate_environment(self, param_name: str, replace_value_dict: dict) -> bool:
        """Check the target environment exists as a key in the replace_value dictionary."""
        if self.environment != "N/A" and self.environment not in replace_value_dict:
            logger.warning(
                f"Target environment '{self.environment}' is not a key in the 'replace_value' dict in {param_name}"
            )
            return False

        logger.debug(f"Target environment: '{self.environment}' is a key in the 'replace_value' dict in {param_name}")
        return True

    def _validate_item_type(self, input_type: Union[str, list]) -> bool:
        """Validate the item type is in scope."""
        input_data_type = type(input_type)

        # Check if item type is valid
        type_validators = {
            str: lambda x: x in self.item_type_in_scope,
            list: lambda x: all(item_type in self.item_type_in_scope for item_type in x),
        }
        if not type_validators[input_data_type](input_type):
            logger.debug(f"Item type: '{input_type}' not in scope")
            return False

        logger.debug(f"Item type: '{input_type}' in scope")
        return True

    def _validate_item_name(self, input_name: Union[str, list]) -> bool:
        """Validate the item name is found in the repository directory."""
        item_name_list = []
        input_data_type = type(input_name)

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
        type_validators = {
            str: lambda x: x in item_name_list,
            list: lambda x: all(item_name in item_name_list for item_name in x),
        }
        if not type_validators[input_data_type](input_name):
            logger.debug(f"Item name: '{input_name}' not found in the repository directory")
            return False

        logger.debug(f"Item name: '{input_name}' found in the repository directory")
        return True

    def _validate_file_path(self, input_path: Union[str, list]) -> bool:
        """Validate the file path exists."""
        input_data_type = type(input_path)

        # Convert input path to Path object
        input_path_new = process_input_path(self.repository_directory, input_path)

        type_validators = {
            str: lambda x: Path(x).exists(),
            list: lambda x: all(Path(path).exists() for path in x),
        }
        if not type_validators[input_data_type](input_path_new):
            logger.debug(f"Path: '{input_path}' not found in the repository directory")
            return False

        logger.debug("Path found in the repository directory")
        return True
