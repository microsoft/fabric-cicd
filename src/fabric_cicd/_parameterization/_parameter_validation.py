# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the ParameterValidation class to validate the parameter file used for deployment configurations."""

import json
import logging
import os
from pathlib import Path
from typing import Union

from fabric_cicd._parameterization._parameterization_utils import (
    check_parameter_structure,
    load_parameters_to_dict,
    process_input_path,
)

# Configure logging to output to the console
logger = logging.getLogger(__name__)


class ParameterValidation:
    """A class to validate the parameter file."""

    def __init__(
        self,
        repository_directory: str,
        item_type_in_scope: list[str],
        environment: str,
        parameter_file_name: str,
    ) -> None:
        """
        Initializes the ParameterValidation instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
            parameter_file_name: The name of the parameter file.
        """
        # Set class variables
        self.repository_directory = repository_directory
        self.item_type_in_scope = item_type_in_scope
        self.environment = environment
        self.parameter_file_name = parameter_file_name

        # Initialize the parameter dictionary
        self._refresh_parameter_file()

    def _refresh_parameter_file(self) -> None:
        """Load parameters if file is present."""
        parameter_file_path = Path(self.repository_directory, self.parameter_file_name)
        self.environment_parameter = {}

        self.environment_parameter = load_parameters_to_dict(
            self.environment_parameter,
            parameter_file_path,
            self.parameter_file_name,
        )

    def _validate_parameter_file(self) -> bool:
        """Validate the parameter file."""
        logger.info("Validating parameter file")
        # Step 1: Validate the parameter file load to a dictionary
        if not self._validate_parameter_file_load():
            return False

        # Step 2: Validate the parameter names in the parameter dictionary
        if not self._validate_parameter_names():
            return False

        # Step 3: Validate the parameter file structure
        if not self._validate_parameter_structure():
            if check_parameter_structure(self.environment_parameter) == "old":
                logger.warning("Validation skipped: old parameter file structure")
                return True

            logger.error("Validation failed: invalid parameter file structure")
            return False

        # Step 4: Validate the parameters in the parameter dictionary
        params = list(self.environment_parameter.keys())
        if len(params) == 1 and self._validate_parameter(params[0]):
            logger.info(f"Validation passed for {params[0]} parameter")
            return True

        find_replace_validation = self._validate_parameter("find_replace")
        spark_pool_validation = self._validate_parameter("spark_pool")

        if find_replace_validation and spark_pool_validation:
            logger.info("Validation passed for find_replace and spark_pool parameters")
            return True

        logger.error("Parameter file validation failed")
        return False

    def _validate_parameter_file_load(self) -> bool:
        """Validate the parameter file load to a dictionary."""
        logger.debug("Validating parameter file load")
        if not self.environment_parameter:
            logger.error("Parameter file load failed")
            return False

        logger.debug("Parameter file load validation passed")
        return True

    def _validate_parameter_names(self) -> bool:
        """Validate the parameter names in the parameter dictionary."""
        logger.debug("Validating parameter names")
        for param in self.environment_parameter:
            if param not in [
                "find_replace",
                "spark_pool",
            ]:
                logger.error(f"Invalid parameter name: '{param}' found in the parameter file")
                return False

        if "find_replace" not in self.environment_parameter:
            logger.warning("find_replace parameter is not present")
        if "spark_pool" not in self.environment_parameter:
            logger.warning("spark_pool parameter is not present")

        logger.debug("Parameter names are valid")
        return True

    def _validate_parameter_structure(self) -> bool:
        """Validate the parameter file structure."""
        logger.debug("Validating parameter structure")
        if check_parameter_structure(self.environment_parameter) == "old":
            logger.warning(
                "The parameter file structure used will no longer be supported in a future version. Please update to the new structure"
            )
            return False
        if check_parameter_structure(self.environment_parameter) == "invalid":
            return False

        logger.debug("Parameter file structure is valid")
        return True

    def _validate_parameter(self, param_name: str) -> bool:
        """Validate the specified parameter."""
        logger.debug(f"Validating {param_name} parameter")
        for parameter_dict in self.environment_parameter[param_name]:
            # Step 1: Validate parameter keys
            if not self._validate_parameter_keys(param_name, list(parameter_dict.keys())):
                logger.error(f"{param_name} contains missing or invalid keys")
                return False

            # Step 2: Validate required values
            if not self._validate_required_values(parameter_dict, param_name):
                logger.error(f"{param_name} contains missing or invalid required values")
                return False

            # Step 3: Validate replace_value dict keys
            logger.debug("Validating replace_value dictionary keys and values")
            self._validate_environment(parameter_dict["replace_value"], param_name)

            # Step 4: Validate replace_value dict
            if not self._validate_replace_value_dict(parameter_dict["replace_value"], param_name):
                logger.error(f"The replace_value dict in {param_name} contains missing or invalid values")
                return False

            # Step 5: Validate optional values
            if not self._validate_optional_values(parameter_dict, param_name):
                logger.error(f"{param_name} contains invalid optional values")
                return False

        logger.debug(f"{param_name} parameter validation passed")
        return True

    def _validate_parameter_keys(self, param_name: str, param_keys: list) -> bool:
        """Validate the keys in the parameter."""
        minimum_set = (
            set(("find_value", "replace_value"))
            if param_name == "find_replace"
            else set(("instance_pool_id", "replace_value"))
        )
        maximum_set = (
            set(("find_value", "replace_value", "item_type", "item_name", "file_path"))
            if param_name == "find_replace"
            else set(("instance_pool_id", "replace_value", "item_name"))
        )
        param_keys_set = set(param_keys)

        logger.debug(f"Validating {param_name} keys")

        # if minimum_set is not a subset of param_key_set, return False
        if not minimum_set <= param_keys_set:
            logger.debug(f"{param_name} is missing keys")
            return False

        # if param_key_set is not a subset of maximum_set, return False
        if not param_keys_set <= maximum_set:
            logger.debug(f"{param_name} contains invalid keys")
            return False

        logger.debug(f"{param_name} contains valid keys")
        return True

    def _validate_required_values(self, param_dict: dict, param_name: str) -> bool:
        """Validate required values in the parameter."""
        required_keys = (
            ("find_value", "replace_value") if param_name == "find_replace" else ("instance_pool_id", "replace_value")
        )

        logger.debug("Validating required values")
        for key in required_keys:
            if not param_dict.get(key):
                logger.debug(f"Missing value for '{key}' key in {param_name}")
                return False
            if key in ["find_value", "instance_pool_id"] and not self._validate_data_type(
                param_dict[key], "string", key
            ):
                return False
            if key == "replace_value" and not self._validate_data_type(param_dict[key], "dictionary", key):
                return False

        logger.debug(f"Required values in {param_name} are valid")
        return True

    def _validate_replace_value_dict(self, replace_value_dict: dict, param_name: str) -> bool:
        """Validate the replace_value dictionary values."""
        if param_name == "find_replace" and not self._validate_find_replace_replace_value(replace_value_dict):
            return False

        if param_name == "spark_pool" and not self._validate_spark_pool_replace_value(replace_value_dict):
            return False

        logger.debug(f"Values in replace_value dict in {param_name} are valid")
        return True

    def _validate_find_replace_replace_value(self, replace_value_dict: dict) -> bool:
        """Validate the replace_value dictionary values in find_replace parameter."""
        for environment in replace_value_dict:
            if not replace_value_dict[environment]:
                logger.debug(f"find_replace is missing a replace_value for {environment} environment")
                return False
            if not self._validate_data_type(replace_value_dict[environment], "string", environment):
                return False

        return True

    def _validate_spark_pool_replace_value(self, replace_value_dict: dict) -> bool:
        """Validate the replace_value dictionary values in spark_pool parameter."""
        for environment, environment_dict in replace_value_dict.items():
            # Check if environment_dict is empty
            if not environment_dict:
                logger.debug(f"spark_pool is missing replace_value for {environment} environment")
                return False
            if not self._validate_data_type(environment_dict, "dictionary", environment + " key"):
                return False

            # Validate keys for the environment
            config_keys = list(environment_dict.keys())
            required_keys = {"type", "name"}
            if not required_keys.issubset(config_keys) or len(config_keys) != len(required_keys):
                logger.debug(
                    f"The '{environment}' environment dict in spark_pool must contain a 'type' and a 'name' key"
                )
                return False
            # Validate values for the environment dict
            for key in config_keys:
                if not environment_dict[key]:
                    logger.debug(
                        f"The '{environment}' environment dict in spark_pool is missing a value for '{key}' key"
                    )
                    return False
                if not self._validate_data_type(environment_dict[key], "string", key + " key"):
                    return False

            if environment_dict["type"] not in ["Capacity", "Workspace"]:
                logger.debug(
                    f"The '{environment}' environment_dict in spark_pool contains an invalid value: '{environment_dict['type']}' for 'type' key"
                )
                return False

        return True

    def _validate_optional_values(self, param_dict: dict, param_name: str) -> bool:
        """Validate the optional values in the parameter."""
        item_type = param_dict.get("item_type")
        item_name = param_dict.get("item_name")
        file_path = param_dict.get("file_path")

        logger.debug("Validating optional values")
        if (param_name == "find_replace" and (not item_type and not item_name and not file_path)) or (
            param_name == "spark_pool" and not item_name
        ):
            logger.debug(f"No optional parameter values in {param_name}, validation passed")
            return True

        optional_values = {
            "item_type": item_type,
            "item_name": item_name,
            "file_path": file_path,
        }
        for param, value in optional_values.items():
            # Check value data type
            if value and not self._validate_data_type(value, "string or list[string]", param):
                return False

        for param, value in optional_values.items():
            if value:
                if param == "item_type" and not self._validate_item_type(value):
                    logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False
                if param == "item_name" and not self._validate_item_name(value):
                    logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False
                if param == "file_path" and not self._validate_file_path(value):
                    logger.debug(f"{param} parameter value in {param_name} is invalid")
                    return False

        logger.debug(f"Optional parameter values in {param_name} are valid")
        return True

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

    def _validate_environment(self, replace_value_dict: dict, param_name: str) -> bool:
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
