# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the ParameterValidation class to validate the parameter file used for deployment configurations."""

import json
import logging
import os
from pathlib import Path
from typing import Union

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd._common._fabric_endpoint import FabricEndpoint
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
        token_credential: TokenCredential = None,
    ) -> None:
        """
        Initializes the ParameterValidation instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
            parameter_file_name: The name of the parameter file.
            token_credential: The token credential to use for API requests.
        """
        from fabric_cicd._common._validate_input import (
            validate_environment,
            validate_item_type_in_scope,
            validate_repository_directory,
            validate_token_credential,
        )

        # Initialize endpoint
        self.endpoint = FabricEndpoint(
            # if credential is not defined, use DefaultAzureCredential
            token_credential=(
                DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
            )
        )

        # Validate and set class variables
        self.repository_directory = validate_repository_directory(repository_directory)
        self.item_type_in_scope = validate_item_type_in_scope(item_type_in_scope, upn_auth=self.endpoint.upn_auth)
        self.environment = validate_environment(environment)
        self.parameter_file_name = parameter_file_name

        # Initialize the parameter dictionary
        self._refresh_parameter_file()

    def _refresh_parameter_file(self) -> None:
        """Loads parameters if file is present."""
        parameter_file_path = Path(self.repository_directory, self.parameter_file_name)
        self.environment_parameter = {}

        self.environment_parameter = load_parameters_to_dict(
            self.environment_parameter,
            parameter_file_path,
            self.parameter_file_name,
        )

    def _validate_parameter_file(self) -> bool:
        """Validates the parameter file."""
        # Step 1: Validate the parameter file load to a dictionary
        if not self._validate_parameter_file_load():
            logger.error("Parameter file validation failed")
            return False

        # Step 2: Validate the parameter names in the parameter dictionary
        if not self._validate_parameter_names():
            logger.error("Parameter file validation failed")
            return False

        # Step 3: Validate the parameter file structure
        if not self._validate_parameter_structure():
            return False

        # Step 4: Validate the parameters in the parameter dictionary
        params = list(self.environment_parameter.keys())
        if len(params) == 1 and self._validate_parameter(params[0]):
            logger.info("Parameter file validation passed")
            return True

        find_replace_validation = self._validate_parameter("find_replace")
        spark_pool_validation = self._validate_parameter("spark_pool")

        if find_replace_validation and spark_pool_validation:
            logger.info("Parameter file validation passed")
            return True

        logger.error("Parameter file validation failed")
        return False

    def _validate_parameter_file_load(self) -> bool:
        """Validates the parameter file load to a dictionary."""
        logger.info("Validating parameter file load")
        if not self.environment_parameter:
            logger.error("Parameter file is empty or does not exist")
            return False

        logger.debug("Parameter file load validation passed")
        return True

    def _validate_parameter_names(self) -> bool:
        """Validates the parameter names in the parameter dictionary."""
        logger.info("Validating parameter names")
        for param in self.environment_parameter:
            if not self._validate_data_type(param, "string") or param not in ["find_replace", "spark_pool"]:
                logger.error(f"Invalid parameter '{param}' in the parameter file")
                return False

        if "find_replace" not in self.environment_parameter:
            logger.warning("find_replace parameter is not present in the dictionary")
        if "spark_pool" not in self.environment_parameter:
            logger.warning("spark_pool parameter is not present in the dictionary")

        logger.debug("Parameter names are valid")
        return True

    def _validate_parameter_structure(self) -> bool:
        """Validates the parameter structure."""
        logger.info("Validating parameter structure")
        if check_parameter_structure(self.environment_parameter) == "old":
            logger.warning("Validation skipped for old parameter file structure")
            return False
        if check_parameter_structure(self.environment_parameter) == "invalid":
            logger.error("Validation failed for invalid parameter file structure")
            return False

        logger.debug("Parameter file structure is valid")
        return True

    def _validate_parameter(self, param_name: str) -> bool:
        """Validates the specified parameter."""
        logger.info(f"Validating {param_name} parameter")
        for parameter_dict in self.environment_parameter[param_name]:
            # Step 1: Validate parameter keys
            if not self._validate_parameter_keys(param_name, list(parameter_dict.keys())):
                return False

            # Step 2: Validate required values
            if not self._validate_required_values(parameter_dict, param_name):
                return False

            # Step 3: Validate replace_value dict keys
            logger.info("Validating replace_value dictionary keys and values")
            if self.environment != "N/A":
                self._validate_environment(parameter_dict["replace_value"], param_name)

            # Step 4: Validate replace_value dict
            if not self._validate_replace_value_dict(parameter_dict["replace_value"], param_name):
                return False

            # Step 5: Validate optional values
            self._validate_optional_values(parameter_dict, param_name)

        logger.info(f"{param_name} parameter validation passed")
        return True

    def _validate_parameter_keys(self, param_name: str, param_keys: list) -> bool:
        """Validates the keys in the specified parameter."""
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

        logger.info(f"Validating {param_name} keys")

        for key in param_keys:
            if not self._validate_data_type(key, "string"):
                return False

        # if minimum_set is not a subset of param_key_set, return False
        if not minimum_set <= param_keys_set:
            logger.debug(f"{param_name} is missing required keys")
            return False

        # if param_key_set is not a subset of maximum_set, return False
        if not param_keys_set <= maximum_set:
            logger.debug(f"{param_name} contains invalid keys")
            return False

        logger.debug(f"{param_name} contains valid keys")
        return True

    def _validate_required_values(self, param_dict: dict, param_name: str) -> bool:
        """Validates required values in the parameter."""
        required_keys = (
            ("find_value", "replace_value") if param_name == "find_replace" else ("instance_pool_id", "replace_value")
        )

        logger.info("Validating required values")
        for key in required_keys:
            if not param_dict.get(key):
                logger.debug(f"Missing value for '{key}' key in {param_name}")
                return False
            if key in ["find_value", "instance_pool_id"] and not self._validate_data_type(param_dict[key], "string"):
                return False
            if key == "replace_value" and not self._validate_data_type(param_dict[key], "dictionary"):
                return False

        logger.debug(f"Required values are present in {param_name} and are of valid data types")
        return True

    def _validate_replace_value_dict(self, replace_value_dict: dict, param_name: str) -> bool:
        """Validates the values in the replace_value dictionary."""
        if param_name == "find_replace" and not self._validate_find_replace_replace_value(replace_value_dict):
            return False

        if param_name == "spark_pool" and not self._validate_spark_pool_replace_value(replace_value_dict):
            return False

        logger.debug(f"Values in replace_value dictionary are valid for {param_name}")
        return True

    def _validate_find_replace_replace_value(self, replace_value_dict: dict) -> bool:
        """Validates the values in the replace_value dictionary for find_replace parameter."""
        for environment in replace_value_dict:
            if not replace_value_dict[environment]:
                logger.debug(f"find_replace is missing a replace_value for {environment} environment")
                return False
            if not self._validate_data_type(replace_value_dict[environment], "string"):
                return False

        return True

    def _validate_spark_pool_replace_value(self, replace_value_dict: dict) -> bool:
        """Validates the values in the replace_value dictionary for spark_pool parameter."""
        for environment, environment_dict in replace_value_dict.items():
            # Check if environment_dict is empty
            if not environment_dict:
                logger.debug(f"spark_pool is missing replace_value for {environment} environment")
                return False
            if not self._validate_data_type(environment_dict, "dictionary"):
                return False

            # Validate keys for the environment
            config_keys = list(environment_dict.keys())
            for key in config_keys:
                if key not in ["type", "name"]:
                    logger.debug(f"'{key}' is an invalid key in {environment} environment for spark_pool")
                    return False
                if key != "type" and key != "name":
                    logger.debug(f"Missing 'type' and/or 'name' key in {environment} environment for spark_pool")
                    return False
                if not self._validate_data_type(environment_dict[key], "string"):
                    logger.debug(f"'Invalid value found for '{key}' key in {environment} environment for spark_pool")
                    return False
                if key == "type" and environment_dict[key] not in ["Capacity", "Workspace"]:
                    logger.debug(
                        f"'{environment_dict[key]}' is an invalid value for '{key}' key in {environment} environment for spark_pool"
                    )
                    return False

        return True

    def _validate_optional_values(self, param_dict: dict, param_name: str) -> bool:
        """Validates the optional values in the parameter."""
        valid_data_type = "string or list"

        item_type = param_dict.get("item_type")
        item_name = param_dict.get("item_name")
        file_path = param_dict.get("file_path")

        logger.info("Validating optional values")
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
        for param, values in optional_values.items():
            if values:
                # Check value data type
                if not self._validate_data_type(values, valid_data_type):
                    logger.warning(f"{param} value is invalid")
                    return False
                if param == "item_type" and not self._validate_item_type(values):
                    logger.warning(f"{param} value is invalid")
                    return False
                if param == "item_name" and not self._validate_item_name(values):
                    logger.warning(f"{param} value is invalid")
                    return False
                if param == "file_path" and not self._validate_file_path(values):
                    logger.warning(f"{param} value is invalid")
                    return False

        logger.debug(f"Optional parameter values are valid for {param_name}")
        return True

    def _validate_data_type(self, input_value: any, valid_data_type: str) -> bool:
        """Validates the data type of the input value."""
        type_dict = {
            "string": str,
            "list": list,
            "dictionary": dict,
            "string or list": Union[str, list],
        }
        if input_value == None:
            logger.debug(f"Value is None and must be {valid_data_type} type")
            return False

        if not isinstance(input_value, type_dict[valid_data_type]):
            logger.debug(f"'{input_value}' must be {valid_data_type} type")
            return False

        if isinstance(input_value, list):
            for item in input_value:
                if not isinstance(item, str):
                    logger.debug(f"'{item}' in list must be string type")
                    return False

        return True

    def _validate_environment(self, replace_value_dict: dict, param_name: str) -> bool:
        """Checks the target environment exists as a key in the replace_value dictionary."""
        if self.environment not in replace_value_dict:
            logger.warning(f"Target environment '{self.environment}' is not a key in 'replace_value' for {param_name}")
            return False

        logger.debug(f"Target environment: '{self.environment}' is a key in 'replace_value' for {param_name}")
        return True

    def _validate_item_type(self, input_type: str) -> bool:
        """Validates the item type is in scope."""
        if isinstance(input_type, list):
            for item_type in input_type:
                if not item_type in self.item_type_in_scope:
                    logger.debug(f"Item type '{item_type}' is not in scope")
                    return False
            logger.debug("Item types are in scope")
            return True

        if not input_type in self.item_type_in_scope:
            logger.debug(f"Item type '{input_type}' is not in scope")
            return False

        logger.debug(f"Item type '{input_type}' is in scope")
        return True

    def _validate_item_name(self, input_name: str) -> bool:
        """Validates the item name is found in the repository directory."""
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

        if isinstance(input_name, list):
            for item_name in input_name:
                if not item_name in item_name_list:
                    logger.debug(f"Item name '{item_name}' is not found in the repository directory")
                    return False
            logger.debug("Item names are found in the repository directory")
            return True

        if not input_name in item_name_list:
            logger.debug(f"Item name '{input_name}' is not found in the repository directory")
            return False

        logger.debug(f"Item name '{input_name}' is found in the repository directory")
        return True

    def _validate_file_path(self, input_path: Union[Path, list, None]) -> bool:
        """Validates the file path exists."""
        # Convert input path to Path object
        input_path_new = process_input_path(self.repository_directory, input_path)

        # Check if path exists
        if isinstance(input_path_new, list):
            for path in input_path_new:
                if not Path(path).exists():
                    logger.debug(f"Path in '{input_path}' is not found in the repository directory")
                    return False
            logger.debug("All paths are found in the repository directory")
            return True

        if not input_path_new.exists():
            logger.debug(f"Path '{input_path}' is not found in the repository directory")
            return False

        logger.debug(f"Path '{input_path}' is found in the repository directory")
        return True
