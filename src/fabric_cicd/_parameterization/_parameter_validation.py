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
    load_parameters_to_dict,
    new_parameter_structure,
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
        if self._validate_parameter_file_load():
            if not new_parameter_structure(self.environment_parameter):
                logger.warning("Validation skipped for old parameter structure")
                return True

            logger.info("Validating the parameters")
            if self._validate_all_parameters():
                logger.info("Parameter file validation passed")
                return True

        logger.error("Parameter file validation failed")
        return False

    def _validate_parameter_file_load(self) -> bool:
        """Validates the parameter file load to a dictionary."""
        return bool(self.environment_parameter)

    def _validate_all_parameters(self) -> bool:
        """Validates the parameters in the parameter dictionary."""
        # Validate the parameter keys in the dictionary
        for key in self.environment_parameter:
            if key not in ["find_replace", "spark_pool"]:
                logger.error(f"Invalid parameter '{key}' in the parameter file")
                return False

        # Validate find_replace and spark_pool parameters
        find_replace_validation = self._validate_parameter("find_replace", "find_value")
        spark_pool_validation = self._validate_parameter("spark_pool", "instance_pool_id")
        parameters = list(self.environment_parameter.keys())

        if len(parameters) == 1:
            parameter = parameters[0]
            if parameter == "find_replace":
                return find_replace_validation
            if parameter == "spark_pool":
                return spark_pool_validation

        return spark_pool_validation and find_replace_validation

    def _validate_parameter(self, param_name: str, find_key_name: str) -> bool:
        """Validates the specified parameter."""
        if not self.environment_parameter.get(param_name):
            return False

        logger.info(f"Validating {param_name} parameter")
        msg_header = f"Error in {param_name}:"

        for parameter_dict in self.environment_parameter[param_name]:
            # Validate parameter keys
            parameter_keys = tuple(parameter_dict.keys())
            if not self._validate_parameter_keys(param_name, parameter_keys, msg_header):
                return False

            # Validate values for find_value and replace_value keys
            if not parameter_dict[find_key_name]:
                logger.debug(f"{msg_header} Missing value for {find_key_name} key")
                return False

            if not self._validate_replace_value(parameter_dict, param_name, msg_header):
                return False

            # Validate values for optional parameters
            if not self._validate_optional_parameters(parameter_dict, param_name, msg_header):
                logger.warning(f"{msg_header} Optional parameters contain invalid values")

        logger.info(f"{param_name} parameter validation passed")
        return True

    def _validate_parameter_keys(self, param_name: str, param_keys: tuple, msg: str) -> bool:
        """Validates the keys in the specified parameter."""
        if param_name == "find_replace":
            minimum_keys = ("find_value", "replace_value")
            maximum_keys = ("find_value", "replace_value", "item_type", "item_name", "file_path")

        if param_name == "spark_pool":
            minimum_keys = ("instance_pool_id", "replace_value")
            maximum_keys = ("instance_pool_id", "replace_value", "item_name")

        minimum_set = set(minimum_keys)
        maximum_set = set(maximum_keys)
        param_keys_set = set(param_keys)

        # if minimum_set is not a subset of key_set, return False
        if not minimum_set <= param_keys_set:
            logger.debug(f"{msg} Missing required keys")
            return False

        # if key_set is not a subset of maximum_set, return False
        if not param_keys_set <= maximum_set:
            logger.debug(f"{msg} Invalid keys found")
            return False

        return True

    def _validate_replace_value(self, param_dict: dict, param_name: str, msg: str) -> bool:
        """Validates replace_value for the specified parameter."""
        check_data_types = {}
        replace_value_dict = param_dict["replace_value"]

        # Check if replace_value dictionary exists
        if not replace_value_dict:
            logger.debug(f"{msg} Missing value for 'replace_value' key")
            return False

        # Check if target environment exists in replace_value dictionary
        if not self._validate_environment(replace_value_dict):
            logger.warning(
                f"Target environment: '{self.environment}' does not exist as a key in 'replace_value' in {param_name}"
            )

        # Validate required values in replace_value
        if param_name == "find_replace":
            check_data_types["find_value"] = param_dict["find_value"]

            for environment in replace_value_dict:
                if not replace_value_dict[environment]:
                    logger.debug(f"{msg} Missing replace_value for {environment}")
                    return False
                check_data_types[environment] = replace_value_dict[environment]

        if param_name == "spark_pool":
            for environment, environment_dict in replace_value_dict.items():
                # Check if environment_dict is empty
                if not environment_dict:
                    logger.debug(f"{msg} Missing replace_value for {environment} environment")
                    return False

                # Validate keys for the environment
                config_keys = tuple(environment_dict.keys())
                for key in config_keys:
                    if key not in ["type", "name"]:
                        logger.debug(f"{msg} '{key}' is an invalid key in {environment} environment")
                        return False
                    if key != "type" and key != "name":
                        logger.debug(f"{msg} Missing 'type' and/or 'name' key in {environment} environment")
                        return False
                    if key == "type" and environment_dict[key] not in ["Capacity", "Workspace"]:
                        logger.debug(
                            f"{msg} '{environment_dict[key]}' is an invalid value for 'type' key in {environment} environment"
                        )
                        return False

            check_data_types["instance_pool_id"] = param_dict["instance_pool_id"]
            check_data_types["type"] = environment_dict["type"]
            check_data_types["name"] = environment_dict["name"]

        for key, value in check_data_types.items():
            if not isinstance(value, str):
                logger.debug(f"{msg} Value {value} must be string type for '{key}'")
                return False

        return True

    def _validate_optional_parameters(self, param_dict: dict, param_name: str, msg: str) -> bool:
        """Validates the optional parameter values for the specified parameter."""
        data_types = Union[str, list, None]

        if param_name == "find_replace":
            item_type = param_dict.get("item_type")
            item_name = param_dict.get("item_name")
            file_path = param_dict.get("file_path")

            check_data_types = {
                "item_type": item_type,
                "item_name": item_name,
                "file_path": file_path,
            }

            # Validate data types before checking actual values
            for key, value in check_data_types.items():
                if not isinstance(value, data_types):
                    logger.debug(f"{msg} Optional value {value} must be string or list type for '{key}'")
                    return False

            if item_type and not self._validate_item_type(item_type):
                logger.debug(f"{msg} Item type '{item_type}' is not in scope")
                return False
            if item_name and not self._validate_item_name(item_name):
                logger.debug(f"{msg} '{item_name}' is not found in the repository directory")
                return False
            if file_path and not self._validate_file_path(file_path):
                logger.debug(f"{msg} '{file_path}' is not found in the repository directory")
                return False

        if param_name == "spark_pool":
            item_name = param_dict.get("item_name")

            # Validate data type before checking actual value
            if not isinstance(item_name, data_types):
                logger.debug(f"{msg} Optional value {item_name} must be string or list type for '{key}'")
                return False

            if item_name and not self._validate_item_name(item_name):
                logger.debug(f"{msg} '{item_name}' is not found in the repository directory")
                return False

        return True

    def _validate_environment(self, replace_dict: dict) -> bool:
        """Checks the target environment exists as a key in the replace_value dictionary."""
        if self.environment == "N/A":
            return True
        return self.environment in replace_dict

    def _validate_item_type(self, input_type: str) -> bool:
        """Validates the item type is in scope."""
        if isinstance(input_type, list):
            return all(item_type in self.item_type_in_scope for item_type in input_type)

        return bool(input_type in self.item_type_in_scope)

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
            return all(item_name in item_name_list for item_name in input_name)

        return bool(input_name in item_name_list)

    def _validate_file_path(self, input_path: Union[Path, list, None]) -> bool:
        """Validates the file path exists."""
        # Convert input path to Path object
        input_path = process_input_path(self.repository_directory, input_path)

        # Check if path exists
        if isinstance(input_path, list):
            return all(Path(path).exists() for path in input_path)

        return input_path.exists()
