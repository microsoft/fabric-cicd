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
        environment: str = "N/A",
        token_credential: TokenCredential = None,
    ) -> None:
        """
        Initializes the ParameterValidation instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from and parameter file lives.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environment: The environment to be used for parameterization.
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

    # Define expected keys for the find_replace and spark_pool parameters
    FIND_REPLACE_REQUIRED_KEYS = ("find_value", "replace_value")
    FIND_REPLACE_ALL_KEYS = ("find_value", "replace_value", "item_type", "item_name", "file_path")
    SPARK_POOL_REQUIRED_KEYS = ("instance_pool_id", "replace_value")
    SPARK_POOL_ALL_KEYS = ("instance_pool_id", "replace_value", "item_name")

    def validate_parameter_file(self) -> bool:
        """Validates the parameter file."""
        if self._validate_parameter_file_load():
            if not new_parameter_structure(self.environment_parameter):
                logger.warning("Validation skipped for old parameter structure")
                return True

            if self._validate_parameters():
                logger.info("Parameter file validation passed")
                return True

        logger.error("Parameter file validation failed")
        return False

    def _validate_parameter_file_load(self) -> bool:
        """Validates the parameter file load to a dictionary."""
        parameter_file_path = Path(self.repository_directory, "parameter.yml")
        self.environment_parameter = {}

        self.environment_parameter = load_parameters_to_dict(
            self.environment_parameter,
            parameter_file_path,
            "parameter.yml",
        )
        return bool(self.environment_parameter)

    def _validate_parameters(self) -> bool:
        """Validates the parameters in the parameter dictionary."""
        logger.info("Validating the parameters")

        if not all(key in ["find_replace", "spark_pool"] for key in self.environment_parameter):
            logger.error("Missing or Invalid parameters in parameter file")
            return False

        find_replace_validation = self._validate_find_replace_parameter()
        spark_pool_validation = self._validate_spark_pool_parameter()

        parameters = list(self.environment_parameter.keys())
        if len(parameters) == 1:
            parameter = parameters[0]
            if parameter == "find_replace":
                return find_replace_validation
            if parameter == "spark_pool":
                return spark_pool_validation

        return spark_pool_validation and find_replace_validation

    def _validate_find_replace_parameter(self) -> bool:
        """Validates the find_replace parameter."""
        if not self.environment_parameter.get("find_replace"):
            return False

        # Validate find_replace keys, required values and optional values
        logger.info("Validating find_replace parameter")
        msg_header = "find_replace validation issue:"

        for parameter_dict in self.environment_parameter["find_replace"]:
            parameter_keys = tuple(parameter_dict.keys())
            if not self._validate_parameter_keys(
                self.FIND_REPLACE_REQUIRED_KEYS, self.FIND_REPLACE_ALL_KEYS, parameter_keys, msg_header
            ):
                return False

            if not parameter_dict["find_value"]:
                logger.debug(f"{msg_header} Missing value for find_value key")
                return False

            if not self._validate_replace_value(parameter_dict, "find_replace", msg_header):
                return False

            if not self._validate_optional_parameters(parameter_dict, "find_replace", msg_header):
                logger.warning(f"{msg_header} Optional values are not valid")

        return True

    def _validate_spark_pool_parameter(self) -> bool:
        """Validates the spark_pool parameter."""
        if not self.environment_parameter.get("spark_pool"):
            return False

        # Validate spark_pool keys, required values and optional values
        logger.info("Validating spark_pool parameter")
        msg_header = "spark_pool validation issue:"

        for parameter_dict in self.environment_parameter["spark_pool"]:
            parameter_keys = tuple(parameter_dict.keys())
            if not self._validate_parameter_keys(
                self.SPARK_POOL_REQUIRED_KEYS, self.SPARK_POOL_ALL_KEYS, parameter_keys, msg_header
            ):
                return False

            if not parameter_dict["instance_pool_id"]:
                logger.debug(f"{msg_header} Missing value for instance_pool_id key")
                return False

            if not self._validate_replace_value(parameter_dict, "spark_pool", msg_header):
                return False

            if not self._validate_optional_parameters(parameter_dict, "spark_pool", msg_header):
                logger.warning(f"{msg_header} Optional values are not valid")

        return True

    def _validate_parameter_keys(self, minimum_keys: tuple, maximum_keys: tuple, param_keys: tuple, msg: str) -> bool:
        """Validates the keys in the parameter."""
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
        """Validates the values in replace_value."""
        required_values = []
        replace_value_dict = param_dict["replace_value"]

        if not replace_value_dict:
            logger.debug(f"{msg} Missing value for 'replace_value' key")
            return False

        if not self._validate_environment(replace_value_dict):
            logger.warning(
                f"Target environment: '{self.environment}' does not exist as a key in 'replace_value' in {param_name}"
            )

        if param_name == "find_replace":
            required_values.append(param_dict["find_value"])

            for environment in param_dict["replace_value"]:
                if not param_dict["replace_value"][environment]:
                    logger.debug(f"{msg} Missing replace_value for {environment} key")
                    return False

                required_values.append(param_dict["replace_value"][environment])

        if param_name == "spark_pool":
            required_values.append(param_dict["instance_pool_id"])

            for environment, value in param_dict["replace_value"].items():
                if not value:
                    logger.debug(f"{msg} Missing replace_value for {environment} key")
                    return False
                if not all(key in value for key in ["type", "name"]) or any(
                    key not in ["type", "name"] for key in value
                ):
                    logger.debug(f"{msg} Missing or Invalid key(s) found in replace_value")
                    return False
                if not value["type"] or value["type"] not in ["Capacity", "Workspace"]:
                    logger.debug(f"{msg} Missing or Invalid 'type' value in replace_value")
                    return False
                if not value["name"]:
                    logger.debug(f"{msg} Missing 'name' value in replace_value")
                    return False

                required_values.append(value["type"])
                required_values.append(value["name"])

        if not self._validate_data_type(required_values):
            logger.debug(f"{msg} Required values must be String type")
            return False

        return True

    def _validate_optional_parameters(self, parameter_dict: dict, param_name: str, msg: str) -> bool:
        """Validates the optional parameter values."""
        if param_name == "find_replace":
            item_type = parameter_dict.get("item_type")
            item_name = parameter_dict.get("item_name")
            file_path = parameter_dict.get("file_path")
            optional_values = [item_type, item_name, file_path]

            if not self._validate_data_type(optional_values, required=False):
                logger.debug(f"{msg} Provided optional values must be String or List type")
                return False
            if item_type and not self._validate_item_type(item_type):
                logger.debug(f"Item type '{item_type}' is not in scope")
                return False
            if item_name and not self._validate_item_name(item_name):
                logger.debug(f"{msg} '{item_name}' not found in the repository directory")
                return False
            if file_path and not self._validate_file_path(file_path):
                logger.debug("File path not found in the repository directory")
                return False

        if param_name == "spark_pool":
            item_name = parameter_dict.get("item_name")
            optional_values = [item_name]

            if not self._validate_data_type(optional_values, required=False):
                logger.debug(f"{msg} Provided optional values must be String or List type")
                return False
            if item_name and not self._validate_item_name(item_name):
                logger.debug(f"{msg} '{item_name}' is not found in the repository directory")
                return False

        return True

    def _validate_data_type(self, value_list: list, required: bool = True) -> bool:
        """Validates the required or optional value data types."""
        if not required:
            optional = Union[str, list, None]
            return all(isinstance(value, optional) for value in value_list)

        return all(isinstance(value, str) for value in value_list)

    def _validate_environment(self, replace_dict: dict) -> bool:
        """Validates the target environment exists as a key in the replace_value dictionary."""
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
        """Validates the file path by resolving a relative path if needed and checking if it exists."""
        output_path = process_input_path(self.repository_directory, input_path)

        if isinstance(output_path, list):
            return all(Path(path).exists() for path in output_path)

        return output_path.exists()
