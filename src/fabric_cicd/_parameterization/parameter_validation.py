# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the ParameterValidation class to validate the parameter file used for deployment configurations."""

import json
import logging
import os
from pathlib import Path

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._parameterization._parameterization_utils import load_parameters_to_dict, new_parameter_structure

# Configure logging to output to the console
logger = logging.getLogger(__name__)


class ParameterValidation:
    """A class to validate the parameter file."""

    def __init__(
        self,
        repository_directory: str,
        item_type_in_scope: list[str],
        environments: list[str],
        token_credential: TokenCredential = None,
    ) -> None:
        """
        Initializes the ParameterValidation instance.

        Args:
            repository_directory: Local directory path of the repository where items are to be deployed from.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            environments: The environments to be used for parameterization.
            token_credential: The token credential to use for API requests.
        """
        from fabric_cicd._common._validate_input import (
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
        self.environments = environments

    def validate_parameters(self) -> bool:
        """Validates parameters"""
        # If parameter file could not be loaded, return False
        if not self._validate_parameter_load():
            logger.debug("Parameter file is not found or invalid.")
            return False

        # Otherwise, validate the parameter dictionary
        if self._validate_parameter_keys():
            logger.debug("Parameter keys are valid.")
            # If both find_replace and spark_pool parameters are present, validate them
            if self._validate_both_parameter_keys() and self._validate_find_replace_parameter():
                logger.debug("Find replace parameter is valid.")
                if self._validate_spark_pool_parameter():
                    logger.debug("Spark pool parameter is valid.")
                    return True
            # If find_replace or spark_pool parameter is present, validate it
            for key in self.environment_parameter:
                if key == "find_replace" and self._validate_find_replace_parameter():
                    logger.debug("Find replace parameter is valid.")
                    return True
                if key == "spark_pool" and self._validate_spark_pool_parameter():
                    logger.debug("Spark pool parameter is valid.")
                    return True

        logger.error("Parameter validation failed.")
        return False

    def _validate_parameter_load(self) -> bool:
        """Validate parameter file load."""
        parameter_file_path = Path(self.repository_directory, "parameter.yml")
        self.environment_parameter = {}

        if Path(parameter_file_path).is_file():
            logger.info(f"Found parameter file '{parameter_file_path}'")
            with Path.open(parameter_file_path) as yaml_file:
                yaml_file_content = yaml_file.read()
                self.environment_parameter = load_parameters_to_dict(
                    self.environment_parameter, yaml_file_content, "parameter.yml"
                )

        return bool(self.environment_parameter)

    def _validate_parameter_keys(self) -> bool:
        """Validate the keys in the parameter dictionary."""
        return bool(key in ["find_replace", "spark_pool"] for key in self.environment_parameter)

    def _validate_both_parameter_keys(self) -> bool:
        """Validates whether both find_replace and spark_pool keys are present in the parameter dictionary."""
        return bool("find_replace" in self.environment_parameter and "spark_pool" in self.environment_parameter)

    def _validate_find_replace_parameter(self) -> bool:
        """Validate the find_replace parameter."""
        if new_parameter_structure(self.environment_parameter, "find_replace"):
            return self._validate_find_replace_new()
        return self._validate_find_replace_old()

    def _validate_spark_pool_parameter(self) -> bool:
        """Validate the spark_pool parameter."""
        if new_parameter_structure(self.environment_parameter, "spark_pool"):
            return self._validate_spark_pool_new()
        return self._validate_spark_pool_old()

    def _validate_new_parameter_structure(self, key: str) -> bool:
        """Returns True if the parameter dictionary contains the new structure, i.e. a list of values when indexed by the key."""
        return isinstance(self.environment_parameter[key], list)

    def _validate_find_replace_new(self) -> bool:
        """Validates the find_replace parameter based on the new structure."""
        for parameter_dict in self.environment_parameter["find_replace"]:
            # Validate required keys
            if not parameter_dict["find_value"] or not parameter_dict["replace_value"]:
                return False
            for environment in parameter_dict["replace_value"]:
                if environment not in self.environments:
                    return False
                if not parameter_dict["replace_value"][environment]:
                    return False
            # Validate optional keys
            item_type = parameter_dict.get("item_type")
            item_name = parameter_dict.get("item_name")
            file_path = parameter_dict.get("file_path")

            if item_type and not self._validate_item_type(item_type):
                return False
            if item_name and not self._validate_item_name(item_name):
                return False
            if file_path and not self._validate_file_path(file_path):
                return False

        return True

    def _validate_find_replace_old(self) -> bool:
        """Validates the find_replace parameter based on the old structure."""
        for find_value, parameter_dict in self.environment_parameter["find_replace"].items():
            if not find_value or not parameter_dict:
                return False
            for environment in parameter_dict:
                if environment not in self.environments:
                    return False
                for value in parameter_dict[environment]:
                    if not value:
                        return False
        return True

    def _validate_spark_pool_new(self) -> bool:
        """Validates the spark_pool parameter based on the new structure."""
        for parameter_dict in self.environment_parameter["spark_pool"]:
            # Validate required keys
            if not parameter_dict["instance_pool_id"] or not parameter_dict["replace_value"]:
                return False
            for environment in parameter_dict["replace_value"]:
                if environment not in self.environments:
                    return False
                for value in parameter_dict["replace_value"][environment]:
                    if not value:
                        return False
                    if value == "type" and parameter_dict["replace_value"][environment][value] not in [
                        "Capacity",
                        "Workspace",
                    ]:
                        return False
                    if value == "name" and not parameter_dict["replace_value"][environment][value]:
                        return False
            # Validate optional keys
            item_name = parameter_dict.get("item_name")
            if item_name and not self._validate_item_name(item_name):
                return False

        return True

    def _validate_spark_pool_old(self) -> bool:
        """Validates the spark_pool parameter based on the old structure."""
        parameter_dict = self.environment_parameter["spark_pool"]
        for pool_id in parameter_dict:
            if not pool_id or not parameter_dict[pool_id]:
                return False
            for value in parameter_dict[pool_id]:
                if not value:
                    return False
                if value == "type" and parameter_dict[pool_id][value] not in [
                    "Capacity",
                    "Workspace",
                ]:
                    return False
                if value == "name" and not parameter_dict[pool_id][value]:
                    return False
        return True

    def _validate_item_type(self, input_type: str) -> bool:
        """Validates the item type."""
        return bool(input_type in self.item_type_in_scope)

    def _validate_item_name(self, input_name: str) -> bool:
        """Validates the item name by scanning item metadata in the repository directory."""
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

        return bool(input_name in item_name_list)

    def _validate_file_path(self, input_path: str) -> bool:
        """Validates the file path."""
        input_path = Path(input_path)

        if not input_path.is_absolute():
            absolute_input_path = Path(self.repository_directory, input_path)
            input_path = absolute_input_path

        return bool(input_path.exists())
