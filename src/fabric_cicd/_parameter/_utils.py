# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Following functions are parameter utilities used by the FabricWorkspace and Parameter classes,
and for debugging the parameter file. The utilities include validating the parameter.yml file, determining
parameter dictionary structure, processing parameter values, and handling parameter value replacements.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Optional, Union

from azure.core.credentials import TokenCredential
from jsonpath_ng.ext import parse

import fabric_cicd.constants as constants

logger = logging.getLogger(__name__)


def find_regex_value(find_regex: str, file_content: str) -> str:
    """A function to find a value in a file using a regex pattern."""
    # Check if the find_pattern is a valid regex
    try:
        regex = re.compile(find_regex)
        match = re.search(regex, file_content)
        if match:
            return match.group(1)

    except re.error as e:
        msg = f"Invalid regex pattern: {find_regex}"
        raise ValueError(msg) from e

    return find_regex


def replace_key_value(param_dict: dict, json_content: str, env: str) -> Union[dict]:
    """A function to replace key values in a JSON using parameterization. It uses jsonpath_ng to find and replace values in the JSON.

    Args:
        param_dict: The parameter dictionary.
        json_content: the JSON content to be modified.
        env: The environment variable to be used for replacement.
    """
    # Try to load the json content to a dictionary
    try:
        data = json.loads(json_content)
    except json.JSONDecodeError as jde:
        raise ValueError(jde) from jde

    # Extract the jsonpath expression from the find_key attribute of the param_dict
    jsonpath_expr = parse(param_dict["find_key"])
    for match in jsonpath_expr.find(data):
        # If the env is present in the replace_value array perform the replacement
        if env in param_dict["replace_value"]:
            try:
                match.full_path.update(data, param_dict["replace_value"][env])
            except Exception as match_e:
                raise ValueError(match_e) from match_e

    return json.dumps(data)


def replace_variables_in_parameter_file(raw_file: str) -> str:
    """
    A function to replace tokens in the parameter.yml file with environment variables.

    Args:
    raw_file: The parameter.yml file content as a string.
    """
    if "enable_environment_variable_replacement" in constants.FEATURE_FLAG:
        # filter os.environ dict to only allow variables that begin with $ENV:
        env_vars = {k[len("$ENV:") :]: v for k, v in os.environ.items() if k.startswith("$ENV:")}
        # block of code to support both variants of the parameters.yml file

        # Perform replacements
        for var_name, var_value in env_vars.items():
            placeholder = f"$ENV:{var_name}"
            if placeholder in raw_file:
                raw_file = raw_file.replace(placeholder, var_value)
                logger.debug(f"Replaced {placeholder} with {var_value}")

        return raw_file
    return raw_file


def validate_parameter_file(
    repository_directory: str,
    item_type_in_scope: list,
    environment: str = "N/A",
    parameter_file_name: str = "parameter.yml",
    token_credential: TokenCredential = None,
) -> bool:
    """
    A wrapper function that validates a parameter.yml file, using
    the Parameter class.

    Args:
        repository_directory: The directory containing the items and parameter.yml file.
        item_type_in_scope: A list of item types to validate.
        environment: The target environment.
        parameter_file_name: The name of the parameter file, default is "parameter.yml".
        token_credential: The token credential to use for authentication, use for SPN auth.
    """
    from azure.identity import DefaultAzureCredential

    from fabric_cicd._common._fabric_endpoint import FabricEndpoint
    from fabric_cicd._common._validate_input import (
        validate_environment,
        validate_item_type_in_scope,
        validate_repository_directory,
        validate_token_credential,
    )

    # Import the Parameter class here to avoid circular imports
    from fabric_cicd._parameter._parameter import Parameter

    endpoint = FabricEndpoint(
        # if credential is not defined, use DefaultAzureCredential
        token_credential=(
            DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
        )
    )
    # Initialize the Parameter object with the validated inputs
    parameter_obj = Parameter(
        repository_directory=validate_repository_directory(repository_directory),
        item_type_in_scope=validate_item_type_in_scope(item_type_in_scope, upn_auth=endpoint.upn_auth),
        environment=validate_environment(environment),
        parameter_file_name=parameter_file_name,
    )
    # Validate with _validate_parameter_file() method
    return parameter_obj._validate_parameter_file()


def is_valid_structure(param_dict: dict, param_name: Optional[str] = None) -> bool:
    """
    Checks the parameter dictionary structure and determines if it
    contains the valid structure (i.e. a list of values when indexed by the key).

    Args:
        param_dict: The parameter dictionary to check.
        param_name: The name of the parameter to check, if specified.
    """
    # Check the structure of the specified parameter
    if param_name:
        return _check_parameter_structure(param_dict.get(param_name))

    # Otherwise, check the structure of the entire parameter dictionary
    param_structure = [
        _check_parameter_structure(param_dict.get(name))
        for name in ["find_replace", "spark_pool"]
        if param_dict.get(name)
    ]
    # Check structure if only one parameter is found
    if len(param_structure) == 1:
        return param_structure[0]
    # Check structure if both parameters are found
    if len(param_structure) == 2 and param_structure[0] == param_structure[1]:
        return param_structure[0]

    return False


def _check_parameter_structure(param_value: any) -> bool:
    """Checks the structure of a parameter value"""
    return isinstance(param_value, list)


def process_input_path(
    repository_directory: Path, input_path: Union[str, list[str], None]
) -> Union[Path, list[Path], None]:
    """
    Processes the input_path value according to its type.

    Args:
        repository_directory: The directory of the repository.
        input_path: The input path value to process (None value, a string value, or list of string values).
    """
    if not input_path:
        return input_path

    if isinstance(input_path, list):
        return [_convert_value_to_path(repository_directory, path) for path in input_path]

    return _convert_value_to_path(repository_directory, input_path)


def _convert_value_to_path(repository_directory: Path, input_path: str) -> Path:
    """
    Converts the input_path string value to a Path object
    and resolves a relative path as an absolute path, if present.
    """
    if not Path(input_path).is_absolute():
        # Strip leading slashes or backslashes
        normalized_path = Path(input_path.lstrip("/\\"))
        # Set the absolute path
        absolute_path = repository_directory / normalized_path
        if absolute_path.exists():
            logger.debug(f"Relative path '{input_path}' resolved as '{absolute_path}'")
        else:
            logger.debug(f"Relative path '{input_path}' does not exist, provide a valid path")

        return absolute_path

    absolute_path = Path(input_path)
    if not absolute_path.exists():
        logger.debug(f"Absolute path '{input_path}' does not exist, provide a valid path")

    return absolute_path


def check_replacement(
    input_type: Union[str, list[str], None],
    input_name: Union[str, list[str], None],
    input_path: Union[Path, list[str], None],
    item_type: str,
    item_name: str,
    file_path: Path,
) -> bool:
    """
    Determines whether a find and replace is applied or not based on the provided optional filters.

    Args:
        input_type: The input item_type value to check.
        input_name: The input item_name value to check.
        input_path: The input file_path value to check.
        item_type: The item_type value to compare with.
        item_name: The item_name value to compare with.
        file_path: The file_path value to compare with.
    """
    # No optional parameters found
    if not input_type and not input_name and not input_path:
        logger.debug("No optional filters found. Find and replace applied in this repository file")
        return True

    # Otherwise, find matches for the optional parameters
    logger.debug("Optional filters found. Checking for matches")

    item_type_match = _find_match(input_type, item_type)
    item_name_match = _find_match(input_name, item_name)
    file_path_match = _find_match(input_path, file_path)

    if item_type_match and item_name_match and file_path_match:
        if input_type:
            logger.debug(f"Item type match found: {item_type_match}")
        if input_name:
            logger.debug(f"Item name match found: {item_name_match}")
        if input_path:
            logger.debug(f"File path match found: {file_path_match}")

        logger.debug("Optional filters match found. Find and replace applied in this repository file")
        return True

    logger.debug("Optional filters match not found. Find and replace skipped for this repository file")
    return False


def _find_match(
    param_value: Union[str, list, Path, None],
    compare_value: Union[str, Path],
) -> bool:
    """
    Checks for a match between the parameter value and
    the compare value based on parameter value type.

    Args:
        param_value: The parameter value to compare (can be a string, list, Path, or None type).
        compare_value: The value to compare with.
    """
    # If no parameter value, checking for matches is not required
    if not param_value:
        return True

    # Otherwise, check for matches based on the parameter value type
    if isinstance(param_value, list):
        match_condition = any(compare_value == value for value in param_value)
    elif isinstance(param_value, (str, Path)):
        match_condition = compare_value == param_value
    else:
        match_condition = False

    return match_condition
