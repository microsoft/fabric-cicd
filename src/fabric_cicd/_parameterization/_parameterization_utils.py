# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Following functions are parameterization utilities used by the FabricWorkspace and
ParameterValidation classes. The utilities include loading the parameter.yml file, determining
parameter dictionary structure and managing parameter value replacements.
"""

import logging
import re
from pathlib import Path
from typing import Optional, Union

import yaml

logger = logging.getLogger(__name__)


def load_parameters_to_dict(param_dict: dict, param_file_path: Path, param_file_name: str) -> dict:
    """
    Loads the parameter file to a dictionary.

    Args:
        param_dict: The dictionary to load the parameter file into.
        param_file_path: The path to the parameter file.
        param_file_name: The name of the parameter file.
    """
    if not Path(param_file_path).is_file():
        logger.warning(f"No parameter file found with path: {param_file_path}")
        return param_dict
    try:
        logger.info(f"Found parameter file '{param_file_name}'")
        with Path.open(param_file_path) as yaml_file:
            yaml_content = yaml_file.read()

            logger.info(f"Validating {param_file_name} content")
            validation_errors = _validate_yaml(yaml_content)
            if validation_errors:
                for error in validation_errors:
                    logger.error(f"Validation error in {param_file_name}: {error}")
                    return param_dict

            param_dict = yaml.full_load(yaml_content)
            logger.info(f"Successfully loaded {param_file_name}")

            # Log a warning for old parameter file structure
            if check_parameter_structure(param_dict) == "old":
                logger.warning(
                    "The parameter file structure used will no longer be supported in a future version. Please update to the new structure"
                )
            return param_dict
    except yaml.YAMLError as e:
        logger.error(f"Error loading {param_file_name}: {e}")
        return param_dict


def _validate_yaml(content: str) -> list[str]:
    """
    Validates the content of a YAML file for invalid characters and unclosed brackets or quotes.

    Args:
        content: The content of the YAML file to validate.
    """
    errors = []

    # Check for invalid characters
    if not re.match(r"^[\x00-\x7F]*$", content):
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


def check_parameter_structure(param_dict: dict, param_name: Optional[str] = None) -> str:
    """
    Checks the parameter dictionary structure and determines if it
    contains the new structure (i.e. a list of values when indexed by the key).

    Args:
        param_dict: The parameter dictionary to check.
        param_name: The name of the parameter to check, if specified.
    """
    # Check if the specified parameter is a list (new structure)
    if param_name:
        if isinstance(param_dict.get(param_name), list):
            return "new"
        if isinstance(param_dict.get(param_name), dict):
            return "old"
        return "invalid"

    # Check if both 'find_replace' and 'spark_pool' are lists (new structure)
    if isinstance(param_dict.get("find_replace"), list) and isinstance(param_dict.get("spark_pool"), list):
        return "new"

    # Check if both 'find_replace' and 'spark_pool' are dictionaries (old structure)
    if isinstance(param_dict.get("find_replace"), dict) and isinstance(param_dict.get("spark_pool"), dict):
        return "old"

    # If neither condition is met, return 'invalid'
    return "invalid"


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
            logger.warning(f"Relative path '{input_path}' resolved as '{absolute_path}'")
        else:
            logger.warning(f"Relative path '{input_path}' does not exist, provide a valid path")
        return absolute_path

    absolute_path = Path(input_path)
    if not absolute_path.exists():
        logger.warning(f"Absolute path '{input_path}' does not exist, provide a valid path")
    return absolute_path


def check_replacement(
    input_type: Union[str, list, None],
    input_name: Union[str, list, None],
    input_path: Union[Path, list, None],
    item_type: str,
    item_name: str,
    file_path: Path,
) -> bool:
    """
    Determines if a replacement should happen based on the provided optional parameter values.

    Args:
        input_type: The input item_type value to check.
        input_name: The input item_name value to check.
        input_path: The input file_path value to check.
        item_type: The item_type value to compare with.
        item_name: The item_name value to compare with.
        file_path: The file_path value to compare with.
    """
    # Condition 1: No optional parameters provided
    if not input_type and not input_name and not input_path:
        logger.debug("No optional parameters were provided. Replacement can happen in any repository file")
        return True

    # Otherwise, find matches for the optional parameters
    item_type_match = _find_match(input_type, item_type)
    item_name_match = _find_match(input_name, item_name)
    file_path_match = _find_match(input_path, file_path)

    # Define match conditions for each parameter combination
    matches_dict = {
        "item_type, item_name, and file_path": (item_type_match and item_name_match and file_path_match),
        "item_type and item_name": (item_type_match and item_name_match and not file_path_match),
        "item_type and file_path": (item_type_match and file_path_match and not item_name_match),
        "item_name and file_path": (item_name_match and file_path_match and not item_type_match),
        "item_type": (item_type_match and not item_name_match and not file_path_match),
        "item_name": (item_name_match and not item_type_match and not file_path_match),
        "file_path": (file_path_match and not item_type_match and not item_name_match),
    }

    logger.debug("Optional parameters were provided. Checking for matches.")
    for param, replace_condition in matches_dict.items():
        if replace_condition:
            logger.debug(
                f"Match found for {param} parameter(s). Replacement may happen in specified repository file(s)"
            )
            return True
    else:
        logger.debug("No match found. Replacement will not happen")
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
    if isinstance(param_value, list):
        match_condition = any(compare_value == value for value in param_value)
    elif isinstance(param_value, (str, Path)):
        match_condition = compare_value == param_value
    else:
        match_condition = False

    return match_condition
