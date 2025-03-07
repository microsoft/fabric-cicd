# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Following functions are parameterization utilities used by the FabricWorkspace and
ParameterValidation classes. The utilities include loading the parameter.yml file, determining
parameter dictionary structure and managing parameter value replacements.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import yaml

logger = logging.getLogger(__name__)


def load_parameters_to_dict(param_dict: dict, param_file_path: Path, param_file_name: str) -> dict:
    """Loads the parameter file to a dictionary."""
    if not Path(param_file_path).is_file():
        logger.debug(f"No parameter file found with path: {param_file_path}")
        return param_dict
    try:
        logger.info(f"Found parameter file '{param_file_name}'")
        with Path.open(param_file_path) as yaml_file:
            yaml_file_content = yaml_file.read()
            param_dict = yaml.full_load(yaml_file_content)
            logger.info(f"Successfully loaded {param_file_name}")
            # Check if the parameter dictionary contains the new structure
            for key in param_dict:
                if not new_parameter_structure(param_dict, key):
                    logger.warning(
                        "The parameter file structure used will no longer be supported in a future version. Please update to the new structure"
                    )
                break
            return param_dict
    except yaml.YAMLError as e:
        logger.error(f"Error loading {param_file_name}: {e}")
        return param_dict


def new_parameter_structure(param_dict: dict, key: Optional[str] = None) -> bool:
    """Checks if the parameter dictionary contains the new structure (a list of values when indexed by the key)."""
    if key:
        return isinstance(param_dict[key], list)

    return all(isinstance(param_dict[param], list) for param in param_dict)


def process_input_path(repository_directory: Path, input_path: Union[str, list]) -> Union[Path, list]:
    """Processes the input_path value according to its type."""
    if isinstance(input_path, list):
        return [_convert_to_file_path(repository_directory, path) for path in input_path]

    return _convert_to_file_path(repository_directory, input_path)


def _convert_to_file_path(repository_directory: Path, input_path: str) -> Path:
    """Converts the input_path to a Path object and ensures a relative path gets resolved as an absolute path."""
    if not Path(input_path).is_absolute():
        # Strip leading slashes or backslashes to normalize the path
        normalized_path = input_path.lstrip("/\\")
        absolute_path = repository_directory / Path(normalized_path)
        if Path(absolute_path).exists():
            logger.warning(f"Relative path '{input_path}' resolved as '{absolute_path}'")
            return Path(absolute_path)

    if not Path(input_path).exists():
        logger.error(f"File '{input_path}' not found, please provide a valid file path")
        return input_path

    return Path(input_path)


def check_replacement(
    input_type: Union[str, list, None],
    input_name: Union[str, list, None],
    input_path: Union[Path, list, None],
    item_type: str,
    item_name: str,
    file_path: Path,
) -> bool:
    """Determines if a replacement should happen based on the provided parameters."""
    # Condition 1: No optional parameters
    if not input_type and not input_name and not input_path:
        logger.debug("No optional parameters were provided. Replace can happen in any file.")
        return True

    # Otherwise, set conditions for the optional parameters
    item_type_match = _find_match(input_type, item_type)
    item_name_match = _find_match(input_name, item_name)
    file_path_match = _find_match(input_path, file_path)

    # List of conditions for replacement
    replace_conditions = [
        # Condition 2: Type, Name, and Path values are present and match
        (item_type_match and item_name_match and file_path_match),
        # Condition 3: Only Type and Name values are present and match
        (item_type_match and item_name_match and not input_path),
        # Condition 4: Only Type and Path values are present and match
        (item_type_match and file_path_match and not input_name),
        # Condition 5: Only Name and Path values are present and match
        (item_name_match and file_path_match and not input_type),
        # Condition 6: Only Type value is present and matches
        (item_type_match and not input_name and not input_path),
        # Condition 7: Only Name value is present and matches
        (item_name_match and not input_type and not input_path),
        # Condition 8: Only Path value is present and matches
        (file_path_match and not input_type and not input_name),
    ]
    logger.debug("Optional parameters were provided. Checking for file matches.")
    return any(replace_conditions)


def _find_match(
    parameter_value: Union[str, list, Path, None],
    compare_value: Union[str, Path],
) -> bool:
    """Checks for a match between the parameter value and the compare value based on parameter value type."""
    # Set match condition based on whether the parameter value is a list, string, Path, or None
    if isinstance(parameter_value, list):
        match_condition = any(compare_value == item for item in parameter_value)
    elif isinstance(parameter_value, (str, Path)):
        match_condition = compare_value == parameter_value
    else:
        match_condition = False
    return match_condition
