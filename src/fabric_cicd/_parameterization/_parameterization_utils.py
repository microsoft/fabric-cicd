import logging
import re
from pathlib import Path
from typing import Optional, Union

import yaml

logger = logging.getLogger(__name__)

"""Prameterization processing and validation functions."""


def load_parameters_to_dict(param_dict: dict, yaml_content: str, file_name: Path) -> dict:
    """Validates and loads the parameter file into a dictionary."""
    if not yaml_content:
        logger.error(f"Error loading {file_name}: No content found in file.")
        return param_dict
    try:
        param_dict = yaml.safe_load(yaml_content)
        logger.info(f"Successfully loaded {file_name}")
        # Check if the parameter dictionary follows the new structure
        param_dict_keys = param_dict.keys()
        for key in param_dict_keys:
            if not new_parameter_structure(param_dict, key):
                logger.warning(
                    "The parameter file structure used will no longer be supported in a future version. Please update to the new structure."
                )
                break
        return param_dict
    except yaml.YAMLError as e:
        logger.error(f"Error loading {file_name}: {e}")
        return param_dict


def new_parameter_structure(param_dict: dict, key: str) -> bool:
    """Checks if the parameter dictionary contains the new structure, i.e. a list of values when indexed by the key."""
    return isinstance(param_dict[key], list)


def check_replacement(
    input_type: Union[str, list, None],
    input_name: Union[str, list, None],
    input_path: Union[str, list, None],
    input_file_regex: Union[str, list, None],
    item_type: str,
    item_name: str,
    file_path: str,
) -> bool:
    """Determines if a replacement should happen based on the provided parameters."""
    # Condition 1: No optional parameters
    if not input_type and not input_name and not input_path and not input_file_regex:
        logger.debug("No optional parameters were provided. Replace can happen in any file.")
        return True

    # Condition 2: Regex parameter provided, ignore other optional parameters
    if input_file_regex:
        logger.debug("file_regex parameter was provided. Checking for file matches.")
        return _check_regex_match(input_file_regex, str(file_path))

    # Otherwise, set conditions for the other optional parameters
    item_type_match = _check_match(input_type, item_type)
    item_name_match = _check_match(input_name, item_name)
    file_path_match = _check_match(input_path, file_path, "path")

    # List of conditions for replacement
    replace_conditions = [
        # Condition 3: Type, Name, and Path values are present and match
        (item_type_match and item_name_match and file_path_match),
        # Condition 4: Only Type and Name values are present and match
        (item_type_match and item_name_match and not input_path),
        # Condition 5: Only Type and Path values are present and match
        (item_type_match and file_path_match and not input_name),
        # Condition 6: Only Name and Path values are present and match
        (item_name_match and file_path_match and not input_type),
        # Condition 7: Only Type value is present and matches
        (item_type_match and not input_name and not input_path),
        # Condition 8: Only Name value is present and matches
        (item_name_match and not input_type and not input_path),
        # Condition 9: Only Path value is present and matches
        (file_path_match and not input_type and not input_name),
    ]
    logger.debug("Other optional parameters were provided. Checking for file matches.")
    return any(replace_conditions)


def _check_regex_match(input_regex: Union[str, list], path_string: str) -> bool:
    """Checks if the regex pattern matches the file path."""
    if input_regex:
        if isinstance(input_regex, list):
            matches = {}
            for regex in input_regex:
                matches[regex] = re.findall(regex, path_string)
            for match in matches.values():
                if match:
                    return True
        return re.search(input_regex, path_string)
    logger.debug(f"No match found for pattern '{input_regex}'")
    return False


def _check_match(
    parameter_value: Union[str, Path, list[Union[str, Path]], None],
    compare_value: Union[str, Path],
    value_type: Optional[str] = None,
) -> bool:
    """Checks for a match between the parameter value and the compare value based on parameter value type."""
    if value_type == "path":
        # Ensures the absolute path is set for a parameter path input
        parameter_value = _resolve_input_path(parameter_value)

    if isinstance(parameter_value, list):
        match_condition = any(compare_value == item for item in parameter_value)

    elif isinstance(parameter_value, (str, Path)):
        match_condition = compare_value == parameter_value

    else:
        match_condition = False

    return match_condition


def _resolve_input_path(input_path: Union[str, list[str], None]) -> Union[Path, list[Path]]:
    """Resolves input path as an absolute path."""
    if isinstance(input_path, list):
        input_path = [Path(path) for path in input_path]
        for path in input_path:
            if not path.is_absolute():
                absolute_input_path = path.resolve()
                if not absolute_input_path.exists():
                    logger.error(f"File '{absolute_input_path}' not found, please provide a valid file path.")
                else:
                    logger.warning(f"Relative file path '{path}' resolved as '{absolute_input_path}'")

                input_path[input_path.index(path)] = absolute_input_path

    elif isinstance(input_path, str):
        input_path = Path(input_path)
        if not input_path.is_absolute():
            absolute_input_path = input_path.resolve()
            if not absolute_input_path.exists():
                logger.error(f"File '{absolute_input_path}' not found, please provide a valid file path.")
            else:
                logger.warning(f"Relative file path '{input_path}' resolved as '{absolute_input_path}'")

            input_path = absolute_input_path

    return input_path
