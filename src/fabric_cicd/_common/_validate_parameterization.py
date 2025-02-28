import logging
import re
from pathlib import Path
from typing import Optional, Union

import yaml

logger = logging.getLogger(__name__)


def load_parameters_to_dict(param_dict: dict, yaml_content: str, file_name: Path) -> dict:
    """Validates and loads the parameter file into a dictionary."""
    if not yaml_content:
        logger.error(f"Error loading {file_name}: No content found in file.")
        return param_dict
    try:
        param_dict = yaml.safe_load(yaml_content)
        logger.info(f"Successfully loaded {file_name}")
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
    """Determines if the parameter dictionary uses the new structure, i.e. a list of values when indexed by the key."""
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
    """Determines if a replacement condition is met."""
    # Condition 1: Zero optional inputs present
    if not input_type and not input_name and not input_path and not input_file_regex:
        print("zero optional inputs present")
        return True

    # Condition 2: Regex input is present, ignore other optional inputs
    if input_file_regex:
        print("input_file_regex present")
        return _check_regex_match(input_file_regex, str(file_path))

    # Otherwise, set conditions for optional parameters based on input value type
    item_type_match = _set_replacement_conditions(input_type, item_type)
    item_name_match = _set_replacement_conditions(input_name, item_name)
    file_path_match = _set_replacement_conditions(input_path, file_path, "path")

    # List of conditions for replacement
    replace_conditions = [
        # Condition 3: Type, Name, and Path inputs are present and match
        (item_type_match and item_name_match and file_path_match),
        # Condition 4: Only Type and Name inputs are present and match
        (item_type_match and item_name_match and not input_path),
        # Condition 5: Only Type and Path inputs are present and match
        (item_type_match and file_path_match and not input_name),
        # Condition 6: Only Name and Path inputs are present and match
        (item_name_match and file_path_match and not input_type),
        # Condition 7: Only Type input is present and matches
        (item_type_match and not input_name and not input_path),
        # Condition 8: Only Name input is present and matches
        (item_name_match and not input_type and not input_path),
        # Condition 9: Only Path input is present and matches
        (file_path_match and not input_type and not input_name),
    ]
    print("other optional inputs present")
    return any(replace_conditions)


def _set_replacement_conditions(
    input_value: Union[str, Path, list[Union[str, Path]], None],
    compare_value: Union[str, Path],
    input_type: Optional[str] = None,
) -> bool:
    """Sets the proper replacement condition based on input_value type."""
    if input_type == "path":
        input_value = _resolve_input_path(input_value)

    if isinstance(input_value, list):
        input_value_condition = any(compare_value == item for item in input_value)
    elif isinstance(input_value, (str, Path)):
        input_value_condition = compare_value == input_value
    else:
        input_value_condition = False

    return input_value_condition


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


def _check_regex_match(input_regex: Union[str, list], path_string: str) -> bool:
    """Determines if a regex pattern matches the file path."""
    if input_regex:
        if isinstance(input_regex, list):
            matches = {}
            for regex in input_regex:
                matches[regex] = re.findall(regex, path_string)
            for match in matches.values():
                if match:
                    return True
        return bool(re.search(input_regex, path_string))
    logger.debug(f"No match found for pattern '{input_regex}'")
    return False
