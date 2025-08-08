# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Following functions are parameter utilities used by the FabricWorkspace and Parameter classes,
and for debugging the parameter file. The utilities include validating the parameter.yml file, determining
parameter dictionary structure, processing parameter values, and handling parameter value replacements.
"""

import glob
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional, Union

from azure.core.credentials import TokenCredential
from jsonpath_ng.ext import parse

import fabric_cicd.constants as constants
from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import InputError, ParsingError

logger = logging.getLogger(__name__)

"""Functions to extract parameter values"""


def extract_find_value(param_dict: dict, file_content: str, filter_match: bool) -> str:
    """
    Extracts the find_value and sets the value. Processes the find_value if a valid regex is provided.

    Args:
        param_dict: The parameter dictionary containing the find_value and is_regex keys.
        file_content: The content of the file where the find_value will be searched.
        filter_match: A boolean to check for a regex match in filtered files only.
    """
    find_value = param_dict.get("find_value")
    is_regex = param_dict.get("is_regex", "").lower() == "true"

    # Only process regex if enabled and file meets filter criteria
    if is_regex and filter_match:
        # Search for a match with the valid regex (validated in the parameter file validation step)
        regex = re.compile(find_value)
        match = re.search(regex, file_content)
        if match:
            if len(match.groups()) != 1:
                msg = f"Regex pattern '{find_value}' must contain exactly one capturing group."
                raise InputError(msg, logger)

            matched_value = match.group(1)
            if matched_value:
                return matched_value

            msg = f"Regex pattern '{find_value}' captured an empty value."
            raise InputError(msg, logger)

        logger.debug(f"No match found for regex '{find_value}' in the file content.")

    # For non-regex or non-matching filters, return the original value
    return find_value


def extract_replace_value(workspace_obj: FabricWorkspace, replace_value: str, get_dataflow_name: bool = False) -> str:
    """Extracts the replace_value and sets the value. Processes the replace_value if a valid variable is provided."""
    if not replace_value.startswith("$"):
        if get_dataflow_name:
            logger.debug(
                "Can't get dataflow name as the replace_value was set to a regular string, not the items variable"
            )
            return None
        return replace_value

    # If $workspace variable, return the workspace ID value
    if replace_value == "$workspace.id":
        if get_dataflow_name:
            msg = "Invalid replace_value variable format: '$workspace.id'. Expected format to get dataflow name: $items.type.name.attribute"
            raise InputError(msg, logger)

        return workspace_obj.workspace_id

    # If $items variable, return the item attribute value if found
    if replace_value.startswith("$items."):
        return _extract_item_attribute(workspace_obj, replace_value, get_dataflow_name)

    # Otherwise, raise an error for invalid variable syntax
    msg = f"Invalid replace_value variable format: '{replace_value}'. Expected format: $items.type.name.attribute or $workspace.id"
    raise InputError(msg, logger)


def _extract_item_attribute(workspace_obj: FabricWorkspace, variable: str, get_dataflow_name: bool) -> str:
    """
    Extracts the item attribute value from the $items variable to set as the replace_value.

    Args:
        workspace_obj: The FabricWorkspace object containing the workspace items dictionary used to access item metadata.
        variable: The $items variable string to be parsed and processed, format: $items.type.name.attribute (supported attributes: id and sqlendpoint).
        get_dataflow_name: A boolean flag to indicate if the dataflow item name should be returned instead of the attribute value.
    """
    error = None
    try:
        # Split the variable into 3 parts (item type, name, and attribute)
        var_parts = variable.removeprefix("$items.").split(".")
        if len(var_parts) != 3:
            msg = f"Invalid $items variable syntax: {variable}. Expected format: $items.type.name.attribute"
            error = ParsingError(msg, logger)
            return None

        item_type = var_parts[0].strip()
        item_name = var_parts[1].strip()
        attribute = var_parts[2].strip()

        # Validate attribute before further processing
        attr_name = attribute.lower()
        if attr_name not in constants.ITEM_ATTR_LOOKUP:
            msg = f"Attribute '{attribute}' is invalid. Supported attributes: {list(constants.ITEM_ATTR_LOOKUP)}"
            error = ParsingError(msg, logger)
            return None

        logger.debug(
            f"Processing $items variable with item_type={item_type}, item_name={item_name}, attribute={attribute}"
        )

        # Refresh the workspace items to get the latest deployed items
        workspace_obj._refresh_deployed_items()

        # Validate item type exists in the deployed workspace
        if item_type not in workspace_obj.workspace_items and not get_dataflow_name:
            msg = f"Item type '{item_type}' is invalid or not found in deployed items"
            error = ParsingError(msg, logger)
            return None

        # Check if the specific item is deployed
        if item_name not in workspace_obj.workspace_items.get(item_type, {}) and not get_dataflow_name:
            msg = f"Item '{item_name}' not found as a deployed {item_type}"
            error = ParsingError(msg, logger)
            return None

        # Special case: set to True in the context of a Dataflow that references another Dataflow
        if get_dataflow_name:
            if (
                item_type in workspace_obj.repository_items
                and item_type == "Dataflow"
                and item_name in workspace_obj.repository_items[item_type]
                and attribute == "id"
            ):
                logger.debug("Source Dataflow reference will be replaced separately")
                return item_name
            # Return None for non-existent item
            return None

        # Get the item's attributes from workspace items
        item_attr_values = workspace_obj.workspace_items[item_type][item_name]

        # Get the attribute value and check if it exists
        attr_value = item_attr_values.get(attr_name)
        if not attr_value:
            msg = f"Value does not exist for attribute '{attribute}' in the {item_type} item '{item_name}'"
            error = ParsingError(msg, logger)
            return None

        logger.debug(f"Found attribute '{attr_name}' with value '{attr_value}'")
        return attr_value

    except Exception as e:
        # If it's not a ParsingError, create a new one
        if not isinstance(e, ParsingError):
            error = ParsingError(f"Error parsing $items variable: {e!s}", logger)
        error = e
        return None

    finally:
        # Raise error at the very end (only once)
        if error is not None:
            raise error


def extract_parameter_filters(workspace_obj: FabricWorkspace, param_dict: dict) -> tuple[str, str, Path]:
    """Extracts the item type, name, and path filters from the parameter dictionary, if present."""
    item_type = param_dict.get("item_type")
    item_name = param_dict.get("item_name")
    file_path = process_input_path(workspace_obj.repository_directory, param_dict.get("file_path"))

    return item_type, item_name, file_path


def process_environment_key(workspace_obj: FabricWorkspace, replace_value_dict: dict) -> dict:
    """Processes the replace_value dictionary to replace the '_ALL_' environment key with the target environment when present."""
    # If there's only one key, check if it's "_ALL_" (case insensitive) and replace it
    if len(replace_value_dict) == 1:
        key = next(iter(replace_value_dict))
        if key.lower() == "_all_":
            replace_value_dict[workspace_obj.environment] = replace_value_dict.pop(key)

    return replace_value_dict


"""Functions to replace key values in JSON"""


def replace_key_value(workspace_obj: FabricWorkspace, param_dict: dict, json_content: str, env: str) -> Union[dict]:
    """A function to replace key values in a JSON using parameterization. It uses jsonpath_ng to find and replace values in the JSON.

    Args:
        workspace_obj: The FabricWorkspace object.
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
    replace_value = process_environment_key(workspace_obj, param_dict["replace_value"])
    for match in jsonpath_expr.find(data):
        # If the env is present in the replace_value array perform the replacement
        if env in replace_value:
            try:
                match.full_path.update(data, replace_value[env])
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


"""Functions to validate the parameter file"""


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
            # CodeQL [SM05139] Public library needing to have a default auth when user doesn't provide token. Not internal Azure product.
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

    # Parameters to validate
    param_names = ["find_replace", "key_value_replace", "spark_pool"]

    # Get only parameters that exist in param_dict
    existing_params = [name for name in param_names if name in param_dict]

    # If no parameters found, return False
    if not existing_params:
        return False

    # Check all existing parameters have the same structure and are valid
    structures = [_check_parameter_structure(param_dict.get(name)) for name in existing_params]

    # All structures must be True and identical
    return all(structures) and len(set(structures)) == 1


def _check_parameter_structure(param_value: any) -> bool:
    """Checks the structure of a parameter value"""
    return isinstance(param_value, list)


"""Functions to process and validate file paths from the optional filter"""


def process_input_path(
    repository_directory: Path, input_path: Union[str, list[str], None], validation_flag: bool = False
) -> list[Path]:
    """
    Processes the input_path value according to its type. Supports both
    regular paths and wildcard paths, including mixed lists.

    Args:
        repository_directory: The directory of the repository.
        input_path: The input path value to process (None, a string, or list of strings).
        validation_flag: Flag to indicate the context of the function call to set the logging type.
    """
    # Set the logging function based on validation_flag
    log_func = logger.error if validation_flag else logger.debug

    # Return empty list for None or empty input
    if not input_path:
        return []

    # Use a set to avoid duplicate paths
    valid_paths = set()

    # Standardize to list for consistent processing
    paths_to_process = [input_path] if isinstance(input_path, str) else input_path

    for path in paths_to_process:
        # Process path based on whether it contains wildcard characters
        has_wildcard = False
        try:
            has_wildcard = glob.has_magic(path)
        except Exception as e:
            log_func(f"Error checking for wildcard in path '{path}': {e}")
            continue

        if has_wildcard:
            _process_wildcard_path(path, repository_directory, valid_paths, log_func)
        else:
            _process_regular_path(path, repository_directory, valid_paths, log_func)

    return list(valid_paths)


def _process_regular_path(
    path: str, repository_directory: Path, valid_paths: set[Path], log_func: logging.Logger
) -> None:
    """Process a regular (non-wildcard) path and add to valid_paths if valid."""
    # Normalize path for consistent handling
    normalized_path = Path(path.lstrip("/\\"))

    # Set the path type based on whether it is absolute or relative
    path_type = "Relative" if not normalized_path.is_absolute() else "Absolute"

    # Validate the path and add to set if valid
    valid_path = _resolve_file_path(normalized_path, repository_directory, path_type, log_func)
    if valid_path:
        valid_paths.add(valid_path)


def _process_wildcard_path(
    path: str, repository_directory: Path, valid_paths: set[Path], log_func: logging.Logger
) -> None:
    """Process a wildcard path and add matching files to valid_paths if valid."""
    search_pattern = _set_wildcard_path_pattern(path, repository_directory, log_func)

    if not search_pattern:
        return

    # Track if matches are found
    initial_paths_count = len(valid_paths)

    # Get all matching files that exist
    try:
        for matched_path in [p for p in repository_directory.glob(search_pattern) if p.is_file()]:
            # Validate path and add to set if valid
            valid_path = _resolve_file_path(matched_path, repository_directory, "Wildcard", log_func)
            if valid_path:
                valid_paths.add(valid_path)

        # Only log if matches were not found
        if len(valid_paths) == initial_paths_count:
            log_func(f"Wildcard path '{path}' did not match any files")

    except Exception as e:
        log_func(f"Error processing wildcard pattern '{search_pattern}': {e}")


def _set_wildcard_path_pattern(wildcard_path: str, repository_directory: Path, log_func: logging.Logger) -> str:
    """Determine the glob search pattern for a wildcard path."""
    normalized_wildcard_path = wildcard_path.replace("\\", "/")

    # Step 1: Validate wildcard pattern syntax
    if not _validate_wildcard_syntax(normalized_wildcard_path, log_func):
        return ""

    # Step 2: Determine search pattern based on path type
    if normalized_wildcard_path.startswith("**/"):
        logger.debug("Recursive wildcard path detected")
        return f"**/{normalized_wildcard_path[3:]}"

    if Path(normalized_wildcard_path).is_absolute():
        logger.debug("Absolute wildcard path detected")
        try:
            # Check if the path is within the repository
            rel_path = Path(normalized_wildcard_path).relative_to(repository_directory)
            return str(rel_path)
        except ValueError:
            log_func(f"Invalid absolute wildcard path. '{wildcard_path}' is outside the repository directory")
            return ""
    else:
        logger.debug("Non-recursive and non-absolute wildcard path detected")
        return normalized_wildcard_path


def _resolve_file_path(
    input_path: Path, repository_directory: Path, path_type: str, log_func: logging.Logger
) -> Optional[Path]:
    """
    Validates that a path exists, is a file, and is within the repository directory.
    Returns the resolved absolute path if valid, None otherwise.
    """
    try:
        # Step 1: Resolve the input path based on its type
        if path_type == "Relative":
            resolved_path = (repository_directory / input_path).resolve()
            logger.debug(f"{path_type} path '{input_path}' resolved as '{resolved_path}'")
        elif path_type == "Absolute":
            resolved_path = input_path.resolve()
        else:
            resolved_path = input_path

        # Step 2: Check if the path is within the repository directory
        try:
            _ = resolved_path.relative_to(repository_directory)
        except ValueError:
            log_func(f"{path_type} path '{input_path}' is outside the repository directory")
            return None

        # Step 3: For non-wildcard paths, check existence and file type
        if path_type != "Wildcard":
            # Check path existence
            if not resolved_path.exists():
                log_func(f"{path_type} path '{input_path}' does not exist")
                return None

            # Check file validation
            if not resolved_path.is_file():
                log_func(f"{path_type} path '{input_path}' is not a file")
                return None

        logger.debug(f"Path '{resolved_path}' is valid and within the repository directory")
        return resolved_path

    except Exception as e:
        log_func(f"Error validating {path_type.lower()} path '{input_path}': {e}")
        return None


def _validate_wildcard_syntax(pattern: str, log_func: logging.Logger) -> bool:
    """Validates wildcard pattern syntax before using glob."""
    # Check for empty or whitespace-only patterns
    if not pattern or pattern.isspace():
        log_func("Wildcard pattern is empty")
        return False

    # Check for problematic absolute paths with recursive patterns
    if pattern.startswith("/") and pattern[1:].startswith("**/"):
        log_func(f"Absolute path with recursive pattern is not allowed: '{pattern}'")
        return False

    # Handle Windows-style absolute paths with recursive patterns
    if re.match(r"^[a-zA-Z]:\\", pattern) and "**\\" in pattern:
        log_func(f"Absolute path with recursive pattern is not allowed: '{pattern}'")
        return False

    # Apply standard validations from constants
    for validation in constants.WILDCARD_PATH_VALIDATIONS:
        if validation["check"](pattern):
            log_func(validation["message"](pattern))
            return False

    # Validate proper nesting of brackets and braces
    if not _validate_nested_brackets_braces(pattern, log_func):
        return False

    # Validate character classes (bracket expressions)
    for section in re.findall(r"\[(.*?)\]", pattern):
        if not section or section.startswith("]") or section.startswith("-") or "--" in section:
            log_func(f"Invalid character class in pattern: '{pattern}'")
            return False

    # Validate brace expansions
    try:
        for section in re.findall(r"\{(.*?)\}", pattern):
            if (
                not section  # Empty braces
                or "," not in section  # No comma separator
                or section.startswith(",")  # Starts with comma
                or section.endswith(",")  # Ends with comma
                or ",," in section
            ):  # Adjacent commas
                log_func(f"Invalid brace expansion in pattern: '{pattern}'")
                return False

    except Exception as e:
        log_func(f"Error validating brace content in pattern '{pattern}': {e}")
        return False

    # Check for path traversal sequences
    pattern_lower = pattern.lower()
    traversal_patterns = [
        "../",
        ".." + os.sep,
        ".." + os.altsep if os.altsep else "",
        "..%2F",
        "..%5C",
        "..%2f",
        "..%5c",
    ]

    for traversal in traversal_patterns:
        if traversal and traversal in pattern_lower:
            log_func(f"Path traversal sequences not allowed: '{pattern}'")
            return False

    return True


def _validate_nested_brackets_braces(pattern: str, log_func: logging.Logger) -> bool:
    """Validates proper nesting of brackets and braces in a wildcard pattern."""
    stack = []

    for pos, char in enumerate(pattern):
        if char in "[{":
            stack.append(char)
        elif char in "]}":
            # Check if stack is empty (closing without opening)
            if not stack:
                log_func(f"Unmatched closing '{char}' at position {pos} in pattern: '{pattern}'")
                return False

            # Check for proper matching
            last_open = stack.pop()
            if (char == "]" and last_open != "[") or (char == "}" and last_open != "{"):
                log_func(f"Mismatched bracket/brace pair '{last_open}{char}' at position {pos} in pattern: '{pattern}'")
                return False

    # Check if all brackets and braces were closed
    if stack:
        log_func(f"Unclosed bracket(s) or brace(s) in pattern: '{pattern}'")
        return False

    return True


"""Functions to determine replacement based on optional filters"""


def check_replacement(
    input_type: Union[str, list[str], None],
    input_name: Union[str, list[str], None],
    input_path: list[Path],
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

        # Optional filters match found. Find and replace applied in this repository file
        return True

    # Optional filters match not found. Find and replace skipped for this repository file
    return False


def _find_match(
    param_value: Union[str, list, None],
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
