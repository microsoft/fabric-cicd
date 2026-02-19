# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Following functions are leveraged to validate user input for the fabric-cicd package
Primarily used for the FabricWorkspace class, but also intended to be leveraged for
any user input throughout the package
"""

import logging
import re
from pathlib import Path
from typing import Optional

from azure.core.credentials import TokenCredential

import fabric_cicd.constants as constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd.constants import FeatureFlag, OperationType
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def validate_data_type(expected_type: str, variable_name: str, input_value: any) -> any:
    """
    Validate the data type of the input value.

    Args:
        expected_type: The expected data type.
        variable_name: The name of the variable.
        input_value: The input value to validate.
    """
    # Mapping of expected types to their validation functions
    type_validators = {
        "string": lambda x: isinstance(x, str),
        "bool": lambda x: isinstance(x, bool),
        "list": lambda x: isinstance(x, list),
        "list[string]": lambda x: isinstance(x, list) and all(isinstance(item, str) for item in x),
        "FabricWorkspace": lambda x: isinstance(x, FabricWorkspace),
        "TokenCredential": lambda x: isinstance(x, TokenCredential),
    }

    # Check if the expected type is valid and if the input matches the expected type
    if expected_type not in type_validators or not type_validators[expected_type](input_value):
        msg = f"The provided {variable_name} is not of type {expected_type}."
        raise InputError(msg, logger)

    return input_value


def validate_item_type_in_scope(input_value: Optional[list]) -> list:
    """
    Validate the item type in scope.

    Args:
        input_value: The input value to validate. If None, defaults to all supported item types.
    """
    accepted_item_types = constants.ACCEPTED_ITEM_TYPES

    # If None, return all accepted item types
    if input_value is None:
        return list(accepted_item_types)

    validate_data_type("list[string]", "item_type_in_scope", input_value)

    for item_type in input_value:
        if item_type not in accepted_item_types:
            msg = f"Invalid or unsupported item type: '{item_type}'. Must be one of {', '.join(accepted_item_types)}."
            raise InputError(msg, logger)

    return input_value


def validate_repository_directory(input_value: str) -> Path:
    """
    Validate the repository directory and convert string to Path object

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("string", "repository_directory", input_value)

    directory = Path(input_value)

    if not directory.is_dir():
        msg = f"The provided repository_directory '{input_value}' does not exist."
        raise InputError(msg, logger)

    if not directory.is_absolute():
        absolute_directory = directory.resolve()
        logger.info(f"Relative directory path '{directory}' resolved as '{absolute_directory}'")
        directory = absolute_directory

    return directory


def validate_workspace_id(input_value: str) -> str:
    """
    Validate the workspace ID.

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("string", "workspace_id", input_value)

    if not re.match(constants.VALID_GUID_REGEX, input_value):
        msg = "The provided workspace_id is not a valid guid."
        raise InputError(msg, logger)

    return input_value


def validate_workspace_name(input_value: str) -> str:
    """
    Validate the workspace name.

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("string", "workspace_name", input_value)

    return input_value


def validate_environment(input_value: str) -> str:
    """
    Validate the environment.

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("string", "environment", input_value)

    return input_value


def validate_fabric_workspace_obj(input_value: FabricWorkspace) -> FabricWorkspace:
    """
    Validate the FabricWorkspace object.

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("FabricWorkspace", "fabric_workspace_obj", input_value)

    return input_value


def validate_token_credential(input_value: TokenCredential) -> TokenCredential:
    """
    Validate the token credential.

    Args:
        input_value: The input value to validate.
    """
    validate_data_type("TokenCredential", "credential", input_value)

    return input_value


def validate_experimental_param(
    param_value: Optional[str],
    required_flag: "FeatureFlag",
    warning_message: str,
    risk_warning: str,
) -> None:
    """
    Generic validation for optional parameters requiring experimental feature flags.

    Args:
        param_value: The parameter value (None means skip validation).
        required_flag: The specific feature flag required (in addition to experimental).
        warning_message: Primary warning message when feature is enabled.
        risk_warning: Risk/caution warning message.

    Raises:
        InputError: If required feature flags are not enabled.
    """
    if param_value is None:
        return

    if (
        FeatureFlag.ENABLE_EXPERIMENTAL_FEATURES.value not in constants.FEATURE_FLAG
        or required_flag.value not in constants.FEATURE_FLAG
    ):
        msg = f"Feature flags 'enable_experimental_features' and '{required_flag.value}' must be set."
        raise InputError(msg, logger)

    logger.warning(warning_message)
    logger.warning(risk_warning)


def validate_items_to_include(items_to_include: Optional[list[str]], operation: "OperationType") -> None:
    """
    Validate items_to_include parameter and check required feature flags.

    Args:
        items_to_include: List of items in "item_name.item_type" format, or None.
        operation: The type of operation being performed (publish or unpublish).

    Raises:
        InputError: If required feature flags are not enabled.
    """
    validate_experimental_param(
        param_value=items_to_include,
        required_flag=FeatureFlag.ENABLE_ITEMS_TO_INCLUDE,
        warning_message=f"Selective {operation.value} is enabled.",
        risk_warning=f"Using items_to_include is risky as it can prevent needed dependencies from being {operation.value}.  Use at your own risk.",
    )


def validate_folder_path_exclude_regex(folder_path_exclude_regex: Optional[str]) -> None:
    """
    Validate folder_path_exclude_regex parameter and check required feature flags.

    Args:
        folder_path_exclude_regex: Regex pattern to exclude items based on their folder path, or None.

    Raises:
        InputError: If required feature flags are not enabled.
    """
    validate_experimental_param(
        param_value=folder_path_exclude_regex,
        required_flag=FeatureFlag.ENABLE_EXCLUDE_FOLDER,
        warning_message="Folder path exclusion is enabled.",
        risk_warning="Using folder_path_exclude_regex is risky as it can prevent needed dependencies from being deployed.  Use at your own risk.",
    )

    if not isinstance(folder_path_exclude_regex, str):
        msg = "folder_path_exclude_regex must be a string."
        raise InputError(msg, logger)

    if folder_path_exclude_regex == "":
        msg = "folder_path_exclude_regex must not be an empty string. Provide a valid regex pattern or omit the parameter."
        raise InputError(msg, logger)


def validate_folder_path_to_include(folder_path_to_include: Optional[list[str]]) -> None:
    """
    Validate folder_path_to_include parameter and check required feature flags.

    Args:
        folder_path_to_include: List of folder paths with format ["/folder1", "/folder2", ...], or None.

    Raises:
        InputError: If required feature flags are not enabled.
    """
    validate_experimental_param(
        param_value=folder_path_to_include,
        required_flag=FeatureFlag.ENABLE_INCLUDE_FOLDER,
        warning_message="Folder path inclusion is enabled.",
        risk_warning="Using folder_path_to_include is risky as it can prevent needed dependencies from being deployed.  Use at your own risk.",
    )

    if not isinstance(folder_path_to_include, list):
        msg = "folder_path_to_include must be a list of folder paths."
        raise InputError(msg, logger)

    if not folder_path_to_include:
        msg = "folder_path_to_include must not be an empty list. Provide folder paths or omit the parameter."
        raise InputError(msg, logger)


def validate_shortcut_exclude_regex(shortcut_exclude_regex: Optional[str]) -> None:
    """
    Validate shortcut_exclude_regex parameter and check required feature flags.

    Args:
        shortcut_exclude_regex: Regex pattern to exclude specific shortcuts from being published, or None.

    Raises:
        InputError: If required feature flags are not enabled.
    """
    validate_experimental_param(
        param_value=shortcut_exclude_regex,
        required_flag=FeatureFlag.ENABLE_SHORTCUT_EXCLUDE,
        warning_message="Shortcut exclusion is enabled.",
        risk_warning="Using shortcut_exclude_regex will selectively exclude shortcuts from being deployed to lakehouses. Use with caution.",
    )
