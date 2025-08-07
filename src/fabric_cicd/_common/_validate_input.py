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

from azure.core.credentials import TokenCredential

import fabric_cicd.constants as constants
from fabric_cicd._common._exceptions import InputError
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


def validate_item_type_in_scope(input_value: list, upn_auth: bool) -> list:
    """
    Validate the item type in scope.

    Args:
        input_value: The input value to validate.
        upn_auth: Whether UPN authentication is used.
    """
    accepted_item_types_upn = constants.ACCEPTED_ITEM_TYPES_UPN
    accepted_item_types_non_upn = constants.ACCEPTED_ITEM_TYPES_NON_UPN

    accepted_item_types = accepted_item_types_upn if upn_auth else accepted_item_types_non_upn

    validate_data_type("list[string]", "item_type_in_scope", input_value)

    for item_type in input_value:
        if item_type not in accepted_item_types:
            msg = (
                f"Invalid or unsupported item type: '{item_type}'. "
                f"For User Identity Authentication, must be one of {', '.join(accepted_item_types_upn)}. "
                f"For Service Principal or Managed Identity Authentication, "
                f"must be one of {', '.join(accepted_item_types_non_upn)}."
            )
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
