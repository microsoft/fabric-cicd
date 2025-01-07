import logging
import os
import re

from azure.core.credentials import TokenCredential

from fabric_cicd._common._exceptions import InputError
from fabric_cicd.fabric_workspace import FabricWorkspace

"""
Following functions are leveraged to validate user input for the fabric-cicd package
Primarily used for the FabricWorkspace class, but also intended to be leveraged for
any user input throughout the package

"""

logger = logging.getLogger(__name__)


def validate_data_type(expected_type, variable_name, input):
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
    if expected_type not in type_validators or not type_validators[expected_type](input):
        raise InputError(f"The provided {variable_name} is not of type {expected_type}.", logger)

    return input


def validate_item_type_in_scope(input, upn_auth):
    accepted_item_types_upn = ["Notebook", "DataPipeline", "Environment"]
    accepted_item_types_non_upn = ["Notebook", "Environment"]

    accepted_item_types = accepted_item_types_upn if upn_auth else accepted_item_types_non_upn

    validate_data_type("list[string]", "item_type_in_scope", input)

    for item_type in input:
        if item_type not in accepted_item_types:
            raise InputError(
                f"Invalid or unsupported item type: '{item_type}'. "
                f"For User Identity Authentication, must be one of {', '.join(accepted_item_types_upn)}. "
                f"For Service Principal or Managed Identity Authentication, "
                f"must be one of {', '.join(accepted_item_types_non_upn)}.",
                logger,
            )

    return input


def validate_repository_directory(input):
    validate_data_type("string", "repository_directory", input)

    if not os.path.isdir(input):
        raise InputError(f"The provided repository_directory '{input}' does not exist.", logger)

    return input


def validate_base_api_url(input):
    validate_data_type("string", "base_api_url", input)

    if not re.match(r"^https:\/\/([a-zA-Z0-9]+)\.fabric\.microsoft\.com\/$", input):
        raise InputError(
            "The provided base_api_url does not follow the 'https://<word>.fabric.microsoft.com/' syntax. "
            "Ensure the URL has a single word in between 'https://' and '.fabric.microsoft.com/', "
            "and only contains alphanumeric characters.",
            logger,
        )

    return input


def validate_workspace_id(input):
    validate_data_type("string", "workspace_id", input)

    if not re.match(r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$", input):
        raise InputError("The provided workspace_id is not a valid guid.", logger)

    return input


def validate_environment(input):
    validate_data_type("string", "environment", input)

    return input


def validate_fabric_workspace_obj(input):
    validate_data_type("FabricWorkspace", "fabric_workspace_obj", input)

    return input


def validate_token_credential(input):
    validate_data_type("TokenCredential", "credential", input)

    return input
