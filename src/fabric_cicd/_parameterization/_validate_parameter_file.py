# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._common._validate_input import (
    validate_environment,
    validate_item_type_in_scope,
    validate_repository_directory,
    validate_token_credential,
)
from fabric_cicd._parameterization import ParameterValidation, change_log_level


def validate_parameter_file(
    repository_directory: str,
    item_type_in_scope: list,
    environment: str = "N/A",
    parameter_file_name: str = "parameter.yml",
    token_credential: TokenCredential = None,
    set_log_level: bool = False,
) -> bool:
    """
    A wrapper function that validates a parameter.yml file, using
    the ParameterValidation class.

    Args:
        repository_directory: The directory containing the items and parameter.yml file.
        item_type_in_scope: A list of item types to validate.
        environment: The target environment.
        parameter_file_name: The name of the parameter file, default is "parameter.yml".
        token_credential: The token credential to use for authentication, use for SPN auth.
        set_log_level: A flag to set the log level to DEBUG.
    """
    # Set log level
    if set_log_level:
        change_log_level()

    endpoint = FabricEndpoint(
        # if credential is not defined, use DefaultAzureCredential
        token_credential=(
            DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
        )
    )

    # Initialize the ParameterValidation object
    pv = ParameterValidation(
        repository_directory=validate_repository_directory(repository_directory),
        item_type_in_scope=validate_item_type_in_scope(item_type_in_scope, upn_auth=endpoint.upn_auth),
        environment=validate_environment(environment),
        parameter_file_name=parameter_file_name,
    )

    # Validate by calling _validate_parameter_file() method
    return pv._validate_parameter_file()
