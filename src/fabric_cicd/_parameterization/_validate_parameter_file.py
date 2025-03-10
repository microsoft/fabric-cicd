# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from pathlib import Path

from azure.core.credentials import TokenCredential

from fabric_cicd._parameterization import ParameterValidation, change_log_level


def validate_parameter_file(
    repository_directory: Path,
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

    # Initialize the ParameterValidation object
    parameter_validation_object = ParameterValidation(
        repository_directory=repository_directory,
        item_type_in_scope=item_type_in_scope,
        environment=environment,
        parameter_file_name=parameter_file_name,
        token_credential=token_credential,
    )

    # Validate by calling _validate_parameter_file() method
    return parameter_validation_object._validate_parameter_file()
