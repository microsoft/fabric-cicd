# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug parameter.yml file locally against the github repo

import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

from fabric_cicd._parameterization._validate_parameter_file import validate_parameter_file

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

# In this example, the parameter.yml file sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define valid item types
item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report"]

# Set target environment
environment = "dev"

validate_parameter_file(
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Uncomment to include target environment in validation
    # environment=environment,
    # Uncomment to use SPN auth
    # token_credential=token_credential,
    # Uncomment to set log level to DEBUG
    set_log_level=True,
)
