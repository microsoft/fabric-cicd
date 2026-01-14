# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug parameter.yml file locally against the github repo

import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

import fabric_cicd.constants as constants
from fabric_cicd import change_log_level
from fabric_cicd._parameter._utils import validate_parameter_file

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

# Uncomment to enable debug
# change_log_level()

# In this example, the parameter.yml file sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define valid item types
item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report"]

# Set target environment
environment = "PPE"

# Uncomment to use a parameter file in a different location (default location is within repository directory)
# Use absolute path
# parameter_file_path = str(root_directory / "sample" / "config" / "parameter.yml")
# or use relative path
# parameter_file_path = "../config/parameter.yml"

# Uncomment to use SPN auth
# client_id = "your-client-id"
# client_secret = "your-client-secret"
# tenant_id = "your-tenant-id"
# token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

validate_parameter_file(
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Comment to exclude target environment in validation
    environment=environment,
    # Uncomment to use a different parameter file name within the repository directory (default name: parameter.yml)
    # Assign to the constant in constants.py or pass in a string directly
    # parameter_file_name=constants.PARAMETER_FILE_NAME,
    # Uncomment to use a parameter file from outside the repository (takes precedence over parameter_file_name)
    # parameter_file_path=parameter_file_path,
    # Uncomment to use SPN auth
    # token_credential=token_credential,
)
