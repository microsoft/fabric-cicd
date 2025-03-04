import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from fabric_cicd._parameterization import (
    ParameterValidation,
    change_log_level,
)

# Uncomment to enable debug
change_log_level()

# In this example, the parameter.yml file sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define valid item types
item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report"]

# Explicitly define the valid environments
defined_environments = ["PPE", "PROD", "dev", "test", "prod"]

# Initialize the ParameterizationValidation object with the required parameters
parameterization_validation = ParameterValidation(
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    environments=defined_environments,
    # Uncomment to use SPN auth
    # token_credential=token_credential,
)

# Validate the parameter.yml file
parameterization_validation.validate_parameters()
