# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug parameter.yml file locally against the github repo

import pprint
import sys
from pathlib import Path

import fabric_cicd.constants as constants
from fabric_cicd import change_log_level
from fabric_cicd._common._validate_parameter import validate_key_value_replace
from fabric_cicd._parameter._parameter import Parameter
from fabric_cicd._parameter._utils import validate_parameter_file

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

# Uncomment to enable debug
change_log_level()

# In this example, the parameter.yml file sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define valid item types
item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report", "VariableLibrary"]

# Set target environment
environment = "PROD"

# Uncomment to use a parameter file in a different location (default location is within repository directory)
# Use absolute path
parameter_file_path = Path(repository_directory) / "parameter.yml"
# or use relative path
# parameter_file_path = "../config/parameter.yml"

# validate_parameter_file(
#     repository_directory=repository_directory,
#     item_type_in_scope=item_type_in_scope,
#     # Comment to exclude target environment in validation
#     environment=environment,
#     # Uncomment to use a different parameter file name within the repository directory (default name: parameter.yml)
#     # Assign to the constant in constants.py or pass in a string directly
#     # parameter_file_name=constants.PARAMETER_FILE_NAME,
#     # Uncomment to use a parameter file from outside the repository (takes precedence over parameter_file_name)
#     # parameter_file_path=parameter_file_path
# )

# rest = validate_key_value_replace(
#     parameter_file_path=Path(parameter_file_path),
#     repository_directory=Path(repository_directory),
#     environment="PROD",
# )

param = Parameter(
    repository_directory=Path(repository_directory),
    item_type_in_scope=item_type_in_scope,
    # Comment to exclude target environment in validation
    environment=environment,
    # Uncomment to use a different parameter file name within the repository directory (default name: parameter.yml)
    # Assign to the constant in constants.py or pass in a string directly
    # parameter_file_name=constants.PARAMETER_FILE_NAME,
    # Uncomment to use a parameter file from outside the repository (takes precedence over parameter_file_name)
    # parameter_file_path=parameter_file_path
)

rest = param._validate_key_value_replacements(environment=environment, as_dict=True)


print(f"debug results for environment '{environment}':")
# print(rest)
pprint.pprint(rest, sort_dicts=False)

# {
#     "total_rules": 5,
#     "rules_without_matches": 3,
#     "total_matches": 2,
#     "matches": [
#         {
#             "rule_id": 0,
#             "json_path": '$.variables[?(@.name=="SQL_Server")].value',
#             "file_path": "/Users/Michiel.Schouten/Documents/code/open-source/fabric-cicd/sample/workspace/Vars.VariableLibrary/variables.json",
#             "match_location": "((variables.[1]).value)",
#             "current_value": "contoso-ppe.database.windows.net",
#             "new_value": "contoso-ppe.database.windows.net",
#             "found": True,
#         },
#         {
#             "rule_id": 1,
#             "json_path": '$.variables[?(@.name=="Environment")].value',
#             "file_path": "/Users/Michiel.Schouten/Documents/code/open-source/fabric-cicd/sample/workspace/Vars.VariableLibrary/variables.json",
#             "match_location": "((variables.[0]).value)",
#             "current_value": "PPE",
#             "new_value": "PPE",
#             "found": True,
#         },
#         {
#             "rule_id": 2,
#             "json_path": '$.variableOverrides[?(@.name=="SQL_Server")].value',
#             "file_path": "Vars.VariableLibrary/valueSets/PROD.json",
#             "found": False,
#             "new_value": None,
#             "error": "No matches found",
#         },
#         {
#             "rule_id": 3,
#             "json_path": '$.variableOverrides[?(@.name=="Environment")].value',
#             "file_path": "Vars.VariableLibrary/valueSets/PROD.json",
#             "found": False,
#             "new_value": None,
#             "error": "No matches found",
#         },
#         {
#             "rule_id": 4,
#             "json_path": '$.schedules[?(@.jobType=="Execute")].enabled',
#             "file_path": "**/.schedules",
#             "found": False,
#             "new_value": False,
#             "error": "No matches found",
#         },
#     ],
# }
