# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Constants for the fabric-cicd package."""

# General
DEFAULT_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"
DEFAULT_API_ROOT_URL = "https://api.powerbi.com"
FEATURE_FLAG = set()

# Item Type
ACCEPTED_ITEM_TYPES_UPN = (
    "DataPipeline",
    "Environment",
    "Notebook",
    "Report",
    "SemanticModel",
    "Lakehouse",
    "MirroredDatabase",
)
ACCEPTED_ITEM_TYPES_NON_UPN = ACCEPTED_ITEM_TYPES_UPN

# Publish
MAX_RETRY_OVERRIDE = {"SemanticModel": 10, "Report": 10}
SHELL_ONLY_PUBLISH = ["Environment", "Lakehouse"]

# REGEX Constants
VALID_GUID_REGEX = r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
WORKSPACE_ID_REFERENCE_REGEX = r'"(default_lakehouse_workspace_id|workspaceId)": "(.*?)"'

# Parameter file configs
PARAMETER_FILE_NAME = "parameter.yml"

# Parameter file validation messages
INVALID_YAML = {"char": "Invalid characters found", "quote": "Unclosed quote: {}"}
INVALID_REPLACE_VALUE_SPARK_POOL = {
    "missing key": "The '{}' environment dict in spark_pool must contain a 'type' and a 'name' key",
    "missing value": "The '{}' environment in spark_pool is missing a value for '{}' key",
    "invalid value": "The '{}' environment in spark_pool must contain 'Capacity' or 'Workspace' as a value for 'type'",
}
PARAMETER_MSGS = {
    "validating": "Validating {}",
    "passed": "Validation passed: {}",
    "failed": "Validation failed with error: {}",
    "terminate": "Validation terminated: {}",
    "found": f"Found {PARAMETER_FILE_NAME} file",
    "not found": "Parameter file not found with path: {}",
    "invalid content": INVALID_YAML,
    "valid load": f"Successfully loaded {PARAMETER_FILE_NAME}",
    "invalid load": f"Error loading {PARAMETER_FILE_NAME} " + "{}",
    "old structure": "The parameter file structure used will no longer be supported after April 24, 2025. Please migrate to the new structure",
    "raise issue": "Raise a GitHub issue here: https://github.com/microsoft/fabric-cicd/issues for migration timeline issues",
    "invalid structure": "Invalid parameter file structure",
    "valid structure": "Parameter file structure is valid",
    "invalid name": "Invalid parameter name '{}' found in the parameter file",
    "valid name": "Parameter names are valid",
    "parameter not found": "{} parameter is not present",
    "invalid data type": "The provided '{}' is not of type {} in {}",
    "missing key": "{} is missing keys",
    "invalid key": "{} contains invalid keys",
    "valid keys": "{} contains valid keys",
    "missing required value": "Missing value for '{}' key in {}",
    "valid required values": "Required values in {} are valid",
    "missing replace value": "{} is missing a replace value for '{}' environment'",
    "valid replace value": "Values in 'replace_value' dict in {} are valid",
    "no target env": "Target environment '{}' is not a key in the 'replace_value' dict in {}",
    "invalid replace value": INVALID_REPLACE_VALUE_SPARK_POOL,
    "no optional": "No optional values provided in {}",
    "invalid item type": "Item type '{}' not in scope",
    "invalid item name": "Item name '{}' not found in the repository directory",
    "invalid file path": "Path '{}' not found in the repository directory",
    "valid optional": "Optional values in {} are valid",
    "valid parameter": "{} parameter is valid",
}


INDENT = "->"
