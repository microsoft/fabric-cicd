"""Constants for the fabric-cicd package."""

# Parameter configs
PARAMETER_FILE_NAME = "parameter.yml"

# Parameter Validation Messages
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
    "old structure": "The parameter file structure used will no longer be supported after April 21, 2025. Please update to the new structure",
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
