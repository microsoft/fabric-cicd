# Parameter
PARAMETER_FILE_NAME = "parameter.yml"

# Parameter Validation Messages
PARAMETER_FILE_FOUND = f"Found {PARAMETER_FILE_NAME} file"
PARAMETER_FILE_NOT_FOUND = "Parameter file not found with path: {}"

VALIDATING_PARAMETER = "Validating {}"
VALIDATION_PASSED = "Validation passed: {}"

INVALID_PARAMETER_NAME = "Invalid parameter name: '{}' found in the parameter file"
PARAMETER_NOT_PRESENT = "{} parameter is not present"

OLD_PARAMETER_FILE_STUCTURE_WARNING = "The parameter file structure used will no longer be supported after April 7, 2025. Please update to the new structure"
INVALID_PARAMETER_FILE_STRUCTURE = "Invalid parameter file structure"


VALID_PARAMETER_FILE_STRUCTURE = "Parameter file structure is valid"
VALID_PARAMETER_NAMES = "Parameter names are valid"
VALID_PARAMETER = "{} parameter is valid"
VALID_PARAMETERS = "find_replace and spark_pool parameters are valid"
INVALID_PARAMETERS = "find_replace and spark_pool parameters are invalid"

MISSING_OR_INVALID_MSG = "{} contains missing or invalid {}"
# VALIDATING_REPLACE_VALUE = "Validating replace_value dictionary keys and values"
VALIDATING_OPTIONAL_VALUE = "{} contains invalid optional values"

MISSING_KEYS = "{} is missing keys"
INVALID_KEYS = "{} contains invalid keys"
VALID_KEYS = "{} contains valid keys"
MISSING_KEY_VALUE = "Missing value for '{}' key in {}"

REQUIRED_VALUES = "Required values in {} are valid"
VALID_REPLACE_VALUE = "Values in replace_value dict in {} are valid"
INVALID_REPLACE_VALUE = "Values in replace_value dict in {} are invalid"
MISSING_REPLACE_VALUE = "{} is missing a replace_value for {} environment"
OPTIONAL_PARAMETERS_MSG = [
    "No optional parameter values in {}, validation passed",
    "{} parameter value in {} is invalid",
    "Optional parameter values in {} are valid",
]


SPARK_POOL_REPLACE_VALUE_ERRORS = [
    "The '{}' environment dict in spark_pool must contain a 'type' and a 'name' key",
    "The '{}' environment dict in spark_pool is missing a value for '{}' key",
    "The '{}' environment_dict in spark_pool contains an invalid value: '{}' for 'type' key",
]

PARAMETER_KEYS_SET = {
    "find_replace": {
        "minimum": {"find_value", "replace_value"},
        "maximum": {"find_value", "replace_value", "item_type", "item_name", "file_path"},
    },
    "spark_pool": {
        "minimum": {"instance_pool_id", "replace_value"},
        "maximum": {"instance_pool_id", "replace_value", "item_name"},
    },
    "spark_pool_replace_value": {"type", "name"},
}
