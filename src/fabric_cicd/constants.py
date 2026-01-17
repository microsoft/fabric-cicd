# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Constants for the fabric-cicd package."""

import os
from enum import Enum


class ItemType(str, Enum):
    """Enumeration of supported Microsoft Fabric item types."""

    APACHE_AIRFLOW_JOB = "ApacheAirflowJob"
    COPY_JOB = "CopyJob"
    DATA_AGENT = "DataAgent"
    DATA_PIPELINE = "DataPipeline"
    DATAFLOW = "Dataflow"
    ENVIRONMENT = "Environment"
    EVENTHOUSE = "Eventhouse"
    EVENTSTREAM = "Eventstream"
    GRAPHQL_API = "GraphQLApi"
    KQL_DASHBOARD = "KQLDashboard"
    KQL_DATABASE = "KQLDatabase"
    KQL_QUERYSET = "KQLQueryset"
    LAKEHOUSE = "Lakehouse"
    MIRRORED_DATABASE = "MirroredDatabase"
    ML_EXPERIMENT = "MLExperiment"
    MOUNTED_DATA_FACTORY = "MountedDataFactory"
    NOTEBOOK = "Notebook"
    ORG_APP = "OrgApp"
    REFLEX = "Reflex"
    REPORT = "Report"
    SEMANTIC_MODEL = "SemanticModel"
    SPARK_JOB_DEFINITION = "SparkJobDefinition"
    SQL_DATABASE = "SQLDatabase"
    USER_DATA_FUNCTION = "UserDataFunction"
    VARIABLE_LIBRARY = "VariableLibrary"
    WAREHOUSE = "Warehouse"


class FeatureFlag(str, Enum):
    """Enumeration of supported feature flags for fabric-cicd."""

    ENABLE_LAKEHOUSE_UNPUBLISH = "enable_lakehouse_unpublish"
    """Set to enable the deletion of Lakehouses."""
    ENABLE_WAREHOUSE_UNPUBLISH = "enable_warehouse_unpublish"
    """Set to enable the deletion of Warehouses."""
    ENABLE_SQLDATABASE_UNPUBLISH = "enable_sqldatabase_unpublish"
    """Set to enable the deletion of SQL Databases."""
    ENABLE_EVENTHOUSE_UNPUBLISH = "enable_eventhouse_unpublish"
    """Set to enable the deletion of Eventhouses."""
    ENABLE_KQLDATABASE_UNPUBLISH = "enable_kqldatabase_unpublish"
    """Set to enable the deletion of KQL Databases (attached to Eventhouses)."""
    ENABLE_SHORTCUT_PUBLISH = "enable_shortcut_publish"
    """Set to enable deploying shortcuts with the lakehouse."""
    DISABLE_WORKSPACE_FOLDER_PUBLISH = "disable_workspace_folder_publish"
    """Set to disable deploying workspace sub folders."""
    CONTINUE_ON_SHORTCUT_FAILURE = "continue_on_shortcut_failure"
    """Set to allow deployment to continue even when shortcuts fail to publish."""
    ENABLE_ENVIRONMENT_VARIABLE_REPLACEMENT = "enable_environment_variable_replacement"
    """Set to enable the use of pipeline variables."""
    ENABLE_EXPERIMENTAL_FEATURES = "enable_experimental_features"
    """Set to enable experimental features, such as selective deployments."""
    ENABLE_ITEMS_TO_INCLUDE = "enable_items_to_include"
    """Set to enable selective publishing/unpublishing of items."""
    ENABLE_EXCLUDE_FOLDER = "enable_exclude_folder"
    """Set to enable folder-based exclusion during publish operations."""
    ENABLE_SHORTCUT_EXCLUDE = "enable_shortcut_exclude"
    """Set to enable selective publishing of shortcuts in a Lakehouse."""
    ENABLE_CONFIG_DEPLOY = "enable_config_deploy"
    """Set to enable config file-based deployment."""
    ENABLE_RESPONSE_COLLECTION = "enable_response_collection"
    """Set to enable collection of API responses during publish operations."""
    DISABLE_PRINT_IDENTITY = "disable_print_identity"
    """Set to disable printing the executing identity name."""
    ENABLE_DEBUG_MODE = "enable_debug_mode"
    """Set to enable debug mode for detailed logging."""


class EnvVar(str, Enum):
    """Enumeration of environment variables used by fabric-cicd."""

    HTTP_TRACE_ENABLED = "FABRIC_CICD_HTTP_TRACE_ENABLED"
    """Set to '1', 'true', or 'yes' to enable HTTP request/response tracing."""
    HTTP_TRACE_FILE = "FABRIC_CICD_HTTP_TRACE_FILE"
    """Path to save HTTP trace output. If set, HTTP tracing is enabled."""
    DEFAULT_API_ROOT_URL = "DEFAULT_API_ROOT_URL"
    """Override the default Power BI API root URL. Defaults to 'https://api.powerbi.com'."""
    FABRIC_API_ROOT_URL = "FABRIC_API_ROOT_URL"
    """Override the Fabric API root URL. Defaults to 'https://api.fabric.microsoft.com'."""
    RETRY_DELAY_OVERRIDE = "FABRIC_CICD_RETRY_DELAY_OVERRIDE"
    """Override retry delay in seconds (e.g., '0' for instant retries - useful in tests)."""


# General
VERSION = "0.1.33"
DEFAULT_GUID = "00000000-0000-0000-0000-000000000000"
DEFAULT_API_ROOT_URL = os.environ.get(EnvVar.DEFAULT_API_ROOT_URL.value, "https://api.powerbi.com")
FABRIC_API_ROOT_URL = os.environ.get(EnvVar.FABRIC_API_ROOT_URL.value, "https://api.fabric.microsoft.com")
FEATURE_FLAG = set()
USER_AGENT = f"ms-fabric-cicd/{VERSION}"

# HTTP Headers
AUTHORIZATION_HEADER = "authorization"

# HTTP Tracing
VALID_ENABLE_FLAGS = ["1", "true", "yes"]

# Item Type
ACCEPTED_ITEM_TYPES = tuple(item_type.value for item_type in ItemType)

# Publish
SHELL_ONLY_PUBLISH = [
    ItemType.LAKEHOUSE.value,
    ItemType.WAREHOUSE.value,
    ItemType.SQL_DATABASE.value,
    ItemType.ML_EXPERIMENT.value,
]

# Items that do not require assigned capacity
NO_ASSIGNED_CAPACITY_REQUIRED = [ItemType.SEMANTIC_MODEL.value, ItemType.REPORT.value]

# REGEX Constants
VALID_GUID_REGEX = r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
WORKSPACE_ID_REFERENCE_REGEX = r"\"?(default_lakehouse_workspace_id|workspaceId|workspace)\"?\s*[:=]\s*\"(.*?)\""
DATAFLOW_SOURCE_REGEX = (
    r'(PowerPlatform\.Dataflows)(?:\(\[\]\))?[\s\S]*?workspaceId\s*=\s*"(.*?)"[\s\S]*?dataflowId\s*=\s*"(.*?)"'
)
INVALID_FOLDER_CHAR_REGEX = r'[~"#.%&*:<>?/\\{|}]'
KQL_DATABASE_FOLDER_PATH_REGEX = r"(?i)^(.*)/[^/]+\.Eventhouse/\.children(?:/.*)?$"

# Item Type to File Mapping (to check for item dependencies)
ITEM_TYPE_TO_FILE = {ItemType.DATA_PIPELINE.value: "pipeline-content.json"}

# Property path to get SQL Endpoint or Eventhouse URI
PROPERTY_PATH_ATTR_MAPPING = {
    ItemType.LAKEHOUSE.value: {
        "sqlendpoint": "body/properties/sqlEndpointProperties/connectionString",
        "sqlendpointid": "body/properties/sqlEndpointProperties/id",
    },
    ItemType.WAREHOUSE.value: {
        "sqlendpoint": "body/properties/connectionString",
    },
    ItemType.EVENTHOUSE.value: {
        "queryserviceuri": "body/properties/queryServiceUri",
    },
}

# Parameter file configs
PARAMETER_FILE_NAME = "parameter.yml"
# Parameters to validate
PARAM_NAMES = ["find_replace", "key_value_replace", "spark_pool", "semantic_model_binding"]

ITEM_ATTR_LOOKUP = ["id", "sqlendpoint", "sqlendpointid", "queryserviceuri"]

# Serial execution order for publishing items (determines dependency order)
# Unpublish order is the reverse of this
SERIAL_ITEM_PUBLISH_ORDER: dict[int, ItemType] = {
    1: ItemType.VARIABLE_LIBRARY,
    2: ItemType.WAREHOUSE,
    3: ItemType.MIRRORED_DATABASE,
    4: ItemType.LAKEHOUSE,
    5: ItemType.SQL_DATABASE,
    6: ItemType.ENVIRONMENT,
    7: ItemType.USER_DATA_FUNCTION,
    8: ItemType.EVENTHOUSE,
    9: ItemType.SPARK_JOB_DEFINITION,
    10: ItemType.NOTEBOOK,
    11: ItemType.SEMANTIC_MODEL,
    12: ItemType.REPORT,
    13: ItemType.COPY_JOB,
    14: ItemType.KQL_DATABASE,
    15: ItemType.KQL_QUERYSET,
    16: ItemType.REFLEX,
    17: ItemType.EVENTSTREAM,
    18: ItemType.KQL_DASHBOARD,
    19: ItemType.DATAFLOW,
    20: ItemType.DATA_PIPELINE,
    21: ItemType.GRAPHQL_API,
    22: ItemType.APACHE_AIRFLOW_JOB,
    23: ItemType.MOUNTED_DATA_FACTORY,
    24: ItemType.ORG_APP,
    25: ItemType.DATA_AGENT,
    26: ItemType.ML_EXPERIMENT,
}

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
    "not set": "Parameter file path is not set",
    "invalid content": INVALID_YAML,
    "valid load": f"Successfully loaded {PARAMETER_FILE_NAME}",
    "invalid load": f"Error loading {PARAMETER_FILE_NAME} " + "{}",
    "invalid structure": "Invalid parameter file structure",
    "valid structure": "Parameter file structure is valid",
    "invalid name": "Invalid parameter name '{}' found in the parameter file",
    "valid name": "Parameter names are valid",
    "invalid data type": "The provided '{}' is not of type {} in {}",
    "missing key": "{} is missing keys",
    "invalid key": "{} contains invalid keys",
    "valid keys": "{} contains valid keys",
    "missing required value": "Missing value for '{}' key in {}",
    "valid required values": "Required values in {} are valid",
    "missing replace value": "{} is missing a replace value for '{}' environment'",
    "valid replace value": "Values in 'replace_value' dict in {} are valid",
    "invalid replace value": INVALID_REPLACE_VALUE_SPARK_POOL,
    "no optional": "No optional values provided in {}",
    "invalid item type": "Item type '{}' not in scope",
    "invalid item name": "Item name '{}' not found in the repository directory",
    "invalid file path": "Number of paths in list '{}' that are invalid or not found in the repository directory: {}",
    "no valid file path": "No valid file path found in the repository directory for {}",
    "valid optional": "Optional values in {} are valid. Checking for file matches in the repository directory",
    "valid parameter": "{} parameter is valid",
    "skip": "The {} '{}' replacement will be skipped due to {} in parameter {}",
    "no target env": "target environment '{}' not found",
    "all target env": "The replace value: '{}' will be applied for any target environment",
    "other target env": "The '{}' environment key can only be used alone. Other environment keys found in replace_value: '{}'",
    "no filter match": "unmatched optional filters",
    # Path resolution messages
    "resolving_relative_path": "Resolving path '{}' to be relative to repository directory",
    "using_param_file_path": "Using parameter file path: '{}'",
    "using_default_param_file_path": "Using default parameter file path: '{}'",
    "param_file_not_found": "Parameter file path not found at: '{}'. The path was resolved from: '{}' relative to repository directory: '{}'",
    "param_path_not_file": "The specified parameter path '{}' exists but is not a file.",
    "both_param_path_and_name": "Both parameter_file_name: '{}' and parameter_file_path: '{}' were provided. Using parameter_file_path",
    # Parameter validation messages
    "param_not_found": "The {} parameter was not found",
    "param_found": "Found the {} parameter",
    "param_count": "{} {} parameters found",
    "regex_ignored": "The provided is_regex value is not set to 'true', regex matching will be ignored.",
    "validation_complete": "Parameter file validation passed",
    "gateway_deprecated": "The 'gateway_binding' parameter is deprecated and will be removed in future releases. Please use 'semantic_model_binding' instead.",
    "duplicate_semantic_model": "Duplicate semantic model names found: {}. Multiple connections to the same semantic model are permitted. Ensure this is intentional.",
    # Template parameter file messages
    "template_file_not_found": "Template parameter file not found: {}",
    "template_file_invalid": "Invalid template parameter file {}: {}",
    "template_file_error": "Error loading template parameter file {}: {}",
    "template_file_loaded": "Successfully loaded template parameter file: {}",
    "template_files_processed": "Successfully processed {} template parameter file(s)",
    "template_files_none_valid": "None of the template parameter files were valid or found, content will not be added",
}

# Wildcard path support validations
WILDCARD_PATH_VALIDATIONS = [
    # Invalid combinations
    {
        "check": lambda p: any(bad in p for bad in ["/**/*/", "**/**", "//", "\\\\", "**/**/"]),
        "message": lambda p: f"Invalid wildcard combination in pattern: '{p}'",
    },
    # Incorrect recursive wildcard format
    {
        "check": lambda p: "**" in p and not ("**/" in p or "/**" in p),
        "message": lambda p: f"Invalid recursive wildcard format (use **/ or /**): '{p}'",
    },
]


INDENT = "->"


# Define supported sections and settings for config file
CONFIG_SECTIONS = {
    "core": {
        "type": dict,
        "settings": ["workspace_id", "workspace", "repository_directory", "item_types_in_scope", "parameter"],
    },
    "publish": {
        "type": dict,
        "settings": ["exclude_regex", "folder_exclude_regex", "items_to_include", "shortcut_exclude_regex", "skip"],
    },
    "unpublish": {"type": dict, "settings": ["exclude_regex", "items_to_include", "skip"]},
    "features": {"type": (list, dict), "settings": []},
    "constants": {"type": dict, "settings": []},
}

# Config deployment validation messages
CONFIG_VALIDATION_MSGS = {
    # File validation
    "file": {
        "path_empty": "Configuration file path must be a non-empty string",
        "invalid_path": "Invalid file path '{}': {}",
        "not_found": "Configuration file not found: {}",
        "not_file": "Path is not a file: {}",
        "yaml_syntax": "Invalid YAML syntax: {}",
        "encoding_error": "File encoding error (expected UTF-8): {}",
        "permission_denied": "Permission denied reading file: {}",
        "unexpected_error": "Unexpected error reading file: {}",
        "empty_file": "Configuration file is empty or contains only comments",
        "not_dict": "Configuration must be a dictionary, got {}",
    },
    # Override validation
    "override": {
        "apply_failed": "Failed to apply config override for section '{}': {}",
        "unsupported_section": "Cannot override unsupported config section: '{}'. Supported: {}",
        "wrong_type": "Override section '{}' must be a {}, got {}",
        "unsupported_setting": "Cannot override unsupported setting '{}.{}'. Supported: {}",
        "cannot_create_core": "Cannot create 'core' section - required section must exist in the config file to override",
        "cannot_create_required": "Cannot create required field 'core.{}'",
        "cannot_create_workspace_id": "Cannot create workspace identifier 'core.{}'",
    },
    # Structure validation
    "structure": {
        "missing_core": "Configuration must contain a 'core' section",
        "core_not_dict": "'core' section must be a dictionary, got {}",
        "missing_workspace_id": "Configuration must specify either 'workspace_id' or 'workspace' in core section",
        "missing_repository_dir": "Configuration must specify 'repository_directory' in core section",
    },
    # Environment validation
    "environment": {
        "no_env_with_mappings": "Configuration contains environment mappings but no environment was provided. Please specify an environment or remove environment mappings.",
        "env_not_found": "Environment '{}' not found in '{}' mappings. Available: {}",
        "empty_mapping": "'{}' environment mapping cannot be empty",
        "invalid_env_key": "Environment key in '{}' must be a non-empty string, got: {}",
        "empty_env_value": "'{}' value for environment '{}' cannot be empty",
    },
    # Field validation
    "field": {
        "string_or_dict": "'{}' must be either a string or environment mapping dictionary (e.g., {{dev: 'dev_value', prod: 'prod_value'}}), got type {}",
        "empty_value": "'{}' cannot be empty",
        "empty_list": "'{}' cannot be empty if specified",
        "invalid_guid": "'{}' must be a valid GUID format: {}",
        "item_types_list_or_dict": "'item_types_in_scope' must be either a list or environment mapping dictionary (e.g., {{dev: ['Notebook'], prod: ['DataPipeline']}}), got type {}",
        "invalid_item_type": "Item type must be a string, got {}: {}",
        "unsupported_item_type_env": "Invalid item type '{}' in environment '{}'. Available types: {}",
        "unsupported_item_type": "Invalid item type '{}'. Available types: {}",
    },
    # Path validation
    "path": {
        "skip": "Skipping {} path resolution due to config file validation failure",
        "absolute": "Using absolute {} path{}: '{}'",
        "git_repo": "{}{} must be in the same git repository as the configuration file. Config repository: {}, {} repository: {}",
        "resolved": "{} '{}' resolved relative to config path{}: '{}'",
        "not_found": "{} not found at resolved path{}: '{}'",
        "not_directory": "{} path exists but is not a directory{}: '{}'",
        "not_file": "{} path exists but is not a file{}: '{}'",
        "invalid": "Invalid {} path '{}'{}: {}",
    },
    # Operation section validation
    "operation": {
        "not_dict": "'{}' section must be a dictionary, got {}",
        "invalid_regex": "'{}' in {} is not a valid regex pattern: {}",
        "items_list_type": "'{}[{}]' must be a string, got {}",
        "items_list_empty": "'{}[{}]' cannot be empty",
        "features_type": "'features' section must be either a list or environment mapping dictionary, got {}",
        "empty_section": "'{}' section cannot be empty if specified",
        "empty_section_env": "'{}.{}' cannot be empty if specified",
        "invalid_constant_key": "Constant key in '{}' must be a non-empty string, got: {}",
        "unknown_constant": "Unknown constant '{}' in '{}' - this constant does not exist in fabric_cicd.constants",
    },
    # Log messages
    "log": {
        "override_section": "Override: {} '{}' section with value: '{}'",
        "override_setting": "Override: {} {}.{} with value: '{}'",
        "override_env_specific": "Override: updated {}.{}.{} with value: '{}'",
        "override_env_mapping": "Override: {}.{} added with environment mapping, with {} value: '{}'",
        "override_added_section": "Override: added '{}' section",
    },
}
