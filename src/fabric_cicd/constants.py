# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Constants for the fabric-cicd package."""

# General
VERSION = "0.1.23"
DEFAULT_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"
DEFAULT_API_ROOT_URL = "https://api.powerbi.com"
FABRIC_API_ROOT_URL = "https://api.fabric.microsoft.com"
FEATURE_FLAG = set()
USER_AGENT = f"ms-fabric-cicd/{VERSION}"

# Item Type
ACCEPTED_ITEM_TYPES_UPN = (
    "DataPipeline",
    "Environment",
    "Notebook",
    "Report",
    "SemanticModel",
    "Lakehouse",
    "MirroredDatabase",
    "VariableLibrary",
    "CopyJob",
    "Eventhouse",
    "KQLDatabase",
    "KQLQueryset",
    "Reflex",
    "Eventstream",
    "Warehouse",
    "SQLDatabase",
    "KQLDashboard",
    "Dataflow",
    "GraphQLApi",
)
ACCEPTED_ITEM_TYPES_NON_UPN = ACCEPTED_ITEM_TYPES_UPN

# Publish
SHELL_ONLY_PUBLISH = ["Environment", "Lakehouse", "Warehouse", "SQLDatabase"]

# REGEX Constants
VALID_GUID_REGEX = r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
WORKSPACE_ID_REFERENCE_REGEX = r"\"?(default_lakehouse_workspace_id|workspaceId|workspace)\"?\s*[:=]\s*\"(.*?)\""
DATAFLOW_ID_REFERENCE_REGEX = r'(dataflowId)\s*=\s*"(.*?)"'
INVALID_FOLDER_CHAR_REGEX = r'[~"#.%&*:<>?/\\{|}]'

# Item Type to File Mapping (to check for item dependencies)
ITEM_TYPE_TO_FILE = {"DataPipeline": "pipeline-content.json", "Dataflow": "mashup.pq"}
# Data Pipeline Activities mapping dictionary: {Key: activity_name, Value: [item_type, item_id_name, api_get_item_lookup]}
DATA_PIPELINE_ACTIVITY_TYPES = {
    "RefreshDataflow": ["workspaceId", "Dataflow", "dataflowId", "dataflows"],
    "PBISemanticModelRefresh": ["groupId", "SemanticModel", "datasetId", "semanticModels"],
}

# Parameter file configs
PARAMETER_FILE_NAME = "parameter.yml"
ITEM_ATTR_LOOKUP = ["id", "sqlendpoint"]

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
    "invalid file path": "Path '{}' not found in the repository directory",
    "valid optional": "Optional values in {} are valid",
    "valid parameter": "{} parameter is valid",
    "skip": "The {} '{}' replacement will be skipped due to {} in parameter {}",
    "no target env": "target environment '{}' not found",
    "no filter match": "unmatched optional filters",
}


INDENT = "->"
