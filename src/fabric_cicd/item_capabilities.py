# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Item type capabilities mapping for documentation matrix generation."""

from typing import Any

# Define capability categories and their descriptions
CAPABILITY_DESCRIPTIONS = {
    "source_control": "Source Control Support",
    "parameterization": "Parameterization (find_replace)",
    "content_deployment": "Content Deployment",
    "connection_management": "Connection Management Required",
    "unpublish_support": "Unpublish Support",
    "initial_config": "Manual Initial Configuration",
    "ordered_deployment": "Supports Ordered Deployment",
}

# Item type capabilities matrix
# Based on detailed analysis of docs/how_to/item_types.md
ITEM_CAPABILITIES: dict[str, dict[str, Any]] = {
    "DataPipeline": {
        "source_control": True,
        "parameterization": True,  # Activities connected to items in different workspace
        "content_deployment": True,
        "connection_management": True,  # Connections not source controlled
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Environment": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": False,  # Shell only, resources not deployed
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Notebook": {
        "source_control": True,
        "parameterization": True,  # Lakehouse references
        "content_deployment": False,  # Resources not deployed
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Report": {
        "source_control": True,
        "parameterization": True,  # Semantic Models outside workspace
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "SemanticModel": {
        "source_control": True,
        "parameterization": True,  # Sources outside workspace
        "content_deployment": True,
        "connection_management": True,  # Manual configuration after deployment
        "unpublish_support": True,
        "initial_config": True,  # Manual connection configuration required
        "ordered_deployment": False,
    },
    "Lakehouse": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": False,  # Shell only
        "connection_management": False,
        "unpublish_support": False,  # Disabled by default, feature flag required
        "initial_config": False,
        "ordered_deployment": False,
    },
    "MirroredDatabase": {
        "source_control": True,
        "parameterization": True,  # Connection source database
        "content_deployment": True,
        "connection_management": True,
        "unpublish_support": True,
        "initial_config": True,  # Manual SAMI permission granting required
        "ordered_deployment": False,
    },
    "VariableLibrary": {
        "source_control": True,
        "parameterization": True,  # Active value set by environment
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "CopyJob": {
        "source_control": True,
        "parameterization": True,  # Connection data sources
        "content_deployment": True,
        "connection_management": True,
        "unpublish_support": True,
        "initial_config": True,  # Manual connection configuration required
        "ordered_deployment": False,
    },
    "Eventhouse": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "KQLDatabase": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": False,  # Data not source controlled
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "KQLQueryset": {
        "source_control": True,
        "parameterization": True,  # KQL database references
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Reflex": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied (same as Activators)
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Eventstream": {
        "source_control": True,
        "parameterization": True,  # Destinations in different workspace
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": True,  # Wait for table population in lakehouse destination
        "ordered_deployment": False,
    },
    "Warehouse": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": False,  # Shell only, DDL deployed separately
        "connection_management": False,
        "unpublish_support": False,  # Disabled by default, feature flag required
        "initial_config": False,
        "ordered_deployment": False,
    },
    "SQLDatabase": {
        "source_control": True,
        "parameterization": False,  # find_replace not applied
        "content_deployment": False,  # Shell only, schema deployed separately
        "connection_management": False,
        "unpublish_support": False,  # Disabled by default, feature flag required
        "initial_config": False,
        "ordered_deployment": False,
    },
    "KQLDashboard": {
        "source_control": True,
        "parameterization": True,  # KQL database references
        "content_deployment": True,
        "connection_management": False,
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
    "Dataflow": {
        "source_control": True,
        "parameterization": True,  # Source/destination items, dataflow dependencies
        "content_deployment": True,
        "connection_management": True,  # Connections not source controlled
        "unpublish_support": True,
        "initial_config": True,  # Manual publish and refresh required
        "ordered_deployment": True,  # Automatic ordered deployment for dataflow dependencies
    },
    "GraphQLApi": {
        "source_control": True,
        "parameterization": True,  # Source workspace and item IDs, connections
        "content_deployment": True,
        "connection_management": True,  # Saved credentials access required
        "unpublish_support": True,
        "initial_config": False,
        "ordered_deployment": False,
    },
}


def generate_capabilities_matrix() -> str:
    """Generate a markdown table showing item type capabilities matrix."""
    if not ITEM_CAPABILITIES:
        return "No item capabilities data available."
    
    # Get all item types from constants to maintain order
    from fabric_cicd.constants import ACCEPTED_ITEM_TYPES_UPN
    
    # Filter to only include items that have capability data
    available_items = [item for item in ACCEPTED_ITEM_TYPES_UPN if item in ITEM_CAPABILITIES]
    
    if not available_items:
        return "No capability data available for supported item types."
    
    # Get capability keys in a consistent order
    capability_keys = list(CAPABILITY_DESCRIPTIONS.keys())
    
    # Generate table header
    header = "| Item Type | " + " | ".join(CAPABILITY_DESCRIPTIONS[key] for key in capability_keys) + " |"
    separator = "|" + "|".join([" --- "] * (len(capability_keys) + 1)) + "|"
    
    # Generate table rows
    rows = []
    for item_type in available_items:
        capabilities = ITEM_CAPABILITIES[item_type]
        row_values = []
        for key in capability_keys:
            value = capabilities.get(key, False)
            # Use checkmarks and X marks for better visual clarity
            symbol = "✓" if value else "✗"
            row_values.append(f" {symbol} ")
        
        row = f"| {item_type} | " + " | ".join(row_values) + " |"
        rows.append(row)
    
    # Combine all parts
    table_parts = [header, separator, *rows]
    
    # Add explanatory text
    explanation = """
> **Legend**: ✓ = Supported/Required, ✗ = Not supported/Not required
>
> **Notes**: 
> - **Source Control Support**: All listed item types support source control integration
> - **Parameterization**: Support for `find_replace` section in `parameter.yml` for environment-specific values
> - **Content Deployment**: Whether the item's content/data is deployed (vs. shell-only deployment)
> - **Connection Management**: Whether manual connection setup/configuration is required
> - **Unpublish Support**: Whether the item supports automatic unpublishing of orphaned items
> - **Manual Initial Configuration**: Whether manual configuration steps are required after initial deployment
> - **Ordered Deployment**: Whether the system automatically handles deployment order for dependencies
"""
    
    return "\n".join(table_parts) + "\n" + explanation