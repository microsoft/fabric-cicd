# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import logging
from typing import Any, Optional

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
from fabric_cicd._common._logging import print_header
from fabric_cicd._common._validate_input import (
    validate_fabric_workspace_obj,
)
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_all_items(fabric_workspace_obj: FabricWorkspace, item_name_exclude_regex: Optional[str] = None) -> dict[str, dict[str, Any]]:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.

    Returns:
        A dictionary containing the published items organized by item type and item name.
        Structure: {item_type: {item_name: item_object}}
        
        Each item_object contains:
        - type: The item type (e.g., "SemanticModel", "Report")
        - name: The display name of the item
        - description: The item description
        - guid: The unique identifier of the published item
        - logical_id: The logical ID from the repository
        - folder_id: The folder ID where the item is located

    Examples:
        Basic usage
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> published_items = publish_all_items(workspace)
        >>> semantic_models = published_items.get("SemanticModel", {})
        >>> reports = published_items.get("Report", {})

        With regex name exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> exclude_regex = ".*_do_not_publish"
        >>> published_items = publish_all_items(workspace, exclude_regex)
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)

    if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
        fabric_workspace_obj._refresh_deployed_folders()
        fabric_workspace_obj._refresh_repository_folders()
        fabric_workspace_obj._publish_folders()

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()

    if item_name_exclude_regex:
        logger.warning(
            "Using item_name_exclude_regex is risky as it can prevent needed dependencies from being deployed.  Use at your own risk."
        )
        fabric_workspace_obj.publish_item_name_exclude_regex = item_name_exclude_regex

    # Collect published items information
    published_items = {}
    
    # Helper function to add items to the published_items dictionary
    def collect_published_items_for_type(item_type: str) -> None:
        if item_type in fabric_workspace_obj.repository_items:
            published_items[item_type] = {}
            for item_name, item_obj in fabric_workspace_obj.repository_items[item_type].items():
                # Convert Item object to a dictionary representation
                published_items[item_type][item_name] = {
                    "type": item_obj.type,
                    "name": item_obj.name,
                    "description": item_obj.description,
                    "guid": item_obj.guid,
                    "logical_id": item_obj.logical_id,
                    "folder_id": item_obj.folder_id,
                    "path": str(item_obj.path),
                }

    if "VariableLibrary" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Variable Libraries")
        items.publish_variablelibraries(fabric_workspace_obj)
        collect_published_items_for_type("VariableLibrary")
    if "Warehouse" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Warehouses")
        items.publish_warehouses(fabric_workspace_obj)
        collect_published_items_for_type("Warehouse")
    if "Lakehouse" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Lakehouses")
        items.publish_lakehouses(fabric_workspace_obj)
        collect_published_items_for_type("Lakehouse")
    if "SQLDatabase" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing SQL Databases")
        items.publish_sqldatabases(fabric_workspace_obj)
        collect_published_items_for_type("SQLDatabase")
    if "MirroredDatabase" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Mirrored Databases")
        items.publish_mirroreddatabase(fabric_workspace_obj)
        collect_published_items_for_type("MirroredDatabase")
    if "Environment" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Environments")
        items.publish_environments(fabric_workspace_obj)
        collect_published_items_for_type("Environment")
    if "Notebook" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Notebooks")
        items.publish_notebooks(fabric_workspace_obj)
        collect_published_items_for_type("Notebook")
    if "SemanticModel" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Semantic Models")
        items.publish_semanticmodels(fabric_workspace_obj)
        collect_published_items_for_type("SemanticModel")
    if "Report" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Reports")
        items.publish_reports(fabric_workspace_obj)
        collect_published_items_for_type("Report")
    if "CopyJob" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Copy Jobs")
        items.publish_copyjobs(fabric_workspace_obj)
        collect_published_items_for_type("CopyJob")
    if "Eventhouse" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Eventhouses")
        items.publish_eventhouses(fabric_workspace_obj)
        collect_published_items_for_type("Eventhouse")
    if "KQLDatabase" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing KQL Databases")
        items.publish_kqldatabases(fabric_workspace_obj)
        collect_published_items_for_type("KQLDatabase")
    if "KQLQueryset" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing KQL Querysets")
        items.publish_kqlquerysets(fabric_workspace_obj)
        collect_published_items_for_type("KQLQueryset")
    if "Reflex" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Activators")
        items.publish_activators(fabric_workspace_obj)
        collect_published_items_for_type("Reflex")
    if "Eventstream" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Eventstreams")
        items.publish_eventstreams(fabric_workspace_obj)
        collect_published_items_for_type("Eventstream")
    if "KQLDashboard" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing KQL Dashboards")
        items.publish_kqldashboard(fabric_workspace_obj)
        collect_published_items_for_type("KQLDashboard")
    if "Dataflow" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Dataflows")
        items.publish_dataflows(fabric_workspace_obj)
        collect_published_items_for_type("Dataflow")
    if "DataPipeline" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing Data Pipelines")
        items.publish_datapipelines(fabric_workspace_obj)
        collect_published_items_for_type("DataPipeline")
    if "GraphQLApi" in fabric_workspace_obj.item_type_in_scope:
        print_header("Publishing GraphQL APIs")
        logger.warning(
            "Only user authentication is supported for GraphQL API items sourced from SQL Analytics Endpoint"
        )
        items.publish_graphqlapis(fabric_workspace_obj)
        collect_published_items_for_type("GraphQLApi")

    # Check Environment Publish
    if "Environment" in fabric_workspace_obj.item_type_in_scope:
        print_header("Checking Environment Publish State")
        items.check_environment_publish_state(fabric_workspace_obj)
    
    return published_items


def unpublish_all_orphan_items(fabric_workspace_obj: FabricWorkspace, item_name_exclude_regex: str = "^$") -> None:
    """
    Unpublishes all orphaned items not present in the repository except for those matching the exclude regex.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being unpublished. Default is '^$' which will exclude nothing.

    Examples:
        Basic usage
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)
        >>> unpublish_orphaned_items(workspace)

        With regex name exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)
        >>> exclude_regex = ".*_do_not_delete"
        >>> unpublish_orphaned_items(workspace, exclude_regex)
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)

    regex_pattern = check_regex(item_name_exclude_regex)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()
    print_header("Unpublishing Orphaned Items")

    # Lakehouses, SQL Databases, and Warehouses can only be unpublished if their feature flags are set
    unpublish_flag_mapping = {
        "Lakehouse": "enable_lakehouse_unpublish",
        "SQLDatabase": "enable_sqldatabase_unpublish",
        "Warehouse": "enable_warehouse_unpublish",
    }

    # Define order to unpublish items
    unpublish_order = []
    for item_type in [
        "GraphQLApi",
        "DataPipeline",
        "Dataflow",
        "Eventstream",
        "Reflex",
        "KQLDashboard",
        "KQLQueryset",
        "KQLDatabase",
        "Eventhouse",
        "CopyJob",
        "Report",
        "SemanticModel",
        "Notebook",
        "Environment",
        "MirroredDatabase",
        "SQLDatabase",
        "Lakehouse",
        "Warehouse",
        "VariableLibrary",
    ]:
        if item_type in fabric_workspace_obj.item_type_in_scope:
            unpublish_flag = unpublish_flag_mapping.get(item_type)
            # Append item_type if no feature flag is required or the corresponding flag is enabled
            if not unpublish_flag or unpublish_flag in constants.FEATURE_FLAG:
                unpublish_order.append(item_type)

    for item_type in unpublish_order:
        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

        to_delete_set = deployed_names - repository_names
        to_delete_list = [name for name in to_delete_set if not regex_pattern.match(name)]

        if item_type in ("Dataflow", "DataPipeline"):
            # Use the find referenced items function specific to the item type
            find_referenced_items_func = (
                items.find_referenced_datapipelines if item_type == "DataPipeline" else items.find_referenced_dataflows
            )
            # Determine order to delete w/o dependencies
            to_delete_list = items.set_unpublish_order(
                fabric_workspace_obj, item_type, to_delete_list, find_referenced_items_func
            )

        for item_name in to_delete_list:
            fabric_workspace_obj._unpublish_item(item_name=item_name, item_type=item_type)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_deployed_folders()
    if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
        fabric_workspace_obj._unpublish_folders()
