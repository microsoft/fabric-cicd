# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import logging
from typing import Optional

import dpath.util as dpath

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError
from fabric_cicd._common._logging import print_header
from fabric_cicd._common._validate_input import (
    validate_fabric_workspace_obj,
)
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_all_items(fabric_workspace_obj: FabricWorkspace, item_name_exclude_regex: Optional[str] = None) -> None:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.


    Examples:
        Basic usage
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)

        With regex name exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> exclude_regex = ".*_do_not_publish"
        >>> publish_all_items(workspace, exclude_regex)
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)

    # check if workspace has assigned capacity, if not, exit
    has_assigned_capacity = None

    response_state = fabric_workspace_obj.endpoint.invoke(
        method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}"
    )

    has_assigned_capacity = dpath.get(response_state, "body/capacityId", default=None)

    if (
        not has_assigned_capacity
        and fabric_workspace_obj.item_type_in_scope not in constants.NO_ASSIGNED_CAPACITY_REQUIRED
    ):
        msg = f"Workspace {fabric_workspace_obj.workspace_id} does not have an assigned capacity. Please assign a capacity before publishing items."
        raise FailedPublishedItemStatusError(msg, logger)

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

    def _should_publish_item_type(item_type: str) -> bool:
        """Check if an item type should be published based on scope and repository content."""
        return (
            item_type in fabric_workspace_obj.item_type_in_scope and item_type in fabric_workspace_obj.repository_items
        )

    if _should_publish_item_type("VariableLibrary"):
        print_header("Publishing Variable Libraries")
        items.publish_variablelibraries(fabric_workspace_obj)
    if _should_publish_item_type("Warehouse"):
        print_header("Publishing Warehouses")
        items.publish_warehouses(fabric_workspace_obj)
    if _should_publish_item_type("Lakehouse"):
        print_header("Publishing Lakehouses")
        items.publish_lakehouses(fabric_workspace_obj)
    if _should_publish_item_type("SQLDatabase"):
        print_header("Publishing SQL Databases")
        items.publish_sqldatabases(fabric_workspace_obj)
    if _should_publish_item_type("MirroredDatabase"):
        print_header("Publishing Mirrored Databases")
        items.publish_mirroreddatabase(fabric_workspace_obj)
    if _should_publish_item_type("Environment"):
        print_header("Publishing Environments")
        items.publish_environments(fabric_workspace_obj)
    if _should_publish_item_type("Notebook"):
        print_header("Publishing Notebooks")
        items.publish_notebooks(fabric_workspace_obj)
    if _should_publish_item_type("SemanticModel"):
        print_header("Publishing Semantic Models")
        items.publish_semanticmodels(fabric_workspace_obj)
    if _should_publish_item_type("Report"):
        print_header("Publishing Reports")
        items.publish_reports(fabric_workspace_obj)
    if _should_publish_item_type("CopyJob"):
        print_header("Publishing Copy Jobs")
        items.publish_copyjobs(fabric_workspace_obj)
    if _should_publish_item_type("Eventhouse"):
        print_header("Publishing Eventhouses")
        items.publish_eventhouses(fabric_workspace_obj)
    if _should_publish_item_type("KQLDatabase"):
        print_header("Publishing KQL Databases")
        items.publish_kqldatabases(fabric_workspace_obj)
    if _should_publish_item_type("KQLQueryset"):
        print_header("Publishing KQL Querysets")
        items.publish_kqlquerysets(fabric_workspace_obj)
    if _should_publish_item_type("Reflex"):
        print_header("Publishing Activators")
        items.publish_activators(fabric_workspace_obj)
    if _should_publish_item_type("Eventstream"):
        print_header("Publishing Eventstreams")
        items.publish_eventstreams(fabric_workspace_obj)
    if _should_publish_item_type("KQLDashboard"):
        print_header("Publishing KQL Dashboards")
        items.publish_kqldashboard(fabric_workspace_obj)
    if _should_publish_item_type("Dataflow"):
        print_header("Publishing Dataflows")
        items.publish_dataflows(fabric_workspace_obj)
    if _should_publish_item_type("DataPipeline"):
        print_header("Publishing Data Pipelines")
        items.publish_datapipelines(fabric_workspace_obj)
    if _should_publish_item_type("GraphQLApi"):
        print_header("Publishing GraphQL APIs")
        logger.warning(
            "Only user authentication is supported for GraphQL API items sourced from SQL Analytics Endpoint"
        )
        items.publish_graphqlapis(fabric_workspace_obj)

    # Check Environment Publish
    if _should_publish_item_type("Environment"):
        print_header("Checking Environment Publish State")
        items.check_environment_publish_state(fabric_workspace_obj)


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
        if item_type in fabric_workspace_obj.item_type_in_scope and item_type in fabric_workspace_obj.deployed_items:
            unpublish_flag = unpublish_flag_mapping.get(item_type)
            # Append item_type if no feature flag is required or the corresponding flag is enabled
            if not unpublish_flag or unpublish_flag in constants.FEATURE_FLAG:
                unpublish_order.append(item_type)

    for item_type in unpublish_order:
        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

        to_delete_set = deployed_names - repository_names
        to_delete_list = [name for name in to_delete_set if not regex_pattern.match(name)]

        if item_type == "DataPipeline":
            find_referenced_items_func = items.find_referenced_datapipelines

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
