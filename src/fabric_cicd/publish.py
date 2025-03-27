# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import base64
import json
import logging
from typing import Optional

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
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
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)
    if item_name_exclude_regex:
        logger.warning(
            "Using item_name_exclude_regex is risky as it can prevent needed dependencies from being deployed.  Use at your own risk."
        )
        fabric_workspace_obj.publish_item_name_exclude_regex = item_name_exclude_regex

    if "Lakehouse" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing Lakehouses")
        items.publish_lakehouses(fabric_workspace_obj)
    if "MirroredDatabase" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing MirroredDatabase")
        items.publish_mirroreddatabase(fabric_workspace_obj)
    if "Environment" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing Environments")
        items.publish_environments(fabric_workspace_obj)
    if "Notebook" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing Notebooks")
        items.publish_notebooks(fabric_workspace_obj)
    if "SemanticModel" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing SemanticModels")
        items.publish_semanticmodels(fabric_workspace_obj)
    if "Report" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing Reports")
        items.publish_reports(fabric_workspace_obj)
    if "DataPipeline" in fabric_workspace_obj.item_type_in_scope:
        _print_header("Publishing DataPipelines")
        items.publish_datapipelines(fabric_workspace_obj)


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
    _print_header("Unpublishing Orphaned Items")

    # Define order to unpublish items
    unpublish_order = []
    for x in ["DataPipeline", "Report", "SemanticModel", "Notebook", "Environment", "MirroredDatabase", "Lakehouse"]:
        if x in fabric_workspace_obj.item_type_in_scope and (
            x != "Lakehouse" or "enable_lakehouse_unpublish" in constants.FEATURE_FLAG
        ):
            unpublish_order.append(x)

    for item_type in unpublish_order:
        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

        to_delete_set = deployed_names - repository_names
        to_delete_list = [name for name in to_delete_set if not regex_pattern.match(name)]

        if item_type == "DataPipeline":
            # need to first define order of delete
            unsorted_pipeline_dict = {}

            for item_name in to_delete_list:
                # Get deployed item definition
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition
                item_guid = fabric_workspace_obj.deployed_items[item_type][item_name].guid
                response = fabric_workspace_obj.endpoint.invoke(
                    method="POST", url=f"{fabric_workspace_obj.base_api_url}/items/{item_guid}/getDefinition"
                )

                for part in response["body"]["definition"]["parts"]:
                    if part["path"] == "pipeline-content.json":
                        # Decode Base64 string to dictionary
                        decoded_bytes = base64.b64decode(part["payload"])
                        decoded_string = decoded_bytes.decode("utf-8")
                        unsorted_pipeline_dict[item_name] = json.loads(decoded_string)

            # Determine order to delete w/o dependencies
            to_delete_list = items.sort_datapipelines(fabric_workspace_obj, unsorted_pipeline_dict, "Deployed")

        for item_name in to_delete_list:
            fabric_workspace_obj._unpublish_item(item_name=item_name, item_type=item_type)


def _print_header(message: str) -> None:
    """
    Prints a header message with a decorative line above and below it.

    Args:
        message: The header message to print.
    """

    def print_with_color(message: str) -> None:
        print(f"\033[32m{message}\033[0m")

    line_separator = "#" * 100
    formatted_message = f"########## {message}"
    formatted_message = f"{formatted_message} {line_separator[len(formatted_message) + 1 :]}"

    print()  # Print a blank line before the header
    print_with_color(line_separator)
    print_with_color(formatted_message)
    print_with_color(line_separator)
    print()  # Print a blank line after the header
