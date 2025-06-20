# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process items with dependencies."""

import base64
import json
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import Callable

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def set_publish_order(
    fabric_workspace_obj: FabricWorkspace, item_type: str, find_referenced_items_func: Callable
) -> list:
    """
    Creates a publish order list for items of the same type, considering their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_type: Type of item to order (e.g., 'DataPipeline', 'Dataflow').
        find_referenced_items_func: Function to find referenced items in content.
    """
    # Get all items of the given type from the repository
    items = fabric_workspace_obj.repository_items.get(item_type, {})

    # Construct the unsorted_dict with an item and its associated file content
    unsorted_dict = {}
    # Set the file name based on the item type (e.g., 'pipeline-content.json' for DataPipeline, 'mashup.pq' for Dataflow)
    file_name = constants.ITEM_TYPE_TO_FILE[item_type]

    for item_name, item_details in items.items():
        with Path(item_details.path, file_name).open(encoding="utf-8") as f:
            raw_file = f.read()

        # If the file is a JSON, load as dict; otherwise, keep as the raw file
        item_content = json.loads(raw_file) if file_name.endswith(".json") else raw_file
        unsorted_dict[item_name] = item_content

    # Return a list of items sorted by their dependencies
    return sort_items(fabric_workspace_obj, unsorted_dict, "Repository", find_referenced_items_func)


def set_unpublish_order(
    fabric_workspace_obj: FabricWorkspace,
    item_type: str,
    unpublish_list: list,
    find_referenced_items_func: Callable,
) -> list:
    """
    Creates an unpublish order list for items of the same type, considering their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_type: Type of item to order (e.g., 'DataPipeline', 'Dataflow').
        unpublish_list: List of items to unpublish.
        find_referenced_items_func: Function to find referenced items in content.
    """
    unsorted_item_dict = {}
    file_name = constants.ITEM_TYPE_TO_FILE[item_type]

    for item_name in unpublish_list:
        # Get deployed item definition
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition
        item_guid = fabric_workspace_obj.deployed_items[item_type][item_name].guid
        response = fabric_workspace_obj.endpoint.invoke(
            method="POST", url=f"{fabric_workspace_obj.base_api_url}/items/{item_guid}/getDefinition"
        )
        for part in response["body"]["definition"]["parts"]:
            if part["path"] == file_name:
                # Decode Base64 string to dictionary
                decoded_bytes = base64.b64decode(part["payload"])
                decoded_string = decoded_bytes.decode("utf-8")
                unsorted_item_dict[item_name] = (
                    json.loads(decoded_string) if file_name.endswith(".json") else decoded_string
                )
                break

    # Determine order to delete w/o dependencies
    return sort_items(fabric_workspace_obj, unsorted_item_dict, "Deployed", find_referenced_items_func)


def sort_items(
    fabric_workspace_obj: FabricWorkspace, unsorted_dict: dict, lookup_type: str, find_referenced_items_func: Callable
) -> list:
    """
    Performs topological sort on items of a given item type based on their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        unsorted_dict: Dictionary mapping items to their file content.
        lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
        find_referenced_items_func: Function to find referenced items in content.
    """
    # Step 1: Create a graph to manage dependencies
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    unpublish_items = []

    # Step 2: Build the graph and count the in-degrees
    for item_name, item_content in unsorted_dict.items():
        logger.debug(f"Processing item: '{item_name}'")
        # In an unpublish case, keep track of items to get unpublished
        if lookup_type == "Deployed":
            unpublish_items.append(item_name)

        referenced_items = find_referenced_items_func(fabric_workspace_obj, item_content, lookup_type)

        for referenced_name in referenced_items:
            graph[referenced_name].append(item_name)
            in_degree[item_name] += 1
        # Ensure every item has an entry in the in-degree map
        if item_name not in in_degree:
            in_degree[item_name] = 0

    logger.debug(f"Graph: {graph}")
    logger.debug(f"In-degree map: {in_degree}")

    # In an unpublish case, adjust in_degree to include entire dependency chain
    if lookup_type == "Deployed":
        for item_name in graph:
            if item_name not in in_degree:
                in_degree[item_name] = 0
            for neighbor in graph[item_name]:
                if neighbor not in in_degree:
                    in_degree[neighbor] += 1

    # Step 3: Perform a topological sort to determine the correct publish order
    zero_in_degree_queue = deque([item_name for item_name in in_degree if in_degree[item_name] == 0])
    sorted_items = []
    logger.debug(f"Zero_in_degree_queue: {zero_in_degree_queue}")

    while zero_in_degree_queue:
        item_name = zero_in_degree_queue.popleft()
        sorted_items.append(item_name)

        for neighbor in graph[item_name]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree_queue.append(neighbor)

    if len(sorted_items) != len(in_degree):
        msg = "There is a cycle in the graph. Cannot determine a valid publish order."
        raise ParsingError(msg, logger)

    # Remove items not present in unpublish list and invert order for deployed sort
    if lookup_type == "Deployed":
        sorted_items = [item_name for item_name in sorted_items if item_name in unpublish_items]
        sorted_items = sorted_items[::-1]

    logger.debug(f"Sorted items in {lookup_type}: {sorted_items}")
    return sorted_items


def lookup_referenced_item(
    fabric_workspace_obj: FabricWorkspace,
    workspace_id: str,
    item_type: str,
    item_id: str,
    api_item_type: str,
    get_name: bool = False,
) -> str:
    """
    Looks up a referenced item in its source workspace and checks if the same
    item name exists in the repository or deployed workspace (indicating the referenced
    item exists in the same workspace as the referencing item). If found, returns either
    the item's name (when get_name is True) or its guid in the target workspace.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        workspace_id: The workspace ID of the item.
        item_type: Type of the item (e.g., 'DataPipeline', 'Dataflow').
        item_id: The guid of the item to look up.
        api_item_type: The API GET item type (e.g., 'dataflows').
        get_name: If True, return the item name instead of the guid.
    """
    # Get the item name using the workspace ID and item ID
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{workspace_id}/{api_item_type}/{item_id}",
    )
    item_name = response.get("body", {}).get("displayName", "")
    logger.debug(f"Looking up item: '{item_name}' with id: '{item_id}' in workspace: '{workspace_id}'")

    # Return name if requested, otherwise return guid if found, or empty string
    return (
        item_name
        if (
            get_name
            and (
                item_name in fabric_workspace_obj.repository_items.get(item_type, {})
                or item_name in fabric_workspace_obj.deployed_items.get(item_type, {})
            )
        )
        else fabric_workspace_obj.deployed_items.get(item_type, {}).get(item_name, Item("", "", "", "")).guid or ""
    )
