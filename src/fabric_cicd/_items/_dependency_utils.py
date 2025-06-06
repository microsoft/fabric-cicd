# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process items with dependencies."""

import json
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import Callable

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import ParsingError

logger = logging.getLogger(__name__)


def set_publish_order(
    fabric_workspace_obj: FabricWorkspace, item_type: str, file_name: str, find_referenced_items_func: Callable
) -> list:
    """
    Creates a publish order list for items of the same type, considering their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        item_type: Type of item to order (e.g., 'DataPipeline', 'Dataflow')
        file_name: Name of file containing item references (e.g., 'pipeline-content.json', 'mashup.pq')
        find_referenced_items_func: Function to find referenced items in content
    """
    # Get all items of the given type from the repository
    items = fabric_workspace_obj.repository_items.get(item_type, {})

    # Construct the unsorted_dict with an item and its associated file content
    unsorted_dict = {}
    for item_name, item_details in items.items():
        with Path(item_details.path, file_name).open(encoding="utf-8") as f:
            raw_file = f.read()

        # If the file is a JSON, load as dict; otherwise, keep as the raw file
        item_content = json.loads(raw_file) if file_name.endswith(".json") else raw_file
        unsorted_dict[item_name] = item_content

    # Return a list of items sorted by their dependencies
    return sort_items(fabric_workspace_obj, unsorted_dict, "Repository", find_referenced_items_func)


def sort_items(
    fabric_workspace_obj: FabricWorkspace, unsorted_dict: dict, lookup_type: str, find_referenced_items_func: Callable
) -> list:
    """
    Performs topological sort on items of a given item type based on their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        unsorted_dict: Dictionary mapping items to their file content
        lookup_type: Source of reference resolution ('Repository' or 'Deployed')
        find_referenced_items_func: Function to find referenced items in content
    """
    # Step 1: Create a graph to manage dependencies
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    unpublish_items = []

    # Step 2: Build the graph and count the in-degrees
    for item_name, item_content in unsorted_dict.items():
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

    return sorted_items
