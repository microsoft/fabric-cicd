# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
import re
from collections import defaultdict, deque
from pathlib import Path

from fabric_cicd._common._exceptions import ParsingError

"""
Functions to process and deploy DataPipeline item.
"""

logger = logging.getLogger(__name__)


def publish_datapipelines(fabric_workspace_obj):
    """Publishes all data pipeline items from the repository in the correct order based on their dependencies."""
    item_type = "DataPipeline"

    # Get all data pipelines from the repository
    pipelines = fabric_workspace_obj.repository_items.get(item_type, {})

    unsorted_pipeline_dict = {}

    # Construct unsorted_pipeline_dict with dict of pipeline
    unsorted_pipeline_dict = {}
    for item_name, item_details in pipelines.items():
        with Path.open(
            Path(item_details["path"], "pipeline-content.json"),
            encoding="utf-8",
        ) as f:
            raw_file = f.read()
        item_content_dict = json.loads(raw_file)

        unsorted_pipeline_dict[item_name] = item_content_dict

    publish_order = sort_datapipelines(fabric_workspace_obj, unsorted_pipeline_dict, "Repository")

    # Publish
    for item_name in publish_order:
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


def sort_datapipelines(fabric_workspace_obj, unsorted_pipeline_dict, lookup_type):
    """
    Output a sorted list that datapipelines should be published or unpublished with based on item dependencies.

    :param item_content_dict: Dict representation of the pipeline-content file.
    :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository)
    """
    # Step 1: Create a graph to manage dependencies
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    unpublish_items = []

    # Step 2: Build the graph and count the in-degrees
    for item_name, item_content_dict in unsorted_pipeline_dict.items():
        # In an unpublish case, keep track of items to get unpublished
        if lookup_type == "Deployed":
            unpublish_items.append(item_name)

        referenced_pipelines = _find_referenced_datapipelines(
            fabric_workspace_obj, item_content_dict=item_content_dict, lookup_type=lookup_type
        )

        for referenced_name in referenced_pipelines:
            graph[referenced_name].append(item_name)
            in_degree[item_name] += 1
        # Ensure every item has an entry in the in-degree map
        if item_name not in in_degree:
            in_degree[item_name] = 0

    # In an unpublish case, adjust in_degree to include entire dependency chain for each pipeline
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


def _find_referenced_datapipelines(fabric_workspace_obj, item_content_dict, lookup_type):
    """
    Scan through item path and find pipeline references (including nested pipelines).

    :param item_content_dict: Dict representation of the pipeline-content file.
    :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    :return: a list of referenced pipeline names.
    """
    item_type = "DataPipeline"
    reference_list = []
    guid_pattern = re.compile(r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")

    def find_datapipeline(input_object):
        """
        Recursively scans through JSON to find all pipeline references.

        :param input_object: Object can be a dict or list present in the input JSON.
        """
        # Check if the current object is a dict
        if isinstance(input_object, dict):
            for value in input_object.values():
                if isinstance(value, str):
                    match = guid_pattern.search(value)
                    if match:
                        # If a valid GUID is found, convert it to name. If name is not None, it's a pipeline and will be added to the reference list
                        referenced_id = match.group(0)
                        referenced_name = fabric_workspace_obj._convert_id_to_name(
                            item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type
                        )
                        if referenced_name:
                            reference_list.append(referenced_name)

                # Recursively search in the value
                else:
                    find_datapipeline(value)

        # Check if the current object is a list
        elif isinstance(input_object, list):
            # Recursively search in each item
            for item in input_object:
                find_datapipeline(item)

    # Start the recursive search from the root of the JSON data
    find_datapipeline(item_content_dict)

    return reference_list


def sort_datapipelines_new(fabric_workspace_obj, unsorted_pipeline_dict, lookup_type):
    """
    Output a sorted list that datapipelines should be published or unpublished with based on item dependencies.

    :param item_content_dict: Dict representation of the pipeline-content file.
    :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository)
    """
    # Step 1: Create a graph to manage dependencies
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    unpublish_items = []

    # Step 2: Build the graph and count the in-degrees
    for item_name, item_content_dict in unsorted_pipeline_dict.items():
        # In an unpublish case, keep track of items to get unpublished
        if lookup_type == "Deployed":
            unpublish_items.append(item_name)

        referenced_pipelines = _find_referenced_datapipelines(
            fabric_workspace_obj, item_content_dict=item_content_dict, lookup_type=lookup_type
        )

        referenced_pipelines = [item["name"] for item in referenced_pipelines.get("DataPipeline", [])]

        for referenced_name in referenced_pipelines:
            graph[referenced_name].append(item_name)
            in_degree[item_name] += 1
        # Ensure every item has an entry in the in-degree map
        if item_name not in in_degree:
            in_degree[item_name] = 0

    # In an unpublish case, adjust in_degree to include entire dependency chain for each pipeline
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


def _find_referenced_items(fabric_workspace_obj, item_content_dict, lookup_type, item_type=None):
    """
    Scan through item path and find item references (including nested ones).

    :param item_content_dict: Dict representation of the JSON file.
    :param item_type: Specifies the item type to include in the reference dict.
    :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    :return: a dictionary of referenced items (key: item type; values: item name, workspace Id).
    """
    reference_dict = {}
    guid_pattern = re.compile(r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")

    # Map activity type to item type
    activity_type_mapping = {
        "ExecutePipeline": "DataPipeline",
        "InvokePipeline": "DataPipeline",
        "RefreshDataflow": "Dataflow",
        "TridentNotebook": "Notebook",
    }

    def find_item(input_object, activity_type=None, workspace_id=None):
        """
        Recursively scans through JSON to find all item references.

        :param input_object: Object can be a dict or list present in the input JSON.
        :param activity_type: Activity type of the current object.
        :param workspace_id: Workspace Id of the current object.
        """
        # Check if the current object is a dictionary
        if isinstance(input_object, dict):
            # Get the current activity type and workspace Id, if available
            activity_type = input_object.get("type", activity_type)
            workspace_id = input_object.get("typeProperties", {}).get("workspaceId", workspace_id)

            # Map the activity type to the defined name
            mapped_activity_type = activity_type_mapping.get(activity_type, activity_type)

            for key, value in input_object.items():
                if isinstance(value, str) and guid_pattern.match(value) and key != "workspaceId":
                    # Convert GUID to name
                    referenced_name = fabric_workspace_obj._convert_id_to_name(
                        item_type=mapped_activity_type, generic_id=value, lookup_type=lookup_type
                    )
                    # Add the reference to the dictionary
                    if mapped_activity_type not in reference_dict:
                        reference_dict[mapped_activity_type] = []

                    reference_dict[mapped_activity_type].append({
                        "name": referenced_name,
                        "workspace_id": workspace_id if key in ["pipelineId", "dataflowId", "notebookId"] else None,
                    })

                else:
                    # Recursively call the function for nested dictionaries or lists
                    find_item(value, mapped_activity_type, workspace_id)

        # Check if the current object is a list
        elif isinstance(input_object, list):
            for item in input_object:
                # Recursively search in each item
                find_item(item, activity_type, workspace_id)

    # Get the list of activities from the input_dict and start the recursive search for references
    find_item(item_content_dict.get("properties", {}).get("activities", []))

    # Filter the output dictionary based on item_type if provided
    if item_type:
        return {item_type: reference_dict.get(item_type, [])}

    # Return the entire dictionary of references if no item_type is provided
    return reference_dict


# def update_workspaceId - call reference
