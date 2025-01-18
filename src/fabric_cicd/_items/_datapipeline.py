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
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=_process_datapipeline_file_contents
        )


def _process_datapipeline_file_contents(fabric_workspace_obj, item_file_obj):
    return _replace_activity_workspace_ids(fabric_workspace_obj, item_file_obj)


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


def _replace_activity_workspace_ids(fabric_workspace_obj, item_file_obj):
    """
    Replaces feature branch workspace ID referenced in data pipeline activities with target workspace ID
    in the raw file content.

    :param raw_file: The raw file content where workspace IDs need to be replaced.
    :return: The raw file content with feature branch workspace IDs replaced by target workspace IDs.
    """
    # Create a dictionary from the raw_file
    item_content_dict = json.loads(item_file_obj.contents)

    def _find_and_replace_activity_workspace_ids(input_object):
        """
        Recursively scans through JSON to find and replace feature branch workspace IDs in nested and
        non-nested activities where workspaceId
        property exists (e.g. Trident Notebook). Note: the function can be modified to process other pipeline
        activities where workspaceId exists.

        :param input_object: Object can be a dictionary or list present in the input JSON.
        """
        # Check if the current object is a dictionary
        if isinstance(input_object, dict):
            target_workspace_id = fabric_workspace_obj.workspace_id

            # Iterate through the activities and search for TridentNotebook activities
            for key, value in input_object.items():
                if key == "type" and value == "TridentNotebook":
                    # Convert the notebook ID to its name
                    item_type = "Notebook"
                    referenced_id = input_object["typeProperties"]["notebookId"]
                    referenced_name = fabric_workspace_obj._convert_id_to_name(
                        item_type=item_type, generic_id=referenced_id, lookup_type="Repository"
                    )
                    # Replace workspace ID with target workspace ID if the referenced notebook exists in the repo
                    if referenced_name:
                        input_object["typeProperties"]["workspaceId"] = target_workspace_id

                # Recursively search in the value
                else:
                    _find_and_replace_activity_workspace_ids(value)

        # Check if the current object is a list
        elif isinstance(input_object, list):
            # Recursively search in each item
            for item in input_object:
                _find_and_replace_activity_workspace_ids(item)

    # Start the recursive search and replace from the root of the JSON data
    _find_and_replace_activity_workspace_ids(item_content_dict)

    # Convert the updated dict back to a JSON string
    item_file_obj.contents = json.dumps(item_content_dict, indent=2)

    return item_file_obj
