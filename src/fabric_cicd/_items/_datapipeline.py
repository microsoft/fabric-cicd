import json
import logging
import os
from collections import defaultdict, deque

from fabric_cicd._common._exceptions import ParsingError

"""
Functions to process and deploy DataPipeline item.
"""

logger = logging.getLogger(__name__)


def publish_datapipelines(fabric_workspace_obj):
    """
    Publishes all data pipeline items from the repository in the correct order based on their dependencies.
    """
    item_type = "DataPipeline"

    # Get all data pipelines from the repository
    pipelines = fabric_workspace_obj.repository_items.get(item_type, {})

    unsorted_pipeline_dict = {}

    # Construct unsorted_pipeline_dict with dict of pipeline
    unsorted_pipeline_dict = {}
    for item_name, item_details in pipelines.items():
        with open(
            os.path.join(item_details["path"], "pipeline-content.json"),
            "r",
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
        raise ParsingError("There is a cycle in the graph. Cannot determine a valid publish order.")

    # Remove items not present in unpublish list and invert order for deployed sort
    if lookup_type == "Deployed":
        sorted_items = [item_name for item_name in sorted_items if item_name in unpublish_items]
        sorted_items = sorted_items[::-1]

    return sorted_items


def _find_referenced_datapipelines(fabric_workspace_obj, item_content_dict, lookup_type):
    """
    Scan through item path and find pipeline references (including nested pipeline activities).

    :param item_content_dict: Dict representation of the pipeline-content file.
    :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    :return: a list of referenced pipeline names.
    """
    item_type = "DataPipeline"
    reference_list = []

    def find_execute_pipeline_activities(input_object):
        """
        Recursively scans through JSON to find all pipeline references.

        :param input_object: Object can be a dict or list present in the input JSON.
        """
        # Check if the current object is a dict
        if isinstance(input_object, dict):
            for key, value in input_object.items():
                referenced_id = None

                # Check for legacy and new pipeline activities
                if key == "type" and value == "ExecutePipeline":
                    referenced_id = input_object["typeProperties"]["pipeline"]["referenceName"]
                elif key == "type" and value == "InvokePipeline":
                    referenced_id = input_object["typeProperties"]["pipelineId"]

                # Add found pipeline reference to list
                if referenced_id is not None:
                    referenced_name = fabric_workspace_obj._convert_id_to_name(
                        item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type
                    )
                    if referenced_name:
                        reference_list.append(referenced_name)

                # Recursively search in the value
                else:
                    find_execute_pipeline_activities(value)

        # Check if the current object is a list
        elif isinstance(input_object, list):
            # Recursively search in each item
            for item in input_object:
                find_execute_pipeline_activities(item)

    # Start the recursive search from the root of the JSON data
    find_execute_pipeline_activities(item_content_dict)

    return reference_list
