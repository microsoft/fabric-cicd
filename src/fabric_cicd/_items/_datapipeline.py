# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy DataPipeline item."""

import json
import logging
import re

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._items._manage_dependencies import lookup_referenced_item, set_publish_order

logger = logging.getLogger(__name__)


def publish_datapipelines(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all data pipeline items from the repository in the correct order based on their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "DataPipeline"

    # Set the order of data pipelines to be published based on their dependencies
    publish_order = set_publish_order(fabric_workspace_obj, item_type, find_referenced_datapipelines)

    fabric_workspace_obj._refresh_deployed_items()

    # Publish
    for item_name in publish_order:
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=func_process_file
        )


def func_process_file(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:  # noqa: ARG001
    """
    Custom file processing for datapipeline items.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    return update_activity_references(workspace_obj, file_obj)


def find_referenced_datapipelines(fabric_workspace_obj: FabricWorkspace, file_content: dict, lookup_type: str) -> list:
    """
    Scan through pipeline file json dictionary and find pipeline references (including nested pipelines).

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_content: Dict representation of the pipeline-content file.
        lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    """
    item_type = "DataPipeline"
    reference_list = []
    guid_pattern = re.compile(constants.VALID_GUID_REGEX)

    # Use the dpath library to search through the dictionary for all values that match the GUID pattern
    for _, value in dpath.search(file_content, "**", yielded=True):
        if isinstance(value, str):
            match = guid_pattern.search(value)
            if match:
                # If a valid GUID is found, convert it to name. If name is not None, it's a pipeline and will be added to the reference list
                referenced_id = match.group(0)
                referenced_name = fabric_workspace_obj._convert_id_to_name(
                    item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type
                )
                # Add pipeline to the reference list if it's not already present
                if referenced_name and referenced_name not in reference_list:
                    reference_list.append(referenced_name)

    return reference_list


def update_activity_references(fabric_workspace_obj: FabricWorkspace, file_obj: File) -> str:
    """
    Updates the item connection referenced in a data pipeline activity where the activity points
    to an item within the same workspace, but the workspace ID is not the default guid (all zeroes)
    and the item ID is a guid rather than a logical ID. The function replaces the workspace ID with
    the target workspace and the item ID with the deployed guid.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_obj: The file object.
    """
    # Create a dictionary from the raw file
    item_content_dict = json.loads(file_obj.contents)
    guid_pattern = re.compile(constants.VALID_GUID_REGEX)

    # dpath library finds and replaces feature branch workspace IDs found in all levels of activities in the dictionary
    for path, activity_value in dpath.search(item_content_dict, "**/type", yielded=True):
        # Ensure the type value is a string and check if it is found in the activities mapping
        if isinstance(activity_value, str) and activity_value in constants.DATA_PIPELINE_ACTIVITY_TYPES:
            workspace_id_str, item_type, item_id_name, api_item_type = constants.DATA_PIPELINE_ACTIVITY_TYPES[
                activity_value
            ]
            # Split the path into components, create a path to 'workspaceId' and get the workspace ID value
            path = path.split("/")
            workspace_id_path = (*path[:-1], "typeProperties", workspace_id_str)
            workspace_id = dpath.get(item_content_dict, workspace_id_path, default=None)

            # Check if the workspace ID is a valid GUID and is not the target workspace ID
            if workspace_id and guid_pattern.match(workspace_id) and workspace_id != fabric_workspace_obj.workspace_id:
                # item_type, item_id_name, api_item_type = constants.DATA_PIPELINE_ACTIVITY_TYPES[activity_value]
                # Create a path to the item's ID and get the item ID value
                item_id_path = (*path[:-1], "typeProperties", item_id_name)
                item_id = dpath.get(item_content_dict, item_id_path)
                # Retrieve the deployed guid for the item in the target workspace
                deployed_guid = lookup_referenced_item(
                    fabric_workspace_obj, workspace_id, item_type, item_id, api_item_type
                )
                if deployed_guid:
                    dpath.set(item_content_dict, workspace_id_path, fabric_workspace_obj.workspace_id)
                    dpath.set(item_content_dict, item_id_path, deployed_guid)

    # Convert the updated dict back to a JSON string
    return json.dumps(item_content_dict, indent=2)
