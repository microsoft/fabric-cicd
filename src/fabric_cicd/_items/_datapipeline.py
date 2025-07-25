# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy DataPipeline item."""

import logging
import re

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._items._manage_dependencies import set_publish_order

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
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


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
