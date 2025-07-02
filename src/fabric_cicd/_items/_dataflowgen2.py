# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Dataflow Gen2 item."""

import logging
import re

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._items._manage_dependencies import lookup_referenced_item, set_publish_order

logger = logging.getLogger(__name__)


def publish_dataflows(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all dataflow items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Dataflow"

    # Set the order of dataflows to be published based on their dependencies
    publish_order = set_publish_order(fabric_workspace_obj, item_type, find_referenced_dataflows)

    fabric_workspace_obj._refresh_deployed_items()

    # Publish
    for item_name in publish_order:
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


def find_referenced_dataflows(fabric_workspace_obj: FabricWorkspace, file_content: str, lookup_type: str) -> list:  # noqa: ARG001
    """
    Scan through the power query file and find dataflow references using regex matching.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_content: mashup.pq file content.
        lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    """
    reference_list = []
    workspace_pattern = re.compile(constants.WORKSPACE_ID_REFERENCE_REGEX)
    dataflow_pattern = re.compile(constants.DATAFLOW_ID_REFERENCE_REGEX)
    guid_pattern = re.compile(constants.VALID_GUID_REGEX)

    # Extract all matches with position, id_type, and guid for sorting in order of appearance in the file
    workspace_matches = [(m.start(), m.group(1), m.group(2)) for m in workspace_pattern.finditer(file_content)]
    dataflow_matches = [(m.start(), m.group(1), m.group(2)) for m in dataflow_pattern.finditer(file_content)]

    # Combine and sort all matches by position
    all_matches = sorted(workspace_matches + dataflow_matches, key=lambda x: x[0])

    # Process matches to find dataflow references
    item_type = "Dataflow"
    api_item_type = "dataflows"
    current_workspace = None
    processed_dataflows = set()

    for _, id_type, guid in all_matches:
        # Keep track of the current workspace and its associated dataflow using the processed_dataflows set
        if id_type == "workspaceId" and guid_pattern.match(guid):
            logger.debug(f"Found valid workspace ID: {guid}")
            current_workspace = guid
        elif (
            id_type == "dataflowId"
            and guid_pattern.match(guid)
            and current_workspace
            and (current_workspace, guid) not in processed_dataflows
        ):
            logger.debug(f"Found valid dataflow ID: {guid} in workspace: {current_workspace}")
            processed_dataflows.add((current_workspace, guid))
            # Get dataflow name
            dataflow_name = lookup_referenced_item(
                fabric_workspace_obj, current_workspace, item_type, guid, api_item_type, True
            )
            # If the dataflow exists in the repo or it's deployed, add it to the reference list if it's not already present
            if dataflow_name and dataflow_name not in reference_list:
                reference_list.append(dataflow_name)

    return reference_list
