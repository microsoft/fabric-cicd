# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Dataflow Gen2 item."""

import logging
import re
from collections import defaultdict

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._items._dependency_utils import set_publish_order

logger = logging.getLogger(__name__)


def publish_dataflows(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all dataflow items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Dataflow"

    # Set the order of items to be published based on their dependencies
    publish_order = set_publish_order(fabric_workspace_obj, item_type, "mashup.pq", _find_referenced_dataflows)

    # Publish
    for item_name in publish_order:
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


def _find_referenced_dataflows(fabric_workspace_obj: FabricWorkspace, file_content: str, lookup_type: str) -> list:  # noqa: ARG001
    """
    Scan through the pq file and find dataflow references using regex matching.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_content: mashup.pq file content.
        lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    """
    reference_list = []
    dataflow_pattern = re.compile(constants.DATAFLOW_ID_REFERENCE_REGEX)
    workspace_pattern = re.compile(constants.WORKSPACE_ID_REFERENCE_REGEX)

    # Find all matches with positions
    workspace_matches = [(m.start(), "workspaceId", m.group(2)) for m in workspace_pattern.finditer(file_content)]
    dataflow_matches = [(m.start(), "dataflowId", m.group(2)) for m in dataflow_pattern.finditer(file_content)]

    # Combine and sort all matches by position
    all_matches = sorted(workspace_matches + dataflow_matches, key=lambda x: x[0])

    # Traverse and build mapping
    workspace_to_dataflows = defaultdict(list)
    current_workspace = None

    for _, kind, guid in all_matches:
        if kind == "workspaceId":
            current_workspace = guid
        elif kind == "dataflowId" and current_workspace:
            workspace_to_dataflows[current_workspace].append(guid)

    # Convert defaultdict to regular dict for output
    workspace_to_dataflows = dict(workspace_to_dataflows)

    for workspace_id, dataflow_id in workspace_to_dataflows.items():
        for dataflow in dataflow_id:
            response = fabric_workspace_obj.endpoint.invoke(
                method="GET",
                url=f"https://msitapi.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataflows/{dataflow}",
            )
            dataflow_name = dpath.get(response, "body/displayName", default=None)
            if (
                dataflow_name
                and (
                    dataflow_name in fabric_workspace_obj.repository_items.get("Dataflow", {})
                    or dataflow_name in fabric_workspace_obj.deployed_items.get("Dataflow", {})
                )
                and dataflow_name not in reference_list
            ):
                reference_list.append(dataflow_name)

    return reference_list
