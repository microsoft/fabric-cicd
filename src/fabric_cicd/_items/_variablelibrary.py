# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Variable Library item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def publish_variablelibraries(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all variable library items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "VariableLibrary"

    var_libraries = fabric_workspace_obj.repository_items.get(item_type, {})

    for item_name in var_libraries:
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)
        activate_value_set(fabric_workspace_obj, var_libraries[item_name])


def activate_value_set(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Activates the value set for the given Variable Library item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
    """
    if fabric_workspace_obj.environment != "N/A":
        active_value_set = fabric_workspace_obj.environment
        body = {"properties": {"activeValueSetName": active_value_set}}

        fabric_workspace_obj.endpoint.invoke(
            method="PATCH", url=f"{fabric_workspace_obj.base_api_url}/VariableLibraries/{item_obj.guid}", body=body
        )

        logger.info(f"Active value set changed to '{active_value_set}'")
