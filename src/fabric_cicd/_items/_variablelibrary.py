# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Variable Library item."""

import json
import logging

from fabric_cicd import FabricWorkspace, constants
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
        if var_libraries[item_name].skip_publish:
            continue
        activate_value_set(fabric_workspace_obj, var_libraries[item_name])


def activate_value_set(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Activates the value set for the given Variable Library item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
    """
    settings_file_obj = next((file for file in item_obj.item_files if file.name == "settings.json"), None)

    if settings_file_obj:
        settings_dict = json.loads(settings_file_obj.contents)
        if fabric_workspace_obj.environment in settings_dict["valueSetsOrder"]:
            active_value_set = fabric_workspace_obj.environment
        else:
            active_value_set = "Default value set"
            logger.warning(
                f"Provided target environment '{fabric_workspace_obj.environment}' does not match any value sets.  Using '{active_value_set}'"
            )

        body = {"properties": {"activeValueSetName": active_value_set}}

        fabric_workspace_obj.endpoint.invoke(
            method="PATCH", url=f"{fabric_workspace_obj.base_api_url}/VariableLibraries/{item_obj.guid}", body=body
        )

        logger.info(f"{constants.INDENT}Active value set changed to '{active_value_set}'")

    else:
        logger.warning(f"settings.json file not found for item {item_obj.name}. Active value set not changed.")
