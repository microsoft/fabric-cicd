# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Lakehouse item."""

import json
import logging

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def publish_lakehouses(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all lakehouse items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
    """
    item_type = "Lakehouse"

    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        creation_payload = next(
            (
                {"enableSchemas": True}
                for file in item.item_files
                if file.name == "lakehouse.metadata.json" and "defaultSchema" in file.contents
            ),
            None,
        )

        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            creation_payload=creation_payload,
            skip_publish_logging=True,
        )

        if "enable_shortcut_publish" in constants.FEATURE_FLAGS:
            publish_shortcuts(fabric_workspace_obj, item)
        logger.info("Published")


def publish_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Publishes all shortcuts for a lakehouse item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to publish shortcuts for
    """
    deployed_shortcuts = list_deployed_shortcuts(fabric_workspace_obj, item_obj)

    shortcut_file_obj = next((file for file in item_obj.item_files if file.name == "shortcuts.metadata.json"), None)

    if not shortcut_file_obj:
        logger.debug("No shortcuts.metadata.json found")
        return

    shortcut_file_obj.contents = fabric_workspace_obj._replace_parameters(shortcut_file_obj, item_obj)
    shortcuts = json.loads(shortcut_file_obj.contents)

    if len(shortcuts) == 0:
        logger.debug("No shortcuts found in shortcuts.metadata.json")
        return

    logger.info("Publishing Shortcuts")
    new_deployed_shortcuts = {f"{shortcut['path']}/{shortcut['name']}": shortcut for shortcut in shortcuts}

    # Delete shortcuts not in the new list
    for deployed_shortcut_path in deployed_shortcuts:
        if deployed_shortcut_path not in new_deployed_shortcuts:
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/delete-shortcut
            fabric_workspace_obj.endpoint.invoke(
                method="DELETE",
                url=f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts/{deployed_shortcut_path}",
            )

    # Deploy and overwrite shortcuts
    for shortcut in new_deployed_shortcuts.values():
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut
        fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite",
            body=shortcut,
        )


def list_deployed_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> list:
    """
    Lists all deployed shortcut paths

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to list the shortcuts for
    """
    request_url = f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts"
    deployed_shortcut_paths = []

    while request_url:
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/list-shortcuts
        response = fabric_workspace_obj.endpoint.invoke(method="GET", url=request_url)

        # Handle cases where the response body is empty
        shortcuts = response["body"].get("value", [])
        deployed_shortcut_paths.extend(f"{shortcut['path']}/{shortcut['name']}" for shortcut in shortcuts)

        request_url = response["header"].get("continuationUri", None)

    return deployed_shortcut_paths
