# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Lakehouse item."""

import json
import logging

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError
from fabric_cicd._common._fabric_endpoint import handle_retry
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

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            continue

        check_sqlendpoint_provision_status(fabric_workspace_obj, item)

        logger.info(f"{constants.INDENT}Published")

    # Need all lakehouses published first to protect interrelationships
    if "enable_shortcut_publish" in constants.FEATURE_FLAG:
        for item_obj in fabric_workspace_obj.repository_items.get(item_type, {}).values():
            # Check if the item is published to avoid any post publish actions
            if item_obj.skip_publish:
                continue
            process_shortcuts(fabric_workspace_obj, item_obj)


def check_sqlendpoint_provision_status(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Check the SQL endpoint status of the published lakehouses

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to check the SQL endpoint status for

    """
    iteration = 1

    while True:
        sql_endpoint_status = None

        response_state = fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{fabric_workspace_obj.base_api_url}/lakehouses/{item_obj.guid}"
        )

        sql_endpoint_status = dpath.get(
            response_state, "body/properties/sqlEndpointProperties/provisioningStatus", default=None
        )

        if sql_endpoint_status == "Success":
            logger.info(f"{constants.INDENT}SQL Endpoint provisioned successfully")
            break

        if sql_endpoint_status == "Failed":
            msg = f"Cannot resolve SQL endpoint for lakehouse {item_obj.name}"
            raise FailedPublishedItemStatusError(msg, logger)

        handle_retry(
            attempt=iteration,
            base_delay=5,
            response_retry_after=30,
            prepend_message=f"{constants.INDENT}SQL Endpoint provisioning in progress",
        )
        iteration += 1


def process_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Publishes all shortcuts for a lakehouse item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to publish shortcuts for
    """
    deployed_shortcuts = list_deployed_shortcuts(fabric_workspace_obj, item_obj)

    shortcut_file_obj = next((file for file in item_obj.item_files if file.name == "shortcuts.metadata.json"), None)

    if shortcut_file_obj:
        shortcut_file_obj.contents = fabric_workspace_obj._replace_parameters(shortcut_file_obj, item_obj)
        shortcut_file_obj.contents = fabric_workspace_obj._replace_logical_ids(shortcut_file_obj.contents)
        shortcut_file_obj.contents = fabric_workspace_obj._replace_workspace_ids(shortcut_file_obj.contents)

        shortcuts = json.loads(shortcut_file_obj.contents) or []
    else:
        logger.debug("No shortcuts.metadata.json found")
        shortcuts = []

    shortcuts_to_publish = {f"{shortcut['path']}/{shortcut['name']}": shortcut for shortcut in shortcuts}

    if shortcuts_to_publish:
        logger.info(f"Publishing Lakehouse '{item_obj.name}' Shortcuts")
        shortcut_paths_to_unpublish = [path for path in deployed_shortcuts if path not in shortcuts_to_publish]
        unpublish_shortcuts(fabric_workspace_obj, item_obj, shortcut_paths_to_unpublish)
        # Deploy and overwrite shortcuts
        publish_shortcuts(fabric_workspace_obj, item_obj, shortcuts_to_publish)


def publish_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item, shortcut_dict: dict) -> None:
    """
    Publishes all shortcuts defined in the list.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to publish shortcuts for
        shortcut_dict: The dict of shortcuts to publish
    """
    for shortcut in shortcut_dict.values():
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut
        try:
            fabric_workspace_obj.endpoint.invoke(
                method="POST",
                url=f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite",
                body=shortcut,
            )
            logger.info(f"{constants.INDENT}{shortcut['name']} Shortcut Published")
        except Exception as e:
            if "continue_on_shortcut_failure" in constants.FEATURE_FLAG:
                logger.warning(
                    f"Failed to publish '{shortcut['name']}'. This usually happens when the lakehouse containing the source for this shortcut is published as a shell and has no data yet."
                )
                logger.info("The publish process will continue with the other items.")
                continue
            msg = f"Failed to publish '{shortcut['name']}' for lakehouse {item_obj.name}"
            raise FailedPublishedItemStatusError(msg, logger) from e


def unpublish_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item, shortcut_paths: list) -> None:
    """
    Unpublishes all shortcuts defined in the list.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to publish shortcuts for
        shortcut_paths: The list of shortcut paths to unpublish
    """
    for deployed_shortcut_path in shortcut_paths:
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/delete-shortcut
        fabric_workspace_obj.endpoint.invoke(
            method="DELETE",
            url=f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts/{deployed_shortcut_path}",
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
