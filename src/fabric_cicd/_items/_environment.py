# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Environment item."""

import logging
import re

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._fabric_endpoint import handle_retry

logger = logging.getLogger(__name__)


def publish_environments(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all environment items from the repository.

    Environments are deployed using the updateDefinition API, and then compute settings and libraries are published separately.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    logger.warning("The underlying legacy Microsoft Fabric Environment APIs will be deprecated by March 2026.")
    logger.warning(
        "Please upgrade to the latest fabric-cicd version before March 2026 to prevent broken Environment item deployments."
    )

    # Check for ongoing publish
    check_environment_publish_state(fabric_workspace_obj, True)

    item_type = "Environment"
    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        # Deploy the environment definition
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            skip_publish_logging=True,
        )
        if item.skip_publish:
            continue
        _publish_environment_metadata(fabric_workspace_obj, item_name=item_name)


def _publish_environment_metadata(fabric_workspace_obj: FabricWorkspace, item_name: str) -> None:
    """
    Publishes compute settings and libraries for a given environment item.

    This process involves two steps:
    1. Check for ongoing publish.
    2. Publish the updated settings.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_name: Name of the environment item whose compute settings are to be published.
    """
    item_type = "Environment"
    item_guid = fabric_workspace_obj.repository_items[item_type][item_name].guid

    # Publish updated settings
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish?preview=False",
    )

    logger.info(f"{constants.INDENT}Publish Submitted")


def check_environment_publish_state(fabric_workspace_obj: FabricWorkspace, initial_check: bool = False) -> None:
    """
    Checks the publish state of environments after deployment

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        initial_check: Flag to ignore publish failures on initial check.
    """
    ongoing_publish = True
    iteration = 1

    environments = fabric_workspace_obj.repository_items.get("Environment", {})

    filtered_environments = [
        k
        for k in environments
        if (
            # Check exclude regex
            (
                not fabric_workspace_obj.publish_item_name_exclude_regex
                or not re.search(fabric_workspace_obj.publish_item_name_exclude_regex, k)
            )
            # Check items_to_include list
            and (
                not fabric_workspace_obj.items_to_include or k + ".Environment" in fabric_workspace_obj.items_to_include
            )
        )
    ]

    logger.info(f"Checking Environment Publish State for {filtered_environments}")

    while ongoing_publish:
        ongoing_publish = False

        response_state = fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{fabric_workspace_obj.base_api_url}/environments/"
        )

        for item in response_state["body"]["value"]:
            item_name = item["displayName"]
            item_state = dpath.get(item, "properties/publishDetails/state", default="").lower()
            if item_name in environments and item_state == "running":
                ongoing_publish = True
            elif item_state in ["failed", "cancelled"] and not initial_check:
                msg = f"Publish {item_state} for {item_name}"
                raise Exception(msg)

        if ongoing_publish:
            handle_retry(
                attempt=iteration,
                base_delay=5,
                response_retry_after=120,
                prepend_message=f"{constants.INDENT}Operation in progress.",
            )
            iteration += 1

    if not initial_check:
        logger.info(f"{constants.INDENT}Published.")
