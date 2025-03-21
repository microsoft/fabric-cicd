# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Environment item."""

import logging

from fabric_cicd import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_environments(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all environment items from the repository.

    Environments can only deploy the shell; compute and spark configurations are published separately.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Environment"
    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            skip_publish_logging=True,
        )

        item_guid = fabric_workspace_obj.repository_items[item_type][item_name].guid
        _publish_environment(fabric_workspace_obj, item_guid=item_guid)


def _publish_environment(fabric_workspace_obj: FabricWorkspace, item_guid: str) -> None:
    """
    Publishes compute settings and libraries for a given environment item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_guid: Guid of the environment item whose compute settings are to be published.
    """
    logger.info("Publishing Libraries & Spark Settings")
    # Publish updated settings
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish",
        base_delay=5,
        retry_after=120,
        max_retries=20,
    )

    logger.info("Published")
