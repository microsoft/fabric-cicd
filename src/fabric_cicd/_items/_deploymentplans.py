# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Deployment Plan item."""

import logging
from collections.abc import Sequence

from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


class DeploymentPlanPublisher(ItemPublisher):
    """Publisher for Deployment Plan items."""

    item_type = ItemType.DEPLOYMENT_PLAN.value


def set_deployment_plan(
    fabric_workspace_obj: FabricWorkspace,
    items_with_context: Sequence[tuple[str, Item, object]],
) -> dict[str, object]:
    """
    Sets the deployment plan to be used by the bulk publish operation.

    Args:
        fabric_workspace_obj: The workspace containing the selected plan, repository items, and deployed items.
        items_with_context: A sequence of tuples containing item names, Item objects, and additional context.

    Returns:
        A dictionary containing the repository plan's logical ID, or an empty dictionary if no plan is selected.

    """
    deployment_plan = fabric_workspace_obj.deployment_plan
    if deployment_plan is None:
        return {}

    # Ensure the deployment plan exists in the repository before proceeding
    repository_plan = fabric_workspace_obj.repository_items.get(ItemType.DEPLOYMENT_PLAN.value, {}).get(deployment_plan)
    if repository_plan is None:
        msg = f"Deployment Plan '{deployment_plan}' must exist in the repository to be executed in the deployment"
        raise InputError(msg, logger)
    # Check if the deployment plan is included in the current publish batch
    in_publish_batch = any(
        item.type == ItemType.DEPLOYMENT_PLAN.value and name == deployment_plan for name, item, _ in items_with_context
    )
    # If the deployment plan is not in the publish batch, ensure it already exists in the target workspace
    if not in_publish_batch:
        deployed_plan = fabric_workspace_obj.deployed_items.get(ItemType.DEPLOYMENT_PLAN.value, {}).get(deployment_plan)
        if deployed_plan is None:
            msg = (
                f"Deployment Plan '{deployment_plan}' must be included in the bulk publish batch "
                "or already exist in the target workspace to be executed in the deployment"
            )
            raise InputError(msg, logger)

    return {
        "deploymentPlan": {"logicalId": repository_plan.logical_id, "referenceType": "ByLogicalId"},
    }
