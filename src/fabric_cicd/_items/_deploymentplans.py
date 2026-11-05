# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Deployment Plan item."""

import logging
from collections.abc import Sequence
from typing import Optional

import yaml

from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


class DeploymentPlanPublisher(ItemPublisher):
    """Publisher for Deployment Plan items."""

    item_type = ItemType.DEPLOYMENT_PLAN.value


def get_plan_variable_libraries(
    fabric_workspace_obj: FabricWorkspace, repository_plan: Item
) -> dict[str, Optional[str]]:
    """Return repository Variable Libraries referenced by an already-resolved plan.

    Maps library names to explicit ActiveValueSetName selections, or None when
    no selection is provided. Does not apply publish filters or activate sets.
    """
    plan_file = next((file for file in repository_plan.item_files if file.name == "plan.yml"), None)
    if plan_file is None:
        msg = f"Deployment Plan '{repository_plan.name}' is missing plan.yml."
        raise InputError(msg, logger)
    try:
        definition = yaml.safe_load(plan_file.contents)
    except yaml.YAMLError as exc:
        msg = f"Deployment Plan '{repository_plan.name}' contains invalid YAML."
        raise InputError(msg, logger) from exc

    libraries_by_id = {
        item.logical_id: name
        for name, item in fabric_workspace_obj.repository_items.get(ItemType.VARIABLE_LIBRARY.value, {}).items()
    }
    selections = {}
    for group in definition.get("groups", []):
        library_name = libraries_by_id.get(group.get("logicalId"))
        if library_name is not None:
            selections[library_name] = next(
                (option["ActiveValueSetName"] for option in group.get("options", []) if "ActiveValueSetName" in option),
                None,
            )
    return selections


def set_deployment_plan(
    fabric_workspace_obj: FabricWorkspace,
    items_with_context: Sequence[tuple[str, Item, object]],
) -> tuple[dict[str, object], dict[str, Optional[str]]]:
    """
    Sets the deployment plan to be used by the bulk publish operation.

    Args:
        fabric_workspace_obj: The workspace containing the selected plan, repository items, and deployed items.
        items_with_context: A sequence of tuples containing item names, Item objects, and additional context.

    Returns:
        Bulk API options and Variable Library selections for the selected plan,
        or two empty dictionaries if no plan is selected.

    """
    deployment_plan = fabric_workspace_obj.deployment_plan
    if deployment_plan is None:
        return {}, {}

    # Ensure the deployment plan exists in the repository before proceeding
    repository_plan = fabric_workspace_obj.repository_items.get(ItemType.DEPLOYMENT_PLAN.value, {}).get(deployment_plan)
    if repository_plan is None:
        msg = f"Deployment Plan '{deployment_plan}' must exist in the repository to be executed in the deployment."
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
                "or already exist in the target workspace to be executed in the deployment."
            )
            raise InputError(msg, logger)

    options = {
        "deploymentPlan": {"logicalId": repository_plan.logical_id, "referenceType": "ByLogicalId"},
    }
    return options, get_plan_variable_libraries(fabric_workspace_obj, repository_plan)
