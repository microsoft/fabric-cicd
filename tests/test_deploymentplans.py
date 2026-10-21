# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk deployment plan options."""

from unittest.mock import MagicMock

import pytest

from fabric_cicd import FabricWorkspace, ItemType
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._common._validate_input import validate_deployment_plan
from fabric_cicd._items._deploymentplans import DeploymentPlanPublisher, set_deployment_plan


@pytest.mark.parametrize("name", ["Plan", " Plan "])
def test_validate_deployment_plan_returns_valid_name(name):
    assert validate_deployment_plan(name) == name


@pytest.mark.parametrize("name", ["", " ", "\t\n"])
def test_validate_deployment_plan_rejects_blank_name(name):
    with pytest.raises(InputError, match="'deployment_plan' must be a non-empty string"):
        validate_deployment_plan(name)


@pytest.mark.parametrize("name", [None, 123, False, [], {}])
def test_validate_deployment_plan_rejects_non_string_name(name):
    with pytest.raises(InputError, match="The provided deployment_plan is not of type string"):
        validate_deployment_plan(name)


def make_workspace():
    workspace = MagicMock(spec=FabricWorkspace)
    workspace._prepare_bulk_item_parts.return_value = []
    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke.return_value = {"body": {}}
    workspace.base_api_url = "https://example.test/workspaces/workspace"
    workspace.deployment_plan = None
    return workspace


def make_plan(name="Plan", logical_id="00000000-0000-0000-0000-000000000000"):
    return Item(ItemType.DEPLOYMENT_PLAN.value, name, "", "", logical_id=logical_id)


def test_set_deployment_plan_returns_bulk_options_for_undeployed_plan():
    plan = make_plan()

    assert set_deployment_plan("Plan", [(plan.name, plan, object())]) == {
        "deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"},
    }


def test_set_deployment_plan_returns_no_options_without_selection():
    assert set_deployment_plan(None, []) == {}


def test_set_deployment_plan_rejects_same_name_of_another_type():
    notebook = Item(ItemType.NOTEBOOK.value, "Plan", "", "", logical_id="notebook-id")

    with pytest.raises(InputError, match="must be included"):
        set_deployment_plan("Plan", [(notebook.name, notebook, object())])


def test_bulk_publish_uses_top_level_plan_logical_id():
    workspace = make_workspace()
    plan = make_plan()
    workspace.deployment_plan = plan.name
    publisher = DeploymentPlanPublisher(workspace)

    batch = [(plan.name, plan, publisher)]
    FabricWorkspace._publish_items(
        workspace, batch, deployment_plan_options=set_deployment_plan(workspace.deployment_plan, batch)
    )

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"] == {
        "allowPairingByName": True,
        "deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"},
    }


def test_bulk_publish_without_plan_preserves_pairing_by_name():
    workspace = make_workspace()
    FabricWorkspace._publish_items(workspace, [])

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"] == {"allowPairingByName": True}


def test_bulk_publish_preserves_per_item_options_with_plan():
    workspace = make_workspace()
    plan = make_plan()
    workspace.deployment_plan = plan.name
    model = Item(ItemType.SEMANTIC_MODEL.value, "Model", "", "", logical_id="model-id")
    model_publisher = MagicMock()
    model_publisher.get_definition_options.return_value = {"allowPurgeData": True}

    batch = [(plan.name, plan, DeploymentPlanPublisher(workspace)), (model.name, model, model_publisher)]
    plan_options = set_deployment_plan(workspace.deployment_plan, batch)
    FabricWorkspace._publish_items(workspace, batch, deployment_plan_options=plan_options)

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"] == {
        "allowPairingByName": True,
        "deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"},
        "itemOptionsByLogicalId": [{"logicalId": "model-id", "options": {"allowPurgeData": True}}],
    }
    assert plan_options == {"deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"}}


@pytest.mark.parametrize(
    ("plans", "message"),
    [
        ([], "must be included"),
        ([make_plan(logical_id="")], "Deployment Plan 'Plan'"),
    ],
)
def test_set_deployment_plan_rejects_invalid_plans_before_request(plans, message):
    workspace = make_workspace()
    workspace.deployment_plan = "Plan"
    batch = [(plan.name, plan, DeploymentPlanPublisher(workspace)) for plan in plans]

    with pytest.raises(InputError, match=message):
        set_deployment_plan(workspace.deployment_plan, batch)

    workspace.endpoint.invoke.assert_not_called()


def test_bulk_publish_does_not_apply_plan_without_explicit_selection():
    workspace = make_workspace()
    plan = make_plan()

    FabricWorkspace._publish_items(workspace, [(plan.name, plan, DeploymentPlanPublisher(workspace))])

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"] == {"allowPairingByName": True}


def test_bulk_publish_selects_named_plan_among_multiple_plans():
    workspace = make_workspace()
    first, selected = make_plan(), make_plan("Selected", "selected-id")
    workspace.deployment_plan = selected.name
    publisher = DeploymentPlanPublisher(workspace)

    batch = [(p.name, p, publisher) for p in (first, selected)]
    FabricWorkspace._publish_items(
        workspace, batch, deployment_plan_options=set_deployment_plan(workspace.deployment_plan, batch)
    )

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"]["deploymentPlan"] == {
        "logicalId": "selected-id",
        "referenceType": "ByLogicalId",
    }
