# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk deployment plan options."""

from unittest.mock import MagicMock

import pytest

from fabric_cicd import FabricWorkspace, ItemType
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._common._validate_input import validate_deployment_plan
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._deploymentplans import (
    DeploymentPlanPublisher,
    _get_plan_variable_libraries,
    set_deployment_plan,
)
from fabric_cicd._items._variablelibrary import VariableLibraryPublisher


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


def make_workspace(deployment_plan=None, repository_items=None, deployed_items=None):
    workspace = MagicMock(spec=FabricWorkspace)
    workspace._prepare_bulk_item_parts.return_value = []
    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke.return_value = {"body": {}}
    workspace.base_api_url = "https://example.test/workspaces/workspace"
    workspace.deployment_plan = deployment_plan
    workspace.repository_items = repository_items if repository_items is not None else {}
    workspace.deployed_items = deployed_items if deployed_items is not None else {}
    return workspace


def make_plan(name="Plan", logical_id="00000000-0000-0000-0000-000000000000"):
    plan = Item(ItemType.DEPLOYMENT_PLAN.value, name, "", "", logical_id=logical_id)
    plan_file = MagicMock()
    plan_file.name = "plan.yml"
    plan_file.contents = "groups: []"
    plan.item_files = [plan_file]
    return plan


@pytest.mark.parametrize(
    ("environment", "expected_set"),
    [("PROD", "PROD"), ("PPE", "PPE"), ("Unknown", "Default value set")],
)
def test_standard_variable_library_activation_uses_workspace_environment(environment, expected_set):
    library = Item(ItemType.VARIABLE_LIBRARY.value, "Vars", "", "library-id")
    settings = MagicMock()
    settings.name = "settings.json"
    settings.contents = '{"valueSetsOrder": ["PPE", "PROD"]}'
    library.item_files = [settings]
    workspace = make_workspace("Plan", {ItemType.VARIABLE_LIBRARY.value: {library.name: library}})
    workspace.environment = environment
    workspace.bulk_publish_enabled = False
    publisher = VariableLibraryPublisher(workspace)

    publisher.publish_one(library.name, library)
    publisher.post_publish_all({library.name: "PROD"})

    workspace._publish_item.assert_called_once_with(item_name=library.name, item_type=library.type)
    workspace.endpoint.invoke.assert_called_once_with(
        method="PATCH",
        url=f"{workspace.base_api_url}/VariableLibraries/{library.guid}",
        body={"properties": {"activeValueSetName": expected_set}},
    )


def test_standard_variable_library_missing_settings_does_not_raise():
    library = Item(ItemType.VARIABLE_LIBRARY.value, "Vars", "", "library-id")
    workspace = make_workspace()
    workspace.bulk_publish_enabled = False
    publisher = VariableLibraryPublisher(workspace)

    publisher.publish_one(library.name, library)

    workspace._publish_item.assert_called_once_with(item_name=library.name, item_type=library.type)
    workspace.endpoint.invoke.assert_not_called()


@pytest.mark.parametrize(("selections", "expected_set"), [(None, "PPE"), ({"Vars": "PROD"}, "PROD")])
def test_bulk_variable_library_activation_only_overrides_with_explicit_selection(selections, expected_set):
    library = Item(ItemType.VARIABLE_LIBRARY.value, "Vars", "", "library-id")
    settings = MagicMock()
    settings.name = "settings.json"
    settings.contents = '{"valueSetsOrder": ["PPE", "PROD"]}'
    library.item_files = [settings]
    workspace = make_workspace(repository_items={ItemType.VARIABLE_LIBRARY.value: {library.name: library}})
    workspace.environment = "PPE"
    workspace.bulk_publish_enabled = True

    VariableLibraryPublisher(workspace).post_publish_all(selections)

    workspace.endpoint.invoke.assert_called_once_with(
        method="PATCH",
        url=f"{workspace.base_api_url}/VariableLibraries/{library.guid}",
        body={"properties": {"activeValueSetName": expected_set}},
    )


def test_bulk_plan_reimports_variable_libraries_after_activation(mocker):
    library = Item(ItemType.VARIABLE_LIBRARY.value, "Vars", "", "library-id")
    notebook = Item(ItemType.NOTEBOOK.value, "Notebook", "", "notebook-id")
    plan = make_plan()
    workspace = make_workspace(
        "Plan",
        {
            ItemType.DEPLOYMENT_PLAN.value: {plan.name: plan},
            ItemType.VARIABLE_LIBRARY.value: {library.name: library},
        },
    )
    workspace._apply_publish_filters.return_value = False
    workspace.items_to_include = None
    workspace.contains_param_item_vars = False
    workspace._dynamic_var_cache = {"$workspace.id": "workspace-id", "$items.VariableLibrary.Vars.id": "stale-id"}
    library_publisher = VariableLibraryPublisher(workspace)
    mocker.patch.object(library_publisher, "get_items_to_publish", return_value={library.name: library})
    activation = mocker.patch.object(library_publisher, "post_publish_all")
    notebook_publisher = MagicMock()
    notebook_publisher.item_type = ItemType.NOTEBOOK.value
    notebook_publisher.get_items_to_publish.return_value = {notebook.name: notebook}
    notebook_publisher.has_async_publish_check = False
    mocker.patch.object(
        ItemPublisher,
        "get_item_types_to_publish",
        return_value=[(1, ItemType.VARIABLE_LIBRARY), (2, ItemType.NOTEBOOK)],
    )
    mocker.patch.object(ItemPublisher, "create", side_effect=[library_publisher, notebook_publisher])
    plan_options = {"deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"}}
    selections = {library.name: "PROD"}
    mocker.patch("fabric_cicd._items._deploymentplans.set_deployment_plan", return_value=(plan_options, selections))
    events = MagicMock()
    events.attach_mock(workspace._publish_items, "publish")
    events.attach_mock(activation, "activate")
    events.attach_mock(workspace._refresh_deployed_items, "refresh")

    ItemPublisher.publish_all_bulk(workspace)

    library_context = (library.name, library, library_publisher)
    assert workspace._publish_items.call_count == 2
    assert workspace._publish_items.call_args_list[0].args[0] == [library_context]
    assert "deployment_plan_options" not in workspace._publish_items.call_args_list[0].kwargs
    assert library_context in workspace._publish_items.call_args_list[1].args[0]
    assert workspace._publish_items.call_args_list[1].kwargs["deployment_plan_options"] == plan_options
    activation.assert_called_once_with(selections)
    assert [event[0] for event in events.mock_calls] == ["publish", "activate", "publish"]
    assert workspace._dynamic_var_cache == {"$workspace.id": "workspace-id"}
    notebook_publisher.post_publish_all.assert_called_once_with()


def test_get_plan_variable_libraries_matches_group_ids_and_preserves_selections():
    plan = make_plan()
    plan_file = MagicMock()
    plan_file.name = "plan.yml"
    plan_file.contents = """
groups:
  - name: NotTheLibraryName
    logicalId: explicit-id
    options:
      - OtherOption: true
      - ActiveValueSetName: PROD
  - name: Fallback
    logicalId: fallback-id
  - name: Notebook
    logicalId: notebook-id
    postActions:
      - job:
          logicalId: unreferenced-id
"""
    plan.item_files = [plan_file]
    libraries = {
        name: Item(ItemType.VARIABLE_LIBRARY.value, name, "", "", logical_id=logical_id)
        for name, logical_id in (
            ("Explicit", "explicit-id"),
            ("Fallback", "fallback-id"),
            ("Unreferenced", "unreferenced-id"),
        )
    }
    workspace = make_workspace(
        "Plan",
        {
            ItemType.DEPLOYMENT_PLAN.value: {"Plan": plan, "Other": make_plan("Other")},
            ItemType.VARIABLE_LIBRARY.value: libraries,
            ItemType.NOTEBOOK.value: {
                "Notebook": Item(ItemType.NOTEBOOK.value, "Notebook", "", "", logical_id="notebook-id")
            },
        },
    )

    workspace.deployment_plan = "Other"
    assert _get_plan_variable_libraries(workspace, plan) == {"Explicit": "PROD", "Fallback": None}
    workspace.deployment_plan = plan.name
    options, selections = set_deployment_plan(workspace, [(plan.name, plan, object())])
    assert options == {"deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"}}
    assert selections == {"Explicit": "PROD", "Fallback": None}
    workspace.endpoint.invoke.assert_not_called()


@pytest.mark.parametrize("contents", ["groups: []", "version: 1.0.0"])
def test_get_plan_variable_libraries_without_references(contents):
    plan = make_plan()
    plan_file = MagicMock()
    plan_file.name = "plan.yml"
    plan_file.contents = contents
    plan.item_files = [plan_file]
    workspace = make_workspace("Plan", {ItemType.DEPLOYMENT_PLAN.value: {"Plan": plan}})

    assert _get_plan_variable_libraries(workspace, plan) == {}


def test_get_plan_variable_libraries_requires_plan_file():
    plan = make_plan()
    plan.item_files = []
    workspace = make_workspace()

    with pytest.raises(InputError, match=r"missing plan\.yml"):
        _get_plan_variable_libraries(workspace, plan)


def test_get_plan_variable_libraries_rejects_invalid_yaml():
    plan = make_plan()
    plan_file = MagicMock()
    plan_file.name = "plan.yml"
    plan_file.contents = "groups: ["
    plan.item_files = [plan_file]
    workspace = make_workspace("Plan", {ItemType.DEPLOYMENT_PLAN.value: {"Plan": plan}})

    with pytest.raises(InputError, match="invalid YAML"):
        _get_plan_variable_libraries(workspace, plan)


def test_set_deployment_plan_returns_bulk_options_for_undeployed_plan():
    plan = make_plan()

    repository_items = {ItemType.DEPLOYMENT_PLAN.value: {plan.name: plan}}
    workspace = make_workspace("Plan", repository_items)
    assert set_deployment_plan(workspace, [(plan.name, plan, object())]) == (
        {
            "deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"},
        },
        {},
    )


def test_set_deployment_plan_returns_no_options_without_selection():
    assert set_deployment_plan(make_workspace(), []) == ({}, {})


def test_set_deployment_plan_rejects_same_name_of_another_type():
    notebook = Item(ItemType.NOTEBOOK.value, "Plan", "", "", logical_id="notebook-id")
    workspace = make_workspace("Plan", {ItemType.NOTEBOOK.value: {"Plan": notebook}})

    with pytest.raises(InputError, match="must exist in the repository"):
        set_deployment_plan(workspace, [(notebook.name, notebook, object())])


@pytest.mark.parametrize(
    "deployed_id",
    ["", "deployed-id"],
)
def test_set_deployment_plan_resolves_existing_target_plan(deployed_id):
    repository_items = {ItemType.DEPLOYMENT_PLAN.value: {"Plan": make_plan(logical_id="repository-id")}}
    deployed_items = {ItemType.DEPLOYMENT_PLAN.value: {"Plan": make_plan(logical_id=deployed_id)}}
    workspace = make_workspace("Plan", repository_items, deployed_items)

    assert set_deployment_plan(workspace, []) == (
        {
            "deploymentPlan": {"logicalId": "repository-id", "referenceType": "ByLogicalId"},
        },
        {},
    )


@pytest.mark.parametrize("in_batch", [False, True])
def test_set_deployment_plan_requires_repository_plan_before_other_lookups(in_batch):
    plan = make_plan(logical_id="target-id")
    batch = [(plan.name, plan, object())] if in_batch else []
    deployed_items = MagicMock()
    workspace = make_workspace("Plan", deployed_items=deployed_items)

    with pytest.raises(InputError, match="must exist in the repository"):
        set_deployment_plan(workspace, batch)

    deployed_items.get.assert_not_called()


def test_set_deployment_plan_rejects_repository_only_plan_outside_batch():
    repository_items = {ItemType.DEPLOYMENT_PLAN.value: {"Plan": make_plan()}}
    workspace = make_workspace("Plan", repository_items)

    with pytest.raises(InputError, match="already exist in the target workspace"):
        set_deployment_plan(workspace, [])


def test_set_deployment_plan_rejects_same_name_target_item_of_another_type():
    repository_items = {ItemType.DEPLOYMENT_PLAN.value: {"Plan": make_plan()}}
    deployed_items = {ItemType.NOTEBOOK.value: {"Plan": Item(ItemType.NOTEBOOK.value, "Plan", "", "notebook-id")}}
    workspace = make_workspace("Plan", repository_items, deployed_items)

    with pytest.raises(InputError, match="already exist in the target workspace"):
        set_deployment_plan(workspace, [])


def test_set_deployment_plan_in_batch_does_not_require_target_lookup():
    plan = make_plan(logical_id="batch-id")
    repository_items = {ItemType.DEPLOYMENT_PLAN.value: {"Plan": plan}}
    deployed_items = MagicMock()
    workspace = make_workspace("Plan", repository_items, deployed_items)

    assert set_deployment_plan(workspace, [(plan.name, plan, object())]) == (
        {
            "deploymentPlan": {"logicalId": "batch-id", "referenceType": "ByLogicalId"},
        },
        {},
    )
    deployed_items.get.assert_not_called()


def test_bulk_publish_uses_top_level_plan_logical_id():
    workspace = make_workspace()
    plan = make_plan()
    workspace.deployment_plan = plan.name
    publisher = DeploymentPlanPublisher(workspace)

    batch = [(plan.name, plan, publisher)]
    workspace.repository_items = {ItemType.DEPLOYMENT_PLAN.value: {plan.name: plan}}
    FabricWorkspace._publish_items(
        workspace,
        batch,
        deployment_plan_options=set_deployment_plan(workspace, batch)[0],
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
    workspace.repository_items = {ItemType.DEPLOYMENT_PLAN.value: {plan.name: plan}}
    plan_options, _selections = set_deployment_plan(workspace, batch)
    FabricWorkspace._publish_items(workspace, batch, deployment_plan_options=plan_options)

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"] == {
        "allowPairingByName": True,
        "deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"},
        "itemOptionsByLogicalId": [{"logicalId": "model-id", "options": {"allowPurgeData": True}}],
    }
    assert plan_options == {"deploymentPlan": {"logicalId": plan.logical_id, "referenceType": "ByLogicalId"}}


def test_set_deployment_plan_rejects_missing_plan_before_request():
    workspace = make_workspace()
    workspace.deployment_plan = "Plan"

    with pytest.raises(InputError, match="must exist in the repository"):
        set_deployment_plan(workspace, [])

    workspace.endpoint.invoke.assert_not_called()


def test_bulk_publish_validates_plan_with_no_publishable_items(mocker):
    workspace = make_workspace("Missing")
    mocker.patch.object(ItemPublisher, "get_item_types_to_publish", return_value=[])

    with pytest.raises(InputError, match="must exist in the repository"):
        ItemPublisher.publish_all_bulk(workspace)

    workspace._publish_items.assert_not_called()


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
    workspace.repository_items = {ItemType.DEPLOYMENT_PLAN.value: {p.name: p for p in (first, selected)}}
    FabricWorkspace._publish_items(
        workspace,
        batch,
        deployment_plan_options=set_deployment_plan(workspace, batch)[0],
    )

    assert workspace.endpoint.invoke.call_args.kwargs["body"]["options"]["deploymentPlan"] == {
        "logicalId": "selected-id",
        "referenceType": "ByLogicalId",
    }
