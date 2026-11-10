# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for Variable Library value set activation."""

import logging
from unittest.mock import MagicMock

import pytest

from fabric_cicd import FabricWorkspace, ItemType
from fabric_cicd._common._exceptions import InvokeError
from fabric_cicd._common._item import Item
from fabric_cicd._items._variablelibrary import VariableLibraryPublisher, activate_value_set


@pytest.fixture
def activation_context():
    library = Item(ItemType.VARIABLE_LIBRARY.value, "Vars", "", "library-id")
    settings = MagicMock()
    settings.name = "settings.json"
    settings.contents = '{"valueSetsOrder": ["PPE", "PROD"]}'
    library.item_files = [settings]
    workspace = MagicMock(spec=FabricWorkspace)
    workspace.endpoint = MagicMock()
    workspace.base_api_url = "https://example.test/workspaces/workspace"
    workspace.environment = "PPE"
    return workspace, library


@pytest.mark.parametrize(
    ("environment", "expected_set"),
    [("PROD", "PROD"), ("PPE", "PPE"), ("Unknown", "Default value set")],
)
def test_standard_activation_uses_workspace_environment(activation_context, environment, expected_set):
    workspace, library = activation_context
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


def test_standard_publish_without_settings_still_publishes_item(activation_context):
    workspace, library = activation_context
    library.item_files = []
    workspace.bulk_publish_enabled = False

    VariableLibraryPublisher(workspace).publish_one(library.name, library)

    workspace._publish_item.assert_called_once_with(item_name=library.name, item_type=library.type)
    workspace.endpoint.invoke.assert_not_called()


@pytest.mark.parametrize(
    ("selections", "expected_set"),
    [(None, "PPE"), ({}, "PPE"), ({"Other": "PROD"}, "PPE"), ({"Vars": None}, "PPE"), ({"Vars": "PROD"}, "PROD")],
)
def test_bulk_activation_only_overrides_with_explicit_selection(activation_context, selections, expected_set):
    workspace, library = activation_context
    workspace.repository_items = {ItemType.VARIABLE_LIBRARY.value: {library.name: library}}
    workspace.bulk_publish_enabled = True

    VariableLibraryPublisher(workspace).post_publish_all(selections)

    workspace.endpoint.invoke.assert_called_once_with(
        method="PATCH",
        url=f"{workspace.base_api_url}/VariableLibraries/{library.guid}",
        body={"properties": {"activeValueSetName": expected_set}},
    )


@pytest.mark.parametrize("selection", ["PROD", "Default value set", "DEV"])
def test_explicit_value_set_is_submitted_to_fabric(activation_context, selection):
    workspace, library = activation_context

    activate_value_set(workspace, library, value_set_override=selection)

    workspace.endpoint.invoke.assert_called_once_with(
        method="PATCH",
        url=f"{workspace.base_api_url}/VariableLibraries/{library.guid}",
        body={"properties": {"activeValueSetName": selection}},
    )


def test_value_set_activation_propagates_fabric_error(activation_context, caplog):
    workspace, library = activation_context
    error = InvokeError("Fabric rejected the value set", logging.getLogger(__name__))
    workspace.endpoint.invoke.side_effect = error

    with pytest.raises(InvokeError) as exc_info:
        activate_value_set(workspace, library, "DEV")

    assert exc_info.value is error
    workspace.endpoint.invoke.assert_called_once_with(
        method="PATCH",
        url=f"{workspace.base_api_url}/VariableLibraries/{library.guid}",
        body={"properties": {"activeValueSetName": "DEV"}},
    )
    assert "Active value set changed" not in caplog.text


@pytest.mark.parametrize("selection", [None, "PROD", "Default value set"])
def test_missing_settings_warns_and_skips_activation(activation_context, caplog, selection):
    workspace, library = activation_context
    library.item_files = []

    activate_value_set(workspace, library, selection)

    workspace.endpoint.invoke.assert_not_called()
    assert "settings.json file not found for item Vars. Active value set not changed." in caplog.text
