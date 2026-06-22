# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test shortcut exclusion functionality."""

import json
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._lakehouse import LakehousePublisher, ShortcutPublisher
from fabric_cicd.constants import FeatureFlag
from fabric_cicd.fabric_workspace import FabricWorkspace


@pytest.fixture
def mock_fabric_workspace():
    """Create a mock FabricWorkspace object."""
    workspace = MagicMock(spec=FabricWorkspace)
    workspace.base_api_url = "https://api.fabric.microsoft.com/v1"
    workspace.shortcut_exclude_regex = None
    workspace.endpoint = MagicMock()

    # Mock the endpoint invoke method to return empty shortcuts list
    def mock_invoke(method, url, **_kwargs):
        if method == "GET" and "shortcuts" in url:
            return {"body": {"value": []}, "header": {}}
        if method == "POST" and "shortcuts" in url:
            return {"body": {"id": "mock-shortcut-id"}}
        return {"body": {}}

    workspace.endpoint.invoke.side_effect = mock_invoke

    # Mock parameter replacement methods to return content as-is
    workspace._replace_parameters = lambda file_obj, _item_obj: file_obj.contents
    workspace._replace_logical_ids = lambda contents: contents
    workspace._replace_workspace_ids = lambda contents: contents

    return workspace


@pytest.fixture
def mock_item():
    """Create a mock Item object."""
    item = MagicMock(spec=Item)
    item.name = "TestLakehouse"
    item.guid = "test-lakehouse-guid"
    return item


def create_shortcut_file(shortcuts_data):
    """Helper to create a mock file object with shortcut data."""
    file_obj = MagicMock()
    file_obj.name = "shortcuts.metadata.json"
    file_obj.contents = json.dumps(shortcuts_data)
    return file_obj


def test_process_shortcuts_with_exclude_regex_filters_shortcuts(mock_fabric_workspace, mock_item):
    """Test that shortcut_exclude_regex correctly filters shortcuts from deployment."""

    # Create shortcuts data
    shortcuts_data = [
        {
            "name": "temp_shortcut1",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/temp1",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "production_shortcut",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/prod",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "temp_shortcut2",
            "path": "/Files",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Files/temp2",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
    ]

    # Create mock file with shortcuts
    shortcut_file = create_shortcut_file(shortcuts_data)
    mock_item.item_files = [shortcut_file]

    # Set exclude regex to filter out shortcuts starting with "temp_"
    mock_fabric_workspace.shortcut_exclude_regex = "^temp_.*"

    # Call process_shortcuts
    ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

    # Verify that only the production_shortcut was published
    post_calls = [
        call
        for call in mock_fabric_workspace.endpoint.invoke.call_args_list
        if call[1].get("method") == "POST" and "shortcuts" in call[1].get("url", "")
    ]

    # Should have only 1 shortcut published (production_shortcut)
    assert len(post_calls) == 1

    # Verify the published shortcut is the production one
    published_shortcut = post_calls[0][1]["body"]
    assert published_shortcut["name"] == "production_shortcut"


def test_process_shortcuts_without_exclude_regex_publishes_all(mock_fabric_workspace, mock_item):
    """Test that when shortcut_exclude_regex is None, all shortcuts are published."""

    # Create shortcuts data
    shortcuts_data = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/s1",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "shortcut2",
            "path": "/Files",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Files/s2",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
    ]

    # Create mock file with shortcuts
    shortcut_file = create_shortcut_file(shortcuts_data)
    mock_item.item_files = [shortcut_file]

    # No exclude regex set (None)
    mock_fabric_workspace.shortcut_exclude_regex = None

    # Call process_shortcuts
    ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

    # Verify that both shortcuts were published
    post_calls = [
        call
        for call in mock_fabric_workspace.endpoint.invoke.call_args_list
        if call[1].get("method") == "POST" and "shortcuts" in call[1].get("url", "")
    ]

    # Should have 2 shortcuts published
    assert len(post_calls) == 2


def test_process_shortcuts_exclude_regex_excludes_all_matching(mock_fabric_workspace, mock_item):
    """Test that shortcut_exclude_regex excludes all matching shortcuts."""

    # Create shortcuts data with all matching the pattern
    shortcuts_data = [
        {
            "name": "temp_shortcut1",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/temp1",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "temp_shortcut2",
            "path": "/Files",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Files/temp2",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
    ]

    # Create mock file with shortcuts
    shortcut_file = create_shortcut_file(shortcuts_data)
    mock_item.item_files = [shortcut_file]

    # Set exclude regex that matches all shortcuts
    mock_fabric_workspace.shortcut_exclude_regex = "^temp_.*"

    # Call process_shortcuts
    ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

    # Verify that no shortcuts were published
    post_calls = [
        call
        for call in mock_fabric_workspace.endpoint.invoke.call_args_list
        if call[1].get("method") == "POST" and "shortcuts" in call[1].get("url", "")
    ]

    # Should have 0 shortcuts published
    assert len(post_calls) == 0


def test_process_shortcuts_with_complex_regex_pattern(mock_fabric_workspace, mock_item):
    """Test shortcut exclusion with a more complex regex pattern."""

    # Create shortcuts data
    shortcuts_data = [
        {
            "name": "dev_temp_shortcut",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/dev_temp",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "prod_shortcut",
            "path": "/Tables",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Tables/prod",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
        {
            "name": "staging_temp_data",
            "path": "/Files",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "path": "Files/staging_temp",
                    "itemId": "test-item-id",
                    "workspaceId": "test-workspace-id",
                    "artifactType": "Lakehouse",
                },
            },
        },
    ]

    # Create mock file with shortcuts
    shortcut_file = create_shortcut_file(shortcuts_data)
    mock_item.item_files = [shortcut_file]

    # Set exclude regex to filter shortcuts containing "_temp"
    mock_fabric_workspace.shortcut_exclude_regex = ".*_temp.*"

    # Call process_shortcuts
    ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

    # Verify that only prod_shortcut was published
    post_calls = [
        call
        for call in mock_fabric_workspace.endpoint.invoke.call_args_list
        if call[1].get("method") == "POST" and "shortcuts" in call[1].get("url", "")
    ]

    # Should have only 1 shortcut published (prod_shortcut)
    assert len(post_calls) == 1

    # Verify the published shortcut is the prod one
    published_shortcut = post_calls[0][1]["body"]
    assert published_shortcut["name"] == "prod_shortcut"


# =============================================================================
# Regression tests: items_to_include + shortcut publishing
# =============================================================================


@pytest.fixture
def shortcut_publish_enabled():
    """Enable the ENABLE_SHORTCUT_PUBLISH feature flag for the duration of a test."""
    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_SHORTCUT_PUBLISH.value)
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


def _make_item(name: str, guid: str = "") -> Item:
    """Create a real Item for testing skip_publish and guid behaviour."""
    return Item(type="Lakehouse", name=name, description="", guid=guid)


def test_excluded_lakehouses_marked_skip_publish_with_items_to_include():
    """
    When items_to_include is set and the workspace contains lakehouses that are
    NOT in the include list, publish_all() must mark those lakehouses
    skip_publish=True so that post_publish_all() does not attempt to publish
    their shortcuts (which would fail with a 400 error because guid is "").
    """
    lh_bronze = _make_item("lh_bronze", guid="bronze-guid")
    lh_silver = _make_item("lh_silver", guid="")  # not deployed to this environment

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.repository_items = {"Lakehouse": {"lh_bronze": lh_bronze, "lh_silver": lh_silver}}
    workspace.items_to_include = ["lh_bronze.Lakehouse"]

    publisher = LakehousePublisher.__new__(LakehousePublisher)
    publisher.fabric_workspace_obj = workspace

    # items_to_include filtering inside get_items_to_publish()
    items = publisher.get_items_to_publish()
    assert "lh_bronze" in items
    assert "lh_silver" not in items

    # Simulate what publish_all() now does: mark excluded items skip_publish=True
    all_items = workspace.repository_items.get("Lakehouse", {})
    for item_name, item_obj in all_items.items():
        if item_name not in items:
            item_obj.skip_publish = True

    # The excluded lakehouse must be marked as skip_publish
    assert lh_silver.skip_publish is True
    # The included lakehouse must NOT be marked as skip_publish
    assert lh_bronze.skip_publish is False


@pytest.mark.usefixtures("shortcut_publish_enabled")
def test_lakehouses_without_guid_are_not_shortcut_published():
    """
    Regression test for the guid guard in post_publish_all().

    Even if skip_publish is somehow False for a lakehouse with no guid, the
    guid guard in post_publish_all() must prevent shortcut publishing for it,
    avoiding the 'items//shortcuts' URL that returns a 400 error.
    """
    lh_bronze = _make_item("lh_bronze", guid="bronze-guid")
    lh_silver = _make_item("lh_silver", guid="")  # empty guid, skip_publish stays False

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.repository_items = {"Lakehouse": {"lh_bronze": lh_bronze, "lh_silver": lh_silver}}
    workspace.items_to_include = ["lh_bronze.Lakehouse"]
    workspace.shortcut_exclude_regex = None

    publisher = LakehousePublisher.__new__(LakehousePublisher)
    publisher.fabric_workspace_obj = workspace

    with patch("fabric_cicd._items._lakehouse.ShortcutPublisher") as mock_shortcut_cls:
        publisher.post_publish_all()

        # ShortcutPublisher should only be instantiated for lh_bronze (has guid)
        calls = mock_shortcut_cls.call_args_list
        assert len(calls) == 1
        assert calls[0][0][1] is lh_bronze


@pytest.mark.usefixtures("shortcut_publish_enabled")
def test_publish_all_marks_excluded_items_skip_publish():
    """
    End-to-end regression: publish_all() must mark items excluded by items_to_include
    as skip_publish=True before post_publish_all() runs, so that shortcut publishing
    is never attempted for lakehouses with empty guids.
    """
    lh_bronze = _make_item("lh_bronze", guid="bronze-guid")
    lh_silver = _make_item("lh_silver", guid="")  # not deployed in this environment

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.repository_items = {"Lakehouse": {"lh_bronze": lh_bronze, "lh_silver": lh_silver}}
    workspace.items_to_include = ["lh_bronze.Lakehouse"]
    workspace.shortcut_exclude_regex = None

    publisher = LakehousePublisher.__new__(LakehousePublisher)
    publisher.fabric_workspace_obj = workspace
    publisher.item_type = "Lakehouse"

    # Track which items get shortcut-published
    shortcut_published_guids = []

    def fake_shortcut_publish_all(self_inner):
        shortcut_published_guids.append(self_inner.item_obj.guid)

    with (
        patch.object(LakehousePublisher, "pre_publish_all", return_value=None),
        patch.object(LakehousePublisher, "publish_one", return_value=None),
        patch("fabric_cicd._items._lakehouse.ShortcutPublisher.publish_all", fake_shortcut_publish_all),
    ):
        publisher.publish_all()

    # lh_silver must be marked as skip_publish after publish_all()
    assert lh_silver.skip_publish is True
    # lh_bronze was in items_to_include so must NOT be skip_publish
    assert lh_bronze.skip_publish is False

    # Shortcuts must only be published for lh_bronze (has guid); never for lh_silver
    assert shortcut_published_guids == ["bronze-guid"]
