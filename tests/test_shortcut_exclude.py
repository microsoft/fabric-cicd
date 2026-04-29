# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test shortcut exclusion functionality."""

import json
from unittest.mock import MagicMock

import pytest

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._lakehouse import ShortcutPublisher
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


def set_deployed_shortcuts(mock_fabric_workspace, deployed_shortcuts):
    """Configure endpoint behavior to return specific deployed shortcuts."""

    def mock_invoke(method, url, **_kwargs):
        if method == "GET" and "shortcuts" in url:
            return {"body": {"value": deployed_shortcuts}, "header": {}}
        if method == "POST" and "shortcuts" in url:
            return {"body": {"id": "mock-shortcut-id"}}
        if method == "DELETE" and "shortcuts" in url:
            return {"body": {}}
        return {"body": {}}

    mock_fabric_workspace.endpoint.invoke.side_effect = mock_invoke


def get_shortcut_post_calls(mock_fabric_workspace):
    """Return all shortcut create/overwrite API calls."""
    return [
        call
        for call in mock_fabric_workspace.endpoint.invoke.call_args_list
        if call[1].get("method") == "POST" and "shortcuts" in call[1].get("url", "")
    ]


@pytest.fixture
def restore_feature_flags():
    """Ensure feature flags are restored after test."""
    original_flags = constants.FEATURE_FLAG.copy()
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


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


def test_shortcut_smart_diff_skips_unchanged_shortcuts(mock_fabric_workspace, mock_item):
    """Smart diff should skip publish for unchanged shortcuts."""
    shortcuts_data = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        },
        {
            "name": "shortcut2",
            "path": "/Files",
            "target": {"type": "OneLake", "oneLake": {"path": "Files/s2", "itemId": "item-2"}},
        },
    ]
    deployed_shortcuts = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
            "serverManaged": {"createdBy": "system"},
        },
        {
            "name": "shortcut2",
            "path": "/Files",
            "target": {"type": "OneLake", "oneLake": {"path": "Files/s2", "itemId": "item-2"}},
            "serverManaged": {"createdBy": "system"},
        },
    ]

    mock_item.item_files = [create_shortcut_file(shortcuts_data)]
    set_deployed_shortcuts(mock_fabric_workspace, deployed_shortcuts)
    constants.FEATURE_FLAG.add("enable_shortcut_smart_diff")
    try:
        ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()
        post_calls = get_shortcut_post_calls(mock_fabric_workspace)
        assert len(post_calls) == 0
    finally:
        constants.FEATURE_FLAG.discard("enable_shortcut_smart_diff")


def test_shortcut_smart_diff_publishes_only_changed_shortcut(mock_fabric_workspace, mock_item):
    """Smart diff should publish only shortcuts with changed fields."""
    shortcuts_data = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        },
        {
            "name": "shortcut2",
            "path": "/Files",
            "target": {"type": "OneLake", "oneLake": {"path": "Files/s2-new", "itemId": "item-2"}},
        },
    ]
    deployed_shortcuts = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        },
        {
            "name": "shortcut2",
            "path": "/Files",
            "target": {"type": "OneLake", "oneLake": {"path": "Files/s2-old", "itemId": "item-2"}},
        },
    ]

    mock_item.item_files = [create_shortcut_file(shortcuts_data)]
    set_deployed_shortcuts(mock_fabric_workspace, deployed_shortcuts)
    constants.FEATURE_FLAG.add("enable_shortcut_smart_diff")
    try:
        ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

        post_calls = get_shortcut_post_calls(mock_fabric_workspace)
        assert len(post_calls) == 1
        assert post_calls[0][1]["body"]["name"] == "shortcut2"
    finally:
        constants.FEATURE_FLAG.discard("enable_shortcut_smart_diff")


def test_shortcut_smart_diff_publishes_client_side_new_field(mock_fabric_workspace, mock_item):
    """Smart diff should publish when repo contains a new field missing from deployed shortcut."""
    shortcuts_data = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
            "clientField": {"owner": "team-a"},
        }
    ]
    deployed_shortcuts = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        }
    ]

    mock_item.item_files = [create_shortcut_file(shortcuts_data)]
    set_deployed_shortcuts(mock_fabric_workspace, deployed_shortcuts)
    constants.FEATURE_FLAG.add("enable_shortcut_smart_diff")
    try:
        ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

        post_calls = get_shortcut_post_calls(mock_fabric_workspace)
        assert len(post_calls) == 1
        assert post_calls[0][1]["body"]["name"] == "shortcut1"
    finally:
        constants.FEATURE_FLAG.discard("enable_shortcut_smart_diff")


def test_shortcut_smart_diff_disabled_keeps_publish_all_behavior(mock_fabric_workspace, mock_item):
    """When smart diff is disabled, unchanged shortcuts are still published."""
    shortcuts_data = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        }
    ]
    deployed_shortcuts = [
        {
            "name": "shortcut1",
            "path": "/Tables",
            "target": {"type": "OneLake", "oneLake": {"path": "Tables/s1", "itemId": "item-1"}},
        }
    ]

    mock_item.item_files = [create_shortcut_file(shortcuts_data)]
    set_deployed_shortcuts(mock_fabric_workspace, deployed_shortcuts)

    ShortcutPublisher(mock_fabric_workspace, mock_item).publish_all()

    post_calls = get_shortcut_post_calls(mock_fabric_workspace)
    assert len(post_calls) == 1
