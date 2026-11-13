# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Protect unrelated workspace folders when orphan deletion is selective."""

from types import SimpleNamespace
from unittest.mock import patch

from fabric_cicd import constants
from fabric_cicd.fabric_workspace import FabricWorkspace
from fabric_cicd.publish import unpublish_all_orphan_items


class RecordingEndpoint:
    def __init__(self, fail_item_delete=False):
        self.calls = []
        self.fail_item_delete = fail_item_delete

    def invoke(self, *, method, url, **_kwargs):
        assert method == "DELETE"
        self.calls.append(url)
        if self.fail_item_delete and "/items/" in url:
            msg = "synthetic delete failure"
            raise RuntimeError(msg)
        return {"status_code": 204, "body": {}}


def make_workspace(folders, deployed, repository=None, fail_item_delete=False):
    ws = FabricWorkspace.__new__(FabricWorkspace)
    ws._api_root_url = "https://api.fabric.microsoft.com"
    ws.workspace_id = "12345678-1234-5678-abcd-1234567890ab"
    ws.endpoint = RecordingEndpoint(fail_item_delete=fail_item_delete)
    ws.deployed_folders = dict(folders)
    ws.deployed_items = {"Notebook": dict(deployed)}
    ws.repository_items = {"Notebook": dict(repository or {})}
    ws.item_type_in_scope = ["Notebook"]
    ws.unpublish_responses = None

    def refresh_items():
        # Model a successful Fabric delete while preserving a failed deletion.
        if not ws.endpoint.fail_item_delete:
            for name, item in list(ws.deployed_items["Notebook"].items()):
                if ws.base_api_url + "/items/" + item.guid in ws.endpoint.calls:
                    ws.deployed_items["Notebook"].pop(name)

    ws._refresh_deployed_items = refresh_items
    ws._refresh_repository_items = lambda: None
    ws._refresh_deployed_folders = lambda: None
    return ws


def run_unpublish(ws, exclude="^$", include=None):
    flags = set()
    if include is not None:
        flags = {"enable_experimental_features", "enable_items_to_include"}
    with patch.object(constants, "FEATURE_FLAG", flags):
        unpublish_all_orphan_items(ws, item_name_exclude_regex=exclude, items_to_include=include)
    return [url.removeprefix(ws.base_api_url) for url in ws.endpoint.calls]


def test_excluding_all_items_preserves_independently_managed_empty_folder():
    ws = make_workspace(
        {"/Active": "active", "/IndependentlyManaged": "independent"},
        {"Keep": SimpleNamespace(folder_id="active", guid="keep-id")},
    )
    assert run_unpublish(ws, exclude=r".*") == []


def test_full_unfiltered_unpublish_preserves_existing_empty_folder_behavior():
    ws = make_workspace(
        {"/Active": "active", "/IndependentlyManaged": "independent"},
        {"Keep": SimpleNamespace(folder_id="active", guid="keep-id")},
        repository={"Keep": object()},
    )
    with patch.object(constants, "FEATURE_FLAG", set()):
        unpublish_all_orphan_items(ws)
    assert [url.removeprefix(ws.base_api_url) for url in ws.endpoint.calls] == ["/folders/independent"]


def test_filtered_item_deletion_prunes_only_selected_folder_ancestry():
    ws = make_workspace(
        {
            "/Managed": "managed",
            "/Managed/Child": "child",
            "/IndependentEmpty": "independent",
            "/Active": "active",
        },
        {
            "DeleteMe": SimpleNamespace(folder_id="child", guid="delete-id"),
            "Protected": SimpleNamespace(folder_id="active", guid="protected-id"),
        },
    )
    assert run_unpublish(ws, exclude=r"^Protected$") == [
        "/items/delete-id",
        "/folders/child",
        "/folders/managed",
    ]


def test_failed_item_delete_never_prunes_its_folder_or_other_folders():
    ws = make_workspace(
        {
            "/Managed": "managed",
            "/Managed/Child": "child",
            "/IndependentEmpty": "independent",
        },
        {"DeleteMe": SimpleNamespace(folder_id="child", guid="delete-id")},
        fail_item_delete=True,
    )
    assert run_unpublish(ws, exclude=r"^NoMatch$") == ["/items/delete-id"]


def test_selectively_deleting_root_item_does_not_clear_other_empty_folders():
    ws = make_workspace(
        {"/IndependentEmpty": "independent"},
        {"RootOrphan": SimpleNamespace(folder_id="", guid="root-id")},
    )
    assert run_unpublish(ws, exclude=r"^NoMatch$") == ["/items/root-id"]


def test_selected_nested_folder_with_remaining_item_remains_protected():
    ws = make_workspace(
        {"/Managed": "managed", "/Managed/Child": "child", "/IndependentEmpty": "independent"},
        {
            "DeleteMe": SimpleNamespace(folder_id="child", guid="delete-id"),
            "Keep": SimpleNamespace(folder_id="child", guid="keep-id"),
        },
    )
    assert run_unpublish(ws, exclude=r"^Keep$") == ["/items/delete-id"]


def test_explicit_item_selection_only_cleans_selected_item_folder():
    ws = make_workspace(
        {"/Selected": "selected", "/IndependentEmpty": "independent"},
        {
            "DeleteMe": SimpleNamespace(folder_id="selected", guid="delete-id"),
            "IgnoreMe": SimpleNamespace(folder_id="", guid="other-id"),
        },
    )
    assert run_unpublish(ws, include=["DeleteMe.Notebook"]) == [
        "/items/delete-id",
        "/folders/selected",
    ]


def test_empty_item_selection_never_cleans_workspace_folders():
    ws = make_workspace(
        {"/IndependentEmpty": "independent"},
        {"IgnoreMe": SimpleNamespace(folder_id="", guid="other-id")},
    )
    assert run_unpublish(ws, include=[]) == []


def test_selected_folder_parent_with_unrelated_empty_child_is_preserved():
    ws = make_workspace(
        {
            "/Managed": "managed",
            "/Managed/Selected": "selected",
            "/Managed/EmptyUnselected": "unselected",
        },
        {"DeleteMe": SimpleNamespace(folder_id="selected", guid="delete-id")},
    )
    assert run_unpublish(ws, exclude=r"^NoMatch$") == [
        "/items/delete-id",
        "/folders/selected",
    ]
