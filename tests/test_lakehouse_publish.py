# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for shell-only and opt-in full definition Lakehouse publishing."""

import base64
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fixtures.credentials import DummyTokenCredential

from fabric_cicd import append_feature_flag, constants, get_supported_feature_flags
from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._item import Item
from fabric_cicd._items._lakehouse import LakehousePublisher, is_data_access_roles_enabled
from fabric_cicd.constants import FeatureFlag, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

VALID_WORKSPACE_ID = "12345678-1234-5678-abcd-1234567890ab"
LAKEHOUSE_NAME = "MyLakehouse"

PLATFORM = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Lakehouse", "displayName": LAKEHOUSE_NAME},
    "config": {"version": "2.0", "logicalId": "11111111-1111-1111-1111-111111111111"},
}

ALM_SETTINGS = {
    "version": "1.0.1",
    "objectTypes": [
        {
            "name": "Shortcuts",
            "state": "Enabled",
            "subObjectTypes": [
                {"name": "Shortcuts.OneLake", "state": "Enabled"},
                {"name": "Shortcuts.AdlsGen2", "state": "Enabled"},
            ],
        },
        {"name": "DataAccessRoles", "state": "Enabled"},
    ],
}

SHORTCUTS = [
    {
        "name": "MyShortcut",
        "path": "/Tables",
        "target": {
            "type": "OneLake",
            "oneLake": {
                "path": "Tables/source",
                "itemId": "00000000-0000-0000-0000-000000000000",
                "workspaceId": "00000000-0000-0000-0000-000000000000",
            },
        },
    }
]

DATA_ACCESS_ROLES = [
    {
        "name": "DefaultReader",
        "kind": "Policy",
        "decisionRules": [
            {
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
                    {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                ],
            }
        ],
        "members": {
            "fabricItemMembers": [
                {
                    "itemAccess": ["ReadAll"],
                    "sourcePath": "00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000000",
                }
            ]
        },
    }
]


@pytest.fixture
def mock_endpoint():
    """Mock FabricEndpoint capturing create, update definition, and metadata update calls."""
    mock = MagicMock()
    mock.calls = []

    def mock_invoke(method, url, body=None, **_kwargs):
        mock.calls.append({"method": method, "url": url, "body": body})
        if method == "POST" and url.endswith("/items"):
            return {"body": {"id": "new-lakehouse-guid"}}
        if method == "GET" and "/lakehouses/" in url:
            return {"body": {"properties": {"sqlEndpointProperties": {"provisioningStatus": "Success"}}}}
        return {"body": {}}

    mock.invoke.side_effect = mock_invoke
    return mock


@pytest.fixture
def workspace(mock_endpoint, tmp_path, monkeypatch):
    """Create a FabricWorkspace with mocked endpoint and no deployed items."""
    monkeypatch.setattr(constants, "FEATURE_FLAG", set())
    with (
        patch("fabric_cicd.fabric_workspace.FabricEndpoint", return_value=mock_endpoint),
        patch.object(FabricWorkspace, "_refresh_deployed_items", new=lambda self: setattr(self, "deployed_items", {})),
        patch.object(
            FabricWorkspace, "_refresh_deployed_folders", new=lambda self: setattr(self, "deployed_folders", {})
        ),
    ):
        ws = FabricWorkspace(
            workspace_id=VALID_WORKSPACE_ID,
            repository_directory=str(tmp_path),
            item_type_in_scope=[ItemType.LAKEHOUSE.value],
            token_credential=DummyTokenCredential(),
        )
        ws.deployed_items = {}
        ws.deployed_folders = {}
        yield ws


@pytest.fixture
def definition_workspace(workspace):
    """Opt in to full definitions through the public feature flag API."""
    append_feature_flag(FeatureFlag.ENABLE_LAKEHOUSE_DEFINITION_PUBLISH.value)
    return workspace


def _add_lakehouse(ws: FabricWorkspace, tmp_path: Path, files: dict, guid: str = "") -> Item:
    """Write the given files into a Lakehouse folder and register it in the workspace."""
    item_path = tmp_path / f"{LAKEHOUSE_NAME}.Lakehouse"
    item_path.mkdir(parents=True, exist_ok=True)
    for name, contents in files.items():
        (item_path / name).write_text(contents if isinstance(contents, str) else json.dumps(contents), encoding="utf-8")

    item = Item(
        type=ItemType.LAKEHOUSE.value,
        name=LAKEHOUSE_NAME,
        description="",
        guid=guid,
        logical_id=PLATFORM["config"]["logicalId"],
        path=item_path,
    )
    item.collect_item_files()
    ws.repository_items = {ItemType.LAKEHOUSE.value: {LAKEHOUSE_NAME: item}}
    return item


def _publish(ws: FabricWorkspace) -> None:
    item = ws.repository_items[ItemType.LAKEHOUSE.value][LAKEHOUSE_NAME]
    LakehousePublisher(ws).publish_one(LAKEHOUSE_NAME, item)


def _definition_call(mock_endpoint) -> dict:
    calls = [
        c
        for c in mock_endpoint.calls
        if c["method"] == "POST" and (c["url"].endswith("/items") or "updateDefinition" in c["url"])
    ]
    assert len(calls) == 1
    return calls[0]


def _parts(body: dict) -> dict:
    return {part["path"]: base64.b64decode(part["payload"]).decode("utf-8") for part in body["definition"]["parts"]}


def test_lakehouse_definition_publish_flag_is_supported():
    """Expose the standard publishing opt-in through the public supported-feature list."""
    assert FeatureFlag.ENABLE_LAKEHOUSE_DEFINITION_PUBLISH.value in get_supported_feature_flags()


@pytest.mark.parametrize("deployed", [False, True], ids=["create", "update"])
@pytest.mark.parametrize("metadata", [None, {}, {"defaultSchema": "dbo"}], ids=["missing", "empty", "schemas"])
def test_default_publishing_is_shell_only(workspace, mock_endpoint, tmp_path, metadata, deployed):
    """Without the flag, definitions are ignored and schema-enabled creation is preserved."""
    files = {
        ".platform": PLATFORM,
        "alm.settings.json": "{not json",
        "data-access-roles.json": DATA_ACCESS_ROLES,
        "shortcuts.metadata.json": SHORTCUTS,
    }
    if metadata is not None:
        files["lakehouse.metadata.json"] = metadata
    item = _add_lakehouse(workspace, tmp_path, files, guid="existing-guid" if deployed else "")

    _publish(workspace)

    writes = [c for c in mock_endpoint.calls if c["method"] in {"POST", "PATCH"}]
    assert len(writes) == 1
    call = writes[0]
    expected_body = {"displayName": LAKEHOUSE_NAME, "description": ""}
    if deployed:
        assert call["method"] == "PATCH"
        assert call["url"].endswith("/items/existing-guid")
    else:
        assert call["method"] == "POST"
        assert call["url"].endswith("/items")
        expected_body.update({"type": ItemType.LAKEHOUSE.value, "folderId": ""})
        if metadata and "defaultSchema" in metadata:
            expected_body["creationPayload"] = {"enableSchemas": True}
        assert item.guid == "new-lakehouse-guid"
    assert call["body"] == expected_body
    assert any(c["method"] == "GET" and "/lakehouses/" in c["url"] for c in mock_endpoint.calls)


def test_create_sends_full_definition_without_creation_payload(definition_workspace, mock_endpoint, tmp_path):
    """New lakehouses are created with a definition; schemas are driven by lakehouse.metadata.json."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {
            ".platform": PLATFORM,
            "lakehouse.metadata.json": {"defaultSchema": "dbo"},
            "alm.settings.json": ALM_SETTINGS,
            "shortcuts.metadata.json": SHORTCUTS,
            "data-access-roles.json": DATA_ACCESS_ROLES,
        },
    )

    _publish(definition_workspace)

    call = _definition_call(mock_endpoint)
    assert call["url"].endswith("/items")
    assert "creationPayload" not in call["body"]
    parts = _parts(call["body"])
    assert set(parts) == {".platform", "lakehouse.metadata.json", "alm.settings.json", "data-access-roles.json"}
    assert json.loads(parts["lakehouse.metadata.json"]) == {"defaultSchema": "dbo"}
    assert json.loads(parts["data-access-roles.json"]) == DATA_ACCESS_ROLES
    assert definition_workspace.repository_items[ItemType.LAKEHOUSE.value][LAKEHOUSE_NAME].guid == "new-lakehouse-guid"


def test_update_uses_update_definition(definition_workspace, mock_endpoint, tmp_path):
    """Deployed lakehouses are updated via updateDefinition instead of a metadata-only PATCH."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {".platform": PLATFORM, "lakehouse.metadata.json": {}, "shortcuts.metadata.json": SHORTCUTS},
        guid="existing-guid",
    )

    _publish(definition_workspace)

    call = _definition_call(mock_endpoint)
    assert call["url"].endswith("/items/existing-guid/updateDefinition?updateMetadata=True")
    assert not any(c["method"] == "PATCH" for c in mock_endpoint.calls)
    assert "shortcuts.metadata.json" not in _parts(call["body"])


def test_alm_settings_shortcuts_disabled_and_other_settings_preserved(definition_workspace, mock_endpoint, tmp_path):
    """Shortcuts (and sub-types) are disabled in alm.settings.json; DataAccessRoles setting is preserved."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {".platform": PLATFORM, "lakehouse.metadata.json": {}, "alm.settings.json": ALM_SETTINGS},
        guid="existing-guid",
    )

    _publish(definition_workspace)

    alm = json.loads(_parts(_definition_call(mock_endpoint)["body"])["alm.settings.json"])
    assert alm["version"] == "1.0.1"
    object_types = {obj["name"]: obj for obj in alm["objectTypes"]}
    assert object_types["Shortcuts"]["state"] == "Disabled"
    assert all(sub["state"] == "Disabled" for sub in object_types["Shortcuts"]["subObjectTypes"])
    assert object_types["DataAccessRoles"]["state"] == "Enabled"


def test_alm_settings_without_shortcuts_entry_gets_one(definition_workspace, mock_endpoint, tmp_path):
    """A Disabled Shortcuts entry is added when alm.settings.json does not define one."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {
            ".platform": PLATFORM,
            "lakehouse.metadata.json": {},
            "alm.settings.json": {
                "version": "1.0.1",
                "objectTypes": [{"name": "DataAccessRoles", "state": "Disabled"}],
            },
        },
    )

    _publish(definition_workspace)

    alm = json.loads(_parts(_definition_call(mock_endpoint)["body"])["alm.settings.json"])
    object_types = {obj["name"]: obj for obj in alm["objectTypes"]}
    assert object_types["Shortcuts"] == {"name": "Shortcuts", "state": "Disabled"}
    assert object_types["DataAccessRoles"]["state"] == "Disabled"


def test_missing_alm_settings_and_metadata_are_synthesized(definition_workspace, mock_endpoint, tmp_path):
    """Missing lakehouse.metadata.json and alm.settings.json are added with safe defaults."""
    _add_lakehouse(definition_workspace, tmp_path, {".platform": PLATFORM, "shortcuts.metadata.json": SHORTCUTS})

    _publish(definition_workspace)

    parts = _parts(_definition_call(mock_endpoint)["body"])
    assert set(parts) == {".platform", "lakehouse.metadata.json", "alm.settings.json"}
    assert json.loads(parts["lakehouse.metadata.json"]) == {}
    assert json.loads(parts["alm.settings.json"]) == {
        "version": "1.0.1",
        "objectTypes": [{"name": "Shortcuts", "state": "Disabled"}],
    }


def test_invalid_alm_settings_raises(definition_workspace, tmp_path):
    """Malformed alm.settings.json raises a parsing error rather than risking shortcut deletion."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {".platform": PLATFORM, "lakehouse.metadata.json": {}, "alm.settings.json": "{not json"},
    )

    with pytest.raises(ParsingError):
        _publish(definition_workspace)


def test_data_access_roles_not_enabled_logs_debug(definition_workspace, tmp_path, caplog):
    """A debug message is logged when data-access-roles.json exists but DataAccessRoles is not enabled."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {".platform": PLATFORM, "lakehouse.metadata.json": {}, "data-access-roles.json": DATA_ACCESS_ROLES},
    )

    with caplog.at_level(logging.DEBUG, logger="fabric_cicd._items._lakehouse"):
        _publish(definition_workspace)

    assert any("DataAccessRoles is not enabled" in record.message for record in caplog.records)


def test_data_access_roles_enabled_no_debug(definition_workspace, tmp_path, caplog):
    """No data access role warning is logged when DataAccessRoles is enabled."""
    _add_lakehouse(
        definition_workspace,
        tmp_path,
        {
            ".platform": PLATFORM,
            "lakehouse.metadata.json": {},
            "alm.settings.json": ALM_SETTINGS,
            "data-access-roles.json": DATA_ACCESS_ROLES,
        },
    )

    with caplog.at_level(logging.DEBUG, logger="fabric_cicd._items._lakehouse"):
        _publish(definition_workspace)

    assert not any("DataAccessRoles is not enabled" in record.message for record in caplog.records)


@pytest.mark.parametrize(
    ("contents", "expected"),
    [
        (None, False),
        ("", False),
        ("{bad", False),
        (json.dumps({"version": "1.0.1", "objectTypes": []}), False),
        (json.dumps({"objectTypes": [{"name": "DataAccessRoles", "state": "Disabled"}]}), False),
        (json.dumps({"objectTypes": [{"name": "DataAccessRoles", "state": "Enabled"}]}), True),
    ],
)
def test_is_data_access_roles_enabled(contents, expected):
    """DataAccessRoles is only considered enabled when explicitly set to Enabled."""
    assert is_data_access_roles_enabled(contents) is expected


@pytest.mark.parametrize("definition_enabled", [False, True], ids=["shell", "definition"])
def test_skipped_lakehouse_does_not_check_sql_endpoint(workspace, mock_endpoint, tmp_path, definition_enabled):
    """Both publish modes check provisioning after publishing but leave filtered items untouched."""
    if definition_enabled:
        append_feature_flag(FeatureFlag.ENABLE_LAKEHOUSE_DEFINITION_PUBLISH.value)
    item = _add_lakehouse(workspace, tmp_path, {".platform": PLATFORM, "lakehouse.metadata.json": {}})

    _publish(workspace)
    assert any(c["method"] == "GET" and "/lakehouses/" in c["url"] for c in mock_endpoint.calls)

    mock_endpoint.calls.clear()
    item.skip_publish = True
    with patch.object(FabricWorkspace, "_apply_publish_filters", return_value=True):
        _publish(workspace)
    assert mock_endpoint.calls == []
