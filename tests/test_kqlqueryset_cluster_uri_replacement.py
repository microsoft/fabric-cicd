# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for KQL Queryset item processing."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._items._kqlqueryset import replace_cluster_uri
from fabric_cicd.constants import ItemType

DATABASE_LOGICAL_ID = "11111111-1111-1111-1111-111111111111"
DATABASE_GUID = "22222222-2222-2222-2222-222222222222"
CLUSTER_URI = "https://example.kusto.fabric.microsoft.com"


def create_queryset_file(tmp_path, cluster_uri="", database_item_id=DATABASE_LOGICAL_ID):
    """Create a KQL Queryset definition file for testing."""
    queryset_path = tmp_path / "RealTimeQueryset.json"
    queryset_path.write_text(
        json.dumps({
            "queryset": {
                "dataSources": [
                    {
                        "clusterUri": cluster_uri,
                        "databaseItemId": database_item_id,
                    }
                ]
            }
        }),
        encoding="utf-8",
    )
    return File(tmp_path, queryset_path)


def create_workspace(database_guid=DATABASE_GUID, query_service_uri=CLUSTER_URI):
    """Create a minimal workspace containing a repository KQL Database."""
    endpoint = MagicMock()
    endpoint.invoke.return_value = {"body": {"properties": {"queryServiceUri": query_service_uri}}}
    database_item = Item(
        type=ItemType.KQL_DATABASE.value,
        name="TestDatabase",
        description="",
        guid=database_guid,
        logical_id=DATABASE_LOGICAL_ID,
    )
    return SimpleNamespace(
        repository_items={ItemType.KQL_DATABASE.value: {database_item.name: database_item}},
        endpoint=endpoint,
        base_api_url="https://api.fabric.microsoft.com/v1/workspaces/workspace-id",
    )


def test_replace_cluster_uri_resolves_database_logical_id(tmp_path):
    """An empty cluster URI is resolved using the database logical ID and deployed GUID."""
    workspace = create_workspace()

    result = json.loads(replace_cluster_uri(workspace, create_queryset_file(tmp_path)))

    assert result["queryset"]["dataSources"][0]["clusterUri"] == CLUSTER_URI
    workspace.endpoint.invoke.assert_called_once_with(
        method="GET",
        url=f"{workspace.base_api_url}/kqlDatabases/{DATABASE_GUID}",
    )


def test_replace_cluster_uri_raises_for_unknown_logical_id(tmp_path):
    """An unknown database logical ID produces a parsing error."""
    workspace = create_workspace()

    with pytest.raises(ParsingError, match="Cannot find a KQL Database source with logical ID"):
        replace_cluster_uri(workspace, create_queryset_file(tmp_path, database_item_id="unknown-id"))

    workspace.endpoint.invoke.assert_not_called()


def test_replace_cluster_uri_raises_for_undeployed_database(tmp_path):
    """A repository database without a deployed GUID cannot supply a cluster URI."""
    workspace = create_workspace(database_guid="")

    with pytest.raises(ParsingError, match="as it is not yet deployed"):
        replace_cluster_uri(workspace, create_queryset_file(tmp_path))

    workspace.endpoint.invoke.assert_not_called()


def test_replace_cluster_uri_raises_when_query_service_uri_is_missing(tmp_path):
    """A deployed database without a query service URI produces a parsing error."""
    workspace = create_workspace(query_service_uri=None)

    with pytest.raises(ParsingError, match="Cannot find the cluster URI for KQL Database 'TestDatabase'"):
        replace_cluster_uri(workspace, create_queryset_file(tmp_path))


def test_replace_cluster_uri_preserves_existing_uri(tmp_path):
    """A populated cluster URI is returned unchanged without calling Fabric."""
    workspace = create_workspace()
    existing_uri = "https://existing.kusto.fabric.microsoft.com"

    result = json.loads(replace_cluster_uri(workspace, create_queryset_file(tmp_path, cluster_uri=existing_uri)))

    assert result["queryset"]["dataSources"][0]["clusterUri"] == existing_uri
    workspace.endpoint.invoke.assert_not_called()
