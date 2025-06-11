# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Real-Time Dashboard item."""

import json
import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def publish_kqldashboard(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all Real-Time Dashboard items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "KQLDashboard"

    fabric_workspace_obj._refresh_deployed_items()

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=func_process_file
        )


def func_process_file(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Custom file processing for KQL Dashboard items.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    # For KQL Dashboard, we do not need to process the file content
    return replace_cluster_uri(workspace_obj, file_obj) if item_obj.type == "KQLDashboard" else file_obj.contents


def replace_cluster_uri(fabric_workspace_obj: FabricWorkspace, file_obj: File) -> str:
    """
    Replaces an empty cluster URI value in a Real-Time Dashboard item with the cluster URI associated
    with its KQL Database source in the raw file content.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_obj: The file object.
    """
    # Create a dictionary from the raw file
    json_content_dict = json.loads(file_obj.contents)

    data_sources = json_content_dict.get("dataSources")

    # Get the KQL Database items from the deployed items
    database_items = fabric_workspace_obj.deployed_items.get("KQLDatabase", {})

    for data_source in data_sources:
        if not data_source:
            msg = "No data sources found in the KQL Dashboard item."
            raise ParsingError(msg, logger)
        if data_source.get("clusterUri") == "":
            database_item_name = data_source.get("name")
            database_item = database_items.get(database_item_name)

            if not database_item:
                msg = f"Cannot find the KQL Database source with name '{database_item_name}' as it is not yet deployed."
                raise ParsingError(msg, logger)

            database_item_guid = database_item.guid
            # Get the cluster URI of the KQL database
            kqldatabase_data = fabric_workspace_obj.endpoint.invoke(
                method="GET",
                url=f"{fabric_workspace_obj.base_api_url}/kqlDatabases/{database_item_guid}",
            )
            kqldatabase_cluster_uri = kqldatabase_data.get("body", {}).get("properties", {}).get("queryServiceUri")
            # Replace the cluster URI value
            if not kqldatabase_cluster_uri:
                msg = f"Cluster URI for KQL Database '{database_item_name}' is not found."
                raise ParsingError(msg, logger)

            data_source["clusterUri"] = kqldatabase_cluster_uri

    return json.dumps(json_content_dict, indent=2)
