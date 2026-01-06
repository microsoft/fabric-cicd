# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Report item."""

import json
import logging
import re

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import ItemDependencyError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)

# GUID pattern for matching semantic model IDs in connection strings
GUID_PATTERN = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


def publish_reports(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all report items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Report"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        exclude_path = r".*\.pbi[/\\].*"
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            exclude_path=exclude_path,
            func_process_file=func_process_file,
        )


def func_process_file(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Custom file processing for report items.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    if file_obj.name == "definition.pbir":
        definition_body = json.loads(file_obj.contents)
        if (
            "datasetReference" in definition_body
            and "byPath" in definition_body["datasetReference"]
            and definition_body["datasetReference"]["byPath"] is not None
        ):
            model_rel_path = definition_body["datasetReference"]["byPath"]["path"]
            model_path = str((item_obj.path / model_rel_path).resolve())
            model_id = workspace_obj._convert_path_to_id("SemanticModel", model_path)

            if not model_id:
                msg = "Semantic model not found in the repository. Cannot deploy a report with a relative path without deploying the model."
                raise ItemDependencyError(msg, logger)

            definition_body["$schema"] = (
                "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/1.0.0/schema.json"
            )

            definition_body["datasetReference"] = {
                "byConnection": {
                    "connectionString": None,
                    "pbiServiceModelId": None,
                    "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                    "pbiModelDatabaseName": f"{model_id}",
                    "name": "EntityDataSource",
                    "connectionType": "pbiServiceXmlaStyleLive",
                }
            }

            return json.dumps(definition_body, indent=4)
    return file_obj.contents


def sync_report_dataset_reference(file_content: str) -> str:
    """
    Synchronizes the pbiModelDatabaseName field with the semanticmodelid from the connectionString.
    This handles cross-workspace report rebinding where the connectionString is parameterized.

    Args:
        file_content: The JSON content of the definition.pbir file after parameterization.

    Returns:
        Updated file content with synchronized dataset reference fields.
    """
    try:
        definition_body = json.loads(file_content)

        # Only process if this is a report with byConnection reference
        if "datasetReference" not in definition_body or "byConnection" not in definition_body["datasetReference"]:
            return file_content

        by_connection = definition_body["datasetReference"]["byConnection"]
        connection_string = by_connection.get("connectionString")

        # If connectionString exists and contains semanticmodelid, sync it with pbiModelDatabaseName
        if connection_string and isinstance(connection_string, str):
            # Extract semanticmodelid from connection string using regex
            # Connection string format: "...semanticmodelid=<guid>..."
            model_id_match = re.search(
                rf"semanticmodelid\s*=\s*({GUID_PATTERN})",
                connection_string,
                re.IGNORECASE,
            )

            if model_id_match:
                semantic_model_id = model_id_match.group(1)
                current_pbi_model_db_name = by_connection.get("pbiModelDatabaseName")

                # Only update if they differ
                if current_pbi_model_db_name != semantic_model_id:
                    logger.debug(
                        f"Syncing pbiModelDatabaseName from '{current_pbi_model_db_name}' to '{semantic_model_id}' "
                        f"to match semanticmodelid in connectionString"
                    )
                    by_connection["pbiModelDatabaseName"] = semantic_model_id
                    definition_body["datasetReference"]["byConnection"] = by_connection
                    return json.dumps(definition_body, indent=4)

        return file_content

    except (json.JSONDecodeError, KeyError, AttributeError) as e:
        logger.debug(f"Could not sync report dataset reference: {e}")
        return file_content
