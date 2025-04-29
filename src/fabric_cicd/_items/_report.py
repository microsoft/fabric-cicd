# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Report item."""

import json
import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import ItemDependencyError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


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
