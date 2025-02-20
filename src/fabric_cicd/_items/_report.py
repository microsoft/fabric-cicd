# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging

from fabric_cicd._common._exceptions import ItemDependencyError

"""
Functions to process and deploy Report item.
"""

logger = logging.getLogger(__name__)


def publish_reports(fabric_workspace_obj):
    """Publishes all report items from the repository."""
    item_type = "Report"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        exclude_path = r".*\.pbi[/\\].*"
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            exclude_path=exclude_path,
            custom_file_processing=custom_file_processing,
        )


def custom_file_processing(workspace_obj, item_obj, file_obj):
    """Custom file processing for report items."""
    if file_obj.name == "definition.pbir":
        definition_body = json.loads(file_obj.contents)
        if "datasetReference" in definition_body and "byPath" in definition_body["datasetReference"]:
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
