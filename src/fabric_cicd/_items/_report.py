# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
from pathlib import Path

from fabric_cicd._common._exceptions import ItemDependencyError

"""
Functions to process and deploy Report item.
"""

logger = logging.getLogger(__name__)


def publish_reports(fabric_workspace_obj):
    """Publishes all report items from the repository."""
    item_type = "Report"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            excluded_directories={".pbi"},
            func_process_file=_process_report_file_contents,
        )


def _process_report_file_contents(fabric_workspace_obj, item_file_obj):
    if item_file_obj.name == "definition.pbir":
        definition_body = json.loads(item_file_obj.contents)

        if definition_body.get("datasetReference", {}).get("byPath", {}) != {}:
            model_rel_path = definition_body["datasetReference"]["byPath"]["path"]
            model_path = str((Path(item_file_obj.item_path) / model_rel_path).resolve())
            model_id = fabric_workspace_obj._convert_path_to_id("SemanticModel", model_path)

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

            item_file_obj.contents = json.dumps(definition_body, indent=4)

    return item_file_obj
