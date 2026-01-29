# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Report item."""

import json
import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._exceptions import ItemDependencyError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import EXCLUDE_PATH_REGEX_MAPPING, ItemType

logger = logging.getLogger(__name__)


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
            model_id = workspace_obj._convert_path_to_id(ItemType.SEMANTIC_MODEL.value, model_path)

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


class ReportPublisher(ItemPublisher):
    """Publisher for Report items."""

    item_type = ItemType.REPORT.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Report item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            exclude_path=EXCLUDE_PATH_REGEX_MAPPING.get(self.item_type),
            func_process_file=func_process_file,
        )
