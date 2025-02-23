# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Lakehouse item."""

import logging

from fabric_cicd import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_lakehouses(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all lakehouse items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
    """
    item_type = "Lakehouse"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        item_files = fabric_workspace_obj.repository_items[item_type][item_name].item_files

        for file in item_files:
            enable_schemas = file.name == "lakehouse-content.json" and "defaultSchema" in file.contents
            creation_payload = {"enableSchemas": enable_schemas}
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, creation_payload=creation_payload)
