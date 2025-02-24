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

    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        creation_payload = None

        for file in item.item_files:
            if file.name == "lakehouse.metadata.json" and "defaultSchema" in file.contents:
                creation_payload = {"enableSchemas": True}
                break

        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, creation_payload=creation_payload)
