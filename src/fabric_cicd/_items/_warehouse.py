# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Warehouse item."""

import json
import logging

from fabric_cicd import FabricWorkspace, constants

logger = logging.getLogger(__name__)


def publish_warehouses(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all warehouse items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
    """
    item_type = "Warehouse"

    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        creation_payload = next(
            (
                json.loads(file.contents)["metadata"]["creationPayload"]
                for file in item.item_files
                if file.name == ".platform" and "creationPayload" in file.contents
            ),
            None,
        )

        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            creation_payload=creation_payload,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            continue

        logger.info(f"{constants.INDENT}Published")
