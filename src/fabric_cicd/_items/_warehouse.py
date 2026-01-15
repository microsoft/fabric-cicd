# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Warehouse item."""

import json
import logging

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


class WarehousePublisher(ItemPublisher):
    """Publisher for Warehouse items."""

    item_type = "Warehouse"

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Warehouse item."""
        creation_payload = next(
            (
                json.loads(file.contents)["metadata"]["creationPayload"]
                for file in item.item_files
                if file.name == ".platform" and "creationPayload" in file.contents
            ),
            None,
        )

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            creation_payload=creation_payload,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            return

        logger.info(f"{constants.INDENT}Published")

    def publish_all(self) -> None:
        """Publish all Warehouse items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
