# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Mounted Data Factory item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)



class MountedDataFactoryPublisher(ItemPublisher):
    """Publisher for Mounted Data Factory items."""

    item_type = "MountedDataFactory"

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Mounted Data Factory item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def publish_all(self) -> None:
        """Publish all Mounted Data Factory items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
