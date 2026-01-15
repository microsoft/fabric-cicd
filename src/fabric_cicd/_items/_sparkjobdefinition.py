# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Spark Job Definition item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)



class SparkJobDefinitionPublisher(ItemPublisher):
    """Publisher for Spark Job Definition items."""

    item_type = "SparkJobDefinition"

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Spark Job Definition item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type, api_format="SparkJobDefinitionV2")

    def publish_all(self) -> None:
        """Publish all Spark Job Definition items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
