# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy SQL Database item."""

import logging

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


class SQLDatabasePublisher(ItemPublisher):
    """Publisher for SQL Database items."""

    item_type = "SQLDatabase"

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single SQL Database item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            return

        logger.info(f"{constants.INDENT}Published")

    def publish_all(self) -> None:
        """Publish all SQL Database items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
