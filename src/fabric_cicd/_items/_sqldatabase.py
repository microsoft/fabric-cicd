# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy SQL Database item."""

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import get_item_logger
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class SQLDatabasePublisher(ItemPublisher):
    """Publisher for SQL Database items."""

    item_type = ItemType.SQL_DATABASE.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single SQL Database item."""
        item_logger = get_item_logger(__name__, item_type=self.item_type, item_name=item_name)
        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            return

        item_logger.info(f"{constants.INDENT}Published")
