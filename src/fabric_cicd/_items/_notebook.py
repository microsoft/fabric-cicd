# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Notebook item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


class NotebookPublisher(ItemPublisher):
    """Publisher for Notebook items."""

    item_type = ItemType.NOTEBOOK.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Notebook item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def publish_all(self) -> None:
        """Publish all Notebook items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
