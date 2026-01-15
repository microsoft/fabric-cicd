# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Data Agent item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


class DataAgentPublisher(ItemPublisher):
    """Publisher for Data Agent items."""

    item_type = "DataAgent"

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Data Agent item."""
        exclude_path = r".*\.platform"
        self.fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=self.item_type, exclude_path=exclude_path
        )

    def publish_all(self) -> None:
        """Publish all Data Agent items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
