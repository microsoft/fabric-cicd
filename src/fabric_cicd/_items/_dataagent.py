# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Data Agent item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


class DataAgentPublisher(ItemPublisher):
    """Publisher for Data Agent items."""

    item_type = ItemType.DATA_AGENT.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Data Agent item."""
        exclude_path = r".*\.platform"
        self.fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=self.item_type, exclude_path=exclude_path
        )
