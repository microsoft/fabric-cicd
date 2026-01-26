# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventhouse item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import EXCLUDE_PATH_REGEX_MAPPING, ItemType

logger = logging.getLogger(__name__)


class EventhousePublisher(ItemPublisher):
    """Publisher for Eventhouse items."""

    item_type = ItemType.EVENTHOUSE.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Eventhouse item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=self.item_type, exclude_path=EXCLUDE_PATH_REGEX_MAPPING.get(self.item_type)
        )
