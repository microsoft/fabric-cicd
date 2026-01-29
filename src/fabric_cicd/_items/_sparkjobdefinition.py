# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Spark Job Definition item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import API_FORMAT_MAPPING, ItemType

logger = logging.getLogger(__name__)


class SparkJobDefinitionPublisher(ItemPublisher):
    """Publisher for Spark Job Definition items."""

    item_type = ItemType.SPARK_JOB_DEFINITION.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Spark Job Definition item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=self.item_type, api_format=API_FORMAT_MAPPING.get(self.item_type)
        )
