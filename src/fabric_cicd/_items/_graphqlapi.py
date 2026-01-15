# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy GraphQL API item."""

import logging

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


class GraphQLApiPublisher(ItemPublisher):
    """Publisher for GraphQL API items."""

    item_type = ItemType.GRAPHQL_API.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single GraphQL API item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def publish_all(self) -> None:
        """Publish all GraphQL API items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
