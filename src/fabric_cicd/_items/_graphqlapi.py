# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy API for GraphQL item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_graphqlapis(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all graphqlapi items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "GraphQLApi"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


class GraphQLApiPublisher(ItemPublisher):
    """Publisher for GraphQL API items."""

    def publish(self) -> None:
        """Publish all GraphQL API items."""
        publish_graphqlapis(self.fabric_workspace_obj)
