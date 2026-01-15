# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy User Data Function item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_userdatafunctions(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all user data function items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "UserDataFunction"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


class UserDataFunctionPublisher(ItemPublisher):
    """Publisher for User Data Function items."""

    def publish_all(self) -> None:
        """Publish all User Data Function items."""
        publish_userdatafunctions(self.fabric_workspace_obj)
