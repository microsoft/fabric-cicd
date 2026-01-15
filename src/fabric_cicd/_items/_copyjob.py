# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Copy Job item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_copyjobs(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all copyjob items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "CopyJob"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


class CopyJobPublisher(ItemPublisher):
    """Publisher for Copy Job items."""

    def publish(self) -> None:
        """Publish all Copy Job items."""
        publish_copyjobs(self.fabric_workspace_obj)
