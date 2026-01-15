# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventhouse item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_eventhouses(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all eventhouse items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
    """
    item_type = "Eventhouse"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        exclude_path = r".*\.children[/\\].*"
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, exclude_path=exclude_path)


class EventhousePublisher(ItemPublisher):
    """Publisher for Eventhouse items."""

    def publish(self) -> None:
        """Publish all Eventhouse items."""
        publish_eventhouses(self.fabric_workspace_obj)
