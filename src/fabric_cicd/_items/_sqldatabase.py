# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy SQL Database item."""

import logging

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_sqldatabases(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all SQL Database items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
    """
    item_type = "SQLDatabase"

    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            continue

        logger.info(f"{constants.INDENT}Published")


class SQLDatabasePublisher(ItemPublisher):
    """Publisher for SQL Database items."""

    def publish(self) -> None:
        """Publish all SQL Database items."""
        publish_sqldatabases(self.fabric_workspace_obj)
