"""Functions to process and deploy Mirrored Azure Databricks Catalog items."""

import logging
from typing import TYPE_CHECKING

from fabric_cicd import constants
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

if TYPE_CHECKING:
    from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


class MirroredAzureDatabricksCatalogPublisher(ItemPublisher):
    """Publisher for Mirrored Azure Databricks Catalog items."""

    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value

    def publish_one(self, item_name: str, _item: "Item") -> None:
        """Publish the catalog and wait for its SQL endpoint to be provisioned."""
        workspace = self.fabric_workspace_obj
        workspace._publish_item(item_name=item_name, item_type=self.item_type, skip_publish_logging=True)
        if _item.skip_publish:
            return

        for attribute_name in ("sqlendpointid", "sqlendpoint"):
            workspace._wait_for_item_attribute_provisioning(self.item_type, _item.guid, item_name, attribute_name)

        logger.info(f"{constants.INDENT}Published {self.item_type} '{item_name}'")
