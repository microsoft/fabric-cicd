"""Functions to process and deploy Mirrored Azure Databricks Catalog items."""

from typing import TYPE_CHECKING

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

if TYPE_CHECKING:
    from fabric_cicd._common._item import Item


class MirroredAzureDatabricksCatalogPublisher(ItemPublisher):
    """Publisher for Mirrored Azure Databricks Catalog items."""

    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value

    def publish_one(self, item_name: str, _item: "Item") -> None:
        """Publish the catalog and wait for referenced asynchronous attributes."""
        from fabric_cicd._items._bulk_publish_utils import get_async_attributes_to_wait_for

        super().publish_one(item_name, _item)
        workspace = self.fabric_workspace_obj
        if _item.skip_publish or not workspace.contains_param_item_vars:
            return

        item_key = f"{self.item_type}.{item_name}"
        attributes = get_async_attributes_to_wait_for(workspace, {item_key})
        for attribute_name in sorted(attributes.get(item_key, ())):
            workspace._wait_for_item_attribute_provisioning(
                self.item_type, _item.guid, item_name, attribute_name
            )
