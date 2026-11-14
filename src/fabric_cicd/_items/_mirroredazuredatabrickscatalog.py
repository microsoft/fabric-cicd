"""Functions to process and deploy Mirrored Azure Databricks Catalog items."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class MirroredAzureDatabricksCatalogPublisher(ItemPublisher):
    """Publisher for Mirrored Azure Databricks Catalog items."""

    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value
