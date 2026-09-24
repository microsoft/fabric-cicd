# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Cosmos DB Database item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class CosmosDBDatabasePublisher(ItemPublisher):
    """Publisher for Cosmos DB Database items."""

    item_type = ItemType.COSMOS_DB_DATABASE.value
