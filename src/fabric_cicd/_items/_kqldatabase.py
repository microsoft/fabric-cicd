# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy KQL Database item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class KQLDatabasePublisher(ItemPublisher):
    """Publisher for KQL Database items."""

    item_type = ItemType.KQL_DATABASE.value
