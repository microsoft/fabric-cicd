# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Map item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class MapPublisher(ItemPublisher):
    """Publisher for Map items."""

    item_type = ItemType.MAP.value
