# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Mounted Data Factory item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class MountedDataFactoryPublisher(ItemPublisher):
    """Publisher for Mounted Data Factory items."""

    item_type = ItemType.MOUNTED_DATA_FACTORY.value
