# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Mirrored Database item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class MirroredDatabasePublisher(ItemPublisher):
    """Publisher for Mirrored Database items."""

    item_type = ItemType.MIRRORED_DATABASE.value
